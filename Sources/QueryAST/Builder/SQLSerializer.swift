import DatabaseKit
import DatabaseTypes

/// Immutable serializer; all mutable text and traversal state belongs to one call.
public struct SQLSerializer: SQLSerializing {
    public let limits: QueryStructuralLimits

    public init(limits: QueryStructuralLimits = .default) { self.limits = limits }

    private func emit(_ body: (inout SQLTextWriter) throws(SQLSerializationError) -> Void) throws(SQLSerializationError) -> String {
        var writer = SQLTextWriter(limits: limits)
        try body(&writer)
        return writer.text
    }

    public func serialize(_ statement: QueryStatement) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.statement(statement) } }
    public func serialize(_ statements: [QueryStatement]) throws(SQLSerializationError) -> String {
        try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in
            try writer.collection(statements.count)
            guard !statements.isEmpty else { throw .invalidStructure("Empty SQL statement block") }
            for (index, statement) in statements.enumerated() {
                if index > 0 { try writer.append("; ") }
                try writer.statement(statement)
            }
        }
    }
    public func serialize(_ query: SelectQuery) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.select(query) } }
    public func serialize(_ expression: Expression) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.expression(expression) } }
    public func serialize(_ literal: Literal) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.literal(literal) } }
    public func serialize(_ type: DataType) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.type(type) } }
    public func serialize(_ source: DataSource) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.source(source) } }
    public func serialize(_ aggregate: AggregateFunction) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.aggregate(aggregate) } }
    public func serialize(_ pattern: MatchPattern) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.match(pattern) } }
    public func serialize(_ path: PathPattern) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.path(path) } }
    public func serialize(_ element: PathElement) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.pathElement(element) } }
    public func serialize(_ node: NodePattern) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.node(node) } }
    public func serialize(_ edge: EdgePattern) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.edge(edge) } }
    public func serialize(_ quantifier: PathQuantifier) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.quantifier(quantifier) } }
    public func serialize(_ mode: PathMode) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.mode(mode) } }
    public func serialize(_ source: GraphTableSource) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.graphTable(source) } }
    public func serialize(_ graph: CreateGraphStatement) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.createGraph(graph) } }
    public func serialize(_ vertex: VertexTableDefinition) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.vertex(vertex) } }
    public func serialize(_ edge: EdgeTableDefinition) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.edgeDefinition(edge) } }
    public func serialize(_ reference: VertexReference) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.reference(reference) } }
    public func serialize(_ label: LabelExpression) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.label(label) } }
    public func serialize(_ properties: PropertiesSpec) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.properties(properties) } }
    public func serialize(_ join: JoinType) throws(SQLSerializationError) -> String { try emit { (writer: inout SQLTextWriter) throws(SQLSerializationError) -> Void in try writer.joinType(join) } }
}

extension SQLTextWriter {
    mutating func list<T>(_ values: [T], separator: String = ", ",
                          _ write: (inout SQLTextWriter, T) throws(SQLSerializationError) -> Void) throws(SQLSerializationError) {
        try collection(values.count)
        for (index, value) in values.enumerated() {
            if index > 0 { try append(separator) }
            try write(&self, value)
        }
    }

    mutating func identifiers(_ values: [String], nonempty: Bool = true) throws(SQLSerializationError) {
        if nonempty, values.isEmpty { throw .invalidStructure("Empty identifier list") }
        try list(values) { (writer: inout SQLTextWriter, value: String) throws(SQLSerializationError) -> Void in try writer.identifier(value) }
    }

    mutating func type(_ value: DataType) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .boolean: try append("BOOLEAN")
        case .smallint: try append("SMALLINT")
        case .integer: try append("INTEGER")
        case .bigint: try append("BIGINT")
        case .real: try append("REAL")
        case .doublePrecision: try append("DOUBLE PRECISION")
        case .decimal(let precision, let scale):
            guard precision == nil || (1...38).contains(precision!),
                  scale == nil || precision != nil && (-1000...1000).contains(scale!) else {
                throw .unsupportedType("Invalid NUMERIC precision or scale")
            }
            try append("NUMERIC")
            if let precision {
                try append("(\(precision)")
                if let scale { try append(", \(scale)") }
                try append(")")
            }
        case .char(let length): try lengthType("CHAR", length)
        case .varchar(let length): try lengthType("VARCHAR", length)
        case .text: try append("TEXT")
        case .date: try append("DATE")
        case .time(let zone, let precision): try temporalType("TIME", zone, precision)
        case .timestamp(let zone, let precision): try temporalType("TIMESTAMP", zone, precision)
        case .interval: try append("INTERVAL")
        case .binary(let length): try lengthType("BINARY", length)
        case .varbinary(let length): try lengthType("VARBINARY", length)
        case .blob: try append("BYTEA")
        case .json: try append("JSON")
        case .jsonb: try append("JSONB")
        case .uuid: try append("UUID")
        case .array(let element):
            if case .array = element { throw .unsupportedType("Nested array DataType") }
            try type(element); try append("[]")
        case .custom(let name, let schema):
            if let schema { try identifier(schema); try append(".") }
            try identifier(name)
        }
    }

    private mutating func lengthType(_ name: String, _ length: Int?) throws(SQLSerializationError) {
        guard length == nil || length! > 0 else { throw .unsupportedType("Nonpositive type length") }
        try append(name)
        if let length { try append("(\(length))") }
    }

    private mutating func temporalType(_ name: String, _ zone: Bool, _ precision: Int?) throws(SQLSerializationError) {
        guard precision == nil || (0...6).contains(precision!) else { throw .unsupportedType("Temporal precision outside 0...6") }
        try append(name)
        if let precision { try append("(\(precision))") }
        if zone { try append(" WITH TIME ZONE") }
    }

    mutating func literal(_ value: Literal) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .null: try append("NULL")
        case .bool(let value): try append(value ? "TRUE" : "FALSE")
        case .int(let value): try primitive(.int64(value), declared: nil)
        case .uint: throw .unsupportedLiteral("Unsigned integer has no SQL width-preserving type")
        case .decimal(let value):
            do { try numeric(QueryNumericValue(value: value, displayScale: max(0, value.scale))) }
            catch let error as SQLSerializationError { throw error }
            catch { throw .unsupportedLiteral("Invalid exact decimal") }
        case .double(let value): try primitive(.float64(value), declared: nil)
        case .sqlUnknownString(let value): try string(value)
        case .string(let value): try primitive(.string(value), declared: nil)
        case .date(let value): try append("DATE "); try string(QueryLiteralEncoding.iso8601(value))
        case .timestamp(let value):
            guard value.nanoseconds.isMultiple(of: 1000) else { throw .unsupportedLiteral("SQL timestamps have microsecond resolution") }
            try append("TIMESTAMP WITH TIME ZONE "); try string(QueryLiteralEncoding.iso8601(value))
        case .uuid(let value): try append("UUID "); try string(value.description)
        case .binary(let value):
            let count = UInt64(value.count).multipliedReportingOverflow(by: 2)
            guard !count.overflow else { throw .outputLimit(actual: .max, maximum: limits.maximumRetainedBytes) }
            let total = count.partialValue.addingReportingOverflow(10)
            guard !total.overflow else { throw .outputLimit(actual: .max, maximum: limits.maximumRetainedBytes) }
            try admit(total.partialValue)
            try append("BYTEA '\\x")
            for byte in value {
                let hex = String(byte, radix: 16)
                if hex.count == 1 { try append("0") }
                try append(hex)
            }
            try append("'")
        case .array(let values):
            try append("ARRAY["); try list(values) { (writer: inout SQLTextWriter, value: Literal) throws(SQLSerializationError) -> Void in try writer.literal(value) }; try append("]")
        case .queryValue(let value, let declared):
            try admitQueryValue(value)
            if let declared, !QueryValueValidator.accepts(value, type: declared) {
                throw .unsupportedMetadata("Literal declaration is incompatible with its value")
            }
            let sqlType: DataType?
            switch declared {
            case .sql(let type): sqlType = type
            case .primitive: throw .unsupportedMetadata("Primitive type declaration has no SQL type identity")
            case nil: sqlType = nil
            }
            if preservesOutputReferenceCasts {
                switch (value, sqlType) {
                case (.primitive(.int32(let integer)), .integer):
                    try primitive(.int32(integer), declared: sqlType)
                    return
                case (.primitive(.int64(let integer)), .bigint) where Int32(exactly: integer) == nil:
                    try primitive(.int64(integer), declared: sqlType)
                    return
                default: break
                }
            }
            if sqlType != nil { try append("CAST(") }
            switch value {
            case .primitive(let value): try primitive(value, declared: sqlType)
            case .numeric(let number): try numeric(number)
            case .json(let value):
                try append("JSON ")
                // UTF-8 materializes only at the required SQL text ownership boundary.
                try admit(UInt64(value.bytes.count) + 2)
                let lexical = value.bytes.withUnsafeBytes { String(decoding: $0, as: UTF8.self) }
                try string(lexical)
            case .jsonb(let value):
                try append("JSONB ")
                do { try string(SQLConstantSyntax.jsonbText(value, limits: limits)) }
                catch let error as SQLSerializationError { throw error }
                catch let error as SQLConstantError { throw constantError(error) }
                catch { throw .constant("SQL constant conversion failed") }
            case .interval(let value): try append("INTERVAL "); try string(SQLConstantSyntax.intervalText(value))
            case .array(let value):
                try append("CAST(")
                do { try string(SQLConstantSyntax.arrayText(value, limits: limits)) }
                catch let error as SQLSerializationError { throw error }
                catch let error as SQLConstantError { throw constantError(error) }
                catch { throw .constant("SQL constant conversion failed") }
                try append(" AS "); try type(.array(value.elementType)); try append(")")
            }
            if let sqlType { try append(" AS "); try type(sqlType); try append(")") }
        case .iri, .blankNode, .typedLiteral, .langLiteral, .dirLangLiteral, .rdfTerm:
            throw .unsupportedLanguage("RDF literal")
        }
    }

    private mutating func admitQueryValue(_ value: QueryValue) throws(SQLSerializationError) {
        let footprint: QueryValueFootprint
        do {
            switch value {
            case .json(let json): footprint = try json.measure()
            case .jsonb(let json): footprint = try json.measure()
            case .array(let array):
                guard UInt64(array.elements.count) <= limits.maximumCollectionElements else { throw SQLSerializationError.structuralLimit("SQL array collection budget exceeded") }
                footprint = try QueryValueValidator.measure(value)
            case .numeric, .interval:
                footprint = QueryValueFootprint(nodeCount: 1, collectionElementCount: 0, retainedByteCount: 0, maximumNestingDepth: 1)
            case .primitive: return
            }
        } catch let error as SQLSerializationError { throw error }
          catch let error as QueryValueError { throw .constant(SQLConstantError.value(error).description) }
          catch { throw .constant("Query value footprint calculation failed") }
        let nextNodes = nodes.addingReportingOverflow(footprint.nodeCount)
        let nextCollections = collectionElements.addingReportingOverflow(footprint.collectionElementCount)
        let nextBytes = retainedValueBytes.addingReportingOverflow(footprint.retainedByteCount)
        let nextDepth = depth.addingReportingOverflow(footprint.maximumNestingDepth)
        guard !nextNodes.overflow, !nextCollections.overflow, !nextBytes.overflow, !nextDepth.overflow,
              nextNodes.partialValue <= limits.maximumTotalNodes,
              nextCollections.partialValue <= limits.maximumCollectionElements,
              nextBytes.partialValue <= limits.maximumRetainedBytes,
              nextDepth.partialValue <= limits.maximumNestingDepth else {
            throw .structuralLimit("SQL rich value budget exceeded")
        }
        nodes = nextNodes.partialValue
        collectionElements = nextCollections.partialValue
        retainedValueBytes = nextBytes.partialValue
    }

    mutating func numeric(_ value: QueryNumericValue) throws(SQLSerializationError) {
        do {
            let text = try SQLConstantSyntax.numericText(value, maximum: limits.maximumRetainedBytes - byteCount)
            if value.displayScale == 0 { try append("CAST("); try append(text); try append(" AS NUMERIC)") }
            else { try append(text) }
        } catch let error as SQLSerializationError { throw error }
          catch let error as SQLConstantError { throw constantError(error) }
          catch { throw .constant("SQL constant conversion failed") }
    }

    private func constantError(_ error: SQLConstantError) -> SQLSerializationError {
        switch error {
        case .resourceLimit:
            return .outputLimit(actual: limits.maximumRetainedBytes == .max ? .max : limits.maximumRetainedBytes + 1, maximum: limits.maximumRetainedBytes)
        case .unsupportedConstant: return .unsupportedLiteral(error.description)
        default: return .constant(error.description)
        }
    }

    mutating func primitive(_ value: FieldValue, declared: DataType?) throws(SQLSerializationError) {
        switch value {
        case .null: try append("NULL")
        case .bool(let value): try append(value ? "TRUE" : "FALSE")
        case .int16(let value):
            if declared == nil { try append("CAST(\(value) AS SMALLINT)") } else { try append(String(value)) }
        case .int32(let value): try append(String(value))
        case .int64(let value):
            if declared == nil && Int32(exactly: value) != nil { try append("CAST(\(value) AS BIGINT)") }
            else { try append(String(value)) }
        case .float32(let value):
            guard value.isFinite else { throw .unsupportedLiteral("Nonfinite REAL") }
            if declared == nil { try append("CAST("); try append(String(value)); try append(" AS REAL)") }
            else { try append(String(value)) }
        case .float64(let value):
            guard value.isFinite else { throw .unsupportedLiteral("Nonfinite DOUBLE PRECISION") }
            if declared == nil { try append("CAST("); try append(String(value)); try append(" AS DOUBLE PRECISION)") }
            else { try append(String(value)) }
        case .decimal(let value): try literal(.decimal(value))
        case .string(let value):
            switch declared {
            case .char(let length):
                guard value.unicodeScalars.count == (length ?? 1) else {
                    throw .unsupportedMetadata("CHAR declaration would pad or truncate the owned string")
                }
            case .varchar(let length):
                guard length == nil || value.unicodeScalars.count <= length! else {
                    throw .unsupportedMetadata("VARCHAR declaration would truncate the owned string")
                }
            default: break
            }
            if declared == nil { try append("CAST(") }
            try string(value)
            if declared == nil { try append(" AS TEXT)") }
        case .date(let value): try literal(.date(value))
        case .timestamp(let value): try literal(.timestamp(value))
        case .uuid(let value): try literal(.uuid(value))
        case .bytes(let value): try literal(.binary(value))
        default: throw .unsupportedLiteral("Primitive value has no SQL representation preserving its type")
        }
    }

    mutating func expression(_ value: Expression) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch value {
        case .literal(let value): try literal(value)
        case .array(let values): try append("ARRAY["); try list(values) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try append("]")
        case .row(let values): try append("ROW("); try list(values) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try append(")")
        case .column(let column):
            if let table = column.table { try identifier(table); try append(".") }
            try identifier(column.column)
        case .parameter(let parameter):
            switch parameter {
            case .position(let position):
                guard position > 0 else { throw .invalidStructure("Parameter positions are one-based") }
                try append("$\(position)")
            case .name(let name):
                guard !name.isEmpty, name.utf8.allSatisfy({ (65...90).contains($0) || (97...122).contains($0) || (48...57).contains($0) || $0 == 95 }),
                      name.utf8.first.map({ !(48...57).contains($0) }) == true else {
                    throw .unsupportedMetadata("Named parameter is not a SQL parameter identifier")
                }
                try append(":" + name)
            }
        case .add(let l, let r): try binary(l, "+", r)
        case .subtract(let l, let r): try binary(l, "-", r)
        case .multiply(let l, let r): try binary(l, "*", r)
        case .divide(let l, let r): try binary(l, "/", r)
        case .modulo(let l, let r): try binary(l, "%", r)
        case .equal(let l, let r): try binary(l, "=", r)
        case .notEqual(let l, let r): try binary(l, "<>", r)
        case .lessThan(let l, let r): try binary(l, "<", r)
        case .lessThanOrEqual(let l, let r): try binary(l, "<=", r)
        case .greaterThan(let l, let r): try binary(l, ">", r)
        case .greaterThanOrEqual(let l, let r): try binary(l, ">=", r)
        case .and(let l, let r): try binary(l, "AND", r)
        case .or(let l, let r): try binary(l, "OR", r)
        case .negate(let value): try append("-("); try expression(value); try append(")")
        case .not(let value): try append("NOT ("); try expression(value); try append(")")
        case .isNull(let value): try append("("); try expression(value); try append(" IS NULL)")
        case .isNotNull(let value): try append("("); try expression(value); try append(" IS NOT NULL)")
        case .like(let value, let pattern):
            try append("("); try expression(value); try append(" LIKE "); try string(pattern); try append(")")
        case .between(let value, let low, let high):
            try append("("); try expression(value); try append(" BETWEEN "); try expression(low); try append(" AND "); try expression(high); try append(")")
        case .inList(let value, let values): try inList(value, values, negated: false)
        case .notInList(let value, let values): try inList(value, values, negated: true)
        case .inSubquery(let value, let query):
            try append("("); try expression(value); try append(" IN ("); try select(query); try append("))")
        case .aggregate(let value): try aggregate(value)
        case .function(let call):
            guard !call.distinct || !call.arguments.isEmpty else { throw .invalidStructure("Generic DISTINCT call requires arguments") }
            if let schema = call.schema { try identifier(schema); try append(".") }
            try identifier(call.name)
            try append("(")
            if call.distinct { try append("DISTINCT ") }
            try list(call.arguments) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try append(")")
        case .caseWhen(let pairs, let otherwise):
            guard !pairs.isEmpty else { throw .invalidStructure("CASE requires a WHEN arm") }
            try collection(pairs.count); try append("CASE")
            for pair in pairs { try append(" WHEN "); try expression(pair.condition); try append(" THEN "); try expression(pair.result) }
            if let otherwise { try append(" ELSE "); try expression(otherwise) }
            try append(" END")
        case .coalesce(let values):
            guard !values.isEmpty else { throw .invalidStructure("COALESCE requires arguments") }
            try append("COALESCE("); try list(values) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try append(")")
        case .nullIf(let left, let right):
            try append("NULLIF("); try expression(left); try append(", "); try expression(right); try append(")")
        case .cast(let value, let target):
            try append("CAST("); try expression(value); try append(" AS "); try type(target); try append(")")
        case .subquery(let query): try append("("); try select(query); try append(")")
        case .exists(let query): try append("EXISTS ("); try select(query); try append(")")
        case .variable, .bound, .regex, .triple, .isTriple, .subject, .predicate, .object:
            throw .unsupportedLanguage("SPARQL expression")
        }
    }

    private mutating func binary(_ left: Expression, _ op: String, _ right: Expression) throws(SQLSerializationError) {
        try append("("); try expression(left); try append(" " + op + " "); try expression(right); try append(")")
    }

    private mutating func inList(_ value: Expression, _ values: [Expression], negated: Bool) throws(SQLSerializationError) {
        guard !values.isEmpty else { throw .invalidStructure("Empty SQL IN list") }
        try append("("); try expression(value); try append(negated ? " NOT IN (" : " IN (")
        try list(values) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try append("))")
    }

    mutating func aggregate(_ aggregate: AggregateFunction) throws(SQLSerializationError) {
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        try enter(); defer { depth -= 1 }
        let name: String
        let value: Expression?
        let distinct: Bool
        let order: [SortKey]?
        switch aggregate {
        case .count(let expression, let flag): name = "COUNT"; value = expression; distinct = flag; order = nil
        case .sum(let expression, let flag): name = "SUM"; value = expression; distinct = flag; order = nil
        case .avg(let expression, let flag): name = "AVG"; value = expression; distinct = flag; order = nil
        case .min(let expression): name = "MIN"; value = expression; distinct = false; order = nil
        case .max(let expression): name = "MAX"; value = expression; distinct = false; order = nil
        case .arrayAgg(let expression, let ordering, let flag): name = "ARRAY_AGG"; value = expression; distinct = flag; order = ordering
        case .groupConcat(let expression, let separator, let flag):
            try append("GROUP_CONCAT(")
            if flag { try append("DISTINCT ") }
            try self.expression(expression)
            if let separator { try append(", "); try string(separator) }
            try append(")")
            return
        case .sample: throw .unsupportedLanguage("SPARQL aggregate")
        }
        guard value != nil || !distinct else { throw .invalidStructure("COUNT(DISTINCT *) is invalid SQL") }
        try append(name + "(")
        if distinct { try append("DISTINCT ") }
        if let value { try expression(value) } else { try append("*") }
        if let order, !order.isEmpty { try append(" ORDER BY "); try sorts(order) }
        try append(")")
    }

    mutating func outputReference(_ value: Expression) throws(SQLSerializationError) {
        if case .literal(.int(let position)) = value {
            try enter(); defer { depth -= 1 }
            try enter(); defer { depth -= 1 }
            try append(String(position))
            return
        }
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        if case .literal(let literal) = value {
            preservesOutputReferenceCasts = false
            if case .queryValue(.primitive(.int32), nil) = literal {
                try append("CAST("); try expression(value); try append(" AS INTEGER)")
            } else if case .queryValue(.primitive(.int64), nil) = literal {
                try append("CAST("); try expression(value); try append(" AS BIGINT)")
            } else {
                try expression(value)
            }
        } else {
            preservesOutputReferenceCasts = true
            try expression(value)
        }
    }

    mutating func sorts(_ keys: [SortKey], outputReferences: Bool = false) throws(SQLSerializationError) {
        try collection(keys.count)
        for (index, key) in keys.enumerated() {
            if index > 0 { try append(", ") }
            if outputReferences { try outputReference(key.expression) }
            else { try expression(key.expression) }
            try append(key.direction == .ascending ? " ASC" : " DESC")
            if let nulls = key.nulls { try append(nulls == .first ? " NULLS FIRST" : " NULLS LAST") }
        }
    }

    mutating func projectionItems(_ values: [ProjectionItem]) throws(SQLSerializationError) {
        guard !values.isEmpty else { throw .invalidStructure("Empty projection") }
        try list(values) { (writer: inout SQLTextWriter, value: ProjectionItem) throws(SQLSerializationError) -> Void in
            try writer.expression(value.expression)
            if let alias = value.alias { try writer.append(" AS "); try writer.identifier(alias) }
        }
    }

    mutating func select(_ query: SelectQuery) throws(SQLSerializationError) {
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        try enter(); defer { depth -= 1 }
        guard !query.reduced, query.dataset == .implicit else { throw .unsupportedLanguage("SPARQL SELECT metadata") }
        guard query.accessPath == nil else { throw .unsupportedMetadata("Physical access path") }
        if let ctes = query.subqueries, !ctes.isEmpty {
            try append("WITH ")
            try list(ctes) { (writer: inout SQLTextWriter, cte: NamedSubquery) throws(SQLSerializationError) -> Void in
                try writer.identifier(cte.name)
                if let columns = cte.columns { try writer.append(" ("); try writer.identifiers(columns); try writer.append(")") }
                try writer.append(" AS ")
                if let hint = cte.materialized { try writer.append(hint == .materialized ? "MATERIALIZED " : "NOT MATERIALIZED ") }
                try writer.append("("); try writer.select(cte.query); try writer.append(")")
            }
            try append(" ")
        }
        try append("SELECT ")
        var distinctProjection = false
        if case .distinctItems = query.projection { distinctProjection = true }
        if query.distinct || distinctProjection { try append("DISTINCT ") }
        switch query.projection {
        case .all: try append("*")
        case .allFrom(let name): try identifier(name); try append(".*")
        case .items(let items), .distinctItems(let items): try projectionItems(items)
        }
        // The parser's no-FROM representation is one zero-column VALUES row.
        if case .values(let rows, let columns) = query.source, rows == [[]], columns == nil || columns == [] {
        } else { try append(" FROM "); try source(query.source) }
        if let filter = query.filter { try append(" WHERE "); try expression(filter) }
        if let group = query.groupBy, !group.isEmpty { try append(" GROUP BY "); try list(group) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.outputReference(value) } }
        if let having = query.having { try append(" HAVING "); try expression(having) }
        if let order = query.orderBy, !order.isEmpty { try append(" ORDER BY "); try sorts(order, outputReferences: true) }
        if let limit = query.limit { try append(" LIMIT \(limit)") }
        if let offset = query.offset { try append(" OFFSET \(offset)") }
    }

    mutating func table(_ table: TableRef) throws(SQLSerializationError) {
        guard table.partitions.isEmpty else { throw .unsupportedMetadata("Table partition bindings") }
        if let schema = table.schema { try identifier(schema); try append(".") }
        try identifier(table.table)
        if let alias = table.alias { try append(" AS "); try identifier(alias) }
    }

    mutating func source(_ source: DataSource) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch source {
        case .table(let value): try table(value)
        case .subquery(let query, let alias): try append("("); try select(query); try append(") AS "); try identifier(alias)
        case .join(let join):
            switch join.type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull, .cross:
                guard join.condition == nil else { throw .invalidStructure("Natural and cross joins cannot have ON or USING") }
            case .inner, .left, .right, .full, .leftLateral:
                guard join.condition != nil else { throw .invalidStructure("Qualified join requires ON or USING") }
            case .lateral: break
            }
            try append("("); try self.source(join.left); try append(" ")
            if join.type == .lateral, join.condition == nil { try append("CROSS JOIN LATERAL") } else { try joinType(join.type) }
            try append(" "); try self.source(join.right)
            if let condition = join.condition {
                switch condition {
                case .on(let value): try append(" ON "); try expression(value)
                case .using(let columns): try append(" USING ("); try identifiers(columns); try append(")")
                }
            }
            try append(")")
        case .graphTable(let value): try graphTable(value)
        // FIXME(INCOMPLETE_IMPLEMENTATION): SQLSerializer.serialize reaches non-singleton VALUES sources here.
        // S08 must provide alias/scope-preserving VALUES grammar and semantic round-trip tests
        // before this source branch may report SQL generation success. SELECT's no-FROM singleton
        // is handled by select(_:); INSERT VALUES retains its separate statement writer.
        case .values:
            throw .unsupportedSource("VALUES source requires the S08 alias and scope grammar")
        case .union(let sources): try setSource(sources, op: "UNION")
        case .unionAll(let sources): try setSource(sources, op: "UNION ALL")
        case .intersect(let sources): try setSource(sources, op: "INTERSECT")
        case .except(let left, let right): try setSource([left, right], op: "EXCEPT")
        case .graphPattern, .namedGraph, .service: throw .unsupportedLanguage("SPARQL source")
        case .logical: throw .unsupportedSource("Logical source has no SQL syntax")
        #if MultiBase
        case .base: throw .unsupportedMetadata("MultiBase source target")
        #endif
        }
    }

    // FIXME(INCOMPLETE_IMPLEMENTATION): SQLSerializer.serialize reaches set-valued DataSource here.
    // S08 must supply an alias/scope-preserving query representation and semantic round-trip tests
    // before set-source SQL generation may report success.
    private mutating func setSource(_ sources: [DataSource], op: String) throws(SQLSerializationError) {
        guard sources.count >= 2 else { throw .invalidStructure("Set source requires two operands") }
        // Current semantics model sets as sources, without an alias-bearing derived-table contract.
        // Emitting a derived table would invent a scope; fail until that contract exists.
        throw .unsupportedSource("Set source requires an explicit alias-bearing query representation: " + op)
    }

    mutating func joinType(_ type: JoinType) throws(SQLSerializationError) {
        switch type {
        case .inner: try append("INNER JOIN")
        case .left: try append("LEFT JOIN")
        case .right: try append("RIGHT JOIN")
        case .full: try append("FULL JOIN")
        case .cross: try append("CROSS JOIN")
        case .natural: try append("NATURAL JOIN")
        case .naturalLeft: try append("NATURAL LEFT JOIN")
        case .naturalRight: try append("NATURAL RIGHT JOIN")
        case .naturalFull: try append("NATURAL FULL JOIN")
        case .lateral: try append("JOIN LATERAL")
        case .leftLateral: try append("LEFT JOIN LATERAL")
        }
    }

    mutating func statement(_ statement: QueryStatement) throws(SQLSerializationError) {
        try enter(); defer { depth -= 1 }
        switch statement {
        case .select(let query): try select(query)
        case .insert(let query):
            try append("INSERT INTO "); try table(query.target)
            if let columns = query.columns { try append(" ("); try identifiers(columns); try append(")") }
            switch query.source {
            case .defaultValues: try append(" DEFAULT VALUES")
            case .select(let query): try append(" "); try select(query)
            case .values(let rows):
                guard !rows.isEmpty, let width = rows.first?.count, width > 0, rows.allSatisfy({ $0.count == width }),
                      query.columns == nil || query.columns!.count == width else { throw .invalidStructure("Invalid INSERT VALUES shape") }
                try append(" VALUES ")
                try list(rows) { (writer: inout SQLTextWriter, row: [Expression]) throws(SQLSerializationError) -> Void in try writer.append("("); try writer.list(row) { (writer: inout SQLTextWriter, value: Expression) throws(SQLSerializationError) -> Void in try writer.expression(value) }; try writer.append(")") }
            }
            if let conflict = query.onConflict {
                try append(" ON CONFLICT ")
                switch conflict {
                case .doNothing: try append("DO NOTHING")
                case .doUpdate(let values, let condition):
                    try append("DO UPDATE SET "); try assignments(values)
                    if let condition { try append(" WHERE "); try expression(condition) }
                }
            }
            try returning(query.returning)
        case .update(let query):
            try append("UPDATE "); try table(query.target); try append(" SET "); try assignments(query.assignments)
            if let source = query.from { try append(" FROM "); try self.source(source) }
            if let filter = query.filter { try append(" WHERE "); try expression(filter) }
            try returning(query.returning)
        case .delete(let query):
            try append("DELETE FROM "); try table(query.target)
            if let source = query.using { try append(" USING "); try self.source(source) }
            if let filter = query.filter { try append(" WHERE "); try expression(filter) }
            try returning(query.returning)
        case .createGraph(let value): try createGraph(value)
        case .dropGraph(let name): try append("DROP PROPERTY GRAPH "); try identifier(name)
        case .sparqlUpdate, .construct, .ask, .describe: throw .unsupportedLanguage("SPARQL statement")
        }
    }

    private mutating func assignments(_ assignments: [Assignment]) throws(SQLSerializationError) {
        guard !assignments.isEmpty else { throw .invalidStructure("Empty assignment list") }
        try list(assignments) { (writer: inout SQLTextWriter, assignment: Assignment) throws(SQLSerializationError) -> Void in try writer.identifier(assignment.column); try writer.append(" = "); try writer.expression(assignment.value) }
    }

    private mutating func returning(_ items: [ProjectionItem]?) throws(SQLSerializationError) {
        if let items { try append(" RETURNING "); try projectionItems(items) }
    }
}
