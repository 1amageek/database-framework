import DatabaseTypes
import DatabaseKit

/// SQL-only typing for the currently executable canonical expression forms.
/// The relation owner supplies name lookup and subquery binding before reads.
struct CanonicalQueryTypeBinder {
    let columns: [QueryColumn]
    private let parameters: [QueryParameter]
    private let resolveColumn: (ColumnRef) throws -> Int
    private let wildcardSlots: (String?) throws -> [Int]
    private let bindSubquery: ((SelectQuery) throws -> [QueryColumn])?

    init(
        columns: [QueryColumn],
        parameters: [QueryParameter] = [],
        resolveColumn: @escaping (ColumnRef) throws -> Int,
        wildcardSlots: @escaping (String?) throws -> [Int],
        bindSubquery: ((SelectQuery) throws -> [QueryColumn])? = nil
    ) {
        self.columns = columns
        self.parameters = parameters
        self.resolveColumn = resolveColumn
        self.wildcardSlots = wildcardSlots
        self.bindSubquery = bindSubquery
    }

    /// The supplied schema is the already-bound FROM scope. Source recursion,
    /// including ON clauses, remains owned by the relation binder.
    func bind(_ query: SelectQuery) throws -> [QueryColumn] {
        if let filter = query.filter { _ = try boolean(filter) }
        if let having = query.having { _ = try boolean(having) }
        for expression in query.groupBy ?? [] { _ = try bind(expression) }
        for key in query.orderBy ?? [] { _ = try bind(key.expression) }
        switch query.projection {
        case .all:
            return try outputColumns(slots: wildcardSlots(nil))
        case .allFrom(let scope):
            return try outputColumns(slots: wildcardSlots(scope))
        case .items(let items), .distinctItems(let items):
            return try items.enumerated().map { index, item in
                let bound = try bind(item.expression)
                return try QueryColumn(
                    number: ordinal(index),
                    name: item.alias ?? Self.outputName(item.expression, index: index),
                    type: bound.type,
                    isNullable: bound.isNullable
                )
            }
        }
    }

    func bind(
        _ expression: DatabaseKit.Expression,
        contextualType: QueryColumnType? = nil
    ) throws -> CanonicalBoundExpression {
        switch expression {
        // FIXME(INCOMPLETE_IMPLEMENTATION): Canonical SQL binding reaches constructor expressions here.
        // S07 ARRAY typing and S08 ROW typing must be implemented before execution can succeed.
        case .array, .row:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL ARRAY/ROW constructor")
        case .literal(let literal):
            let type = try literalType(literal, contextualType: contextualType)
            return node(expression, type, literal.isNull)
        case .column(let reference):
            let slot = try resolveColumn(reference)
            let column = try column(at: slot)
            return CanonicalBoundExpression(
                expression: expression, type: column.type,
                isNullable: column.isNullable, columnSlot: slot
            )
        case .parameter(let reference):
            let matches = parameters.filter { parameter in
                switch reference {
                case .position(let position): return parameter.position == position
                case .name(let name): return parameter.name == name
                }
            }
            guard matches.count == 1, let parameter = matches.first else {
                throw DatabaseExpressionEvaluationError.unboundParameter
            }
            guard !parameter.value.isNull || parameter.type != nil || contextualType != nil else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped NULL parameter")
            }
            let type = try literalType(
                .queryValue(parameter.value, type: parameter.type),
                contextualType: contextualType
            )
            return node(expression, type, parameter.value.isNull)
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs):
            let children = try bindCommon([lhs, rhs], defaultType: .sql(.bigint))
            let type = try arithmeticType(children.map(\.type))
            return node(expression, type, children.contains { $0.isNullable }, children)
        case .negate(let operand):
            let child = try bind(operand, contextualType: .sql(.bigint))
            var type = try arithmeticType([child.type])
            if Self.numericKind(type) == .unsigned { type = .sql(.decimal(precision: nil, scale: nil)) }
            return node(expression, type, child.isNullable, [child])
        case .equal(let lhs, let rhs), .notEqual(let lhs, let rhs),
             .lessThan(let lhs, let rhs), .lessThanOrEqual(let lhs, let rhs),
             .greaterThan(let lhs, let rhs), .greaterThanOrEqual(let lhs, let rhs):
            let children = try bindCommon([lhs, rhs])
            try requirePrimitiveOperation(children, operation: "comparison")
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .and(let lhs, let rhs), .or(let lhs, let rhs):
            let children = try [boolean(lhs), boolean(rhs)]
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .not(let operand):
            let child = try boolean(operand)
            return node(expression, .sql(.boolean), child.isNullable, [child])
        case .isNull(let operand), .isNotNull(let operand):
            return node(expression, .sql(.boolean), false, [try bind(operand)])
        case .like(let operand, _):
            let child = try bind(operand, contextualType: .sql(.text))
            guard Self.isText(child.type) else { throw mismatch("LIKE") }
            return node(expression, .sql(.boolean), child.isNullable, [child])
        case .between(let operand, let low, let high):
            let children = try bindCommon([operand, low, high])
            try requirePrimitiveOperation(children, operation: "BETWEEN")
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .inList(let operand, let values), .notInList(let operand, let values):
            let children = try bindCommon([operand] + values)
            try requirePrimitiveOperation(children, operation: "IN")
            return node(expression, .sql(.boolean), !values.isEmpty && children.contains { $0.isNullable }, children)
        case .caseWhen(let pairs, let fallback):
            let conditions = try pairs.map { try boolean($0.condition) }
            var results = pairs.map(\.result)
            if let fallback { results.append(fallback) }
            else { results.append(.literal(.null)) }
            let branches = try bindCommon(results, defaultType: contextualType ?? .sql(.text))
            let type = try commonType(branches.map(\.type))
            return node(expression, type, branches.contains { $0.isNullable }, conditions + branches)
        case .coalesce(let expressions):
            guard !expressions.isEmpty else { throw mismatch("COALESCE") }
            let children = try bindCommon(expressions, defaultType: contextualType ?? .sql(.text))
            return node(expression, try commonType(children.map(\.type)), children.allSatisfy(\.isNullable), children)
        case .nullIf(let lhs, let rhs):
            let children = try bindCommon([lhs, rhs], defaultType: contextualType ?? .sql(.text))
            try requirePrimitiveOperation(children, operation: "NULLIF")
            return node(expression, try commonType(children.map(\.type)), true, children)
        case .cast(let operand, let target):
            let type = QueryColumnType.sql(target)
            try validate(type)
            if case .literal(let literal) = operand, literal.isNull {
                return node(expression, type, true, [try bind(operand, contextualType: type)])
            }
            var scalarType = target
            while case .array(let element) = scalarType { scalarType = element }
            // FIXME(INCOMPLETE_IMPLEMENTATION): SQL CAST binding reaches nonliteral/non-NULL temporal typmods here.
            // S06 must implement precision conversion and rounding before these casts can succeed.
            switch scalarType {
            case .time(_, .some), .timestamp(_, .some):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("temporal precision conversion")
            default: break
            }
            let child = try bind(operand, contextualType: type)
            try validateCast(from: child.type, to: type)
            return node(expression, type, child.isNullable, [child])
        case .function(let function):
            return try bindFunction(function, expression: expression)
        case .aggregate(let aggregate):
            return try bindAggregate(aggregate, expression: expression)
        case .subquery(let query):
            let column = try scalarColumn(query)
            return node(expression, column.type, true)
        case .exists(let query):
            _ = try subqueryColumns(query)
            return node(expression, .sql(.boolean), false)
        case .inSubquery(let operand, let query):
            let column = try scalarColumn(query)
            let child = try bind(operand, contextualType: column.type)
            _ = try commonType([child.type, column.type])
            try requirePrimitiveOperation([child], operation: "IN subquery")
            return node(expression, .sql(.boolean), true, [child])
        case .variable, .bound, .regex, .triple, .isTriple, .subject, .predicate, .object:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL binding does not admit this expression")
        }
    }

    private func bindFunction(
        _ function: FunctionCall, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        let name = function.name.uppercased()
        guard ["LOWER", "UPPER", "LENGTH", "ABS"].contains(name) else {
            throw DatabaseExpressionEvaluationError.unsupportedFunction(function.name)
        }
        guard function.arguments.count == 1, !function.distinct else { throw mismatch(name) }
        let child = try bind(function.arguments[0], contextualType: name == "ABS" ? .sql(.bigint) : .sql(.text))
        let type: QueryColumnType
        switch name {
        case "LOWER", "UPPER":
            guard Self.isText(child.type) else { throw mismatch(name) }
            type = .sql(.text)
        case "LENGTH":
            guard Self.isText(child.type) || Self.isBytes(child.type) || Self.isPrimitiveArray(child.type) else {
                throw mismatch(name)
            }
            type = .sql(.bigint)
        default:
            type = try arithmeticType([child.type])
        }
        return node(expression, type, child.isNullable, [child])
    }

    private func bindAggregate(
        _ aggregate: AggregateFunction, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        switch aggregate {
        case .count(let operand, _):
            let children = try operand.map { [try bind($0)] } ?? []
            return node(expression, .sql(.bigint), false, children)
        case .sum(let operand, _), .avg(let operand, _):
            let child = try bind(operand, contextualType: .sql(.bigint))
            let numericType = try arithmeticType([child.type])
            let type: QueryColumnType
            if Self.numericKind(numericType) == .floating {
                type = .sql(.doublePrecision)
            } else if case .avg = aggregate, Self.numericKind(numericType) != .decimal {
                // The current integer reducer returns double for fractional AVG.
                type = .sql(.doublePrecision)
            } else {
                type = .sql(.decimal(precision: nil, scale: nil))
            }
            return node(expression, type, true, [child])
        case .min(let operand), .max(let operand), .sample(let operand):
            let child = try bind(operand)
            try requirePrimitiveOperation([child], operation: "aggregate")
            return node(expression, child.type, true, [child])
        case .groupConcat(let operand, _, _):
            let child = try bind(operand, contextualType: .sql(.text))
            guard Self.isText(child.type) else { throw mismatch("GROUP_CONCAT") }
            return node(expression, .sql(.text), true, [child])
        case .arrayAgg(let operand, let ordering, _):
            let child = try bind(operand)
            guard let kind = Self.primitiveKind(child.type), !Self.isPrimitiveArray(child.type) else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("ARRAY_AGG of rich or nested values")
            }
            let keys = try (ordering ?? []).map { try bind($0.expression) }
            return node(expression, .primitive(kind, isArray: true), true, [child] + keys)
        }
    }

    private func boolean(_ expression: DatabaseKit.Expression) throws -> CanonicalBoundExpression {
        let bound = try bind(expression, contextualType: .sql(.boolean))
        guard bound.type == .sql(.boolean) || bound.type == .primitive(.bool) else { throw mismatch("boolean expression") }
        return bound
    }

    private func bindCommon(
        _ expressions: [DatabaseKit.Expression], defaultType: QueryColumnType = .sql(.text)
    ) throws -> [CanonicalBoundExpression] {
        var strongTypes: [QueryColumnType] = []
        for expression in expressions where !isContextualNull(expression) {
            strongTypes.append(try bind(expression).type)
        }
        let type = strongTypes.isEmpty ? defaultType : try commonType(strongTypes)
        return try expressions.map { try bind($0, contextualType: type) }
    }

    private func isContextualNull(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(.null), .literal(.queryValue(.primitive(.null), type: nil)): return true
        case .parameter(let reference):
            let matches = parameters.filter { parameter in
                switch reference {
                case .position(let position): return parameter.position == position
                case .name(let name): return parameter.name == name
                }
            }
            return matches.count == 1 && matches[0].type == nil && matches[0].value.isNull
        case .coalesce(let children): return children.allSatisfy(isContextualNull)
        case .caseWhen(let pairs, let fallback):
            return pairs.allSatisfy { isContextualNull($0.result) } && (fallback.map(isContextualNull) ?? true)
        default: return false
        }
    }

    func commonType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        guard let first = types.first else { return .sql(.text) }
        if types.allSatisfy({ $0 == first }) { return first }
        if types.allSatisfy(Self.isText) { return .sql(.text) }
        if types.allSatisfy(Self.isBytes) { return .sql(.blob) }
        if types.allSatisfy({ Self.numericKind($0) != nil }) {
            let kinds = types.compactMap(Self.numericKind)
            if kinds.contains(.decimal) && kinds.contains(.floating) {
                return .sql(.decimal(precision: nil, scale: nil))
            }
            return try arithmeticType(types)
        }
        if !types.contains(where: Self.isPrimitiveArray),
           let kind = Self.primitiveKind(first), types.allSatisfy({ Self.primitiveKind($0) == kind }) {
            return first
        }
        throw mismatch("common result type")
    }

    private func arithmeticType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        let kinds = try types.map { type in
            guard let kind = Self.numericKind(type) else { throw mismatch("numeric expression") }
            return kind
        }
        if kinds.contains(.decimal) {
            guard !kinds.contains(.floating) else { throw mismatch("exact and floating arithmetic") }
            return .sql(.decimal(precision: nil, scale: nil))
        }
        if kinds.contains(.floating) { return .sql(.doublePrecision) }
        if kinds.contains(.signed) && kinds.contains(.unsigned) { return .sql(.decimal(precision: nil, scale: nil)) }
        return kinds.contains(.unsigned) ? .primitive(.uint64) : .sql(.bigint)
    }

    private func literalType(_ literal: Literal, contextualType: QueryColumnType?) throws -> QueryColumnType {
        switch literal {
        case .null: return contextualType ?? .sql(.text)
        case .bool: return .sql(.boolean)
        case .int: return .sql(.bigint)
        case .uint: return .primitive(.uint64)
        case .decimal(let value):
            _ = try QueryNumericValue(value: value, displayScale: max(0, value.scale))
            return .sql(.decimal(precision: nil, scale: nil))
        case .double(let value):
            guard value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .sql(.doublePrecision)
        case .string: return .sql(.text)
        case .date: return .sql(.date)
        case .timestamp: return .sql(.timestamp(withTimeZone: true))
        case .binary: return .sql(.blob)
        case .uuid: return .sql(.uuid)
        case .array(let elements):
            let children = try elements.filter { !$0.isNull }.map { try literalType($0, contextualType: nil) }
            if children.isEmpty, let contextualType, Self.isPrimitiveArray(contextualType) { return contextualType }
            guard let first = children.first, let kind = Self.primitiveKind(first),
                  children.allSatisfy({ Self.primitiveKind($0) == kind && !Self.isPrimitiveArray($0) }) else {
                throw mismatch("primitive literal array requires one declared element type")
            }
            return .primitive(kind, isArray: true)
        case .queryValue(let value, let explicitType):
            if let explicitType {
                try validate(explicitType)
                guard QueryValueValidator.accepts(value, type: explicitType) else { throw mismatch("typed literal") }
                return explicitType
            }
            if value.isNull { return contextualType ?? .sql(.text) }
            let type = try Self.type(of: value)
            guard QueryValueValidator.accepts(value, type: type) else { throw mismatch("literal value") }
            return type
        case .iri, .blankNode, .typedLiteral, .langLiteral, .dirLangLiteral, .rdfTerm:
            return .primitive(.rdfTerm)
        }
    }

    static func type(of value: QueryValue) throws -> QueryColumnType {
        switch value {
        case .numeric: return .sql(.decimal(precision: nil, scale: nil))
        case .json: return .sql(.json)
        case .jsonb: return .sql(.jsonb)
        case .interval: return .sql(.interval)
        case .array(let value): return .sql(.array(value.elementType))
        case .primitive(let value):
            switch value {
            case .null: return .sql(.text)
            case .bool: return .sql(.boolean)
            case .int8: return .primitive(.int8)
            case .int16: return .sql(.smallint)
            case .int32: return .sql(.integer)
            case .int64: return .sql(.bigint)
            case .uint8: return .primitive(.uint8)
            case .uint16: return .primitive(.uint16)
            case .uint32: return .primitive(.uint32)
            case .uint64: return .primitive(.uint64)
            case .float32(let value):
                guard value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                return .sql(.real)
            case .float64(let value):
                guard value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                return .sql(.doublePrecision)
            case .decimal: return .sql(.decimal(precision: nil, scale: nil))
            case .string: return .sql(.text)
            case .bytes: return .sql(.blob)
            case .date: return .sql(.date)
            case .time: return .sql(.time(withTimeZone: false))
            case .dateTime: return .sql(.timestamp(withTimeZone: false))
            case .timestamp: return .sql(.timestamp(withTimeZone: true))
            case .timeSpan: return .primitive(.timeSpan)
            case .calendarPeriod: return .primitive(.calendarPeriod)
            case .geographicPoint: return .primitive(.geographicPoint)
            case .geographicPosition: return .primitive(.geographicPosition)
            case .vector: return .primitive(.vector)
            case .uuid: return .sql(.uuid)
            case .object: return .primitive(.object)
            case .reference: return .primitive(.reference)
            case .rdfTerm: return .primitive(.rdfTerm)
            case .array(let values):
                let types = try values.filter { !$0.isNull }.map { try type(of: .primitive($0)) }
                guard let first = types.first, let kind = primitiveKind(first),
                      types.allSatisfy({ primitiveKind($0) == kind && !isPrimitiveArray($0) }) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                }
                return .primitive(kind, isArray: true)
            }
        }
    }

    private func validateCast(from source: QueryColumnType, to target: QueryColumnType) throws {
        if source == target { return }
        if Self.numericKind(source) != nil && Self.numericKind(target) != nil {
            if Self.numericKind(source) == .decimal && Self.numericKind(target) != .decimal { throw mismatch("CAST") }
            return
        }
        if Self.isText(source), case .sql(.decimal) = target { return }
        if Self.isText(source), target == .sql(.uuid) { return }
        if Self.isPrimitiveArray(source) == Self.isPrimitiveArray(target),
           Self.primitiveKind(source) == Self.primitiveKind(target), Self.primitiveKind(source) != nil { return }
        throw DatabaseExpressionEvaluationError.invalidCast(String(describing: target))
    }

    private func requirePrimitiveOperation(_ children: [CanonicalBoundExpression], operation: String) throws {
        for child in children {
            switch child.type {
            case .sql(.json), .sql(.jsonb), .sql(.interval), .sql(.array):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of rich values")
            default: break
            }
        }
    }

    private func scalarColumn(_ query: SelectQuery) throws -> QueryColumn {
        let columns = try subqueryColumns(query)
        guard columns.count == 1, let column = columns.first else { throw mismatch("scalar subquery column count") }
        return column
    }

    private func subqueryColumns(_ query: SelectQuery) throws -> [QueryColumn] {
        guard let bindSubquery else {
            throw DatabaseExpressionEvaluationError.unsupportedExpression("subquery binding context is required")
        }
        return try bindSubquery(query)
    }

    private func outputColumns(slots: [Int]) throws -> [QueryColumn] {
        try slots.enumerated().map { index, slot in
            let input = try column(at: slot)
            return try QueryColumn(number: ordinal(index), name: input.name, type: input.type, isNullable: input.isNullable)
        }
    }

    private func column(at slot: Int) throws -> QueryColumn {
        guard columns.indices.contains(slot) else { throw mismatch("column slot outside relation schema") }
        let column = columns[slot]
        try validate(column.type)
        return column
    }

    private func ordinal(_ index: Int) throws -> UInt32 {
        guard let value = UInt32(exactly: index), value < UInt32.max else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return value + 1
    }

    private func validate(_ type: QueryColumnType) throws {
        guard QueryValueValidator.accepts(.primitive(.null), type: type) else { throw mismatch("unresolved type") }
    }

    private func node(
        _ expression: DatabaseKit.Expression, _ type: QueryColumnType,
        _ nullable: Bool, _ children: [CanonicalBoundExpression] = []
    ) -> CanonicalBoundExpression {
        CanonicalBoundExpression(expression: expression, type: type, isNullable: nullable, children: children)
    }

    private func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }

    private enum NumericKind { case signed, unsigned, floating, decimal }

    private static func numericKind(_ type: QueryColumnType) -> NumericKind? {
        guard !isPrimitiveArray(type) else { return nil }
        switch primitiveKind(type) {
        case .int8?, .int16?, .int32?, .int64?: return .signed
        case .uint8?, .uint16?, .uint32?, .uint64?: return .unsigned
        case .float32?, .float64?: return .floating
        case .decimal?: return .decimal
        default: return nil
        }
    }

    private static func primitiveKind(_ type: QueryColumnType) -> FieldSchemaType? {
        switch type {
        case .primitive(let kind, _): return kind
        case .sql(let sql):
            switch sql {
            case .boolean: return .bool
            case .smallint: return .int16
            case .integer: return .int32
            case .bigint: return .int64
            case .real: return .float32
            case .doublePrecision: return .float64
            case .decimal: return .decimal
            case .char, .varchar, .text: return .string
            case .binary, .varbinary, .blob: return .bytes
            case .date: return .date
            case .time(false, _): return .time
            case .timestamp(false, _): return .dateTime
            case .timestamp(true, _): return .timestamp
            case .uuid: return .uuid
            default: return nil
            }
        }
    }

    private static func isText(_ type: QueryColumnType) -> Bool { !isPrimitiveArray(type) && primitiveKind(type) == .string }
    private static func isBytes(_ type: QueryColumnType) -> Bool { !isPrimitiveArray(type) && primitiveKind(type) == .bytes }
    private static func isPrimitiveArray(_ type: QueryColumnType) -> Bool {
        if case .primitive(_, let isArray) = type { return isArray }
        return false
    }

    private static func outputName(_ expression: DatabaseKit.Expression, index: Int) -> String {
        switch expression {
        case .column(let column): return column.column
        case .aggregate(let aggregate):
            switch aggregate {
            case .count: return "count"
            case .sum: return "sum"
            case .avg: return "avg"
            case .min: return "min"
            case .max: return "max"
            case .groupConcat: return "group_concat"
            case .sample: return "sample"
            case .arrayAgg: return "array_agg"
            }
        default: return "column\(index)"
        }
    }

    /// Primitive relation cells keep a primitive carrier for the existing
    /// evaluator. Rich canonical values never enter this conversion branch.
    static func normalizeSourceCarrier(_ value: QueryValue, to type: QueryColumnType) throws -> QueryValue {
        if case .primitive(let primitive) = value, !value.isNull,
           case .sql(.decimal) = type {
            let canonical: FieldValue
            do { canonical = try RelationalValueIdentity.canonicalize(primitive).value }
            catch { throw DatabaseExpressionEvaluationError.numericOverflow }
            if let decimal = exactDecimal(canonical) {
                let result = QueryValue.primitive(.decimal(decimal))
                guard QueryValueValidator.accepts(result, type: type) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "primitive numeric source normalization")
                }
                return result
            }
        }
        return try normalize(value, to: type)
    }

    static func normalize(
        _ value: QueryValue, to type: QueryColumnType,
        permitsFloatingRounding: Bool = false
    ) throws -> QueryValue {
        if QueryValueValidator.accepts(value, type: type) { return value }
        guard case .primitive(let primitive) = value, !isPrimitiveArray(type), let kind = primitiveKind(type) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result normalization")
        }
        let normalized: QueryValue
        if kind == .decimal, let decimal = exactDecimal(primitive) {
            normalized = .numeric(try QueryNumericValue(value: decimal, displayScale: max(0, decimal.scale)))
        } else if let integer = integer(primitive) {
            let scalar: FieldValue
            switch kind {
            case .int8: guard let value = Int8(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int8(value)
            case .int16: guard let value = Int16(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int16(value)
            case .int32: guard let value = Int32(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int32(value)
            case .int64: guard let value = Int64(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .int64(value)
            case .uint8: guard let value = UInt8(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint8(value)
            case .uint16: guard let value = UInt16(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint16(value)
            case .uint32: guard let value = UInt32(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint32(value)
            case .uint64: guard let value = UInt64(exactly: integer) else { throw DatabaseExpressionEvaluationError.numericOverflow }; scalar = .uint64(value)
            case .float32:
                let value = permitsFloatingRounding ? Float(integer) : Float(exactly: integer)
                guard let value, value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                scalar = .float32(value)
            case .float64:
                let value = permitsFloatingRounding ? Double(integer) : Double(exactly: integer)
                guard let value, value.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
                scalar = .float64(value)
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound integer normalization")
            }
            normalized = .primitive(scalar)
        } else if kind == .float64, case .float32(let scalar) = primitive, scalar.isFinite {
            normalized = .primitive(.float64(Double(scalar)))
        } else if kind == .float32, case .float64(let scalar) = primitive {
            let result = permitsFloatingRounding ? Float(scalar) : Float(exactly: scalar)
            guard let result, result.isFinite else { throw DatabaseExpressionEvaluationError.numericOverflow }
            normalized = .primitive(.float32(result))
        } else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result normalization")
        }
        guard QueryValueValidator.accepts(normalized, type: type) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound result does not satisfy declared type")
        }
        return normalized
    }

    private static func integer(_ value: FieldValue) -> Int128? {
        switch value {
        case .int8(let value): return Int128(value)
        case .int16(let value): return Int128(value)
        case .int32(let value): return Int128(value)
        case .int64(let value): return Int128(value)
        case .uint8(let value): return Int128(value)
        case .uint16(let value): return Int128(value)
        case .uint32(let value): return Int128(value)
        case .uint64(let value): return Int128(value)
        default: return nil
        }
    }

    private static func exactDecimal(_ value: FieldValue) -> ExactDecimal? {
        if case .decimal(let decimal) = value { return decimal }
        return integer(value).map { ExactDecimal(coefficient: $0, scale: 0) }
    }
}
