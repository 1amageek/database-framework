import DatabaseTypes
import DatabaseKit

/// SQL-only typing for the currently executable canonical expression forms.
/// The relation owner supplies name lookup and subquery binding before reads.
struct CanonicalQueryTypeBinder {
    let columns: [QueryColumn]
    private let parameterState: CanonicalParameterBindingState
    private let ownsParameterState: Bool
    private let allowingSPARQLFunctions: Bool
    private let occurrences: QueryOccurrenceArena?
    private let root: QueryOccurrenceID?
    private var currentOccurrence: QueryOccurrenceID?
    private var isNested = false
    private let resolveColumn: (ColumnRef) throws -> Int
    private let wildcardSlots: (String?) throws -> [Int]
    private let bindSubquery: ((SelectQuery) throws -> [QueryColumn])?
    private let bindLocatedSubquery: ((SelectQuery, QueryOccurrenceID?) throws -> CanonicalBoundSelect)?
    private let workMeter: DatabaseWorkMeter?

    init(
        columns: [QueryColumn],
        parameters: [QueryParameter] = [],
        parameterState: CanonicalParameterBindingState? = nil,
        allowingSPARQLFunctions: Bool = false,
        occurrences: QueryOccurrenceArena? = nil,
        root: QueryOccurrenceID? = nil,
        resolveColumn: @escaping (ColumnRef) throws -> Int,
        wildcardSlots: @escaping (String?) throws -> [Int],
        bindSubquery: ((SelectQuery) throws -> [QueryColumn])? = nil,
        bindLocatedSubquery: ((SelectQuery, QueryOccurrenceID?) throws -> CanonicalBoundSelect)? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) {
        self.columns = columns
        self.parameterState = parameterState ?? CanonicalParameterBindingState(parameters: parameters)
        self.ownsParameterState = parameterState == nil
        self.allowingSPARQLFunctions = allowingSPARQLFunctions
        self.occurrences = occurrences
        self.root = root
        self.currentOccurrence = root
        self.resolveColumn = resolveColumn
        self.wildcardSlots = wildcardSlots
        self.bindSubquery = bindSubquery
        self.bindLocatedSubquery = bindLocatedSubquery
        self.workMeter = workMeter
    }

    /// The supplied schema is the already-bound FROM scope. Source recursion,
    /// including ON clauses, remains owned by the relation binder.
    func bind(_ query: SelectQuery) throws -> [QueryColumn] {
        try bindSelect(query).columns
    }

    func bindSelect(_ query: SelectQuery, occurrence: QueryOccurrenceID? = nil) throws -> CanonicalBoundSelect {
        if let occurrence {
            var scoped = self
            scoped.currentOccurrence = occurrence
            return try scoped.bindSelect(query)
        }
        let output: [QueryColumn]
        let projection: [CanonicalBoundExpression]
        switch query.projection {
        case .all:
            output = try outputColumns(slots: wildcardSlots(nil)); projection = []
        case .allFrom(let scope):
            do { output = try outputColumns(slots: wildcardSlots(scope)); projection = [] }
            catch let cause as DatabaseExpressionEvaluationError {
                guard occurrences != nil else { throw cause }
                throw CanonicalSQLBindingFailure(cause: .expression(cause),
                    occurrence: childBinder(.projection, 0).currentOccurrence, root: root)
            }
        case .items(let items), .distinctItems(let items):
            projection = try items.enumerated().map { index, item in
                try childBinder(.projection, index).bind(item.expression, parameterContext: .output)
            }
            output = try items.enumerated().map { index, item in
                let bound = projection[index]
                return try QueryColumn(
                    number: ordinal(index),
                    name: item.alias ?? Self.outputName(item.expression, index: index),
                    type: bound.type,
                    isNullable: bound.isNullable
                )
            }
        }
        let filter = try query.filter.map { try childBinder(.filter).boolean($0) }
        let groupBy = try (query.groupBy ?? []).enumerated().map { try childBinder(.groupBy, $0.offset).bind($0.element) }
        let having = try query.having.map { try childBinder(.having).boolean($0) }
        let orderBy = try (query.orderBy ?? []).enumerated().map { try childBinder(.orderBy, $0.offset).bind($0.element.expression) }
        if ownsParameterState { try parameterState.finalize() }
        return CanonicalBoundSelect(columns: output, projection: projection, filter: filter,
                                    groupBy: groupBy, having: having, orderBy: orderBy)
    }

    func bind(
        _ expression: DatabaseKit.Expression,
        contextualType: QueryColumnType? = nil,
        parameterContext: CanonicalParameterBindingState.Context = .unconstrained,
        occurrence: QueryOccurrenceID? = nil
    ) throws -> CanonicalBoundExpression {
        var scoped = self
        if let occurrence { scoped.currentOccurrence = occurrence }
        do {
            let result = try scoped.bindExpression(expression, contextualType: contextualType, parameterContext: parameterContext)
            if ownsParameterState && !isNested { try parameterState.finalize() }
            return result
        }
        catch let cause as DatabaseExpressionEvaluationError {
            guard occurrences != nil else { throw cause }
            throw CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: scoped.currentOccurrence, root: root)
        }
    }

    private func bindExpression(
        _ expression: DatabaseKit.Expression,
        contextualType: QueryColumnType?,
        parameterContext: CanonicalParameterBindingState.Context
    ) throws -> CanonicalBoundExpression {
        switch expression {
        // FIXME(INCOMPLETE_IMPLEMENTATION): Canonical SQL binding reaches constructor expressions here.
        // S07 ARRAY typing and S08 ROW typing must be implemented before execution can succeed.
        case .array, .row:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL ARRAY/ROW constructor")
        case .literal(let literal):
            let type = try literalType(literal, contextualType: contextualType)
            if case .sqlUnknownString(let text) = literal {
                return node(.literal(.queryValue(try unknownString(text, to: type), type: type)), type, false)
            }
            return node(expression, type, literal.isNull)
        case .column(let reference):
            let slot = try resolveColumn(reference)
            let column = try column(at: slot)
            return CanonicalBoundExpression(
                expression: expression, type: column.type,
                isNullable: column.isNullable, columnSlot: slot, occurrence: currentOccurrence
            )
        case .parameter(let reference):
            let parameter = try parameterState.resolve(reference, contextualType: contextualType,
                context: contextualType == nil ? parameterContext : .required,
                occurrence: currentOccurrence, root: root)
            let type = try literalType(
                .queryValue(parameter.value, type: parameter.type),
                contextualType: contextualType
            )
            return node(.literal(.queryValue(parameter.value, type: type)), type, parameter.value.isNull)
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs):
            let children = try bindCommon([lhs, rhs], defaultType: .sql(.bigint))
            try requirePrimitiveOperation(children, operation: "arithmetic")
            let type = try arithmeticType(children.map { $0.type })
            return node(expression, type, children.contains { $0.isNullable }, children)
        case .negate(let operand):
            let child = try childBinder(.operand, 0).bind(operand, contextualType: .sql(.bigint))
            try requirePrimitiveOperation([child], operation: "negation")
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
            let children = try [childBinder(.operand, 0).boolean(lhs), childBinder(.operand, 1).boolean(rhs)]
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .not(let operand):
            let child = try childBinder(.operand, 0).boolean(operand)
            return node(expression, .sql(.boolean), child.isNullable, [child])
        case .isNull(let operand), .isNotNull(let operand):
            return node(expression, .sql(.boolean), false, [try childBinder(.operand, 0).bind(operand)])
        case .like(let operand, _):
            let child = try childBinder(.operand, 0).bind(operand, contextualType: .sql(.text))
            guard Self.isText(child.type) else { throw mismatch("LIKE") }
            return node(expression, .sql(.boolean), child.isNullable, [child])
        case .between(let operand, let low, let high):
            let children = try bindCommon([operand, low, high])
            try requirePrimitiveOperation(children, operation: "BETWEEN")
            return node(expression, .sql(.boolean), children.contains { $0.isNullable }, children)
        case .inList(let operand, let values), .notInList(let operand, let values):
            if allowingSPARQLFunctions, values.contains(where: Self.isSPARQLProducer) {
                let child = try childBinder(.operand, 0).bind(operand)
                var others: [CanonicalBoundExpression] = []
                var common = child.type
                for (index, value) in values.enumerated() {
                    let scoped = childBinder(.argument, index)
                    if Self.isSPARQLProducer(value) { try scoped.bindSPARQLProducer(value) }
                    else {
                        let bound = try scoped.bind(value, contextualType: child.type)
                        do { common = try commonType([common, bound.type]) }
                        catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: bound.occurrence) }
                        others.append(bound)
                    }
                }
                try requirePrimitiveOperation([child] + others, operation: "IN")
                return node(expression, .sql(.boolean), true)
            }
            let addresses = [childBinder(.operand, 0).currentOccurrence] + values.indices.map { childBinder(.argument, $0).currentOccurrence }
            let children = try bindCommon([operand] + values, addresses: addresses)
            try requirePrimitiveOperation(children, operation: "IN")
            return node(expression, .sql(.boolean), !values.isEmpty && children.contains { $0.isNullable }, children)
        case .caseWhen(let pairs, let fallback):
            let conditions = try pairs.enumerated().map { try childBinder(.caseCondition, $0.offset).boolean($0.element.condition) }
            let results = [fallback ?? .literal(.null)] + pairs.map { $0.result }
            let addresses = [childBinder(.caseElse).currentOccurrence] + pairs.indices.map { childBinder(.caseResult, $0).currentOccurrence }
            let ordered = try bindCommon(results, defaultType: contextualType ?? .sql(.text), addresses: addresses)
            let branches = Array(ordered.dropFirst()) + [ordered[0]]
            try requirePrimitiveOperation(branches, operation: "CASE")
            let type = try commonType(branches.map { $0.type })
            return node(expression, type, branches.contains { $0.isNullable }, conditions + branches)
        case .coalesce(let expressions):
            guard !expressions.isEmpty else { throw mismatch("COALESCE") }
            let children = try bindCommon(expressions, defaultType: contextualType ?? .sql(.text), field: .argument)
            try requirePrimitiveOperation(children, operation: "COALESCE")
            return node(expression, try commonType(children.map { $0.type }), children.allSatisfy { $0.isNullable }, children)
        case .nullIf(let lhs, let rhs):
            let children = try bindCommon([lhs, rhs], defaultType: contextualType ?? .sql(.text))
            try requirePrimitiveOperation(children, operation: "NULLIF")
            return node(expression, try commonType(children.map { $0.type }), true, children)
        case .cast(let operand, let target):
            let type = QueryColumnType.sql(target)
            do { try validate(type) }
            catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: childBinder(.castType).currentOccurrence) }
            let child = try childBinder(.operand, 0).bind(operand, contextualType: type)
            do { try validateCast(from: child.type, to: type) }
            catch let cause as DatabaseExpressionEvaluationError { throw located(cause, at: childBinder(.castType).currentOccurrence) }
            if Self.isStaticallyNull(child.expression) {
                return node(.literal(.queryValue(.primitive(.null), type: type)), type, true)
            }
            if child.type == type { return child }
            // FIXME(INCOMPLETE_IMPLEMENTATION): Nonidentity typmod CAST reaches this binding gate.
            // S04 numeric rounding and S05 length conversion must execute before these signatures can be admitted.
            switch target {
            case .decimal(.some, _), .decimal(_, .some), .char(.some), .varchar(.some),
                 .binary(.some), .varbinary(.some):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("CAST type modifier conversion")
            default: break
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
            if Self.isText(child.type) {
                switch target {
                case .date, .time, .timestamp:
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("temporal input conversion")
                default: break
                }
            }
            try requirePrimitiveOperation([child], operation: "CAST")
            return node(expression, type, child.isNullable, [child])
        case .function(let function):
            return try bindFunction(function, expression: expression)
        case .aggregate(let aggregate):
            return try bindAggregate(aggregate, expression: expression)
        case .subquery(let query):
            let binding = try bindLocatedSubquery?(query, childBinder(.subquery).currentOccurrence)
            let column = try scalarColumn(query, binding: binding)
            return node(expression, column.type, true, subqueryBinding: binding)
        case .exists(let query):
            let binding = try bindLocatedSubquery?(query, childBinder(.subquery).currentOccurrence)
            if binding == nil { _ = try subqueryColumns(query) }
            return node(expression, .sql(.boolean), false, subqueryBinding: binding)
        case .inSubquery(let operand, let query):
            let binding = try bindLocatedSubquery?(query, childBinder(.subquery).currentOccurrence)
            let column = try scalarColumn(query, binding: binding)
            let child = try childBinder(.operand, 0).bind(operand, contextualType: column.type)
            _ = try commonType([child.type, column.type])
            try requirePrimitiveOperation([child], operation: "IN subquery")
            return node(expression, .sql(.boolean), true, [child], subqueryBinding: binding)
        case .variable, .bound, .regex, .triple, .isTriple, .subject, .predicate, .object:
            throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL binding does not admit this expression")
        }
    }

    private func bindFunction(
        _ function: FunctionCall, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        let name = function.name
        guard function.schema == nil || function.schema == "pg_catalog",
              ["lower", "upper", "length", "abs"].contains(name) else {
            throw DatabaseExpressionEvaluationError.unsupportedFunction(function.name)
        }
        guard function.arguments.count == 1, !function.distinct else { throw mismatch(name) }
        let child = try childBinder(.argument, 0).bind(function.arguments[0], contextualType: name == "abs" ? .sql(.doublePrecision) : .sql(.text))
        try requirePrimitiveOperation([child], operation: name)
        let type: QueryColumnType
        switch name {
        case "lower", "upper":
            guard Self.isText(child.type) else { throw mismatch(name) }
            type = .sql(.text)
        case "length":
            guard Self.isText(child.type) || Self.isBytes(child.type) else {
                throw mismatch(name)
            }
            type = .sql(.integer)
        default:
            guard Self.numericKind(child.type) != nil else { throw mismatch(name) }
            if Self.numericKind(child.type) == .unsigned { type = .primitive(.uint64) }
            else if child.type == .primitive(.int8) { type = .sql(.bigint) }
            else { type = child.type }
        }
        return node(expression, type, child.isNullable, [child])
    }

    private func bindAggregate(
        _ aggregate: AggregateFunction, expression: DatabaseKit.Expression
    ) throws -> CanonicalBoundExpression {
        switch aggregate {
        case .count(let operand, _):
            let children = try operand.map { [try childBinder(.aggregateArgument).bind($0)] } ?? []
            return node(expression, .sql(.bigint), false, children)
        case .sum(let operand, _), .avg(let operand, _):
            let child = try childBinder(.aggregateArgument).bind(operand, contextualType: .sql(.bigint))
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
            let child = try childBinder(.aggregateArgument).bind(operand)
            try requirePrimitiveOperation([child], operation: "aggregate")
            return node(expression, child.type, true, [child])
        case .groupConcat(let operand, _, _):
            let child = try childBinder(.aggregateArgument).bind(operand, contextualType: .sql(.text))
            guard Self.isText(child.type) else { throw mismatch("GROUP_CONCAT") }
            return node(expression, .sql(.text), true, [child])
        case .arrayAgg(let operand, let ordering, _):
            let child = try childBinder(.aggregateArgument).bind(operand)
            try requirePrimitiveOperation([child], operation: "ARRAY_AGG")
            guard let kind = Self.primitiveKind(child.type), !Self.isPrimitiveArray(child.type) else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("ARRAY_AGG of rich or nested values")
            }
            let keys = try (ordering ?? []).enumerated().map { try childBinder(.aggregateOrderBy, $0.offset).bind($0.element.expression) }
            return node(expression, .primitive(kind, isArray: true), true, [child] + keys)
        }
    }

    private func boolean(_ expression: DatabaseKit.Expression) throws -> CanonicalBoundExpression {
        let bound = try bind(expression, contextualType: .sql(.boolean))
        guard bound.type == .sql(.boolean) || bound.type == .primitive(.bool) else { throw mismatch("boolean expression") }
        return bound
    }

    private func bindCommon(
        _ expressions: [DatabaseKit.Expression], defaultType: QueryColumnType = .sql(.text),
        field: QueryOccurrenceField = .operand, addresses: [QueryOccurrenceID?]? = nil
    ) throws -> [CanonicalBoundExpression] {
        var strongTypes: [QueryColumnType] = []
        var bound: [Int: CanonicalBoundExpression] = [:]
        for (index, expression) in expressions.enumerated() where try !isContextualUnknown(expression) {
            var scoped = childBinder(field, index)
            if let addresses { scoped.currentOccurrence = addresses[index] }
            let child = try scoped.bind(expression)
            bound[index] = child
            strongTypes.append(child.type)
        }
        var type = strongTypes.first ?? defaultType
        for index in expressions.indices where bound[index] != nil {
            guard let child = bound[index] else { continue }
            do { type = try commonType([type, child.type]) }
            catch let cause as DatabaseExpressionEvaluationError {
                guard occurrences != nil else { throw cause }
                throw CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: child.occurrence, root: root)
            }
        }
        return try expressions.enumerated().map { index, expression in
            var scoped = childBinder(field, index)
            if let addresses { scoped.currentOccurrence = addresses[index] }
            return try coerce(bound[index] ?? scoped.bind(expression, contextualType: type), to: type)
        }
    }

    private func isContextualUnknown(_ expression: DatabaseKit.Expression) throws -> Bool {
        switch expression {
        case .literal(.null), .literal(.sqlUnknownString), .literal(.queryValue(.primitive(.null), type: nil)): return true
        case .parameter(let reference):
            return try parameterState.declaredType(reference) == nil
        case .coalesce(let children): return try children.allSatisfy(isContextualUnknown)
        case .caseWhen(let pairs, let fallback):
            return try pairs.allSatisfy { try isContextualUnknown($0.result) } && (fallback.map(isContextualUnknown) ?? true)
        default: return false
        }
    }

    func commonSourceType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        if types.allSatisfy({ Self.numericKind($0) != nil }),
           types.contains(where: { Self.numericKind($0) == .decimal }),
           types.contains(where: { Self.numericKind($0) == .floating }) {
            return .sql(.decimal(precision: nil, scale: nil))
        }
        return try commonType(types)
    }

    func commonType(_ types: [QueryColumnType]) throws -> QueryColumnType {
        guard let first = types.first else { return .sql(.text) }
        if types.allSatisfy({ $0 == first }) { return first }
        if types.allSatisfy(Self.isText) { return .sql(.text) }
        if types.allSatisfy(Self.isBytes) { return .sql(.blob) }
        if types.allSatisfy({ Self.numericKind($0) != nil }) {
            let kinds = types.compactMap(Self.numericKind)
            if kinds.contains(.decimal) && kinds.contains(.floating) {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("exact and floating common coercion")
            }
            if kinds.contains(.decimal) { return .sql(.decimal(precision: nil, scale: nil)) }
            if kinds.contains(.floating) {
                return types.contains(where: { Self.primitiveKind($0) == .float64 })
                    ? .sql(.doublePrecision) : .sql(.real)
            }
            if kinds.contains(.signed) && kinds.contains(.unsigned) {
                return .sql(.decimal(precision: nil, scale: nil))
            }
            let ranked = types.compactMap { type -> (QueryColumnType, Int)? in
                let rank: Int
                switch Self.primitiveKind(type) {
                case .int8?, .uint8?: rank = 8
                case .int16?, .uint16?: rank = 16
                case .int32?, .uint32?: rank = 32
                case .int64?, .uint64?: rank = 64
                default: return nil
                }
                return (type, rank)
            }
            guard let selected = ranked.max(by: { $0.1 < $1.1 }) else { throw mismatch("numeric common type") }
            return selected.0
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
        case .sqlUnknownString: return contextualType ?? .sql(.text)
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
                var kind: FieldSchemaType?
                for element in values where !element.isNull {
                    if case .array = element {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                    }
                    guard let elementKind = primitiveKind(try type(of: .primitive(element))),
                          kind == nil || kind == elementKind else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                    }
                    kind = elementKind
                }
                guard let kind else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "untyped primitive array")
                }
                return .primitive(kind, isArray: true)
            }
        }
    }

    private func validateCast(from source: QueryColumnType, to target: QueryColumnType) throws {
        if source == target { return }
        if case .sql(.time(let sourceZone, _)) = source,
           case .sql(.time(let targetZone, _)) = target, sourceZone == targetZone { return }
        if Self.numericKind(source) != nil && Self.numericKind(target) != nil {
            if Self.numericKind(source) == .decimal && Self.numericKind(target) != .decimal { throw mismatch("CAST") }
            if Self.numericKind(source) == .floating,
               Self.numericKind(target) == .signed || Self.numericKind(target) == .unsigned || Self.numericKind(target) == .decimal {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("floating to exact CAST")
            }
            return
        }
        if Self.isText(source), case .sql(.decimal) = target { return }
        if Self.isText(source), target == .sql(.uuid) { return }
        if Self.isText(source) {
            switch target {
            case .sql(.date), .sql(.time), .sql(.timestamp): return
            default: break
            }
        }
        if Self.isText(target), Self.numericKind(source) == .signed || Self.numericKind(source) == .unsigned { return }
        if Self.isPrimitiveArray(source) == Self.isPrimitiveArray(target),
           Self.primitiveKind(source) == Self.primitiveKind(target), Self.primitiveKind(source) != nil { return }
        throw DatabaseExpressionEvaluationError.invalidCast("unsupported source-to-target conversion")
    }

    private func coerce(_ child: CanonicalBoundExpression, to type: QueryColumnType) throws -> CanonicalBoundExpression {
        guard child.type != type else { return child }
        guard (Self.numericKind(child.type) != nil && Self.numericKind(type) != nil)
                || (Self.primitiveKind(child.type) == .bool && Self.primitiveKind(type) == .bool
                    && !Self.isPrimitiveArray(child.type) && !Self.isPrimitiveArray(type))
                || (Self.isText(child.type) && Self.isText(type))
                || (Self.isBytes(child.type) && Self.isBytes(type)) else { throw mismatch("implicit coercion") }
        let target: DataType
        switch type {
        case .sql(let sql): target = sql
        case .primitive(.bool, false): target = .boolean
        case .primitive(.int16, false): target = .smallint
        case .primitive(.int32, false): target = .integer
        case .primitive(.int64, false): target = .bigint
        case .primitive(.float32, false): target = .real
        case .primitive(.float64, false): target = .doublePrecision
        case .primitive(.decimal, false): target = .decimal(precision: nil, scale: nil)
        case .primitive(.string, false): target = .text
        case .primitive(.bytes, false): target = .blob
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("primitive common coercion")
        }
        try validateCast(from: child.type, to: type)
        return node(.cast(child.expression, targetType: target), type, child.isNullable, [child])
    }

    private static func isSPARQLProducer(_ expression: DatabaseKit.Expression) -> Bool {
        if case .function(let function) = expression {
            return function.schema == nil && function.name == "sparql"
        }
        return false
    }

    private func bindSPARQLProducer(_ expression: DatabaseKit.Expression) throws {
        guard case .function(let function) = expression, (2...3).contains(function.arguments.count), !function.distinct else {
            throw mismatch("SPARQL arguments")
        }
        for (index, argument) in function.arguments.enumerated() {
            if index == 0, case .column(let name) = argument {
                guard name.table == nil else { throw mismatch("SPARQL entity identity") }
                continue
            }
            let scoped = childBinder(.argument, index)
            let bound = try scoped.bind(argument, contextualType: .sql(.text))
            guard Self.isText(bound.type),
                  case .literal(let literal) = bound.expression,
                  case .queryValue(.primitive(.string), _) = literal else {
                if case .literal(.string) = bound.expression { continue }
                let cause = mismatch("SPARQL string argument")
                guard occurrences != nil else { throw cause }
                throw CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: bound.occurrence, root: root)
            }
        }
    }

    private static func isStaticallyNull(_ expression: DatabaseKit.Expression) -> Bool {
        switch expression {
        case .literal(let literal): return literal.isNull
        case .cast(let nested, _): return isStaticallyNull(nested)
        default: return false
        }
    }

    private func unknownString(_ text: String, to type: QueryColumnType) throws -> QueryValue {
        let scratch: DatabaseIntermediateReservation?
        if let workMeter, Self.numericKind(type) != nil || type == .sql(.boolean) || type == .primitive(.bool) {
            scratch = try workMeter.reserveIntermediate(
                bytes: Self.inputScratchFootprint(utf8Count: text.utf8.count, type: type), at: .bindingCandidate)
        } else {
            scratch = nil
        }
        defer { scratch?.release() }
        let value: QueryValue
        switch type {
        case .sql(.text), .sql(.char), .sql(.varchar), .primitive(.string, false):
            value = .primitive(.string(text))
        case .sql(.boolean), .primitive(.bool, false):
            switch Self.trimmed(text).lowercased() {
            case "true", "t", "yes", "y", "on", "1": value = .primitive(.bool(true))
            case "false", "f", "no", "n", "off", "0": value = .primitive(.bool(false))
            default: throw mismatch("boolean input")
            }
        case .sql(.smallint), .sql(.integer), .sql(.bigint),
             .primitive(.int8, false), .primitive(.int16, false), .primitive(.int32, false), .primitive(.int64, false),
             .primitive(.uint8, false), .primitive(.uint16, false), .primitive(.uint32, false), .primitive(.uint64, false):
            let input = try numericInput(text)
            var digits = input[...]
            let negative = digits.first == "-"
            if digits.first == "-" || digits.first == "+" { digits = digits.dropFirst() }
            let radix: Int
            if digits.hasPrefix("0x") || digits.hasPrefix("0X") { radix = 16; digits = digits.dropFirst(2) }
            else if digits.hasPrefix("0o") || digits.hasPrefix("0O") { radix = 8; digits = digits.dropFirst(2) }
            else if digits.hasPrefix("0b") || digits.hasPrefix("0B") { radix = 2; digits = digits.dropFirst(2) }
            else { radix = 10 }
            guard let magnitude = Int128(digits, radix: radix), magnitude >= 0 else {
                throw DatabaseExpressionEvaluationError.invalidCast("integer input")
            }
            let integer = negative ? -magnitude : magnitude
            let carrier: FieldValue
            if Self.numericKind(type) == .unsigned {
                guard let unsigned = UInt64(exactly: integer) else {
                    throw DatabaseExpressionEvaluationError.invalidCast("unsigned integer input")
                }
                carrier = .uint64(unsigned)
            } else {
                guard let signed = Int64(exactly: integer) else {
                    throw DatabaseExpressionEvaluationError.invalidCast("integer input")
                }
                carrier = .int64(signed)
            }
            value = try Self.normalize(.primitive(carrier), to: type)
        case .sql(.real), .sql(.doublePrecision), .primitive(.float32, false), .primitive(.float64, false):
            guard let number = Double(Self.trimmed(text)), number.isFinite else {
                throw DatabaseExpressionEvaluationError.invalidCast("floating-point input")
            }
            value = try Self.normalize(.primitive(.float64(number)), to: type, permitsFloatingRounding: true)
        case .sql(.decimal), .primitive(.decimal, false):
            guard case .decimal(let number) = Literal.parseDecimal(try numericInput(text)) else {
                throw DatabaseExpressionEvaluationError.invalidCast("numeric input")
            }
            value = .primitive(.decimal(number))
        case .sql(.uuid):
            guard let uuid = DatabaseTypes.UUID(canonicalString: text) else {
                throw DatabaseExpressionEvaluationError.invalidCast("uuid")
            }
            value = .primitive(.uuid(uuid))
        default: throw DatabaseExpressionEvaluationError.unsupportedExpression("SQL unknown string input for unsupported target type")
        }
        guard QueryValueValidator.accepts(value, type: type) else { throw mismatch("SQL unknown string input") }
        return value
    }

    private static func inputScratchFootprint(utf8Count: Int, type: QueryColumnType) throws -> UInt64 {
        let count = UInt64(utf8Count).addingReportingOverflow(1)
        guard !count.overflow, let elementCount = Int(exactly: count.partialValue) else {
            throw DatabaseExpressionEvaluationError.numericOverflow
        }
        // The existing input kernels can retain trim/filter/digit/sign copies together.
        var footprint = try DatabaseIntermediateFootprint(bytes: count.partialValue)
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<String>.stride)))
            .multiplied(by: 8)
        if Self.numericKind(type) == .decimal {
            // Literal.parseDecimal splits exponent and fraction before rejecting malformed input.
            let layout = try DatabaseRetainedArrayLayout.validated(
                containerByteCount: UInt64(MemoryLayout<[Substring]>.stride),
                elementCapacitySlotByteCount: UInt64(MemoryLayout<Substring>.stride),
                sharedOwnerByteCount: 32, appendAdmissionByteCount: 16)
            let growth = try layout.growth(from: 0, toFit: elementCount)
            footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: layout.containerByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).multiplied(by: 2))
        }
        return footprint.bytes
    }

    private static func trimmed(_ text: String) -> String {
        String(text.drop(while: { $0.isWhitespace }).reversed().drop(while: { $0.isWhitespace }).reversed())
    }

    private func numericInput(_ text: String) throws -> String {
        let input = Self.trimmed(text)
        var previous: Character?
        for index in input.indices {
            let character = input[index]
            if character == "_" {
                let next = input.index(after: index)
                guard previous?.hexDigitValue != nil, next != input.endIndex,
                      input[next].hexDigitValue != nil else { throw mismatch("numeric input separator") }
            }
            previous = character
        }
        return String(input.filter { $0 != "_" })
    }

    private func requirePrimitiveOperation(_ children: [CanonicalBoundExpression], operation: String) throws {
        for child in children {
            try requirePrimitiveOperation(child.children, operation: operation)
            switch child.expression {
            case .column, .subquery:
                if case .sql(.decimal) = child.type {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of canonical numeric source")
                }
            default: break
            }
            if case .literal(.queryValue(let value, _)) = child.expression, !value.isNull {
                guard case .primitive = value else {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of rich values")
                }
            }
            switch child.type {
            case .sql(.json), .sql(.jsonb), .sql(.interval), .sql(.array):
                throw DatabaseExpressionEvaluationError.unsupportedExpression("\(operation) of rich values")
            default: break
            }
        }
    }

    private func scalarColumn(_ query: SelectQuery, binding: CanonicalBoundSelect? = nil) throws -> QueryColumn {
        let columns = try binding?.columns ?? subqueryColumns(query)
        guard columns.count == 1, let column = columns.first else { throw mismatch("scalar subquery column count") }
        return column
    }

    private func subqueryColumns(_ query: SelectQuery) throws -> [QueryColumn] {
        if let bindLocatedSubquery { return try bindLocatedSubquery(query, childBinder(.subquery).currentOccurrence).columns }
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
        _ nullable: Bool, _ children: [CanonicalBoundExpression] = [],
        subqueryBinding: CanonicalBoundSelect? = nil
    ) -> CanonicalBoundExpression {
        CanonicalBoundExpression(expression: expression, type: type, isNullable: nullable, children: children,
                                 occurrence: currentOccurrence, subqueryBinding: subqueryBinding)
    }

    private func childBinder(_ field: QueryOccurrenceField, _ index: Int? = nil) -> Self {
        var child = self
        child.isNested = true
        child.currentOccurrence = currentOccurrence.flatMap { parent in
            occurrences?.child(of: parent, field: field, index: index.flatMap(UInt32.init(exactly:)))
        }
        return child
    }

    private func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }

    private func located(_ cause: DatabaseExpressionEvaluationError, at occurrence: QueryOccurrenceID?) -> any Error {
        guard occurrences != nil else { return cause }
        return CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: occurrence, root: root)
    }

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
