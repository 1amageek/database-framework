import DatabaseKit

/// A checked expression signature. Slot indices address the binding schema,
/// independently of public column numbers and labels.
struct CanonicalBoundExpression: Sendable {
    let expression: DatabaseKit.Expression
    let type: QueryColumnType
    let isNullable: Bool
    let columnSlot: Int?
    let children: [CanonicalBoundExpression]
    let occurrence: QueryOccurrenceID?
    let subqueryBinding: CanonicalBoundSelect?
    let castContext: CanonicalQueryTypeBinder.CastContext
    let sourceAlignment: Bool
    let wholeRow: CanonicalBoundWholeRow?
    let fieldOrdinal: Int?
    let comparisonMode: CanonicalSQLCompositeComparisonMode
    let collation: SQLCollation?
    let subqueryComparisonTypes: [QueryColumnType]
    let bindingMetadata: CanonicalBindingMetadata?
    let constructionLease: DatabaseQueryPayloadLease?
    let metadataLease: DatabaseQueryPayloadLease?

    init(
        expression: DatabaseKit.Expression,
        type: QueryColumnType,
        isNullable: Bool,
        columnSlot: Int? = nil,
        children: [CanonicalBoundExpression] = [],
        occurrence: QueryOccurrenceID? = nil,
        subqueryBinding: CanonicalBoundSelect? = nil,
        castContext: CanonicalQueryTypeBinder.CastContext = .explicit,
        sourceAlignment: Bool = false,
        wholeRow: CanonicalBoundWholeRow? = nil,
        fieldOrdinal: Int? = nil,
        comparisonMode: CanonicalSQLCompositeComparisonMode = .record,
        collation: SQLCollation? = nil,
        subqueryComparisonTypes: [QueryColumnType] = [],
        metadataLease: DatabaseQueryPayloadLease? = nil,
        constructionLease: DatabaseQueryPayloadLease? = nil,
        bindingMetadata: CanonicalBindingMetadata? = nil
    ) {
        self.expression = expression
        self.type = type
        self.isNullable = isNullable
        self.columnSlot = columnSlot
        self.children = children
        self.occurrence = occurrence
        self.subqueryBinding = subqueryBinding
        self.castContext = castContext
        self.sourceAlignment = sourceAlignment
        self.wholeRow = wholeRow
        self.fieldOrdinal = fieldOrdinal
        self.comparisonMode = comparisonMode
        self.collation = collation
        self.subqueryComparisonTypes = subqueryComparisonTypes
        self.metadataLease = metadataLease
        self.constructionLease = constructionLease
        self.bindingMetadata = bindingMetadata
    }

    /// Normalize value-dependent primitive evaluator representations to the
    /// checked signature before publishing a positional cell.
    func normalize(_ value: QueryValue) throws -> QueryValue {
        guard isNullable || !value.isNull else {
            throw DatabaseExpressionEvaluationError.typeMismatch(
                operation: "nonnullable bound expression"
            )
        }
        let permitsFloatingRounding: Bool
        if case .cast = expression { permitsFloatingRounding = true }
        else { permitsFloatingRounding = false }
        return try CanonicalQueryTypeBinder.normalize(
            value, to: type, permitsFloatingRounding: permitsFloatingRounding
        )
    }

    /// Substitutes admitted positional cells without evaluating an unselected
    /// branch. Aggregate and subquery owners retain their own input frames.
    func materializing(in values: [QueryValue]) throws -> DatabaseKit.Expression {
        try materializing(in: Optional(values))
    }

    var loweredExpression: DatabaseKit.Expression {
        get throws { try materializing(in: nil) }
    }

    private func materializing(in values: [QueryValue]?) throws -> DatabaseKit.Expression {
        if let columnSlot, let values {
            guard values.indices.contains(columnSlot) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound column slot outside row")
            }
            return .literal(.queryValue(values[columnSlot], type: type))
        }
        func child(_ index: Int) throws -> DatabaseKit.Expression {
            guard children.indices.contains(index) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound expression child count")
            }
            return try children[index].materializing(in: values)
        }
        let result: DatabaseKit.Expression
        switch expression {
        case .row: result = try .row(children.indices.map(child))
        case .field(_, let name): result = try .field(child(0), name: name)
        case .fieldExpansion: result = try .fieldExpansion(child(0))
        case .collate(_, let collation): result = try .collate(child(0), collation: collation)
        case .array: result = try .array(children.indices.map(child))
        case .arrayAccess(_, let subscripts):
            var index = 1
            func next() throws -> DatabaseKit.Expression { defer { index += 1 }; return try child(index) }
            let selected = try subscripts.map { dimension -> ArraySubscript in
                switch dimension {
                case .index: return try .index(next())
                case .slice(let lower, let upper): return try .slice(lower: lower == nil ? nil : next(), upper: upper == nil ? nil : next())
                }
            }
            result = try .arrayAccess(child(0), subscripts: selected)
        case .collectionOperation(let operation, _, _): result = try .collectionOperation(operation, left: child(0), right: child(1))
        case .greatest: result = try .greatest(children.indices.map(child))
        case .least: result = try .least(children.indices.map(child))
        case .truthTest(_, let value, let negated): result = try .truthTest(child(0), value: value, negated: negated)
        case .isDistinctFrom(_, _, let negated): result = try .isDistinctFrom(child(0), child(1), negated: negated)
        case .quantifiedComparison(_, let comparison, let quantifier, let source):
            let lowered: QuantifiedComparisonSource
            switch source {
            case .array: lowered = try .array(child(1))
            case .subquery(let query): lowered = .subquery(query)
            }
            result = try .quantifiedComparison(child(0), comparison: comparison, quantifier: quantifier, source: lowered)
        case .concatenate: result = try .concatenate(child(0), child(1))
        case .add: result = try .add(child(0), child(1))
        case .subtract: result = try .subtract(child(0), child(1))
        case .multiply: result = try .multiply(child(0), child(1))
        case .divide: result = try .divide(child(0), child(1))
        case .modulo: result = try .modulo(child(0), child(1))
        case .equal: result = try .equal(child(0), child(1))
        case .notEqual: result = try .notEqual(child(0), child(1))
        case .lessThan: result = try .lessThan(child(0), child(1))
        case .lessThanOrEqual: result = try .lessThanOrEqual(child(0), child(1))
        case .greaterThan: result = try .greaterThan(child(0), child(1))
        case .greaterThanOrEqual: result = try .greaterThanOrEqual(child(0), child(1))
        case .and: result = try .and(child(0), child(1))
        case .or: result = try .or(child(0), child(1))
        case .negate: result = try .negate(child(0))
        case .not: result = try .not(child(0))
        case .isNull: result = try .isNull(child(0))
        case .isNotNull: result = try .isNotNull(child(0))
        case .patternMatch(_, _, let escape, let kind):
            result = try .patternMatch(subject: child(0), pattern: child(1),
                escape: escape == nil ? nil : child(2), kind: kind)
        case .between: result = try .between(child(0), low: child(1), high: child(2))
        case .inList: result = try .inList(child(0), values: children.indices.dropFirst().map(child))
        case .notInList: result = try .notInList(child(0), values: children.indices.dropFirst().map(child))
        case .caseWhen(let operand, let pairs, let fallback):
            let offset = operand == nil ? 0 : 1
            let resultsOffset = offset + pairs.count
            result = try .caseWhen(operand: operand == nil ? nil : child(0), cases: pairs.indices.map { index in
                let condition: DatabaseKit.Expression
                if operand != nil {
                    let comparison = children[offset + index]
                    guard case .equal = comparison.expression, comparison.children.count == 2 else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "simple CASE selected equality")
                    }
                    condition = try comparison.children[1].materializing(in: values)
                } else { condition = try child(offset + index) }
                return CaseWhenPair(condition: condition, result: try child(resultsOffset + index))
            }, elseResult: fallback == nil ? nil : child(resultsOffset + pairs.count))
        case .coalesce: result = try .coalesce(children.indices.map(child))
        case .nullIf: result = try .nullIf(child(0), child(1))
        case .cast(_, let target): result = try .cast(child(0), targetType: target)
        case .function(let function):
            result = try .function(FunctionCall(name: function.name, arguments: children.indices.map(child),
                                               distinct: function.distinct, schema: function.schema,
                                               finalArrayIsVariadic: function.finalArrayIsVariadic))
        case .inSubquery(_, let query): result = try .inSubquery(child(0), subquery: query)
        case .aggregate(let aggregate):
            func argument() throws -> DatabaseKit.Expression {
                guard let first = children.first else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "bound aggregate argument")
                }
                return try first.loweredExpression
            }
            let lowered: AggregateFunction
            switch aggregate {
            case .count(let operand, let distinct): lowered = try .count(operand == nil ? nil : argument(), distinct: distinct)
            case .sum(_, let distinct): lowered = try .sum(argument(), distinct: distinct)
            case .avg(_, let distinct): lowered = try .avg(argument(), distinct: distinct)
            case .min: lowered = try .min(argument())
            case .max: lowered = try .max(argument())
            case .sample: lowered = try .sample(argument())
            case .groupConcat(_, let separator, let distinct):
                lowered = try .groupConcat(argument(), separator: separator, distinct: distinct)
            case .arrayAgg(_, let ordering, let distinct):
                let keys = try ordering.map { keys in
                    try keys.enumerated().map { index, key in
                        SortKey(try children[index + 1].loweredExpression, direction: key.direction, nulls: key.nulls)
                    }
                }
                lowered = try .arrayAgg(argument(), orderBy: keys, distinct: distinct)
            }
            result = .aggregate(lowered)
        case .currentValue, .subquery, .exists, .literal, .parameter, .column,
.arraySubquery, .wholeRow, .variable, .bound, .regex, .triple, .isTriple, .subject, .predicate, .object:
            result = expression
        }
        // The primitive kernels can return wider carriers than their selected
        // signatures. A lazy CAST enforces the signature at each operation.
        switch expression {
        case .add, .subtract, .multiply, .divide, .modulo, .negate,
             .function, .caseWhen, .coalesce, .nullIf:
            switch type {
            case .sql(let target):
                switch target {
                case .smallint, .integer, .bigint, .real, .doublePrecision, .decimal:
                    return .cast(result, targetType: target)
                default: break
                }
            case .primitive(let kind, false):
                let target: DataType?
                switch kind {
                case .int16: target = .smallint
                case .int32: target = .integer
                case .int64: target = .bigint
                case .float32: target = .real
                case .float64: target = .doublePrecision
                default: target = nil
                }
                if let target { return .cast(result, targetType: target) }
            default: break
            }
        default: break
        }
        return result
    }
}
