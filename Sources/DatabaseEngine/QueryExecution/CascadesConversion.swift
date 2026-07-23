// CascadesConversion.swift
// DatabaseEngine - Conversion between Cascades optimizer types and QueryIR

import Core
import QueryIR

// MARK: - PredicateExpr → QueryIR.Expression

extension PredicateExpr {
    /// Convert a Cascades PredicateExpr to a QueryIR Expression.
    ///
    /// Every valid PredicateExpr has a direct QueryIR equivalent.
    ///
    /// A LIKE predicate whose value is not a string is rejected rather than
    /// silently substituting a different pattern.
    public func toExpression() throws(CascadesConversionError) -> QueryIR.Expression {
        switch self {
        case .comparison(let field, let op, let value):
            let col = QueryIR.Expression.column(QueryIR.ColumnRef(column: field))
            let lit = QueryIR.Expression.literal(value.toLiteral())
            switch op {
            case .eq: return .equal(col, lit)
            case .ne: return .notEqual(col, lit)
            case .lt: return .lessThan(col, lit)
            case .le: return .lessThanOrEqual(col, lit)
            case .gt: return .greaterThan(col, lit)
            case .ge: return .greaterThanOrEqual(col, lit)
            case .like:
                guard case .string(let pattern) = value else {
                    throw .likePatternRequiresString(field: field)
                }
                return .like(col, pattern: pattern)
            case .ilike:
                guard case .string = value else {
                    throw .ilikePatternRequiresString(field: field)
                }
                return .function(QueryIR.FunctionCall(
                    name: "ILIKE",
                    arguments: [col, lit]
                ))
            case .in:
                if case .array(let elements) = value {
                    return .inList(col, values: elements.map { .literal($0.toLiteral()) })
                }
                return .inList(col, values: [lit])
            }
        case .and(let exprs):
            guard let first = exprs.first else {
                return .literal(.bool(true))
            }
            var result = try first.toExpression()
            for expression in exprs.dropFirst() {
                result = .and(result, try expression.toExpression())
            }
            return result
        case .or(let exprs):
            guard let first = exprs.first else {
                return .literal(.bool(false))
            }
            var result = try first.toExpression()
            for expression in exprs.dropFirst() {
                result = .or(result, try expression.toExpression())
            }
            return result
        case .not(let inner):
            return .not(try inner.toExpression())
        case .isNull(let field):
            return .isNull(.column(QueryIR.ColumnRef(column: field)))
        case .isNotNull(let field):
            return .isNotNull(.column(QueryIR.ColumnRef(column: field)))
        case .true:
            return .literal(.bool(true))
        case .false:
            return .literal(.bool(false))
        }
    }
}

// MARK: - QueryIR.Expression → PredicateExpr (Reverse: partial)

extension PredicateExpr {
    /// Attempt to create a PredicateExpr from a QueryIR Expression.
    ///
    /// Returns `nil` for expression patterns not representable as PredicateExpr:
    /// - Variables, subqueries, aggregates, functions (except ILIKE)
    /// - BETWEEN, CASE WHEN, CAST, COALESCE, NULLIF
    /// - Triple patterns, property paths, EXISTS
    ///
    /// Throws when a recognized pattern contains a canonical literal that the
    /// narrower `FieldValue` optimizer model cannot represent exactly.
    public init?(
        _ expression: QueryIR.Expression
    ) throws(LiteralConversionError) {
        switch expression {
        // Comparison: column op literal
        case .equal(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .eq) else { return nil }
            self = result
        case .notEqual(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .ne) else { return nil }
            self = result
        case .lessThan(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .lt) else { return nil }
            self = result
        case .lessThanOrEqual(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .le) else { return nil }
            self = result
        case .greaterThan(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .gt) else { return nil }
            self = result
        case .greaterThanOrEqual(let lhs, let rhs):
            guard let result = try Self.extractComparison(lhs: lhs, rhs: rhs, op: .ge) else { return nil }
            self = result

        // LIKE
        case .like(.column(let col), let pattern):
            self = .comparison(field: col.column, op: .like, value: .string(pattern))

        // IN list
        case .inList(.column(let col), let values):
            var fieldValues: [FieldValue] = []
            for v in values {
                guard case .literal(let literal) = v else { return nil }
                fieldValues.append(try literal.toFieldValue())
            }
            self = .comparison(field: col.column, op: .in, value: .array(fieldValues))

        // NOT IN list
        case .notInList(.column(let col), let values):
            var fieldValues: [FieldValue] = []
            for v in values {
                guard case .literal(let literal) = v else { return nil }
                fieldValues.append(try literal.toFieldValue())
            }
            self = .not(.comparison(field: col.column, op: .in, value: .array(fieldValues)))

        // Logical
        case .and(let lhs, let rhs):
            guard let left = try PredicateExpr(lhs),
                  let right = try PredicateExpr(rhs) else { return nil }
            self = .and([left, right])
        case .or(let lhs, let rhs):
            guard let left = try PredicateExpr(lhs),
                  let right = try PredicateExpr(rhs) else { return nil }
            self = .or([left, right])
        case .not(let inner):
            guard let pred = try PredicateExpr(inner) else { return nil }
            self = .not(pred)

        // Null checks
        case .isNull(.column(let col)):
            self = .isNull(field: col.column)
        case .isNotNull(.column(let col)):
            self = .isNotNull(field: col.column)

        // Boolean literals
        case .literal(.bool(true)):
            self = .true
        case .literal(.bool(false)):
            self = .false

        default:
            return nil
        }
    }

    /// Extract column-op-literal pattern.
    private static func extractComparison(
        lhs: QueryIR.Expression,
        rhs: QueryIR.Expression,
        op: ComparisonOp
    ) throws(LiteralConversionError) -> PredicateExpr? {
        guard case .column(let col) = lhs,
              case .literal(let literal) = rhs else { return nil }
        return .comparison(
            field: col.column,
            op: op,
            value: try literal.toFieldValue()
        )
    }
}

// MARK: - SortKeyExpr → QueryIR.SortKey

extension SortKeyExpr {
    /// Convert a Cascades SortKeyExpr to a QueryIR SortKey.
    public func toSortKey() -> QueryIR.SortKey {
        QueryIR.SortKey(
            .column(QueryIR.ColumnRef(column: field)),
            direction: ascending ? .ascending : .descending,
            nulls: nullsFirst ? .first : .last
        )
    }
}

// MARK: - QueryIR.SortKey → SortKeyExpr (Reverse: partial)

extension SortKeyExpr {
    /// Attempt to create a SortKeyExpr from a QueryIR SortKey.
    ///
    /// Returns `nil` if the sort key's expression is not a simple column reference.
    public init?(_ sortKey: QueryIR.SortKey) {
        guard case .column(let col) = sortKey.expression else { return nil }
        self.init(
            field: col.column,
            ascending: sortKey.direction == .ascending,
            nullsFirst: sortKey.nulls == .first
        )
    }
}
