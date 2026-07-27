// ExpressionConversion.swift
// DatabaseEngine - Conversion between Predicate<T> and Expression

import DatabaseKit
import DatabaseTypes

// MARK: - Predicate<T> → Expression

extension Predicate {
    /// Convert a type-safe Predicate to a type-erased QueryIR Expression.
    ///
/// The result is serializable, inspectable, and suitable for query planning
    /// or caching. Values without a QueryIR literal representation fail with a
    /// typed error.
    ///
    /// - Note: The original zero-copy evaluation closures are NOT preserved in the IR.
    ///   Use the original Predicate for in-process evaluation.
    public func toExpression() throws(LiteralConversionError) -> Expression {
        switch self {
        case .comparison(let fc):
            return try fc.toExpression()
        case .and(let predicates):
            guard let first = predicates.first else {
                return .literal(.bool(true))
            }
            var expression = try first.toExpression()
            for predicate in predicates.dropFirst() {
                expression = .and(expression, try predicate.toExpression())
            }
            return expression
        case .or(let predicates):
            guard let first = predicates.first else {
                return .literal(.bool(false))
            }
            var expression = try first.toExpression()
            for predicate in predicates.dropFirst() {
                expression = .or(expression, try predicate.toExpression())
            }
            return expression
        case .not(let predicate):
            return .not(try predicate.toExpression())
        case .true:
            return .literal(.bool(true))
        case .false:
            return .literal(.bool(false))
        }
    }
}

// MARK: - FieldComparison<T> → Expression

extension FieldComparison {
    /// Convert a FieldComparison to a QueryIR Expression.
    ///
    /// Uses the compiled field identity and the comparison operator
    /// to construct a column-based expression.
    public func toExpression() throws(LiteralConversionError) -> Expression {
        try op.toExpression(column: fieldName, value: value)
    }
}

// MARK: - Expression → Predicate<T> (Reverse: partial)

extension Expression {
    /// Attempt to convert a QueryIR Expression back to a type-safe Predicate.
    ///
    /// Returns `nil` for patterns that cannot be represented as a Predicate:
    /// - Subqueries, EXISTS, aggregate functions
    /// - Function calls (CONTAINS, STRSTARTS, etc.)
    /// - Variables, triple patterns, CAST, CASE WHEN
    /// - Arithmetic expressions used as boolean
    /// - Column names that don't match any field in the target type
    ///
    /// Successfully converted predicates retain the exact compiled field
    /// identity and use the generated `Persistable` field traversal.
    public func toPredicate<T: Persistable>(
        for type: T.Type
    ) throws(LiteralConversionError) -> Predicate<T>? {
        switch self {
        // Comparison: column op literal
        case .equal(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .equal)
        case .notEqual(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .notEqual)
        case .lessThan(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .lessThan)
        case .lessThanOrEqual(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .lessThanOrEqual)
        case .greaterThan(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .greaterThan)
        case .greaterThanOrEqual(let lhs, let rhs):
            return try columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .greaterThanOrEqual)

        // Logical operators
        case .and(let lhs, let rhs):
            guard let left: Predicate<T> = try lhs.toPredicate(for: type),
                  let right: Predicate<T> = try rhs.toPredicate(for: type) else { return nil }
            return .and([left, right])
        case .or(let lhs, let rhs):
            guard let left: Predicate<T> = try lhs.toPredicate(for: type),
                  let right: Predicate<T> = try rhs.toPredicate(for: type) else { return nil }
            return .or([left, right])
        case .not(let inner):
            guard let pred: Predicate<T> = try inner.toPredicate(for: type) else { return nil }
            return .not(pred)

        // Null checks
        case .isNull(.column(let col)):
            guard let field = T.persistedFieldIdentity(
                named: col.column
            ) else { return nil }
            return .comparison(FieldComparison<T>(
                field: field,
                op: .isNil,
                value: .null
            ))
        case .isNotNull(.column(let col)):
            guard let field = T.persistedFieldIdentity(
                named: col.column
            ) else { return nil }
            return .comparison(FieldComparison<T>(
                field: field,
                op: .isNotNil,
                value: .null
            ))

        // IN list
        case .inList(.column(let col), let values):
            guard let field = T.persistedFieldIdentity(
                named: col.column
            ) else { return nil }
            var collected: [FieldValue] = []
            for v in values {
                guard case .literal(let literal) = v else { return nil }
                collected.append(try literal.toFieldValue())
            }
            return .comparison(FieldComparison<T>(
                field: field,
                op: .in,
                value: .array(collected)
            ))

        // NOT IN list
        case .notInList(.column(let col), let values):
            guard let field = T.persistedFieldIdentity(
                named: col.column
            ) else { return nil }
            var collected: [FieldValue] = []
            for v in values {
                guard case .literal(let literal) = v else { return nil }
                collected.append(try literal.toFieldValue())
            }
            return .comparison(FieldComparison<T>(
                field: field,
                op: .notIn,
                value: .array(collected)
            ))

        // Boolean literals
        case .literal(.bool(true)):
            return .true
        case .literal(.bool(false)):
            return .false

        // Unsupported patterns
        default:
            return nil
        }
    }

    /// Extract column-op-literal pattern into a FieldComparison-based Predicate.
    ///
    /// Resolves the QueryIR column to one exact compiled schema identity.
    private func columnLiteralPredicate<T: Persistable>(
        lhs: Expression,
        rhs: Expression,
        op: ComparisonOperator
    ) throws(LiteralConversionError) -> Predicate<T>? {
        guard case .column(let col) = lhs,
              case .literal(let literal) = rhs else { return nil }
        let fieldValue = try literal.toFieldValue()
        guard let field = T.persistedFieldIdentity(
            named: col.column
        ) else { return nil }
        return .comparison(FieldComparison<T>(
            field: field,
            op: op,
            value: fieldValue
        ))
    }
}

private extension Persistable {
    static func persistedFieldIdentity(named name: String) -> FieldIdentity? {
        for schema in fieldSchemas where schema.name == name && schema.fieldNumber > 0 {
            return FieldIdentity(name: schema.name, number: schema.fieldNumber)
        }
        return nil
    }
}

// MARK: - Array Helper

extension Array where Element == Expression {
    /// Reduce an array of expressions with a binary combinator.
    /// Returns `.literal(.bool(true))` for empty arrays, single element for count == 1.
    func reduceExpressions(with combine: (Expression, Expression) -> Expression) -> Expression {
        guard let first = self.first else {
            return .literal(.bool(true))
        }
        return self.dropFirst().reduce(first, combine)
    }
}

// MARK: - Partial AND Pushdown

/// Result of splitting a top-level conjunction into push-down and residual parts.
///
/// Each top-level AND conjunct is attempted for conversion to `Predicate<T>`:
/// conjuncts that convert are returned in `pushed`; the remainder are returned in
/// `residual` so the caller can evaluate them in-memory over the fetched rows.
struct SplitAndResult<T: Persistable>: Sendable {
    /// Conjuncts successfully converted to typed predicates (push-down candidates).
    let pushed: [Predicate<T>]
    /// Conjuncts that could not be converted; must be evaluated as residual.
    let residual: [Expression]
}

extension Expression {
    /// Split this expression into pushable typed predicates and residual conjuncts.
    ///
    /// Flattens the top-level AND structure and attempts `toPredicate(for:)` on each
    /// conjunct. Successful conversions are returned as `Predicate<T>`; failures are
    /// returned as `Expression` for residual in-memory evaluation. This enables
    /// partial push-down: the convertible part of `A AND B AND C` engages the typed
    /// fetch path (index selection, range scans) even when one conjunct cannot be
    /// represented as a typed predicate.
    /// Canonical literals that cannot be represented by `FieldValue` are errors,
    /// rather than being mislabeled as unsupported expression syntax.
    func splitAnd<T: Persistable>(
        for type: T.Type
    ) throws(LiteralConversionError) -> SplitAndResult<T> {
        var pushed: [Predicate<T>] = []
        var residual: [Expression] = []
        var pending: [Expression] = [self]
        while let conjunct = pending.popLast() {
            if case .and(let lhs, let rhs) = conjunct {
                pending.append(rhs)
                pending.append(lhs)
                continue
            }
            if let predicate: Predicate<T> = try conjunct.toPredicate(for: type) {
                pushed.append(predicate)
            } else {
                residual.append(conjunct)
            }
        }
        return SplitAndResult(pushed: pushed, residual: residual)
    }
}
