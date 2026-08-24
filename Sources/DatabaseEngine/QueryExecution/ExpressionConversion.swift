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
        case .not:
            // Predicate<T> has two-valued Boolean semantics. SQL NOT must
            // preserve UNKNOWN for nullable operands, so it is evaluated by
            // the canonical three-valued expression evaluator instead.
            return nil

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
                let value = try literal.toFieldValue()
                // `x NOT IN (..., NULL)` is UNKNOWN when no non-null member
                // matches. Predicate<T>.notIn would return true instead.
                guard !value.isNull else { return nil }
                collected.append(value)
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
        // Typed field comparisons order NULL as a canonical value, whereas SQL
        // comparisons with NULL yield UNKNOWN. Keep these expressions residual.
        guard !fieldValue.isNull else { return nil }
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
        guard let number = fieldNumber(for: name), number > 0 else {
            return nil
        }
        return FieldIdentity(name: name, number: number)
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
        for type: T.Type,
        sourceQualifier: String
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
            guard conjunct.referencesOnlySourceQualifier(sourceQualifier) else {
                residual.append(conjunct)
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

    private func referencesOnlySourceQualifier(_ sourceQualifier: String) -> Bool {
        switch self {
        case .column(let column):
            return column.table == nil || column.table == sourceQualifier
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs),
                .and(let lhs, let rhs), .or(let lhs, let rhs),
                .nullIf(let lhs, let rhs):
            return lhs.referencesOnlySourceQualifier(sourceQualifier)
                && rhs.referencesOnlySourceQualifier(sourceQualifier)
        case .negate(let operand), .not(let operand),
                .isNull(let operand), .isNotNull(let operand),
                .like(let operand, _), .regex(let operand, _, _),
                .cast(let operand, _), .isTriple(let operand),
                .subject(let operand), .predicate(let operand),
                .object(let operand):
            return operand.referencesOnlySourceQualifier(sourceQualifier)
        case .between(let operand, let lower, let upper):
            return operand.referencesOnlySourceQualifier(sourceQualifier)
                && lower.referencesOnlySourceQualifier(sourceQualifier)
                && upper.referencesOnlySourceQualifier(sourceQualifier)
        case .inList(let operand, let values),
                .notInList(let operand, let values):
            return operand.referencesOnlySourceQualifier(sourceQualifier)
                && values.allSatisfy {
                    $0.referencesOnlySourceQualifier(sourceQualifier)
                }
        case .inSubquery, .subquery, .exists:
            return false
        case .aggregate(let aggregate):
            switch aggregate {
            case .count(let expression, _):
                return expression?.referencesOnlySourceQualifier(
                    sourceQualifier
                ) ?? true
            case .sum(let expression, _), .avg(let expression, _),
                    .min(let expression), .max(let expression),
                    .groupConcat(let expression, _, _),
                    .sample(let expression):
                return expression.referencesOnlySourceQualifier(
                    sourceQualifier
                )
            case .arrayAgg(let expression, let orderBy, _):
                return expression.referencesOnlySourceQualifier(
                    sourceQualifier
                ) && (orderBy ?? []).allSatisfy {
                    $0.expression.referencesOnlySourceQualifier(
                        sourceQualifier
                    )
                }
            }
        case .function(let function):
            return function.arguments.allSatisfy {
                $0.referencesOnlySourceQualifier(sourceQualifier)
            }
        case .caseWhen(let cases, let elseResult):
            return cases.allSatisfy {
                $0.condition.referencesOnlySourceQualifier(sourceQualifier)
                    && $0.result.referencesOnlySourceQualifier(sourceQualifier)
            } && (elseResult?.referencesOnlySourceQualifier(sourceQualifier)
                ?? true)
        case .coalesce(let expressions):
            return expressions.allSatisfy {
                $0.referencesOnlySourceQualifier(sourceQualifier)
            }
        case .triple(let subject, let predicate, let object):
            return subject.referencesOnlySourceQualifier(sourceQualifier)
                && predicate.referencesOnlySourceQualifier(sourceQualifier)
                && object.referencesOnlySourceQualifier(sourceQualifier)
        case .literal, .variable, .parameter, .bound:
            return true
        }
    }
}
