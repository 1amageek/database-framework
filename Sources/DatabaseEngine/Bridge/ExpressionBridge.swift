// ExpressionBridge.swift
// DatabaseEngine - Bridge between Predicate<T> and QueryIR.Expression

import Foundation
import Core
import QueryIR

// MARK: - Predicate<T> → QueryIR.Expression (Forward: always succeeds)

extension Predicate {
    /// Convert a type-safe Predicate to a type-erased QueryIR Expression.
    ///
    /// This conversion always succeeds. All Predicate cases have direct QueryIR equivalents.
    /// The result is serializable, inspectable, and suitable for query planning or caching.
    ///
    /// - Note: The original zero-copy evaluation closures are NOT preserved in the IR.
    ///   Use the original Predicate for in-process evaluation.
    public func toExpression() -> QueryIR.Expression {
        switch self {
        case .comparison(let fc):
            return fc.toExpression()
        case .and(let predicates):
            return predicates.map { $0.toExpression() }
                .reduceExpressions(with: { .and($0, $1) })
        case .or(let predicates):
            return predicates.map { $0.toExpression() }
                .reduceExpressions(with: { .or($0, $1) })
        case .not(let predicate):
            return .not(predicate.toExpression())
        case .true:
            return .literal(.bool(true))
        case .false:
            return .literal(.bool(false))
        }
    }
}

// MARK: - FieldComparison<T> → QueryIR.Expression

extension FieldComparison {
    /// Convert a FieldComparison to a QueryIR Expression.
    ///
    /// Uses the field name (derived from the KeyPath) and the comparison operator
    /// to construct a column-based expression.
    public func toExpression() -> QueryIR.Expression {
        op.toExpression(column: fieldName, value: value)
    }
}

// MARK: - QueryIR.Expression → Predicate<T> (Reverse: partial)

extension QueryIR.Expression {
    /// Attempt to convert a QueryIR Expression back to a type-safe Predicate.
    ///
    /// Returns `nil` for patterns that cannot be represented as a Predicate:
    /// - Subqueries, EXISTS, aggregate functions
    /// - Function calls (CONTAINS, STRSTARTS, etc.)
    /// - Variables, triple patterns, CAST, CASE WHEN
    /// - Arithmetic expressions used as boolean
    /// - Column names that don't match any field in the target type
    ///
    /// Successfully converted predicates use `FieldReader`-based evaluation
    /// (via `dynamicMember` subscript). They do NOT have zero-copy KeyPath closures.
    public func toPredicate<T: Persistable>(for type: T.Type) -> Predicate<T>? {
        switch self {
        // Comparison: column op literal
        case .equal(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .equal)
        case .notEqual(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .notEqual)
        case .lessThan(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .lessThan)
        case .lessThanOrEqual(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .lessThanOrEqual)
        case .greaterThan(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .greaterThan)
        case .greaterThanOrEqual(let lhs, let rhs):
            return columnLiteralPredicate(lhs: lhs, rhs: rhs, op: .greaterThanOrEqual)

        // Logical operators
        case .and(let lhs, let rhs):
            guard let left: Predicate<T> = lhs.toPredicate(for: type),
                  let right: Predicate<T> = rhs.toPredicate(for: type) else { return nil }
            return .and([left, right])
        case .or(let lhs, let rhs):
            guard let left: Predicate<T> = lhs.toPredicate(for: type),
                  let right: Predicate<T> = rhs.toPredicate(for: type) else { return nil }
            return .or([left, right])
        case .not(let inner):
            guard let pred: Predicate<T> = inner.toPredicate(for: type) else { return nil }
            return .not(pred)

        // Null checks
        case .isNull(.column(let col)):
            guard T.allFields.contains(col.column) else { return nil }
            let fieldName = col.column
            return .comparison(FieldComparison<T>(
                keyPath: \T.id as AnyKeyPath,  // placeholder — evaluate closure bypasses it
                op: .isNil,
                value: .null,
                evaluate: { model in
                    FieldReader.readFieldValue(from: model, fieldName: fieldName) == .null
                }
            ))
        case .isNotNull(.column(let col)):
            guard T.allFields.contains(col.column) else { return nil }
            let fieldName = col.column
            return .comparison(FieldComparison<T>(
                keyPath: \T.id as AnyKeyPath,
                op: .isNotNil,
                value: .null,
                evaluate: { model in
                    FieldReader.readFieldValue(from: model, fieldName: fieldName) != .null
                }
            ))

        // IN list
        case .inList(.column(let col), let values):
            guard T.allFields.contains(col.column) else { return nil }
            var collected: [FieldValue] = []
            for v in values {
                guard case .literal(let lit) = v,
                      let fv = lit.toFieldValue() else { return nil }
                collected.append(fv)
            }
            let fieldValues = collected  // immutable copy for Sendable capture
            let arrayValue = FieldValue.array(fieldValues)
            let fieldName = col.column
            return .comparison(FieldComparison<T>(
                keyPath: \T.id as AnyKeyPath,
                op: .in,
                value: arrayValue,
                evaluate: { model in
                    let modelValue = FieldReader.readFieldValue(from: model, fieldName: fieldName)
                    return fieldValues.contains { modelValue.isEqual(to: $0) }
                }
            ))

        // NOT IN list
        case .notInList(.column(let col), let values):
            guard T.allFields.contains(col.column) else { return nil }
            var collected: [FieldValue] = []
            for v in values {
                guard case .literal(let lit) = v,
                      let fv = lit.toFieldValue() else { return nil }
                collected.append(fv)
            }
            let fieldValues = collected
            let arrayValue = FieldValue.array(fieldValues)
            let fieldName = col.column
            return .comparison(FieldComparison<T>(
                keyPath: \T.id as AnyKeyPath,
                op: .in,
                value: arrayValue,
                evaluate: { model in
                    let modelValue = FieldReader.readFieldValue(from: model, fieldName: fieldName)
                    return !fieldValues.contains { modelValue.isEqual(to: $0) }
                }
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
    /// Uses FieldReader for evaluation since KeyPath resolution from field names
    /// is not available on Persistable. The `keyPath` in FieldComparison is set
    /// to a placeholder; the custom evaluate closure handles actual evaluation.
    private func columnLiteralPredicate<T: Persistable>(
        lhs: QueryIR.Expression,
        rhs: QueryIR.Expression,
        op: ComparisonOperator
    ) -> Predicate<T>? {
        guard case .column(let col) = lhs,
              case .literal(let lit) = rhs,
              let fv = lit.toFieldValue() else { return nil }
        let fieldName = col.column
        guard T.allFields.contains(fieldName) else { return nil }
        return .comparison(FieldComparison<T>(
            keyPath: \T.id as AnyKeyPath,  // placeholder — evaluate closure bypasses it
            op: op,
            value: fv,
            evaluate: ExpressionBridgeEvaluator.makeEvaluator(
                fieldName: fieldName,
                op: op,
                value: fv
            )
        ))
    }
}

// MARK: - Evaluation Closure Builder

/// Builds `@Sendable` evaluation closures for FieldReader-based comparison.
///
/// Used by the reverse bridge (Expression → Predicate) where typed KeyPaths
/// are not available. All closures use `FieldReader.readFieldValue` for field access
/// and `FieldValue` comparison methods for type-safe evaluation.
enum ExpressionBridgeEvaluator {
    static func makeEvaluator<T: Persistable>(
        fieldName: String,
        op: ComparisonOperator,
        value: FieldValue
    ) -> @Sendable (T) -> Bool {
        { model in
            let modelValue = FieldReader.readFieldValue(from: model, fieldName: fieldName)

            switch op {
            case .isNil:
                return modelValue == .null
            case .isNotNil:
                return modelValue != .null
            default:
                break
            }

            if modelValue == .null { return false }

            switch op {
            case .equal:
                return modelValue.isEqual(to: value)
            case .notEqual:
                return !modelValue.isEqual(to: value)
            case .lessThan:
                return modelValue.isLessThan(value)
            case .lessThanOrEqual:
                return modelValue.isLessThan(value) || modelValue.isEqual(to: value)
            case .greaterThan:
                return value.isLessThan(modelValue)
            case .greaterThanOrEqual:
                return value.isLessThan(modelValue) || modelValue.isEqual(to: value)
            case .contains:
                if let str = FieldReader.read(from: model, fieldName: fieldName) as? String,
                   let substr = value.stringValue {
                    return str.contains(substr)
                }
                return false
            case .hasPrefix:
                if let str = FieldReader.read(from: model, fieldName: fieldName) as? String,
                   let prefix = value.stringValue {
                    return str.hasPrefix(prefix)
                }
                return false
            case .hasSuffix:
                if let str = FieldReader.read(from: model, fieldName: fieldName) as? String,
                   let suffix = value.stringValue {
                    return str.hasSuffix(suffix)
                }
                return false
            case .in:
                if let arrayValues = value.arrayValue {
                    return arrayValues.contains { modelValue.isEqual(to: $0) }
                }
                return false
            case .isNil, .isNotNil:
                return false
            }
        }
    }
}

// MARK: - Array Helper

extension Array where Element == QueryIR.Expression {
    /// Reduce an array of expressions with a binary combinator.
    /// Returns `.literal(.bool(true))` for empty arrays, single element for count == 1.
    func reduceExpressions(with combine: (QueryIR.Expression, QueryIR.Expression) -> QueryIR.Expression) -> QueryIR.Expression {
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
    let residual: [QueryIR.Expression]
}

extension QueryIR.Expression {
    /// Return the top-level AND conjuncts of this expression in left-to-right order.
    ///
    /// `AND(a, AND(b, c))` → `[a, b, c]`. Non-AND expressions return `[self]`.
    func flattenAnd() -> [QueryIR.Expression] {
        if case .and(let lhs, let rhs) = self {
            return lhs.flattenAnd() + rhs.flattenAnd()
        }
        return [self]
    }

    /// Split this expression into pushable typed predicates and residual conjuncts.
    ///
    /// Flattens the top-level AND structure and attempts `toPredicate(for:)` on each
    /// conjunct. Successful conversions are returned as `Predicate<T>`; failures are
    /// returned as `QueryIR.Expression` for residual in-memory evaluation. This enables
    /// partial push-down: the convertible part of `A AND B AND C` engages the typed
    /// fetch path (index selection, range scans) even when one conjunct cannot be
    /// represented as a typed predicate.
    func splitAnd<T: Persistable>(for type: T.Type) -> SplitAndResult<T> {
        var pushed: [Predicate<T>] = []
        var residual: [QueryIR.Expression] = []
        for conjunct in flattenAnd() {
            if let predicate: Predicate<T> = conjunct.toPredicate(for: type) {
                pushed.append(predicate)
            } else {
                residual.append(conjunct)
            }
        }
        return SplitAndResult(pushed: pushed, residual: residual)
    }
}
