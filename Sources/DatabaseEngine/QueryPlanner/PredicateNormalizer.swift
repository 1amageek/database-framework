// PredicateNormalizer.swift
// QueryPlanner - Predicate normalization (CNF/DNF conversion)

import Foundation
import Core
import FoundationDB

/// Normalizes predicates to Conjunctive Normal Form (CNF) or Disjunctive Normal Form (DNF)
///
/// CNF is preferred for index matching as it exposes AND conditions at the top level,
/// making it easier to match against composite indexes.
public struct PredicateNormalizer<T: Persistable> {

    public init() {}

    // MARK: - Convert to QueryCondition

    /// Convert a Predicate to QueryCondition
    public func convert(_ predicate: Predicate<T>) -> QueryCondition<T> {
        switch predicate {
        case .comparison(let comparison):
            return .field(convertComparison(comparison, source: predicate))

        case .and(let predicates):
            let conditions = predicates.map { convert($0) }
            return .conjunction(conditions).simplified()

        case .or(let predicates):
            let conditions = predicates.map { convert($0) }
            return .disjunction(conditions).simplified()

        case .not(let inner):
            return negateCondition(convert(inner))

        case .true:
            return .alwaysTrue

        case .false:
            return .alwaysFalse
        }
    }

    // MARK: - Convert to CNF

    /// Convert a QueryCondition to Conjunctive Normal Form
    ///
    /// In CNF, the top-level is a conjunction (AND) of disjunctions (OR).
    /// Example: (A OR B) AND (C OR D) AND E
    public func toCNF(_ condition: QueryCondition<T>) -> QueryCondition<T> {
        let simplified = condition.simplified()

        switch simplified {
        case .field, .alwaysTrue, .alwaysFalse:
            return simplified

        case .conjunction(let conditions):
            // Already a conjunction, recursively convert children
            let cnfChildren = conditions.map { toCNF($0) }
            return .conjunction(cnfChildren).simplified()

        case .disjunction(let conditions):
            // Convert each child to CNF first
            let cnfChildren = conditions.map { toCNF($0) }

            // Distribute OR over AND
            // (A AND B) OR C  =>  (A OR C) AND (B OR C)
            return distributeCNF(cnfChildren)
        }
    }

    // MARK: - Convert to DNF

    /// Convert a QueryCondition to Disjunctive Normal Form
    ///
    /// In DNF, the top-level is a disjunction (OR) of conjunctions (AND).
    /// Example: (A AND B) OR (C AND D) OR E
    public func toDNF(_ condition: QueryCondition<T>) -> QueryCondition<T> {
        let simplified = condition.simplified()

        switch simplified {
        case .field, .alwaysTrue, .alwaysFalse:
            return simplified

        case .disjunction(let conditions):
            // Already a disjunction, recursively convert children
            let dnfChildren = conditions.map { toDNF($0) }
            return .disjunction(dnfChildren).simplified()

        case .conjunction(let conditions):
            // Convert each child to DNF first
            let dnfChildren = conditions.map { toDNF($0) }

            // Distribute AND over OR
            // (A OR B) AND C  =>  (A AND C) OR (B AND C)
            return distributeDNF(dnfChildren)
        }
    }

    // MARK: - Private Helpers

    /// Convert a FieldComparison to FieldCondition
    private func convertComparison(
        _ comparison: FieldComparison<T>,
        source: Predicate<T>
    ) -> any FieldConditionProtocol<T> {
        let fieldRef = FieldReference<T>(
            anyKeyPath: comparison.keyPath,
            fieldName: comparison.fieldName
        )

        switch comparison.op {
        case .equal:
            return ScalarFieldCondition<T>.equals(
                field: fieldRef,
                value: toTupleElement(comparison.value),
                predicate: source
            )

        case .notEqual:
            return ScalarFieldCondition<T>(
                field: fieldRef,
                constraintType: .notEquals,
                values: [toTupleElement(comparison.value)],
                sourcePredicate: source
            )

        case .lessThan:
            return ScalarFieldCondition<T>.range(
                field: fieldRef,
                type: .lessThan,
                value: toTupleElement(comparison.value),
                predicate: source
            )

        case .lessThanOrEqual:
            return ScalarFieldCondition<T>.range(
                field: fieldRef,
                type: .lessThanOrEqual,
                value: toTupleElement(comparison.value),
                predicate: source
            )

        case .greaterThan:
            return ScalarFieldCondition<T>.range(
                field: fieldRef,
                type: .greaterThan,
                value: toTupleElement(comparison.value),
                predicate: source
            )

        case .greaterThanOrEqual:
            return ScalarFieldCondition<T>.range(
                field: fieldRef,
                type: .greaterThanOrEqual,
                value: toTupleElement(comparison.value),
                predicate: source
            )

        case .contains:
            return StringPatternFieldCondition<T>(
                field: fieldRef,
                constraint: StringPatternConstraint(
                    type: .contains,
                    pattern: stringValue(from: comparison.value) ?? ""
                ),
                sourcePredicate: source
            )

        case .hasPrefix:
            return StringPatternFieldCondition<T>(
                field: fieldRef,
                constraint: StringPatternConstraint(
                    type: .prefix,
                    pattern: stringValue(from: comparison.value) ?? ""
                ),
                sourcePredicate: source
            )

        case .hasSuffix:
            return StringPatternFieldCondition<T>(
                field: fieldRef,
                constraint: StringPatternConstraint(
                    type: .suffix,
                    pattern: stringValue(from: comparison.value) ?? ""
                ),
                sourcePredicate: source
            )

        case .in:
            let values: [any TupleElement]
            if let arrayValue = comparison.value as? [Any] {
                values = arrayValue.map { toTupleElementFromAny($0) }
            } else {
                values = [toTupleElement(comparison.value)]
            }
            return ScalarFieldCondition<T>.in(
                field: fieldRef,
                values: values,
                predicate: source
            )

        case .isNil:
            return ScalarFieldCondition<T>.isNull(field: fieldRef, predicate: source)

        case .isNotNil:
            return ScalarFieldCondition<T>(
                field: fieldRef,
                constraintType: .isNotNull,
                values: [],
                sourcePredicate: source
            )
        }
    }

    /// Convert any value to TupleElement
    private func toTupleElement(_ value: any Sendable) -> any TupleElement {
        toTupleElementFromAny(value)
    }

    /// Convert Any to TupleElement
    private func toTupleElementFromAny(_ value: Any) -> any TupleElement {
        // Handle common types that are TupleElementConvertible
        switch value {
        case let v as Int: return v
        case let v as Int64: return v
        case let v as Int32: return Int64(v)
        case let v as Int16: return Int64(v)
        case let v as Int8: return Int64(v)
        case let v as UInt: return Int64(v)
        case let v as UInt64: return Int64(bitPattern: v)
        case let v as UInt32: return Int64(v)
        case let v as UInt16: return Int64(v)
        case let v as UInt8: return Int64(v)
        case let v as Double: return v
        case let v as Float: return Double(v)
        case let v as String: return v
        case let v as Bool: return v
        case let v as Data: return [UInt8](v)
        case let v as UUID: return v.uuidString
        case let v as Date: return v.timeIntervalSince1970
        default:
            // Fallback: convert to string representation
            return String(describing: value)
        }
    }

    /// Extract string value from comparison value
    private func stringValue(from value: any Sendable) -> String? {
        if let str = value as? String {
            return str
        }
        return nil
    }

    /// Negate a condition (NOT)
    private func negateCondition(_ condition: QueryCondition<T>) -> QueryCondition<T> {
        switch condition {
        case .field(let fieldCondition):
            // Use the protocol's negated() method
            return .field(fieldCondition.negated())

        case .conjunction(let conditions):
            // NOT(A AND B) = NOT(A) OR NOT(B) (De Morgan)
            return .disjunction(conditions.map { negateCondition($0) })

        case .disjunction(let conditions):
            // NOT(A OR B) = NOT(A) AND NOT(B) (De Morgan)
            return .conjunction(conditions.map { negateCondition($0) })

        case .alwaysTrue:
            return .alwaysFalse

        case .alwaysFalse:
            return .alwaysTrue
        }
    }

    /// Distribute OR over AND for CNF conversion
    ///
    /// (A AND B) OR C => (A OR C) AND (B OR C)
    private func distributeCNF(_ conditions: [QueryCondition<T>]) -> QueryCondition<T> {
        guard !conditions.isEmpty else { return .alwaysFalse }

        // Find first conjunction to distribute
        var conjunctions: [[QueryCondition<T>]] = []
        var literals: [QueryCondition<T>] = []

        for condition in conditions {
            if case .conjunction(let children) = condition {
                conjunctions.append(children)
            } else {
                literals.append(condition)
            }
        }

        if conjunctions.isEmpty {
            // No conjunctions to distribute
            return .disjunction(conditions).simplified()
        }

        // Distribute: (A AND B) OR rest => (A OR rest) AND (B OR rest)
        let firstConj = conjunctions[0]
        let rest: [QueryCondition<T>]
        if conjunctions.count > 1 || !literals.isEmpty {
            var remaining: [QueryCondition<T>] = literals
            for i in 1..<conjunctions.count {
                remaining.append(.conjunction(conjunctions[i]))
            }
            rest = remaining
        } else {
            rest = literals
        }

        let distributed: [QueryCondition<T>] = firstConj.map { child in
            if rest.isEmpty {
                return child
            } else {
                return toCNF(.disjunction([child] + rest))
            }
        }

        return .conjunction(distributed).simplified()
    }

    /// Distribute AND over OR for DNF conversion
    ///
    /// (A OR B) AND C => (A AND C) OR (B AND C)
    private func distributeDNF(_ conditions: [QueryCondition<T>]) -> QueryCondition<T> {
        guard !conditions.isEmpty else { return .alwaysTrue }

        // Find first disjunction to distribute
        var disjunctions: [[QueryCondition<T>]] = []
        var literals: [QueryCondition<T>] = []

        for condition in conditions {
            if case .disjunction(let children) = condition {
                disjunctions.append(children)
            } else {
                literals.append(condition)
            }
        }

        if disjunctions.isEmpty {
            // No disjunctions to distribute
            return .conjunction(conditions).simplified()
        }

        // Distribute: (A OR B) AND rest => (A AND rest) OR (B AND rest)
        let firstDisj = disjunctions[0]
        let rest: [QueryCondition<T>]
        if disjunctions.count > 1 || !literals.isEmpty {
            var remaining: [QueryCondition<T>] = literals
            for i in 1..<disjunctions.count {
                remaining.append(.disjunction(disjunctions[i]))
            }
            rest = remaining
        } else {
            rest = literals
        }

        let distributed: [QueryCondition<T>] = firstDisj.map { child in
            if rest.isEmpty {
                return child
            } else {
                return toDNF(.conjunction([child] + rest))
            }
        }

        return .disjunction(distributed).simplified()
    }
}

// MARK: - Predicate Combination

extension PredicateNormalizer {
    /// Combine multiple predicates with AND
    public func combinePredicates(_ predicates: [Predicate<T>]) -> Predicate<T>? {
        guard !predicates.isEmpty else { return nil }

        if predicates.count == 1 {
            return predicates[0]
        }

        return .and(predicates)
    }
}
