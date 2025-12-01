// PredicateNormalizer.swift
// QueryPlanner - Predicate normalization (CNF/DNF conversion)

import Core

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
    ) -> FieldCondition<T> {
        let fieldRef = FieldReference<T>(
            anyKeyPath: comparison.keyPath,
            fieldName: comparison.fieldName,
            fieldType: Any.self
        )

        let constraint: FieldConstraint

        switch comparison.op {
        case .equal:
            constraint = .equals(comparison.value)

        case .notEqual:
            constraint = .notEquals(comparison.value)

        case .lessThan:
            constraint = .range(.lessThan(comparison.value))

        case .lessThanOrEqual:
            constraint = .range(.lessThanOrEqual(comparison.value))

        case .greaterThan:
            constraint = .range(.greaterThan(comparison.value))

        case .greaterThanOrEqual:
            constraint = .range(.greaterThanOrEqual(comparison.value))

        case .contains:
            constraint = .stringPattern(StringPatternConstraint(
                type: .contains,
                pattern: stringValue(from: comparison.value) ?? ""
            ))

        case .hasPrefix:
            constraint = .stringPattern(StringPatternConstraint(
                type: .prefix,
                pattern: stringValue(from: comparison.value) ?? ""
            ))

        case .hasSuffix:
            constraint = .stringPattern(StringPatternConstraint(
                type: .suffix,
                pattern: stringValue(from: comparison.value) ?? ""
            ))

        case .in:
            if let values = comparison.value.value as? [Any] {
                constraint = .in(values.map { AnySendable($0) })
            } else {
                constraint = .in([comparison.value])
            }

        case .isNil:
            constraint = .isNull(true)

        case .isNotNil:
            constraint = .isNull(false)
        }

        return FieldCondition(
            field: fieldRef,
            constraint: constraint,
            sourcePredicate: source
        )
    }

    /// Extract string value from AnySendable
    private func stringValue(from value: AnySendable) -> String? {
        value.value as? String
    }

    /// Negate a condition (NOT)
    private func negateCondition(_ condition: QueryCondition<T>) -> QueryCondition<T> {
        switch condition {
        case .field(let fieldCondition):
            // Negate the field constraint
            let negatedConstraint = negateConstraint(fieldCondition.constraint)
            return .field(FieldCondition(
                field: fieldCondition.field,
                constraint: negatedConstraint,
                sourcePredicate: fieldCondition.sourcePredicate
            ))

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

    /// Negate a field constraint
    private func negateConstraint(_ constraint: FieldConstraint) -> FieldConstraint {
        switch constraint {
        case .equals(let value):
            return .notEquals(value)

        case .notEquals(let value):
            return .equals(value)

        case .range(let bound):
            // NOT(x > a) = x <= a
            // NOT(x < b) = x >= b
            // NOT(a < x < b) = x <= a OR x >= b (complex, keep as notEquals for simplicity)
            if let lower = bound.lower, bound.upper == nil {
                let newUpper = RangeBound.Bound(value: lower.value, inclusive: !lower.inclusive)
                return .range(RangeBound(lower: nil, upper: newUpper))
            } else if let upper = bound.upper, bound.lower == nil {
                let newLower = RangeBound.Bound(value: upper.value, inclusive: !upper.inclusive)
                return .range(RangeBound(lower: newLower, upper: nil))
            } else {
                // Complex range negation - just mark as not equals
                return .notEquals(bound.lower?.value ?? bound.upper?.value ?? AnySendable(Optional<Any>.none as Any))
            }

        case .in(let values):
            // NOT IN negates the membership check
            return .notIn(values)

        case .notIn(let values):
            // NOT (NOT IN) = IN
            return .in(values)

        case .isNull(let isNull):
            return .isNull(!isNull)

        case .textSearch, .spatial, .vectorSimilarity, .stringPattern:
            // These don't have simple negations
            return constraint
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
