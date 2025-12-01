// QueryCondition.swift
// QueryPlanner - Normalized query condition representation

import Core

/// Normalized representation of query conditions for planning
///
/// This represents the query predicate in a normalized form suitable for
/// index matching and cost estimation.
public indirect enum QueryCondition<T: Persistable>: @unchecked Sendable {
    /// Single field condition
    case field(FieldCondition<T>)

    /// Conjunction (AND) - all must be true
    case conjunction([QueryCondition<T>])

    /// Disjunction (OR) - at least one must be true
    case disjunction([QueryCondition<T>])

    /// Always true (no filter)
    case alwaysTrue

    /// Always false (empty result)
    case alwaysFalse
}

// MARK: - Convenience Methods

extension QueryCondition {
    /// Check if this condition is always true
    public var isAlwaysTrue: Bool {
        if case .alwaysTrue = self { return true }
        return false
    }

    /// Check if this condition is always false
    public var isAlwaysFalse: Bool {
        if case .alwaysFalse = self { return true }
        return false
    }

    /// Check if this is a simple field condition
    public var isFieldCondition: Bool {
        if case .field = self { return true }
        return false
    }

    /// Check if this is a conjunction (AND)
    public var isConjunction: Bool {
        if case .conjunction = self { return true }
        return false
    }

    /// Check if this is a disjunction (OR)
    public var isDisjunction: Bool {
        if case .disjunction = self { return true }
        return false
    }

    /// Get all field conditions (flattened from any structure)
    public var allFieldConditions: [FieldCondition<T>] {
        switch self {
        case .field(let condition):
            return [condition]
        case .conjunction(let conditions):
            return conditions.flatMap { $0.allFieldConditions }
        case .disjunction(let conditions):
            return conditions.flatMap { $0.allFieldConditions }
        case .alwaysTrue, .alwaysFalse:
            return []
        }
    }

    /// Get all referenced field names
    public var referencedFields: Set<String> {
        Set(allFieldConditions.map { $0.field.fieldName })
    }

    /// Simplify the condition by removing redundant nodes
    public func simplified() -> QueryCondition<T> {
        switch self {
        case .field:
            return self

        case .conjunction(let conditions):
            let simplified = conditions.map { $0.simplified() }

            // Remove alwaysTrue conditions
            let filtered = simplified.filter { !$0.isAlwaysTrue }

            // If any condition is alwaysFalse, the whole conjunction is false
            if filtered.contains(where: { $0.isAlwaysFalse }) {
                return .alwaysFalse
            }

            // Flatten nested conjunctions
            var flattened: [QueryCondition<T>] = []
            for cond in filtered {
                if case .conjunction(let nested) = cond {
                    flattened.append(contentsOf: nested)
                } else {
                    flattened.append(cond)
                }
            }

            switch flattened.count {
            case 0:
                return .alwaysTrue
            case 1:
                return flattened[0]
            default:
                return .conjunction(flattened)
            }

        case .disjunction(let conditions):
            let simplified = conditions.map { $0.simplified() }

            // Remove alwaysFalse conditions
            let filtered = simplified.filter { !$0.isAlwaysFalse }

            // If any condition is alwaysTrue, the whole disjunction is true
            if filtered.contains(where: { $0.isAlwaysTrue }) {
                return .alwaysTrue
            }

            // Flatten nested disjunctions
            var flattened: [QueryCondition<T>] = []
            for cond in filtered {
                if case .disjunction(let nested) = cond {
                    flattened.append(contentsOf: nested)
                } else {
                    flattened.append(cond)
                }
            }

            switch flattened.count {
            case 0:
                return .alwaysFalse
            case 1:
                return flattened[0]
            default:
                return .disjunction(flattened)
            }

        case .alwaysTrue, .alwaysFalse:
            return self
        }
    }
}

// MARK: - Description

extension QueryCondition: CustomStringConvertible {
    public var description: String {
        switch self {
        case .field(let condition):
            return "\(condition.field.fieldName) \(describeConstraint(condition.constraint))"
        case .conjunction(let conditions):
            let parts = conditions.map { $0.description }
            return "(\(parts.joined(separator: " AND ")))"
        case .disjunction(let conditions):
            let parts = conditions.map { $0.description }
            return "(\(parts.joined(separator: " OR ")))"
        case .alwaysTrue:
            return "TRUE"
        case .alwaysFalse:
            return "FALSE"
        }
    }

    private func describeConstraint(_ constraint: FieldConstraint) -> String {
        switch constraint {
        case .equals(let value):
            return "= \(value.value)"
        case .notEquals(let value):
            return "!= \(value.value)"
        case .range(let bound):
            var parts: [String] = []
            if let lower = bound.lower {
                parts.append(lower.inclusive ? ">=" : ">")
                parts.append("\(lower.value.value)")
            }
            if let upper = bound.upper {
                parts.append(upper.inclusive ? "<=" : "<")
                parts.append("\(upper.value.value)")
            }
            return parts.joined(separator: " ")
        case .in(let values):
            return "IN [\(values.count) values]"
        case .notIn(let values):
            return "NOT IN [\(values.count) values]"
        case .isNull(let isNull):
            return isNull ? "IS NULL" : "IS NOT NULL"
        case .textSearch(let constraint):
            return "MATCH '\(constraint.terms.joined(separator: " "))'"
        case .spatial(let constraint):
            switch constraint.type {
            case .withinDistance(_, let radius):
                return "WITHIN \(radius)m"
            case .withinBounds:
                return "WITHIN BOUNDS"
            case .withinPolygon:
                return "WITHIN POLYGON"
            }
        case .vectorSimilarity(let constraint):
            return "SIMILAR(k=\(constraint.k))"
        case .stringPattern(let constraint):
            return "\(constraint.type) '\(constraint.pattern)'"
        }
    }
}
