// ExecutionPattern.swift
// GraphIndex - SPARQL-like graph pattern algebra
//
// Represents composed graph patterns following SPARQL algebra.

import Foundation

/// Represents a graph pattern that can be composed
///
/// **Design**: Algebraic representation of SPARQL graph patterns
/// following the SPARQL Algebra specification. Patterns can be composed
/// to create complex queries with joins, optionals, unions, and filters.
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 18.2 (SPARQL Algebra)
public indirect enum ExecutionPattern: Sendable {

    /// Basic Graph Pattern (BGP): a set of triple patterns
    ///
    /// Multiple patterns in a BGP are implicitly joined.
    /// This is the fundamental building block.
    case basic([ExecutionTriple])

    /// Join of two graph patterns (AND semantics)
    ///
    /// Both patterns must match. Variables shared between patterns
    /// are joined (must have equal values).
    case join(ExecutionPattern, ExecutionPattern)

    /// Left outer join (OPTIONAL semantics)
    ///
    /// Left pattern must match; right pattern is optional.
    /// If right pattern doesn't match, variables from right
    /// will be unbound in the result.
    case optional(ExecutionPattern, ExecutionPattern)

    /// Union of two graph patterns (OR semantics)
    ///
    /// Either pattern can match. Results from both branches
    /// are combined.
    case union(ExecutionPattern, ExecutionPattern)

    /// Filter applied to a graph pattern
    ///
    /// Pattern must match AND filter expression must evaluate to true.
    case filter(ExecutionPattern, FilterExpression)

    /// Group by pattern with aggregation
    ///
    /// Groups results by specified variables and applies aggregate functions.
    /// Optionally includes a HAVING filter applied after aggregation.
    ///
    /// - Parameters:
    ///   - pattern: The source pattern to group
    ///   - groupVariables: Variables to group by
    ///   - aggregates: Aggregate expressions to compute
    ///   - having: Optional filter on aggregate results
    case groupBy(ExecutionPattern, groupVariables: [String], aggregates: [AggregateExpression], having: FilterExpression?)

    /// Property path pattern
    ///
    /// Matches paths between subject and object using property path expression.
    /// Supports transitive closure, inverse paths, sequences, and alternatives.
    ///
    /// - Parameters:
    ///   - subject: The subject term (variable or value)
    ///   - path: The property path expression
    ///   - object: The object term (variable or value)
    /// Difference of two graph patterns (MINUS semantics)
    ///
    /// Keep left bindings that have no compatible solution in right.
    /// Compatible = agree on all shared variables AND share at least one variable.
    /// If no shared variables, left binding is always kept.
    ///
    /// **Reference**: W3C SPARQL 1.1, Section 18.5
    case minus(ExecutionPattern, ExecutionPattern)

    case propertyPath(subject: ExecutionTerm, path: ExecutionPropertyPath, object: ExecutionTerm)

    // MARK: - Variables

    /// All variables referenced in this pattern
    public var variables: Set<String> {
        switch self {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { result, pattern in
                result.formUnion(pattern.variables)
            }
        case .join(let left, let right):
            return left.variables.union(right.variables)
        case .optional(let left, let right):
            return left.variables.union(right.variables)
        case .union(let left, let right):
            return left.variables.union(right.variables)
        case .filter(let pattern, _):
            return pattern.variables
        case .groupBy(_, let groupVariables, let aggregates, _):
            // Output variables are group variables + aggregate aliases
            var result = Set(groupVariables)
            for agg in aggregates {
                result.insert(agg.alias)
            }
            return result
        case .minus(let left, _):
            return left.variables  // MINUS does not project right variables
        case .propertyPath(let subject, _, let object):
            var result = Set<String>()
            if case .variable(let name) = subject { result.insert(name) }
            if case .variable(let name) = object { result.insert(name) }
            return result
        }
    }

    /// Variables that must be bound (appear in required patterns)
    ///
    /// For OPTIONAL, only variables from the left side are required.
    public var requiredVariables: Set<String> {
        switch self {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { result, pattern in
                result.formUnion(pattern.variables)
            }
        case .join(let left, let right):
            return left.requiredVariables.union(right.requiredVariables)
        case .optional(let left, _):
            return left.requiredVariables  // Only left is required
        case .union(let left, let right):
            // Variables required in BOTH branches are required overall
            return left.requiredVariables.intersection(right.requiredVariables)
        case .filter(let pattern, _):
            return pattern.requiredVariables
        case .groupBy(_, let groupVariables, let aggregates, _):
            // All output variables are required after grouping
            var result = Set(groupVariables)
            for agg in aggregates {
                result.insert(agg.alias)
            }
            return result
        case .minus(let left, _):
            return left.requiredVariables
        case .propertyPath(let subject, _, let object):
            var result = Set<String>()
            if case .variable(let name) = subject { result.insert(name) }
            if case .variable(let name) = object { result.insert(name) }
            return result
        }
    }

    /// Variables that might be unbound (from OPTIONAL or UNION)
    public var optionalVariables: Set<String> {
        variables.subtracting(requiredVariables)
    }

    // MARK: - Pattern Analysis

    /// Whether this is an empty pattern
    public var isEmpty: Bool {
        switch self {
        case .basic(let patterns):
            return patterns.isEmpty
        case .join(let left, let right):
            return left.isEmpty && right.isEmpty
        case .optional(let left, _):
            return left.isEmpty
        case .union(let left, let right):
            return left.isEmpty && right.isEmpty
        case .filter(let pattern, _):
            return pattern.isEmpty
        case .groupBy(let pattern, _, _, _):
            return pattern.isEmpty
        case .minus(let left, _):
            return left.isEmpty
        case .propertyPath:
            return false  // Property paths are never empty
        }
    }

    /// Extract all triple patterns (flattening the structure)
    public var allExecutionTriples: [ExecutionTriple] {
        switch self {
        case .basic(let patterns):
            return patterns
        case .join(let left, let right):
            return left.allExecutionTriples + right.allExecutionTriples
        case .optional(let left, let right):
            return left.allExecutionTriples + right.allExecutionTriples
        case .union(let left, let right):
            return left.allExecutionTriples + right.allExecutionTriples
        case .filter(let pattern, _):
            return pattern.allExecutionTriples
        case .groupBy(let pattern, _, _, _):
            return pattern.allExecutionTriples
        case .minus(let left, let right):
            return left.allExecutionTriples + right.allExecutionTriples
        case .propertyPath:
            return []  // Property paths don't have direct triple patterns
        }
    }

    /// Number of triple patterns
    public var patternCount: Int {
        allExecutionTriples.count
    }

    // MARK: - Convenience Constructors

    /// Create a basic pattern from a single triple pattern
    public static func single(_ pattern: ExecutionTriple) -> ExecutionPattern {
        .basic([pattern])
    }

    /// Create a basic pattern from string literals
    public static func triple(_ subject: String, _ predicate: String, _ object: String) -> ExecutionPattern {
        .basic([ExecutionTriple(subject, predicate, object)])
    }

    /// Create an empty pattern
    public static var empty: ExecutionPattern {
        .basic([])
    }

    /// Return a new pattern with graph term set on all contained triples
    public func withGraph(_ graphTerm: ExecutionTerm) -> ExecutionPattern {
        switch self {
        case .basic(let triples):
            return .basic(triples.map { $0.withGraph(graphTerm) })
        case .join(let left, let right):
            return .join(left.withGraph(graphTerm), right.withGraph(graphTerm))
        case .optional(let left, let right):
            return .optional(left.withGraph(graphTerm), right.withGraph(graphTerm))
        case .union(let left, let right):
            return .union(left.withGraph(graphTerm), right.withGraph(graphTerm))
        case .filter(let pattern, let expression):
            return .filter(pattern.withGraph(graphTerm), expression)
        case .groupBy(let pattern, let groupVars, let aggs, let having):
            return .groupBy(pattern.withGraph(graphTerm), groupVariables: groupVars, aggregates: aggs, having: having)
        case .minus(let left, let right):
            return .minus(left.withGraph(graphTerm), right.withGraph(graphTerm))
        case .propertyPath(let subject, let path, let object):
            return .propertyPath(subject: subject, path: path, object: object)
        }
    }
}

// MARK: - CustomStringConvertible

extension ExecutionPattern: CustomStringConvertible {
    public var description: String {
        switch self {
        case .basic(let patterns):
            let pats = patterns.map { $0.description }.joined(separator: " . ")
            return "{ \(pats) }"
        case .join(let left, let right):
            return "\(left) JOIN \(right)"
        case .optional(let left, let right):
            return "\(left) OPTIONAL \(right)"
        case .union(let left, let right):
            return "\(left) UNION \(right)"
        case .filter(let pattern, let expr):
            return "\(pattern) FILTER(\(expr))"
        case .groupBy(let pattern, let groupVars, let aggregates, let having):
            var result = "\(pattern) GROUP BY \(groupVars.joined(separator: ", "))"
            if !aggregates.isEmpty {
                result += " AGGREGATES(\(aggregates.map { $0.description }.joined(separator: ", ")))"
            }
            if let having = having {
                result += " HAVING(\(having))"
            }
            return result
        case .minus(let left, let right):
            return "\(left) MINUS \(right)"
        case .propertyPath(let subject, let path, let object):
            return "{ \(subject) \(path) \(object) }"
        }
    }
}

// MARK: - Equatable

extension ExecutionPattern: Equatable {
    public static func == (lhs: ExecutionPattern, rhs: ExecutionPattern) -> Bool {
        switch (lhs, rhs) {
        case (.basic(let l), .basic(let r)):
            return l == r
        case (.join(let ll, let lr), .join(let rl, let rr)):
            return ll == rl && lr == rr
        case (.optional(let ll, let lr), .optional(let rl, let rr)):
            return ll == rl && lr == rr
        case (.union(let ll, let lr), .union(let rl, let rr)):
            return ll == rl && lr == rr
        case (.filter(let lp, let le), .filter(let rp, let re)):
            return lp == rp && le == re
        case (.groupBy(let lp, let lgv, let lagg, let lh), .groupBy(let rp, let rgv, let ragg, let rh)):
            return lp == rp && lgv == rgv && lagg == ragg && lh == rh
        case (.minus(let ll, let lr), .minus(let rl, let rr)):
            return ll == rl && lr == rr
        case (.propertyPath(let ls, let lp, let lo), .propertyPath(let rs, let rp, let ro)):
            return ls == rs && lp == rp && lo == ro
        default:
            return false
        }
    }
}
