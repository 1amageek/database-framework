// GraphPattern.swift
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
public indirect enum GraphPattern: Sendable {

    /// Basic Graph Pattern (BGP): a set of triple patterns
    ///
    /// Multiple patterns in a BGP are implicitly joined.
    /// This is the fundamental building block.
    case basic([TriplePattern])

    /// Join of two graph patterns (AND semantics)
    ///
    /// Both patterns must match. Variables shared between patterns
    /// are joined (must have equal values).
    case join(GraphPattern, GraphPattern)

    /// Left outer join (OPTIONAL semantics)
    ///
    /// Left pattern must match; right pattern is optional.
    /// If right pattern doesn't match, variables from right
    /// will be unbound in the result.
    case optional(GraphPattern, GraphPattern)

    /// Union of two graph patterns (OR semantics)
    ///
    /// Either pattern can match. Results from both branches
    /// are combined.
    case union(GraphPattern, GraphPattern)

    /// Filter applied to a graph pattern
    ///
    /// Pattern must match AND filter expression must evaluate to true.
    case filter(GraphPattern, FilterExpression)

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
        }
    }

    /// Extract all triple patterns (flattening the structure)
    public var allTriplePatterns: [TriplePattern] {
        switch self {
        case .basic(let patterns):
            return patterns
        case .join(let left, let right):
            return left.allTriplePatterns + right.allTriplePatterns
        case .optional(let left, let right):
            return left.allTriplePatterns + right.allTriplePatterns
        case .union(let left, let right):
            return left.allTriplePatterns + right.allTriplePatterns
        case .filter(let pattern, _):
            return pattern.allTriplePatterns
        }
    }

    /// Number of triple patterns
    public var patternCount: Int {
        allTriplePatterns.count
    }

    // MARK: - Convenience Constructors

    /// Create a basic pattern from a single triple pattern
    public static func single(_ pattern: TriplePattern) -> GraphPattern {
        .basic([pattern])
    }

    /// Create a basic pattern from string literals
    public static func triple(_ subject: String, _ predicate: String, _ object: String) -> GraphPattern {
        .basic([TriplePattern(subject, predicate, object)])
    }

    /// Create an empty pattern
    public static var empty: GraphPattern {
        .basic([])
    }
}

// MARK: - CustomStringConvertible

extension GraphPattern: CustomStringConvertible {
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
        }
    }
}

// MARK: - Equatable

extension GraphPattern: Equatable {
    public static func == (lhs: GraphPattern, rhs: GraphPattern) -> Bool {
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
        default:
            return false
        }
    }
}
