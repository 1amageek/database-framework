// ExecutionTriple.swift
// GraphIndex - SPARQL-like triple pattern
//
// Represents a single triple pattern (subject, predicate, object) in a query.

import Foundation

/// A single triple pattern in a SPARQL-like query
///
/// Represents: (subject, predicate, object) where each position can be
/// a variable, value, or wildcard.
///
/// **Usage**:
/// ```swift
/// // Pattern: ?person knows "Alice"
/// let pattern = ExecutionTriple(
///     subject: "?person",
///     predicate: "knows",
///     object: "Alice"
/// )
///
/// // Using graph terminology (from, edge, to)
/// let pattern2 = ExecutionTriple(
///     from: "?person",
///     edge: "knows",
///     to: "Alice"
/// )
/// ```
///
/// **Reference**: W3C SPARQL 1.1, Section 5.1 Basic Graph Patterns
public struct ExecutionTriple: Sendable, Hashable {

    /// Subject position (or "from" in graph terminology)
    public let subject: ExecutionTerm

    /// Predicate position (or "edge" in graph terminology)
    public let predicate: ExecutionTerm

    /// Object position (or "to" in graph terminology)
    public let object: ExecutionTerm

    /// Named graph position (nil = no graph constraint)
    public let graph: ExecutionTerm?

    // MARK: - Initialization

    /// Create a triple pattern with explicit terms
    public init(
        subject: ExecutionTerm,
        predicate: ExecutionTerm,
        object: ExecutionTerm,
        graph: ExecutionTerm? = nil
    ) {
        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.graph = graph
    }

    /// Create a triple pattern using graph terminology
    public init(
        from: ExecutionTerm,
        edge: ExecutionTerm,
        to: ExecutionTerm,
        graph: ExecutionTerm? = nil
    ) {
        self.subject = from
        self.predicate = edge
        self.object = to
        self.graph = graph
    }

    /// Create a triple pattern from string literals
    ///
    /// Strings starting with "?" are interpreted as variables.
    public init(
        _ subject: String,
        _ predicate: String,
        _ object: String
    ) {
        self.subject = ExecutionTerm(stringLiteral: subject)
        self.predicate = ExecutionTerm(stringLiteral: predicate)
        self.object = ExecutionTerm(stringLiteral: object)
        self.graph = nil
    }

    // MARK: - Variables

    /// All named variables in this pattern (recursively collects from quotedTriple)
    public var variables: Set<String> {
        var vars = Set<String>()
        Self.collectVariables(from: subject, into: &vars)
        Self.collectVariables(from: predicate, into: &vars)
        Self.collectVariables(from: object, into: &vars)
        if let graph {
            Self.collectVariables(from: graph, into: &vars)
        }
        return vars
    }

    /// Recursively collect variable names from an ExecutionTerm
    private static func collectVariables(from term: ExecutionTerm, into vars: inout Set<String>) {
        switch term {
        case .variable(let name):
            vars.insert(name)
        case .quotedTriple(let s, let p, let o):
            collectVariables(from: s, into: &vars)
            collectVariables(from: p, into: &vars)
            collectVariables(from: o, into: &vars)
        case .value, .wildcard:
            break
        }
    }

    /// Check if a variable appears in this pattern
    public func contains(variable: String) -> Bool {
        variables.contains(variable)
    }

    // MARK: - Bound Positions

    /// Which positions are bound (have concrete values)
    public var boundPositions: (subject: Bool, predicate: Bool, object: Bool) {
        (
            subject: subject.isBound,
            predicate: predicate.isBound,
            object: object.isBound
        )
    }

    /// Number of bound positions (0-3)
    public var boundCount: Int {
        var count = 0
        if subject.isBound { count += 1 }
        if predicate.isBound { count += 1 }
        if object.isBound { count += 1 }
        return count
    }

    // MARK: - Selectivity

    /// Selectivity score for join ordering optimization
    ///
    /// Higher score = more selective (fewer results expected).
    /// - Bound positions contribute 10 points each
    /// - Predicate position has slight bonus (predicates often have fewer distinct values)
    ///
    /// **Reference**: Join order optimization in RDF-3X and similar systems
    public var selectivityScore: Int {
        var score = 0
        if subject.isBound { score += 10 }
        if predicate.isBound { score += 12 }  // Predicates are often more selective
        if object.isBound { score += 10 }
        return score
    }

    /// Whether this pattern has any bound values
    public var hasBoundValues: Bool {
        subject.isBound || predicate.isBound || object.isBound
    }

    /// Whether this pattern is fully bound (point lookup)
    public var isFullyBound: Bool {
        subject.isBound && predicate.isBound && object.isBound
    }

    // MARK: - Variable Substitution

    /// Create a new pattern with variables substituted from a VariableBinding
    ///
    /// Used during join execution to convert variable patterns into
    /// more selective patterns.
    ///
    /// - Parameter binding: The variable binding containing values
    /// - Returns: New pattern with matching variables replaced by values
    public func substitute(_ binding: VariableBinding) -> ExecutionTriple {
        ExecutionTriple(
            subject: subject.substitute(binding),
            predicate: predicate.substitute(binding),
            object: object.substitute(binding),
            graph: graph?.substitute(binding)
        )
    }

    /// Create a new pattern with the graph term set
    public func withGraph(_ graphTerm: ExecutionTerm) -> ExecutionTriple {
        ExecutionTriple(
            subject: subject,
            predicate: predicate,
            object: object,
            graph: graphTerm
        )
    }
}

// MARK: - CustomStringConvertible

extension ExecutionTriple: CustomStringConvertible {
    public var description: String {
        if let graph {
            return "GRAPH \(graph) { (\(subject), \(predicate), \(object)) }"
        }
        return "(\(subject), \(predicate), \(object))"
    }
}

// MARK: - Comparable (for sorting by selectivity)

extension ExecutionTriple: Comparable {
    public static func < (lhs: ExecutionTriple, rhs: ExecutionTriple) -> Bool {
        // Higher selectivity score should come first (descending order)
        lhs.selectivityScore > rhs.selectivityScore
    }
}
