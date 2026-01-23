/// MatchPattern.swift
/// SQL/PGQ MATCH pattern types
///
/// Reference:
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)
/// - GQL (Graph Query Language) specification

import Foundation

// Note: Core MatchPattern, PathPattern, PathElement, NodePattern, EdgePattern,
// EdgeDirection, PathQuantifier, and PathMode types are defined in DataSource.swift
// This file provides additional utilities and builder helpers for SQL/PGQ patterns.

// MARK: - MatchPattern Builders

extension MatchPattern {
    /// Create a simple path match
    public static func path(_ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(elements: elements)])
    }

    /// Create a path match with a path variable
    public static func path(_ variable: String, _ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(pathVariable: variable, elements: elements)])
    }

    /// Create a path match with mode
    public static func path(mode: PathMode, _ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(elements: elements, mode: mode)])
    }

    /// Create a match with WHERE clause
    public func `where`(_ condition: Expression) -> MatchPattern {
        MatchPattern(paths: self.paths, where: condition)
    }
}

// MARK: - PathPattern Builders

extension PathPattern {
    /// Create a path from node-edge-node sequence
    public static func simple(
        from source: NodePattern,
        via edge: EdgePattern,
        to target: NodePattern
    ) -> PathPattern {
        PathPattern(elements: [.node(source), .edge(edge), .node(target)])
    }

    /// Create a variable-length path
    public static func variable(
        from source: NodePattern,
        via edge: EdgePattern,
        quantifier: PathQuantifier,
        to target: NodePattern
    ) -> PathPattern {
        let innerPath = PathPattern(elements: [.edge(edge)])
        return PathPattern(elements: [
            .node(source),
            .quantified(innerPath, quantifier: quantifier),
            .node(target)
        ])
    }
}

// MARK: - PathElement Helpers

extension PathElement {
    /// Create a node element
    public static func n(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .node(NodePattern(variable: variable, labels: labels, properties: properties))
    }

    /// Create a node with a single label
    public static func n(_ variable: String? = nil, label: String) -> PathElement {
        .node(NodePattern(variable: variable, labels: [label]))
    }

    /// Create an outgoing edge element
    public static func outgoing(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .outgoing))
    }

    /// Create an outgoing edge with a single label
    public static func outgoing(_ variable: String? = nil, label: String) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: [label], direction: .outgoing))
    }

    /// Create an incoming edge element
    public static func incoming(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .incoming))
    }

    /// Create an incoming edge with a single label
    public static func incoming(_ variable: String? = nil, label: String) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: [label], direction: .incoming))
    }

    /// Create an undirected edge element
    public static func undirected(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .undirected))
    }

    /// Create an any-direction edge element
    public static func anyDirection(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .any))
    }
}

// MARK: - PathQuantifier Helpers

extension PathQuantifier {
    /// Create a bounded range quantifier: {min, max}
    public static func bounded(_ min: Int, _ max: Int) -> PathQuantifier {
        .range(min: min, max: max)
    }

    /// Create a minimum-only quantifier: {min,}
    public static func atLeast(_ min: Int) -> PathQuantifier {
        .range(min: min, max: nil)
    }

    /// Create a maximum-only quantifier: {,max}
    public static func atMost(_ max: Int) -> PathQuantifier {
        .range(min: nil, max: max)
    }
}

// MARK: - Pattern Validation

extension MatchPattern {
    /// Validate that the pattern is well-formed
    public func validate() -> [PatternValidationError] {
        var errors: [PatternValidationError] = []

        for (index, path) in paths.enumerated() {
            errors.append(contentsOf: path.validate(pathIndex: index))
        }

        return errors
    }
}

extension PathPattern {
    /// Validate that the path pattern is well-formed
    public func validate(pathIndex: Int) -> [PatternValidationError] {
        var errors: [PatternValidationError] = []

        // Check that path alternates between nodes and edges
        var expectNode = true
        for (elemIndex, element) in elements.enumerated() {
            switch element {
            case .node:
                if !expectNode {
                    errors.append(.unexpectedElement(
                        path: pathIndex,
                        element: elemIndex,
                        expected: "edge",
                        found: "node"
                    ))
                }
                expectNode = false

            case .edge:
                if expectNode {
                    errors.append(.unexpectedElement(
                        path: pathIndex,
                        element: elemIndex,
                        expected: "node",
                        found: "edge"
                    ))
                }
                expectNode = true

            case .quantified(let innerPath, _):
                // Quantified patterns should be validated recursively
                errors.append(contentsOf: innerPath.validate(pathIndex: pathIndex))
                // Determine what comes after based on inner pattern's last element
                if let lastElement = innerPath.elements.last {
                    switch lastElement {
                    case .node:
                        // Inner ends with node, next should be edge
                        expectNode = false
                    case .edge:
                        // Inner ends with edge, next should be node
                        expectNode = true
                    case .quantified, .alternation:
                        // For nested patterns, assume edge-like (expect node next)
                        expectNode = true
                    }
                }
                // If empty inner pattern, don't change expectNode

            case .alternation(let alternatives):
                // All alternatives should be valid
                for alt in alternatives {
                    errors.append(contentsOf: alt.validate(pathIndex: pathIndex))
                }
                // Check first alternative to determine what comes after
                // All alternatives should have the same structure
                if let firstAlt = alternatives.first, let lastElement = firstAlt.elements.last {
                    switch lastElement {
                    case .node:
                        expectNode = false
                    case .edge:
                        expectNode = true
                    case .quantified, .alternation:
                        expectNode = true
                    }
                }
            }
        }

        // Path should start and end with nodes (or be empty)
        if !elements.isEmpty {
            if case .edge = elements.first {
                errors.append(.pathMustStartWithNode(path: pathIndex))
            }
            if case .edge = elements.last {
                errors.append(.pathMustEndWithNode(path: pathIndex))
            }
        }

        return errors
    }
}

/// Pattern validation errors
public enum PatternValidationError: Error, Sendable, Equatable {
    case unexpectedElement(path: Int, element: Int, expected: String, found: String)
    case pathMustStartWithNode(path: Int)
    case pathMustEndWithNode(path: Int)
    case invalidQuantifier(path: Int, message: String)
    case duplicateVariable(name: String)
}

// MARK: - Variable Collection

extension MatchPattern {
    /// Collect all pattern variables
    public var variables: Set<String> {
        var vars = Set<String>()
        for path in paths {
            if let pathVar = path.pathVariable {
                vars.insert(pathVar)
            }
            for element in path.elements {
                collectVariables(from: element, into: &vars)
            }
        }
        return vars
    }

    private func collectVariables(from element: PathElement, into vars: inout Set<String>) {
        switch element {
        case .node(let node):
            if let v = node.variable { vars.insert(v) }
        case .edge(let edge):
            if let v = edge.variable { vars.insert(v) }
        case .quantified(let path, _):
            for elem in path.elements {
                collectVariables(from: elem, into: &vars)
            }
        case .alternation(let alts):
            for alt in alts {
                for elem in alt.elements {
                    collectVariables(from: elem, into: &vars)
                }
            }
        }
    }
}

// MARK: - Pattern Serialization (SQL/PGQ syntax)

extension MatchPattern {
    /// Generate SQL/PGQ MATCH clause syntax
    public func toSQL() -> String {
        var result = "MATCH "
        result += paths.map { $0.toSQL() }.joined(separator: ", ")
        if let whereClause = `where` {
            result += " WHERE \(whereClause.toSQL())"
        }
        return result
    }
}

extension PathPattern {
    /// Generate SQL/PGQ path pattern syntax
    public func toSQL() -> String {
        var result = ""
        if let pathVar = pathVariable {
            result += "\(pathVar) = "
        }
        if let mode = mode {
            result += "\(mode.toSQL()) "
        }
        result += elements.map { $0.toSQL() }.joined()
        return result
    }
}

extension PathElement {
    /// Generate SQL/PGQ path element syntax
    public func toSQL() -> String {
        switch self {
        case .node(let node):
            return node.toSQL()
        case .edge(let edge):
            return edge.toSQL()
        case .quantified(let path, let quant):
            return "(\(path.toSQL()))\(quant.toSQL())"
        case .alternation(let alts):
            return "(" + alts.map { $0.toSQL() }.joined(separator: "|") + ")"
        }
    }
}

extension NodePattern {
    /// Generate SQL/PGQ node pattern syntax
    public func toSQL() -> String {
        var result = "("
        if let v = variable { result += v }
        if let labels = labels, !labels.isEmpty {
            result += ":" + labels.joined(separator: ":")
        }
        if let props = properties, !props.isEmpty {
            result += " {" + props.map { "\($0.0): \($0.1.toSQL())" }.joined(separator: ", ") + "}"
        }
        result += ")"
        return result
    }
}

extension EdgePattern {
    /// Generate SQL/PGQ edge pattern syntax
    public func toSQL() -> String {
        var inner = ""
        if let v = variable { inner += v }
        if let labels = labels, !labels.isEmpty {
            inner += ":" + labels.joined(separator: ":")
        }
        if let props = properties, !props.isEmpty {
            inner += " {" + props.map { "\($0.0): \($0.1.toSQL())" }.joined(separator: ", ") + "}"
        }

        switch direction {
        case .outgoing:
            return "-[\(inner)]->"
        case .incoming:
            return "<-[\(inner)]-"
        case .undirected:
            return "-[\(inner)]-"
        case .any:
            return "<-[\(inner)]->"
        }
    }
}

extension PathQuantifier {
    /// Generate SQL/PGQ quantifier syntax
    public func toSQL() -> String {
        switch self {
        case .exactly(let n):
            return "{\(n)}"
        case .range(let min, let max):
            let minStr = min.map(String.init) ?? ""
            let maxStr = max.map(String.init) ?? ""
            return "{\(minStr),\(maxStr)}"
        case .zeroOrMore:
            return "*"
        case .oneOrMore:
            return "+"
        case .zeroOrOne:
            return "?"
        }
    }
}

extension PathMode {
    /// Generate SQL/PGQ path mode syntax
    public func toSQL() -> String {
        switch self {
        case .walk:
            return "WALK"
        case .trail:
            return "TRAIL"
        case .acyclic:
            return "ACYCLIC"
        case .simple:
            return "SIMPLE"
        case .anyShortest:
            return "ANY SHORTEST"
        case .allShortest:
            return "ALL SHORTEST"
        case .shortestK(let k):
            return "SHORTEST \(k)"
        }
    }
}

// MARK: - Expression SQL Generation

extension Expression {
    /// Generate basic SQL expression syntax (simplified)
    public func toSQL() -> String {
        switch self {
        case .literal(let lit):
            return lit.toSQL()
        case .column(let col):
            return col.description
        case .variable(let v):
            return v.name
        case .equal(let l, let r):
            return "(\(l.toSQL()) = \(r.toSQL()))"
        case .notEqual(let l, let r):
            return "(\(l.toSQL()) <> \(r.toSQL()))"
        case .lessThan(let l, let r):
            return "(\(l.toSQL()) < \(r.toSQL()))"
        case .lessThanOrEqual(let l, let r):
            return "(\(l.toSQL()) <= \(r.toSQL()))"
        case .greaterThan(let l, let r):
            return "(\(l.toSQL()) > \(r.toSQL()))"
        case .greaterThanOrEqual(let l, let r):
            return "(\(l.toSQL()) >= \(r.toSQL()))"
        case .and(let l, let r):
            return "(\(l.toSQL()) AND \(r.toSQL()))"
        case .or(let l, let r):
            return "(\(l.toSQL()) OR \(r.toSQL()))"
        case .not(let e):
            return "NOT \(e.toSQL())"
        case .isNull(let e):
            return "\(e.toSQL()) IS NULL"
        case .isNotNull(let e):
            return "\(e.toSQL()) IS NOT NULL"
        case .add(let l, let r):
            return "(\(l.toSQL()) + \(r.toSQL()))"
        case .subtract(let l, let r):
            return "(\(l.toSQL()) - \(r.toSQL()))"
        case .multiply(let l, let r):
            return "(\(l.toSQL()) * \(r.toSQL()))"
        case .divide(let l, let r):
            return "(\(l.toSQL()) / \(r.toSQL()))"
        default:
            return "<expr>"
        }
    }
}

extension Literal {
    /// Generate SQL literal syntax
    public func toSQL() -> String {
        switch self {
        case .null:
            return "NULL"
        case .bool(let v):
            return v ? "TRUE" : "FALSE"
        case .int(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .string(let v):
            return "'\(v.replacingOccurrences(of: "'", with: "''"))'"
        default:
            return description
        }
    }
}
