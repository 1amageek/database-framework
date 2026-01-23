/// MatchPatternBuilder.swift
/// Fluent builder for SQL/PGQ MATCH patterns
///
/// Reference:
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)
/// - GQL (Graph Query Language)

import Foundation

/// Result builder for constructing MATCH patterns
@resultBuilder
public struct MatchPatternResultBuilder {
    public static func buildBlock(_ components: PathElement...) -> [PathElement] {
        components
    }

    public static func buildArray(_ components: [[PathElement]]) -> [PathElement] {
        components.flatMap { $0 }
    }

    public static func buildOptional(_ component: [PathElement]?) -> [PathElement] {
        component ?? []
    }

    public static func buildEither(first component: [PathElement]) -> [PathElement] {
        component
    }

    public static func buildEither(second component: [PathElement]) -> [PathElement] {
        component
    }
}

// MARK: - Builder Functions

/// Create a node pattern element
public func node(
    _ variable: String? = nil,
    labels: [String]? = nil,
    properties: [(String, Expression)]? = nil
) -> PathElement {
    .node(NodePattern(variable: variable, labels: labels, properties: properties))
}

/// Create a node with a single label
public func node(_ variable: String? = nil, label: String) -> PathElement {
    .node(NodePattern(variable: variable, labels: [label]))
}

/// Create a node with properties
public func node(
    _ variable: String? = nil,
    label: String,
    where properties: [(String, Expression)]
) -> PathElement {
    .node(NodePattern(variable: variable, labels: [label], properties: properties))
}

/// Create an outgoing edge: -[...]->
public func edge(
    direction: EdgeDirection,
    _ variable: String? = nil,
    labels: [String]? = nil,
    properties: [(String, Expression)]? = nil
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: direction))
}

/// Create an outgoing edge with a single label
public func edge(
    direction: EdgeDirection,
    _ variable: String? = nil,
    label: String
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: [label], direction: direction))
}

/// Create an outgoing edge: -[...]->
public func outgoing(
    _ variable: String? = nil,
    labels: [String]? = nil
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: labels, direction: .outgoing))
}

/// Create an outgoing edge with single label
public func outgoing(_ variable: String? = nil, label: String) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: [label], direction: .outgoing))
}

/// Create an incoming edge: <-[...]-
public func incoming(
    _ variable: String? = nil,
    labels: [String]? = nil
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: labels, direction: .incoming))
}

/// Create an incoming edge with single label
public func incoming(_ variable: String? = nil, label: String) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: [label], direction: .incoming))
}

/// Create an undirected edge: -[...]-
public func undirected(
    _ variable: String? = nil,
    labels: [String]? = nil
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: labels, direction: .undirected))
}

/// Create an any-direction edge: <-[...]->
public func anyDirection(
    _ variable: String? = nil,
    labels: [String]? = nil
) -> PathElement {
    .edge(EdgePattern(variable: variable, labels: labels, direction: .any))
}

/// Create a quantified path element
public func quantified(
    _ quantifier: PathQuantifier,
    @MatchPatternResultBuilder _ build: () -> [PathElement]
) -> PathElement {
    .quantified(PathPattern(elements: build()), quantifier: quantifier)
}

/// Create a quantified edge (shorthand for common pattern)
public func quantifiedEdge(
    _ quantifier: PathQuantifier,
    direction: EdgeDirection,
    labels: [String]? = nil
) -> PathElement {
    .quantified(
        PathPattern(elements: [
            .edge(EdgePattern(labels: labels, direction: direction)),
            .node(NodePattern())
        ]),
        quantifier: quantifier
    )
}

/// Create an alternation of path patterns
public func alternation(
    @AlternationBuilder _ build: () -> [PathPattern]
) -> PathElement {
    .alternation(build())
}

/// Result builder for alternation
@resultBuilder
public struct AlternationBuilder {
    public static func buildBlock(_ patterns: PathPattern...) -> [PathPattern] {
        patterns
    }
}

// MARK: - Match Pattern Builder

/// Fluent builder for MATCH patterns
public struct MatchPatternBuilder: Sendable {
    private var paths: [PathPattern]
    private var whereCondition: Expression?

    public init() {
        self.paths = []
        self.whereCondition = nil
    }

    /// Add a path pattern
    public func path(
        _ variable: String? = nil,
        mode: PathMode? = nil,
        @MatchPatternResultBuilder _ build: () -> [PathElement]
    ) -> MatchPatternBuilder {
        var builder = self
        builder.paths.append(PathPattern(
            pathVariable: variable,
            elements: build(),
            mode: mode
        ))
        return builder
    }

    /// Add a simple path (node-edge-node)
    public func simplePath(
        from source: NodePattern,
        via edge: EdgePattern,
        to target: NodePattern,
        variable: String? = nil
    ) -> MatchPatternBuilder {
        var builder = self
        builder.paths.append(PathPattern(
            pathVariable: variable,
            elements: [.node(source), .edge(edge), .node(target)]
        ))
        return builder
    }

    /// Add a variable-length path
    public func variablePath(
        from source: NodePattern,
        via edge: EdgePattern,
        quantifier: PathQuantifier,
        to target: NodePattern,
        variable: String? = nil,
        mode: PathMode? = nil
    ) -> MatchPatternBuilder {
        var builder = self
        builder.paths.append(PathPattern(
            pathVariable: variable,
            elements: [
                .node(source),
                .quantified(
                    PathPattern(elements: [.edge(edge), .node(NodePattern())]),
                    quantifier: quantifier
                ),
                .node(target)
            ],
            mode: mode
        ))
        return builder
    }

    /// Add WHERE condition
    public func `where`(_ condition: Expression) -> MatchPatternBuilder {
        var builder = self
        if let existing = builder.whereCondition {
            builder.whereCondition = .and(existing, condition)
        } else {
            builder.whereCondition = condition
        }
        return builder
    }

    /// Build the MatchPattern
    public func build() -> MatchPattern {
        MatchPattern(paths: paths, where: whereCondition)
    }
}

// MARK: - Convenience Constructors

extension MatchPattern {
    /// Create using the builder DSL
    public static func build(@MatchPatternResultBuilder _ build: () -> [PathElement]) -> MatchPattern {
        MatchPattern(paths: [PathPattern(elements: build())])
    }

    /// Create a path match builder
    public static func builder() -> MatchPatternBuilder {
        MatchPatternBuilder()
    }
}

// MARK: - PathPattern Convenience

extension PathPattern {
    /// Create using the builder DSL
    public static func build(
        _ variable: String? = nil,
        mode: PathMode? = nil,
        @MatchPatternResultBuilder _ build: () -> [PathElement]
    ) -> PathPattern {
        PathPattern(pathVariable: variable, elements: build(), mode: mode)
    }
}

// MARK: - Common Pattern Templates

extension MatchPatternBuilder {
    /// Friend relationship pattern: (a)-[:FRIEND]->(b)
    public func friend(from: String, to: String, edgeLabel: String = "FRIEND") -> MatchPatternBuilder {
        simplePath(
            from: NodePattern(variable: from),
            via: EdgePattern(labels: [edgeLabel], direction: .outgoing),
            to: NodePattern(variable: to)
        )
    }

    /// Parent-child pattern
    public func parentChild(parent: String, child: String, edgeLabel: String = "PARENT_OF") -> MatchPatternBuilder {
        simplePath(
            from: NodePattern(variable: parent),
            via: EdgePattern(labels: [edgeLabel], direction: .outgoing),
            to: NodePattern(variable: child)
        )
    }

    /// Ancestor pattern (transitive)
    public func ancestor(
        descendant: String,
        ancestor: String,
        edgeLabel: String = "PARENT_OF",
        maxDepth: Int? = nil
    ) -> MatchPatternBuilder {
        let quantifier: PathQuantifier = maxDepth.map { .range(min: 1, max: $0) } ?? .oneOrMore
        return variablePath(
            from: NodePattern(variable: descendant),
            via: EdgePattern(labels: [edgeLabel], direction: .incoming),
            quantifier: quantifier,
            to: NodePattern(variable: ancestor)
        )
    }

    /// Shortest path pattern
    public func shortestPath(
        from source: String,
        sourceLabel: String? = nil,
        to target: String,
        targetLabel: String? = nil,
        via edgeLabel: String? = nil,
        maxHops: Int? = nil
    ) -> MatchPatternBuilder {
        let quantifier: PathQuantifier = maxHops.map { .range(min: 1, max: $0) } ?? .oneOrMore
        var builder = self
        builder.paths.append(PathPattern(
            elements: [
                .node(NodePattern(variable: source, labels: sourceLabel.map { [$0] })),
                .quantified(
                    PathPattern(elements: [
                        .edge(EdgePattern(labels: edgeLabel.map { [$0] }, direction: .outgoing)),
                        .node(NodePattern())
                    ]),
                    quantifier: quantifier
                ),
                .node(NodePattern(variable: target, labels: targetLabel.map { [$0] }))
            ],
            mode: .anyShortest
        ))
        return builder
    }
}

// MARK: - Expression Helpers for WHERE

extension Expression {
    /// Create a property access expression: variable.property
    public static func property(_ variable: String, _ property: String) -> Expression {
        .column(ColumnRef(table: variable, column: property))
    }
}
