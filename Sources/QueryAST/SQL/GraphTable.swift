/// GraphTable.swift
/// SQL/PGQ GRAPH_TABLE clause types
///
/// Reference:
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import Foundation

// Note: Core GraphTableSource and GraphTableColumn types are defined in DataSource.swift
// This file provides additional utilities for GRAPH_TABLE operations.

// MARK: - GraphTableSource Builders

extension GraphTableSource {
    /// Create a GRAPH_TABLE source with a simple path match
    public static func match(
        graph: String,
        from source: NodePattern,
        via edge: EdgePattern,
        to target: NodePattern
    ) -> GraphTableSource {
        GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [.node(source), .edge(edge), .node(target)])
            ])
        )
    }

    /// Create a GRAPH_TABLE source with columns
    public static func match(
        graph: String,
        pattern: MatchPattern,
        columns: [(Expression, String)]
    ) -> GraphTableSource {
        GraphTableSource(
            graphName: graph,
            matchPattern: pattern,
            columns: columns.map { GraphTableColumn(expression: $0.0, alias: $0.1) }
        )
    }

    /// Add columns to the graph table
    public func returning(_ columns: [(Expression, String)]) -> GraphTableSource {
        GraphTableSource(
            graphName: graphName,
            matchPattern: matchPattern,
            columns: columns.map { GraphTableColumn(expression: $0.0, alias: $0.1) }
        )
    }
}

// MARK: - SQL Generation

extension GraphTableSource {
    /// Generate SQL/PGQ GRAPH_TABLE syntax
    public func toSQL() -> String {
        var result = "GRAPH_TABLE(\(graphName),\n"
        result += "  \(matchPattern.toSQL())"
        if let cols = columns, !cols.isEmpty {
            result += "\n  COLUMNS ("
            result += cols.map { "\($0.expression.toSQL()) AS \($0.alias)" }.joined(separator: ", ")
            result += ")"
        }
        result += "\n)"
        return result
    }
}

// MARK: - Graph Table Analysis

extension GraphTableSource {
    /// Returns all variables defined in the match pattern
    public var definedVariables: Set<String> {
        matchPattern.variables
    }

    /// Returns all columns exposed by the COLUMNS clause
    public var exposedColumns: [String] {
        columns?.map(\.alias) ?? []
    }

    /// Validate the graph table source
    public func validate() -> [GraphTableValidationError] {
        var errors: [GraphTableValidationError] = []

        // Validate match pattern
        let patternErrors = matchPattern.validate()
        errors.append(contentsOf: patternErrors.map { .patternError($0) })

        // Validate columns reference valid variables
        let definedVars = definedVariables
        if let cols = columns {
            for col in cols {
                let referencedVars = collectVariables(from: col.expression)
                for v in referencedVars {
                    if !definedVars.contains(v) {
                        errors.append(.undefinedVariable(v, in: col.alias))
                    }
                }
            }
        }

        return errors
    }

    private func collectVariables(from expr: Expression) -> Set<String> {
        var vars = Set<String>()
        collectVariablesRecursive(from: expr, into: &vars)
        return vars
    }

    private func collectVariablesRecursive(from expr: Expression, into vars: inout Set<String>) {
        switch expr {
        case .variable(let v):
            vars.insert(v.name)
        case .column(let col):
            // In GRAPH_TABLE context, columns might reference pattern variables
            vars.insert(col.column)
        case .add(let l, let r), .subtract(let l, let r), .multiply(let l, let r),
             .divide(let l, let r), .equal(let l, let r), .and(let l, let r), .or(let l, let r):
            collectVariablesRecursive(from: l, into: &vars)
            collectVariablesRecursive(from: r, into: &vars)
        case .not(let e), .negate(let e), .isNull(let e), .isNotNull(let e):
            collectVariablesRecursive(from: e, into: &vars)
        case .function(let call):
            for arg in call.arguments {
                collectVariablesRecursive(from: arg, into: &vars)
            }
        default:
            break
        }
    }
}

/// Graph table validation errors
public enum GraphTableValidationError: Error, Sendable, Equatable {
    case patternError(PatternValidationError)
    case undefinedVariable(String, in: String)
    case duplicateColumnAlias(String)
    case emptyPattern
}

// MARK: - Path Finding Extensions

extension GraphTableSource {
    /// Create a shortest path query
    public static func shortestPath(
        graph: String,
        from source: NodePattern,
        via edgeLabel: String,
        to target: NodePattern,
        maxHops: Int? = nil
    ) -> GraphTableSource {
        let quantifier: PathQuantifier = maxHops.map { .range(min: 1, max: $0) } ?? .oneOrMore
        let edge = EdgePattern(labels: [edgeLabel], direction: .outgoing)

        return GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(
                    elements: [
                        .node(source),
                        .quantified(
                            PathPattern(elements: [.edge(edge), .node(NodePattern())]),
                            quantifier: quantifier
                        ),
                        .node(target)
                    ],
                    mode: .anyShortest
                )
            ])
        )
    }

    /// Create an all shortest paths query
    public static func allShortestPaths(
        graph: String,
        from source: NodePattern,
        via edgeLabel: String,
        to target: NodePattern
    ) -> GraphTableSource {
        let edge = EdgePattern(labels: [edgeLabel], direction: .outgoing)

        return GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(
                    elements: [
                        .node(source),
                        .quantified(
                            PathPattern(elements: [.edge(edge), .node(NodePattern())]),
                            quantifier: .oneOrMore
                        ),
                        .node(target)
                    ],
                    mode: .allShortest
                )
            ])
        )
    }

    /// Create a reachability query
    public static func reachable(
        graph: String,
        from source: NodePattern,
        via edgeLabel: String,
        maxDepth: Int? = nil
    ) -> GraphTableSource {
        let quantifier: PathQuantifier = maxDepth.map { .range(min: 1, max: $0) } ?? .oneOrMore
        let edge = EdgePattern(labels: [edgeLabel], direction: .outgoing)

        return GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(
                    elements: [
                        .node(source),
                        .quantified(
                            PathPattern(elements: [.edge(edge), .node(NodePattern())]),
                            quantifier: quantifier
                        )
                    ],
                    mode: .simple  // No cycles
                )
            ])
        )
    }
}

// MARK: - Common Pattern Templates

extension GraphTableSource {
    /// Friend-of-friend pattern (2 hops)
    public static func friendOfFriend(
        graph: String,
        person: String,
        friendEdge: String = "FRIEND"
    ) -> GraphTableSource {
        GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(
                    elements: [
                        .n("p", labels: ["Person"], properties: [("id", .string(person))]),
                        .outgoing(label: friendEdge),
                        .n("f1", label: "Person"),
                        .outgoing(label: friendEdge),
                        .n("f2", label: "Person")
                    ]
                )
            ]),
            columns: [
                GraphTableColumn(expression: .col("f2", "id"), alias: "friend_of_friend"),
                GraphTableColumn(expression: .col("f1", "id"), alias: "via")
            ]
        )
    }

    /// Triangle pattern (for community detection)
    public static func triangle(
        graph: String,
        edgeLabel: String
    ) -> GraphTableSource {
        GraphTableSource(
            graphName: graph,
            matchPattern: MatchPattern(paths: [
                PathPattern(
                    elements: [
                        .n("a"),
                        .outgoing(label: edgeLabel),
                        .n("b"),
                        .outgoing(label: edgeLabel),
                        .n("c"),
                        .outgoing(label: edgeLabel),
                        .n("a")  // Back to start
                    ]
                )
            ]),
            columns: [
                GraphTableColumn(expression: .col("a", "id"), alias: "node1"),
                GraphTableColumn(expression: .col("b", "id"), alias: "node2"),
                GraphTableColumn(expression: .col("c", "id"), alias: "node3")
            ]
        )
    }
}
