// PathPatternQuery.swift
// GraphIndex - Variable-length path query API
//
// Provides query builder for variable-length path patterns.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - PathPatternQueryBuilder

/// Query builder for variable-length path patterns
///
/// Extends graph queries with support for path length constraints,
/// similar to Cypher's `*min..max` syntax.
///
/// **Usage**:
/// ```swift
/// // Find paths of exactly 2 hops
/// let paths = try await context.pathPattern(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .via("follows")
///     .length(.exactly(2))
///     .execute()
///
/// // Find paths between 2 and 5 hops
/// let paths = try await context.pathPattern(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .length(.range(2, 5))
///     .limit(100)
///     .execute()
///
/// // Find all reachable nodes within 3 hops
/// let paths = try await context.pathPattern(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .length(.atMost(3))
///     .execute()
/// ```
public struct PathPatternQueryBuilder<T: Persistable>: Sendable {

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    private var sourceNode: String?
    private var targetNode: String?
    private var edgeLabelFilter: String?
    private var pathLengthValue: PathLength = .one
    private var limitCount: Int = 1000
    private var maxNodesValue: Int = 10_000

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
    }

    // MARK: - Fluent Configuration

    /// Set source node (required)
    ///
    /// - Parameter nodeID: Source node ID
    /// - Returns: Updated builder
    public func from(_ nodeID: String) -> Self {
        var copy = self
        copy.sourceNode = nodeID
        return copy
    }

    /// Set optional target node
    ///
    /// If specified, only paths ending at this node are returned.
    /// If not specified, all reachable paths within the length constraint are returned.
    ///
    /// - Parameter nodeID: Target node ID
    /// - Returns: Updated builder
    public func to(_ nodeID: String) -> Self {
        var copy = self
        copy.targetNode = nodeID
        return copy
    }

    /// Filter by edge label
    ///
    /// - Parameter label: Edge label to match
    /// - Returns: Updated builder
    public func via(_ label: String) -> Self {
        var copy = self
        copy.edgeLabelFilter = label
        return copy
    }

    /// Set path length constraint
    ///
    /// **Examples**:
    /// ```swift
    /// .length(.exactly(2))     // Exactly 2 hops
    /// .length(.range(1, 5))    // 1 to 5 hops
    /// .length(.atLeast(3))     // 3 or more hops
    /// .length(.atMost(4))      // 0 to 4 hops
    /// ```
    ///
    /// - Parameter length: Path length specification
    /// - Returns: Updated builder
    public func length(_ length: PathLength) -> Self {
        var copy = self
        copy.pathLengthValue = length
        return copy
    }

    /// Limit the number of paths returned
    ///
    /// - Parameter count: Maximum number of paths
    /// - Returns: Updated builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    /// Set maximum nodes to explore
    ///
    /// - Parameter count: Maximum number of nodes
    /// - Returns: Updated builder
    public func maxNodes(_ count: Int) -> Self {
        var copy = self
        copy.maxNodesValue = count
        return copy
    }

    // MARK: - Execution

    /// Execute and return all matching paths
    ///
    /// - Returns: Array of paths matching the pattern
    /// - Throws: PathPatternQueryError if configuration is invalid
    public func execute() async throws -> [GraphPath<T>] {
        guard !fromFieldName.isEmpty else {
            throw PathPatternQueryError.indexNotConfigured
        }

        guard let source = sourceNode else {
            throw PathPatternQueryError.missingSource
        }

        let indexSubspace = try await getIndexSubspace()
        let effectiveMax = pathLengthValue.effectiveMax(defaultLimit: 10)

        // Use BFS to find all paths within the length constraint
        var paths: [GraphPath<T>] = []
        var visited: Set<String> = []
        var currentPaths: [(path: [String], edges: [String])] = [([source], [])]

        // BFS level by level
        for depth in 0..<effectiveMax {
            guard !currentPaths.isEmpty && paths.count < limitCount else { break }

            var nextPaths: [(path: [String], edges: [String])] = []

            // Process current level paths in batches
            let currentNodes = Set(currentPaths.map { $0.path.last! })

            for node in currentNodes {
                if visited.count >= maxNodesValue { break }

                let neighbors = try await getNeighbors(
                    from: node,
                    edgeLabel: edgeLabelFilter,
                    indexSubspace: indexSubspace
                )

                for (neighborNode, edge) in neighbors {
                    // Find all paths ending at this node
                    for (path, edges) in currentPaths where path.last == node {
                        // Skip if path already contains this node (avoid cycles)
                        guard !path.contains(neighborNode) else { continue }

                        let newPath = path + [neighborNode]
                        let newEdges = edges + [edge]
                        let newDepth = depth + 1

                        // Check if this path matches the length constraint
                        if pathLengthValue.matches(newDepth) {
                            // Check target constraint if specified
                            if let target = targetNode {
                                if neighborNode == target {
                                    let graphPath = GraphPath<T>(
                                        nodeIDs: newPath,
                                        edgeLabels: newEdges,
                                        weights: nil
                                    )
                                    paths.append(graphPath)
                                }
                            } else {
                                let graphPath = GraphPath<T>(
                                    nodeIDs: newPath,
                                    edgeLabels: newEdges,
                                    weights: nil
                                )
                                paths.append(graphPath)
                            }
                        }

                        // Continue exploring if we haven't reached max depth
                        if newDepth < effectiveMax {
                            nextPaths.append((newPath, newEdges))
                        }

                        if paths.count >= limitCount { break }
                    }
                }

                visited.insert(node)
            }

            currentPaths = nextPaths
        }

        return Array(paths.prefix(limitCount))
    }

    /// Execute and return just the end nodes (without full paths)
    ///
    /// More efficient when you only need to know which nodes are reachable.
    ///
    /// - Returns: Array of node IDs at the end of matching paths
    public func executeNodes() async throws -> [String] {
        guard !fromFieldName.isEmpty else {
            throw PathPatternQueryError.indexNotConfigured
        }

        guard let source = sourceNode else {
            throw PathPatternQueryError.missingSource
        }

        let indexSubspace = try await getIndexSubspace()
        let effectiveMax = pathLengthValue.effectiveMax(defaultLimit: 10)

        var resultNodes: Set<String> = []
        var visited: Set<String> = [source]
        var currentLevel: Set<String> = [source]

        // If min is 0, source itself is a result
        if pathLengthValue.matches(0) && (targetNode == nil || targetNode == source) {
            resultNodes.insert(source)
        }

        // BFS level by level
        for depth in 1...effectiveMax {
            guard !currentLevel.isEmpty && resultNodes.count < limitCount else { break }

            var nextLevel: Set<String> = []

            for node in currentLevel {
                if visited.count >= maxNodesValue { break }

                let neighbors = try await getNeighbors(
                    from: node,
                    edgeLabel: edgeLabelFilter,
                    indexSubspace: indexSubspace
                )

                for (neighborNode, _) in neighbors {
                    guard !visited.contains(neighborNode) else { continue }

                    visited.insert(neighborNode)
                    nextLevel.insert(neighborNode)

                    // Check if this depth matches the length constraint
                    if pathLengthValue.matches(depth) {
                        // Check target constraint if specified
                        if let target = targetNode {
                            if neighborNode == target {
                                resultNodes.insert(neighborNode)
                            }
                        } else {
                            resultNodes.insert(neighborNode)
                        }
                    }

                    if resultNodes.count >= limitCount { break }
                }
            }

            currentLevel = nextLevel
        }

        return Array(resultNodes.prefix(limitCount))
    }

    /// Count paths matching the pattern
    ///
    /// - Returns: Number of matching paths
    public func count() async throws -> Int {
        let paths = try await execute()
        return paths.count
    }

    // MARK: - Private Methods

    private func getIndexSubspace() async throws -> Subspace {
        let indexName = "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"

        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            throw PathPatternQueryError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        return typeSubspace.subspace(indexName)
    }

    private func getNeighbors(
        from nodeID: String,
        edgeLabel: String?,
        indexSubspace: Subspace
    ) async throws -> [(node: String, edge: String)] {
        let outgoingSubspace = indexSubspace.subspace(Int64(0))

        var prefixElements: [any TupleElement] = []
        if let label = edgeLabel {
            prefixElements.append(label)
        }
        prefixElements.append(nodeID)

        let prefix = Subspace(prefix: outgoingSubspace.prefix + Tuple(prefixElements).pack())
        let (beginKey, endKey) = prefix.range()

        let hasEdgeLabel = edgeLabel != nil
        let defaultEdge = edgeLabel ?? ""

        return try await queryContext.withTransaction { transaction in
            var results: [(node: String, edge: String)] = []

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                let elements = try prefix.unpack(key)

                guard !elements.isEmpty else { continue }

                let targetIndex = elements.count - 1
                guard let lastElement = elements[targetIndex] else { continue }

                let target: String
                if let str = lastElement as? String {
                    target = str
                } else {
                    target = String(describing: lastElement)
                }

                let edge: String
                if hasEdgeLabel {
                    edge = defaultEdge
                } else if elements.count >= 2, let edgeElement = elements[0] {
                    if let str = edgeElement as? String {
                        edge = str
                    } else {
                        edge = String(describing: edgeElement)
                    }
                } else {
                    edge = ""
                }

                results.append((target, edge))
            }

            return results
        }
    }
}

// MARK: - PathPatternEntryPoint

/// Entry point for variable-length path pattern queries
public struct PathPatternEntryPoint<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the graph index fields
    public func index<V1, V2, V3>(
        _ from: KeyPath<T, V1>,
        _ edge: KeyPath<T, V2>,
        _ to: KeyPath<T, V3>
    ) -> PathPatternQueryBuilder<T> {
        let fromField = T.fieldName(for: from)
        let edgeField = T.fieldName(for: edge)
        let toField = T.fieldName(for: to)
        return PathPatternQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromField,
            edgeFieldName: edgeField,
            toFieldName: toField
        )
    }

    /// Use the default graph index
    public func defaultIndex() -> PathPatternQueryBuilder<T> {
        let descriptor = T.indexDescriptors.first { desc in
            desc.kindIdentifier == GraphIndexKind<T>.identifier
        }

        guard let desc = descriptor,
              let kind = desc.kind as? GraphIndexKind<T> else {
            return PathPatternQueryBuilder(
                queryContext: queryContext,
                fromFieldName: "",
                edgeFieldName: "",
                toFieldName: ""
            )
        }

        return PathPatternQueryBuilder(
            queryContext: queryContext,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a variable-length path pattern query
    ///
    /// Find all paths matching a variable-length pattern.
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Find all paths of 2-5 hops from alice
    /// let paths = try await context.pathPattern(Edge.self)
    ///     .defaultIndex()
    ///     .from("alice")
    ///     .via("follows")
    ///     .length(.range(2, 5))
    ///     .execute()
    ///
    /// for path in paths {
    ///     print(path.nodeIDs.joined(separator: " -> "))
    /// }
    /// ```
    ///
    /// - Parameter type: The Persistable type representing graph edges
    /// - Returns: Entry point for configuring the path pattern query
    public func pathPattern<T: Persistable>(_ type: T.Type) -> PathPatternEntryPoint<T> {
        PathPatternEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Errors

/// Errors for path pattern query operations
public enum PathPatternQueryError: Error, CustomStringConvertible {
    case indexNotConfigured
    case indexNotFound(String)
    case missingSource

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .missingSource:
            return "Missing source node. Use .from() to specify the source."
        }
    }
}
