// PathPatternQuery.swift
// GraphIndex - Variable-length path query API
//
// Provides query builder for variable-length path patterns.

import DatabaseKit
import DatabaseEngine
import StorageKit

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
    private let index: DeclaredPropertyGraphIndex

    private var sourceNode: String?
    private var targetNode: String?
    private var edgeLabelFilter: String?
    private var pathLengthValue: PathLength = .one
    private var limitCount: Int = 1000
    private var maxNodesValue: Int = 10_000

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex
    ) {
        self.queryContext = queryContext
        self.index = index
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
    public func execute() async throws -> [GraphPath] {
        guard let source = sourceNode else {
            throw PathPatternQueryError.missingSource
        }
        try validateLimits()

        return try await queryContext.withTransaction { transaction in
            guard let resolvedIndex = try await PropertyGraphIndexResolver
                .resolve(
                    index,
                    for: T.self,
                    in: queryContext,
                    transaction: transaction
                ) else {
                return []
            }
            let snapshot = GraphReadSnapshot(
                transaction: transaction.storageTransaction,
                monotonicClock: queryContext.context.container.monotonicClock
            )
            return try await executePaths(
                source: .identifier(source),
                target: targetNode.map(GraphIdentity.identifier),
                scanner: resolvedIndex.scanner(snapshot: snapshot),
                transaction: transaction.storageTransaction
            )
        }
    }

    /// Execute and return just the end nodes (without full paths)
    ///
    /// More efficient when you only need to know which nodes are reachable.
    ///
    /// - Returns: Array of node IDs at the end of matching paths
    public func executeNodes() async throws -> [String] {
        guard let source = sourceNode else {
            throw PathPatternQueryError.missingSource
        }
        try validateLimits()

        let identities: [GraphIdentity] = try await queryContext
            .withTransaction { transaction in
            guard let resolvedIndex = try await PropertyGraphIndexResolver
                .resolve(
                    index,
                    for: T.self,
                    in: queryContext,
                    transaction: transaction
                ) else {
                return []
            }
            let snapshot = GraphReadSnapshot(
                transaction: transaction.storageTransaction,
                monotonicClock: queryContext.context.container.monotonicClock
            )
            return try await executeEndNodes(
                source: .identifier(source),
                target: targetNode.map(GraphIdentity.identifier),
                scanner: resolvedIndex.scanner(snapshot: snapshot),
                transaction: transaction.storageTransaction
            )
        }
        return try identities.map { try $0.requirePropertyGraphIdentifier() }
    }

    /// Count paths matching the pattern
    ///
    /// - Returns: Number of matching paths
    public func count() async throws -> Int {
        let paths = try await execute()
        return paths.count
    }

    // MARK: - Private Methods

    private func validateLimits() throws {
        guard limitCount > 0 else {
            throw PathPatternQueryError.invalidLimit(limitCount)
        }
        guard maxNodesValue > 0 else {
            throw PathPatternQueryError.invalidMaximumNodes(maxNodesValue)
        }
    }

    private func executePaths(
        source: GraphIdentity,
        target: GraphIdentity?,
        scanner: GraphEdgeScanner,
        transaction: any TransactionAccess
    ) async throws -> [GraphPath] {
        typealias PartialPath = (nodes: [GraphIdentity], edges: [GraphIdentity])
        let maximumDepth = pathLengthValue.effectiveMax(defaultLimit: 10)
        var results: [GraphPath] = []
        var exploredNodes: Set<GraphIdentity> = [source]
        var currentPaths: [PartialPath] = [([source], [])]

        if pathLengthValue.matches(0), target == nil || target == source {
            results.append(GraphPath(singleNode: source))
            if results.count == limitCount { return results }
        }
        guard maximumDepth > 0 else { return results }

        for depth in 1...maximumDepth {
            guard !currentPaths.isEmpty else { break }
            var frontier: Set<GraphIdentity> = []
            for path in currentPaths {
                guard let last = path.nodes.last else {
                    throw PathPatternQueryError.inconsistentTraversalState
                }
                frontier.insert(last)
            }
            let newFrontierNodes = frontier.subtracting(exploredNodes)
            guard exploredNodes.count + newFrontierNodes.count <= maxNodesValue else {
                throw PathPatternQueryError.maximumNodesReached(maxNodesValue)
            }
            exploredNodes.formUnion(frontier)

            var pathsBySource: [GraphIdentity: [PartialPath]] = [:]
            for path in currentPaths {
                guard let last = path.nodes.last else {
                    throw PathPatternQueryError.inconsistentTraversalState
                }
                pathsBySource[last, default: []].append(path)
            }
            var nextPaths: [PartialPath] = []

            let edgeSequence = scanner.batchScanOutgoing(
                from: Array(frontier),
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier),
                transaction: transaction
            )
            var edgeCursor = edgeSequence.makeCursor()
            while let edge = try await edgeCursor.next() {
                guard let sourcePaths = pathsBySource[edge.source] else {
                    throw PathPatternQueryError.inconsistentTraversalState
                }
                for path in sourcePaths {
                    guard !path.nodes.contains(edge.target) else { continue }
                    var nodes = path.nodes
                    nodes.append(edge.target)
                    var labels = path.edges
                    labels.append(edge.edgeLabel)

                    if pathLengthValue.matches(depth), target == nil || target == edge.target {
                        results.append(
                            try GraphPath(
                                nodeIDs: nodes,
                                edgeLabels: labels,
                                weights: nil
                            )
                        )
                        if results.count == limitCount { return results }
                    }
                    if depth < maximumDepth {
                        nextPaths.append((nodes, labels))
                    }
                }
            }
            currentPaths = nextPaths
        }

        return results
    }

    private func executeEndNodes(
        source: GraphIdentity,
        target: GraphIdentity?,
        scanner: GraphEdgeScanner,
        transaction: any TransactionAccess
    ) async throws -> [GraphIdentity] {
        let maximumDepth = pathLengthValue.effectiveMax(defaultLimit: 10)
        var orderedResults: [GraphIdentity] = []
        var resultSet: Set<GraphIdentity> = []
        var visited: Set<GraphIdentity> = [source]
        var currentLevel: Set<GraphIdentity> = [source]

        if pathLengthValue.matches(0), target == nil || target == source {
            orderedResults.append(source)
            resultSet.insert(source)
            if orderedResults.count == limitCount { return orderedResults }
        }
        guard maximumDepth > 0 else { return orderedResults }

        for depth in 1...maximumDepth {
            guard !currentLevel.isEmpty else { break }
            let edges = scanner.batchScanOutgoing(
                from: Array(currentLevel),
                edgeLabel: edgeLabelFilter.map(GraphIdentity.identifier),
                transaction: transaction
            )
            var nextLevel: Set<GraphIdentity> = []

            var edgeCursor = edges.makeCursor()
            while let edge = try await edgeCursor.next() {
                guard !visited.contains(edge.target) else { continue }
                guard visited.count < maxNodesValue else {
                    throw PathPatternQueryError.maximumNodesReached(maxNodesValue)
                }
                visited.insert(edge.target)
                nextLevel.insert(edge.target)

                if pathLengthValue.matches(depth),
                   (target == nil || target == edge.target),
                   resultSet.insert(edge.target).inserted {
                    orderedResults.append(edge.target)
                    if orderedResults.count == limitCount { return orderedResults }
                }
            }
            currentLevel = nextLevel
        }

        return orderedResults
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
        _ from: Field<T, V1>,
        _ edge: Field<T, V2>,
        _ to: Field<T, V3>
    ) throws -> PathPatternQueryBuilder<T> {
        return PathPatternQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                signature: PropertyGraphIndexSignature(
                    sourceFieldName: from.name,
                    labelFieldName: edge.name,
                    targetFieldName: to.name
                ),
                for: T.self,
                in: queryContext
            )
        )
    }

    /// Select an entity-owned graph index by its exact declared name.
    public func index(named indexName: String) throws -> PathPatternQueryBuilder<T> {
        PathPatternQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                named: indexName,
                for: T.self,
                in: queryContext
            )
        )
    }

    /// Use the default graph index
    public func defaultIndex() throws -> PathPatternQueryBuilder<T> {
        return PathPatternQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.unique(
                for: T.self,
                in: queryContext
            )
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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
    case invalidLimit(Int)
    case invalidMaximumNodes(Int)
    case maximumNodesReached(Int)
    case inconsistentTraversalState

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .missingSource:
            return "Missing source node. Use .from() to specify the source."
        case .invalidLimit(let limit):
            return "Path result limit must be positive: \(limit)"
        case .invalidMaximumNodes(let maximum):
            return "Maximum explored nodes must be positive: \(maximum)"
        case .maximumNodesReached(let maximum):
            return "Path traversal reached its maximum node limit: \(maximum)"
        case .inconsistentTraversalState:
            return "Path traversal produced an empty partial path"
        }
    }
}
