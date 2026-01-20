// ShortestPathQuery.swift
// GraphIndex - Query API for shortest path algorithms
//
// Provides FDBContext extension and fluent query builder for shortest path queries.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - ShortestPathEntryPoint

/// Entry point for shortest path queries
///
/// **Usage**:
/// ```swift
/// import GraphIndex
///
/// let result = try await context.shortestPath(Edge.self)
///     .index(\.source, \.label, \.target)
///     .from("alice")
///     .to("bob")
///     .via("follows")
///     .execute()
/// ```
public struct ShortestPathEntryPoint<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the graph index fields
    ///
    /// - Parameters:
    ///   - from: KeyPath to the source/subject field
    ///   - edge: KeyPath to the edge/predicate field
    ///   - to: KeyPath to the target/object field
    /// - Returns: Shortest path query builder
    public func index<V1, V2, V3>(
        _ from: KeyPath<T, V1>,
        _ edge: KeyPath<T, V2>,
        _ to: KeyPath<T, V3>
    ) -> ShortestPathQueryBuilder<T> {
        let fromField = T.fieldName(for: from)
        let edgeField = T.fieldName(for: edge)
        let toField = T.fieldName(for: to)
        return ShortestPathQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromField,
            edgeFieldName: edgeField,
            toFieldName: toField
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: Shortest path query builder configured with the default index
    public func defaultIndex() -> ShortestPathQueryBuilder<T> {
        let descriptor = T.indexDescriptors.first { desc in
            desc.kindIdentifier == GraphIndexKind<T>.identifier
        }

        guard let desc = descriptor,
              let kind = desc.kind as? GraphIndexKind<T> else {
            // Return a builder that will fail on execute
            return ShortestPathQueryBuilder(
                queryContext: queryContext,
                fromFieldName: "",
                edgeFieldName: "",
                toFieldName: ""
            )
        }

        return ShortestPathQueryBuilder(
            queryContext: queryContext,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField
        )
    }
}

// MARK: - ShortestPathQueryBuilder

/// Query builder for shortest path
///
/// Provides a fluent API for configuring and executing shortest path queries.
///
/// **Usage**:
/// ```swift
/// // Find shortest path
/// let result = try await context.shortestPath(Edge.self)
///     .index(\.source, \.label, \.target)
///     .from("alice")
///     .to("bob")
///     .via("follows")
///     .maxDepth(10)
///     .bidirectional(true)
///     .execute()
///
/// if let path = result.path {
///     print("Distance: \(path.length)")
///     print("Path: \(path.nodeIDs.joined(separator: " -> "))")
/// }
///
/// // Check if connected
/// let connected = try await context.shortestPath(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .to("bob")
///     .isConnected()
/// ```
public struct ShortestPathQueryBuilder<T: Persistable>: Sendable {

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    private var sourceNode: String?
    private var targetNode: String?
    private var edgeLabelFilter: String?
    private var configMaxDepth: Int = 10
    private var configBidirectional: Bool = true
    private var configBatchSize: Int = 100
    private var configMaxNodes: Int = 100_000

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

    /// Set source node
    ///
    /// - Parameter nodeID: Source node ID
    /// - Returns: Updated builder
    public func from(_ nodeID: String) -> Self {
        var copy = self
        copy.sourceNode = nodeID
        return copy
    }

    /// Set target node
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

    /// Set maximum search depth
    ///
    /// - Parameter depth: Maximum number of hops
    /// - Returns: Updated builder
    public func maxDepth(_ depth: Int) -> Self {
        var copy = self
        copy.configMaxDepth = depth
        return copy
    }

    /// Enable/disable bidirectional BFS
    ///
    /// Bidirectional BFS is significantly faster for long paths
    /// but requires the graph to support incoming edge lookups.
    ///
    /// - Parameter enabled: true to use bidirectional BFS
    /// - Returns: Updated builder
    public func bidirectional(_ enabled: Bool) -> Self {
        var copy = self
        copy.configBidirectional = enabled
        return copy
    }

    /// Set batch size for transaction operations
    ///
    /// - Parameter size: Number of nodes per batch
    /// - Returns: Updated builder
    public func batchSize(_ size: Int) -> Self {
        var copy = self
        copy.configBatchSize = size
        return copy
    }

    /// Set maximum nodes to explore
    ///
    /// - Parameter count: Maximum number of nodes
    /// - Returns: Updated builder
    public func maxNodesExplored(_ count: Int) -> Self {
        var copy = self
        copy.configMaxNodes = count
        return copy
    }

    /// Apply a configuration preset
    ///
    /// - Parameter configuration: Configuration to apply
    /// - Returns: Updated builder
    public func configuration(_ configuration: ShortestPathConfiguration) -> Self {
        var copy = self
        copy.configMaxDepth = configuration.maxDepth
        copy.configBidirectional = configuration.bidirectional
        copy.configBatchSize = configuration.batchSize
        copy.configMaxNodes = configuration.maxNodesExplored
        return copy
    }

    // MARK: - Execution

    /// Execute the shortest path query
    ///
    /// - Returns: ShortestPathResult containing the path or nil if not connected
    /// - Throws: ShortestPathQueryError if configuration is invalid
    public func execute() async throws -> ShortestPathResult<T> {
        guard !fromFieldName.isEmpty else {
            throw ShortestPathQueryError.indexNotConfigured
        }

        guard let source = sourceNode else {
            throw ShortestPathQueryError.missingSource
        }

        guard let target = targetNode else {
            throw ShortestPathQueryError.missingTarget
        }

        let indexSubspace = try await getIndexSubspace()

        let config = ShortestPathConfiguration(
            maxDepth: configMaxDepth,
            bidirectional: configBidirectional,
            batchSize: configBatchSize,
            maxNodesExplored: configMaxNodes
        )

        let finder = ShortestPathFinder<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: config
        )

        return try await finder.findShortestPath(
            from: source,
            to: target,
            edgeLabel: edgeLabelFilter
        )
    }

    /// Find all shortest paths (when multiple exist)
    ///
    /// - Returns: AllShortestPathsResult containing all shortest paths
    /// - Throws: ShortestPathQueryError if configuration is invalid
    public func executeAll() async throws -> AllShortestPathsResult<T> {
        guard !fromFieldName.isEmpty else {
            throw ShortestPathQueryError.indexNotConfigured
        }

        guard let source = sourceNode else {
            throw ShortestPathQueryError.missingSource
        }

        guard let target = targetNode else {
            throw ShortestPathQueryError.missingTarget
        }

        let indexSubspace = try await getIndexSubspace()

        let config = ShortestPathConfiguration(
            maxDepth: configMaxDepth,
            bidirectional: false,  // All paths requires unidirectional BFS
            batchSize: configBatchSize,
            maxNodesExplored: configMaxNodes
        )

        let finder = ShortestPathFinder<T>(
            database: queryContext.context.container.database,
            subspace: indexSubspace,
            configuration: config
        )

        return try await finder.findAllShortestPaths(
            from: source,
            to: target,
            edgeLabel: edgeLabelFilter,
            maxDepth: configMaxDepth
        )
    }

    /// Check if source and target are connected
    ///
    /// More efficient than execute() when you only need to know
    /// if a connection exists.
    ///
    /// - Returns: true if connected, false otherwise
    public func isConnected() async throws -> Bool {
        let result = try await execute()
        return result.isConnected
    }

    /// Get the distance (hop count) between source and target
    ///
    /// - Returns: Distance, or nil if not connected
    public func distance() async throws -> Int? {
        let result = try await execute()
        return result.distance.map { Int($0) }
    }

    // MARK: - Private Methods

    private func getIndexSubspace() async throws -> Subspace {
        let indexName = "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"

        guard let _ = queryContext.schema.indexDescriptor(named: indexName) else {
            throw ShortestPathQueryError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        return typeSubspace.subspace(indexName)
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a shortest path query
    ///
    /// Find the shortest path between two nodes in a graph.
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Find shortest path
    /// let result = try await context.shortestPath(Edge.self)
    ///     .index(\.source, \.label, \.target)
    ///     .from("alice")
    ///     .to("bob")
    ///     .via("follows")
    ///     .execute()
    ///
    /// if let path = result.path {
    ///     print("Distance: \(path.length)")
    ///     print("Path: \(path.nodeIDs.joined(separator: " -> "))")
    /// } else {
    ///     print("No path exists")
    /// }
    ///
    /// // Using default index
    /// let connected = try await context.shortestPath(Edge.self)
    ///     .defaultIndex()
    ///     .from("alice")
    ///     .to("charlie")
    ///     .isConnected()
    /// ```
    ///
    /// - Parameter type: The Persistable type representing graph edges
    /// - Returns: Entry point for configuring the shortest path query
    public func shortestPath<T: Persistable>(_ type: T.Type) -> ShortestPathEntryPoint<T> {
        ShortestPathEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Errors

/// Errors for shortest path query operations
public enum ShortestPathQueryError: Error, CustomStringConvertible {
    /// Index not configured
    case indexNotConfigured

    /// Index not found
    case indexNotFound(String)

    /// Missing source node
    case missingSource

    /// Missing target node
    case missingTarget

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .missingSource:
            return "Missing source node. Use .from() to specify the source."
        case .missingTarget:
            return "Missing target node. Use .to() to specify the target."
        }
    }
}
