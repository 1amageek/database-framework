// GraphTraverser.swift
// GraphIndex - Graph traversal API for graph indexes (adjacency strategy)
//
// Provides efficient graph traversal operations using graph indexes.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - GraphTraverserConfiguration

/// Configuration for graph traversal operations
///
/// Provides tunable parameters for BFS traversal behavior.
///
/// **Usage**:
/// ```swift
/// let config = GraphTraverserConfiguration(
///     batchSize: 200,       // Process more nodes per transaction
///     maxDepth: 5,          // Allow deeper traversal
///     maxNodes: 50000       // Allow more nodes
/// )
///
/// let traverser = GraphTraverser<Edge>(
///     database: database,
///     subspace: subspace,
///     configuration: config
/// )
/// ```
public struct GraphTraverserConfiguration: Sendable {
    /// Batch size for transaction processing
    ///
    /// Larger values reduce transaction count but increase memory usage.
    /// Default: 100
    public let batchSize: Int

    /// Default maximum traversal depth for `traverse()` method
    ///
    /// Can be overridden per-call.
    /// Default: 3
    public let defaultMaxDepth: Int

    /// Default maximum nodes to visit for `traverse()` method
    ///
    /// Can be overridden per-call.
    /// Default: 10000
    public let defaultMaxNodes: Int

    /// Default configuration
    public static let `default` = GraphTraverserConfiguration()

    /// Initialize with custom parameters
    ///
    /// - Parameters:
    ///   - batchSize: Batch size for transaction processing (default: 100)
    ///   - defaultMaxDepth: Default maximum traversal depth (default: 3)
    ///   - defaultMaxNodes: Default maximum nodes to visit (default: 10000)
    public init(
        batchSize: Int = 100,
        defaultMaxDepth: Int = 3,
        defaultMaxNodes: Int = 10000
    ) {
        self.batchSize = batchSize
        self.defaultMaxDepth = defaultMaxDepth
        self.defaultMaxNodes = defaultMaxNodes
    }
}

// MARK: - GraphTraverser

/// Graph traverser for adjacency index queries
///
/// Provides efficient graph traversal operations using FDB range scans.
/// All traversals use snapshot reads for large result sets.
///
/// **Key Features**:
/// - 1-hop neighbor queries (single range scan)
/// - Multi-hop BFS traversal
/// - Bidirectional BFS (when index supports it)
/// - Resumable traversal with continuation tokens
/// - Configurable batch size, depth, and node limits
///
/// **Usage**:
/// ```swift
/// let traverser = GraphTraverser<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// // Find direct neighbors
/// for try await edge in traverser.neighbors(from: "user123", label: "FOLLOWS") {
///     print(edge)
/// }
///
/// // Multi-hop traversal
/// for try await (depth, nodeID) in traverser.traverse(from: "user123", maxDepth: 3) {
///     print("Depth \(depth): \(nodeID)")
/// }
/// ```
public final class GraphTraverser<Edge: Persistable>: Sendable {
    // MARK: - Types

    /// Direction for edge traversal
    public enum Direction: Sendable {
        /// Follow edges from source to target
        case outgoing

        /// Follow edges from target to source (requires bidirectional index)
        case incoming
    }

    /// Edge information exposed during traversal
    ///
    /// Contains the actual edge label from the graph index, not the filter condition.
    /// This allows traversal callbacks to access the real edge metadata.
    public struct TraversalEdgeInfo: Sendable {
        /// Source node ID
        public let source: String

        /// Target node ID
        public let target: String

        /// Edge label from the graph index.
        ///
        /// This is the actual stored label, not the filter parameter:
        /// - Empty string `""` for unlabeled graphs
        /// - The actual label string for labeled graphs
        ///
        /// **Note**: This is NOT nil even for unlabeled edges.
        /// Use `.isEmpty` to check for unlabeled edges.
        public let edgeLabel: String

        /// Distance from the starting node in the traversal
        public let distance: Int

        public init(source: String, target: String, edgeLabel: String, distance: Int) {
            self.source = source
            self.target = target
            self.edgeLabel = edgeLabel
            self.distance = distance
        }
    }

    /// Cursor for deterministic pagination in graph traversal
    ///
    /// Provides deterministic pagination by tracking
    /// the exact position within sorted node lists.
    ///
    /// **Determinism Guarantee**:
    /// - Nodes at each depth are sorted alphabetically
    /// - Cursor tracks (depth, nodeIndex) position
    /// - Same parameters always produce same ordering
    ///
    /// **Usage**:
    /// ```swift
    /// var cursor: TraversalCursor? = nil
    /// var allNodes: [String] = []
    ///
    /// repeat {
    ///     let page = try await traverser.traversePaginated(
    ///         from: "root",
    ///         pageSize: 100,
    ///         cursor: cursor
    ///     )
    ///     allNodes.append(contentsOf: page.nodes)
    ///     cursor = page.nextCursor
    /// } while cursor != nil
    /// ```
    public struct TraversalCursor: Sendable, Codable, Equatable {
        /// Current depth level (0 = start node)
        public let depth: Int

        /// Index within the sorted node list at this depth
        public let nodeIndex: Int

        /// Hash of traversal parameters for validation
        ///
        /// Used to detect mismatched cursors (e.g., using cursor from
        /// different start node or label filter).
        internal let parametersHash: UInt64

        public init(depth: Int, nodeIndex: Int, parametersHash: UInt64) {
            self.depth = depth
            self.nodeIndex = nodeIndex
            self.parametersHash = parametersHash
        }

        /// Compute hash of traversal parameters for cursor validation
        internal static func hashParameters(
            startNode: String,
            edgeLabel: String?,
            direction: Direction,
            maxDepth: Int
        ) -> UInt64 {
            var hasher = Hasher()
            hasher.combine(startNode)
            hasher.combine(edgeLabel)
            hasher.combine(direction == .outgoing ? 0 : 1)
            hasher.combine(maxDepth)
            return UInt64(bitPattern: Int64(hasher.finalize()))
        }
    }

    /// Result of paginated graph traversal with deterministic ordering
    ///
    /// Provides cursor-based pagination that guarantees deterministic results.
    public struct TraversalPage: Sendable {
        /// Nodes returned in this page (sorted alphabetically within each depth)
        public let nodes: [String]

        /// Edges discovered during traversal.
        ///
        /// Contains the actual edge labels from the index:
        /// - Empty string `""` for unlabeled graphs
        /// - The actual label string for labeled graphs
        public let edges: [(source: String, target: String, label: String)]

        /// Current depth level being traversed
        public let depth: Int

        /// Cursor for fetching the next page (nil if no more results)
        public let nextCursor: TraversalCursor?

        /// Whether there are more results to fetch
        public var hasMore: Bool { nextCursor != nil }

        /// Whether the traversal completed without hitting limits
        public let isComplete: Bool

        /// Reason for incompleteness (if any)
        public let limitReason: LimitReason?

        public init(
            nodes: [String],
            edges: [(source: String, target: String, label: String)],
            depth: Int,
            nextCursor: TraversalCursor?,
            isComplete: Bool,
            limitReason: LimitReason?
        ) {
            self.nodes = nodes
            self.edges = edges
            self.depth = depth
            self.nextCursor = nextCursor
            self.isComplete = isComplete
            self.limitReason = limitReason
        }
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let subspace: Subspace

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration for traversal behavior
    private let configuration: GraphTraverserConfiguration

    // MARK: - Initialization

    /// Initialize graph traverser
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - configuration: Traversal configuration (default: `.default`)
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        configuration: GraphTraverserConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(indexSubspace: subspace)
    }

    // MARK: - 1-Hop Queries

    /// Find direct neighbors from a node using GraphEdgeScanner
    ///
    /// Performs a single range scan on the adjacency index.
    /// Returns edges matching the source node and optional label filter.
    /// Uses snapshot reads for better performance on large result sets.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - label: Optional edge label filter (nil = all labels)
    ///   - direction: Traversal direction (default: outgoing)
    /// - Returns: AsyncThrowingStream of target node IDs
    public func neighbors(
        from nodeID: String,
        label: String? = nil,
        direction: Direction = .outgoing
    ) -> AsyncThrowingStream<String, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await database.withTransaction(configuration: .default) { transaction in
                        if direction == .outgoing {
                            for try await edgeInfo in self.scanner.scanOutgoing(
                                from: nodeID,
                                edgeLabel: label,
                                transaction: transaction
                            ) {
                                continuation.yield(edgeInfo.target)
                            }
                        } else {
                            for try await edgeInfo in self.scanner.scanIncoming(
                                to: nodeID,
                                edgeLabel: label,
                                transaction: transaction
                            ) {
                                continuation.yield(edgeInfo.source)
                            }
                        }
                        return ()
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Find direct neighbors with edge information
    ///
    /// Similar to `neighbors(from:)` but provides full edge metadata
    /// including the actual edge label from the index.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - label: Optional edge label filter (nil = all labels)
    ///   - direction: Traversal direction (default: outgoing)
    /// - Returns: AsyncThrowingStream of TraversalEdgeInfo
    public func neighborsWithEdgeInfo(
        from nodeID: String,
        label: String? = nil,
        direction: Direction = .outgoing
    ) -> AsyncThrowingStream<TraversalEdgeInfo, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await database.withTransaction(configuration: .default) { transaction in
                        if direction == .outgoing {
                            for try await edgeInfo in self.scanner.scanOutgoing(
                                from: nodeID,
                                edgeLabel: label,
                                transaction: transaction
                            ) {
                                continuation.yield(TraversalEdgeInfo(
                                    source: edgeInfo.source,
                                    target: edgeInfo.target,
                                    edgeLabel: edgeInfo.edgeLabel,
                                    distance: 1
                                ))
                            }
                        } else {
                            for try await edgeInfo in self.scanner.scanIncoming(
                                to: nodeID,
                                edgeLabel: label,
                                transaction: transaction
                            ) {
                                continuation.yield(TraversalEdgeInfo(
                                    source: edgeInfo.source,
                                    target: edgeInfo.target,
                                    edgeLabel: edgeInfo.edgeLabel,
                                    distance: 1
                                ))
                            }
                        }
                        return ()
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Find direct neighbors with edge data
    ///
    /// Similar to `neighbors(from:)` but also loads the full edge data.
    ///
    /// **Important**: The `edgeLoader` callback receives the **actual** edge label
    /// from the index, not the filter parameter. For unlabeled graphs, this will
    /// be an empty string `""`.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - label: Optional edge label filter (nil = all labels)
    ///   - direction: Traversal direction
    ///   - edgeLoader: Function to load edge data by (source, target, actualLabel)
    /// - Returns: AsyncThrowingStream of edges
    public func neighborsWithEdges(
        from nodeID: String,
        label: String? = nil,
        direction: Direction = .outgoing,
        edgeLoader: @escaping @Sendable (String, String, String) async throws -> Edge?
    ) -> AsyncThrowingStream<Edge, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    // Use neighborsWithEdgeInfo to get actual edge labels
                    for try await edgeInfo in self.neighborsWithEdgeInfo(from: nodeID, label: label, direction: direction) {
                        // Pass the ACTUAL edge label from the index, not the filter
                        if let edge = try await edgeLoader(edgeInfo.source, edgeInfo.target, edgeInfo.edgeLabel) {
                            continuation.yield(edge)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    // MARK: - Multi-Hop Traversal

    /// Traverse graph using BFS with GraphEdgeScanner
    ///
    /// Performs breadth-first traversal up to specified depth.
    /// Uses multiple transactions for large traversals to avoid 5-second limit.
    ///
    /// **Transaction Safety**:
    /// - Each depth level may use a separate transaction
    /// - Snapshot reads used to minimize conflicts
    /// - Results may not be fully consistent across levels
    ///
    /// - Parameters:
    ///   - nodeID: Starting node ID
    ///   - maxDepth: Maximum traversal depth (default: from configuration)
    ///   - label: Optional edge label filter (nil = all labels)
    ///   - direction: Traversal direction
    ///   - maxNodes: Maximum nodes to visit (default: from configuration)
    /// - Returns: AsyncThrowingStream of (depth, nodeID) pairs
    public func traverse(
        from nodeID: String,
        maxDepth: Int? = nil,
        label: String? = nil,
        direction: Direction = .outgoing,
        maxNodes: Int? = nil
    ) -> AsyncThrowingStream<(depth: Int, nodeID: String), Error> {
        let effectiveMaxDepth = maxDepth ?? configuration.defaultMaxDepth
        let effectiveMaxNodes = maxNodes ?? configuration.defaultMaxNodes
        let batchSize = configuration.batchSize

        return AsyncThrowingStream { continuation in
            Task {
                do {
                    var visited = Set<String>([nodeID])
                    var currentLevel = Set<String>([nodeID])
                    var depth = 0

                    // Yield starting node at depth 0
                    continuation.yield((depth: 0, nodeID: nodeID))

                    while depth < effectiveMaxDepth && !currentLevel.isEmpty && visited.count < effectiveMaxNodes {
                        depth += 1
                        var nextLevel = Set<String>()

                        // Process current level in batches to avoid transaction limits
                        // Sort nodes for deterministic ordering
                        let nodes = currentLevel.sorted()

                        for batchStart in stride(from: 0, to: nodes.count, by: batchSize) {
                            let batchEnd = min(batchStart + batchSize, nodes.count)
                            let batch = Array(nodes[batchStart..<batchEnd])

                            // Copy current visited set for filtering (Sendable)
                            let currentVisited = visited

                            // Each batch in its own transaction using GraphEdgeScanner
                            let discoveredTargets: [String] = try await database.withTransaction(configuration: .default) { transaction in
                                var targets: [String] = []

                                // Use batch scan from GraphEdgeScanner
                                let edgeInfos: [EdgeInfo]
                                if direction == .outgoing {
                                    edgeInfos = try await self.scanner.batchScanOutgoing(
                                        from: batch,
                                        edgeLabel: label,
                                        transaction: transaction
                                    )
                                } else {
                                    edgeInfos = try await self.scanner.batchScanIncoming(
                                        to: batch,
                                        edgeLabel: label,
                                        transaction: transaction
                                    )
                                }

                                for edgeInfo in edgeInfos {
                                    // Get the neighbor based on direction
                                    let neighbor = direction == .outgoing ? edgeInfo.target : edgeInfo.source

                                    // Filter using copy of visited set
                                    if !currentVisited.contains(neighbor) && !targets.contains(neighbor) {
                                        targets.append(neighbor)

                                        if currentVisited.count + targets.count >= effectiveMaxNodes {
                                            break
                                        }
                                    }
                                }

                                return targets
                            }

                            // Update visited and nextLevel outside transaction
                            for target in discoveredTargets {
                                if !visited.contains(target) {
                                    visited.insert(target)
                                    nextLevel.insert(target)
                                }
                            }

                            if visited.count >= effectiveMaxNodes {
                                break
                            }
                        }

                        // Yield nodes at current depth (sorted for deterministic order)
                        for node in nextLevel.sorted() {
                            continuation.yield((depth: depth, nodeID: node))
                        }

                        currentLevel = nextLevel
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    // MARK: - Paginated Traversal

    /// Traverse graph with deterministic cursor-based pagination
    ///
    /// Provides fully deterministic pagination:
    /// - Nodes at each depth are sorted alphabetically
    /// - Cursor tracks exact (depth, index) position
    /// - Same parameters always produce same results
    ///
    /// **Usage**:
    /// ```swift
    /// var cursor: TraversalCursor? = nil
    /// var allNodes: [String] = []
    ///
    /// repeat {
    ///     let page = try await traverser.traversePaginated(
    ///         from: "root",
    ///         pageSize: 100,
    ///         cursor: cursor
    ///     )
    ///     allNodes.append(contentsOf: page.nodes)
    ///     cursor = page.nextCursor
    /// } while cursor != nil
    /// ```
    ///
    /// - Parameters:
    ///   - nodeID: Starting node ID
    ///   - edgeLabel: Optional edge label filter (nil = all labels)
    ///   - direction: Traversal direction
    ///   - maxDepth: Maximum traversal depth (default: from configuration)
    ///   - pageSize: Maximum nodes to return per page
    ///   - cursor: Cursor from previous call to resume (nil for first page)
    /// - Returns: TraversalPage with nodes and next cursor
    /// - Throws: Error if cursor parameters don't match current query
    public func traversePaginated(
        from nodeID: String,
        edgeLabel: String? = nil,
        direction: Direction = .outgoing,
        maxDepth: Int? = nil,
        pageSize: Int = 100,
        cursor: TraversalCursor? = nil
    ) async throws -> TraversalPage {
        let effectiveMaxDepth = maxDepth ?? configuration.defaultMaxDepth
        let batchSize = configuration.batchSize

        // Compute parameters hash for cursor validation
        let paramsHash = TraversalCursor.hashParameters(
            startNode: nodeID,
            edgeLabel: edgeLabel,
            direction: direction,
            maxDepth: effectiveMaxDepth
        )

        // Validate cursor if provided
        if let cursor = cursor, cursor.parametersHash != paramsHash {
            throw TraversalError.invalidCursor("Cursor parameters do not match current query")
        }

        // Resume state from cursor
        let startDepth = cursor?.depth ?? 0
        let startIndex = cursor?.nodeIndex ?? 0

        var resultNodes: [String] = []
        var resultEdges: [(source: String, target: String, label: String)] = []
        var visited = Set<String>([nodeID])
        var currentDepth = 0
        var currentLevelSorted: [String] = [nodeID]
        var totalNodesExplored = 0
        let maxNodesLimit = configuration.defaultMaxNodes

        // Track edges for each node (neighbor -> edge info)
        var nodeToEdge: [String: (source: String, target: String, label: String)] = [:]

        // If starting from beginning, include start node
        if startDepth == 0 && startIndex == 0 {
            resultNodes.append(nodeID)
        }

        // Build up state to the cursor position
        while currentDepth < effectiveMaxDepth && !currentLevelSorted.isEmpty {
            currentDepth += 1

            // Collect all neighbors for current level
            var nextLevelSet = Set<String>()
            nodeToEdge.removeAll()

            for batchStart in stride(from: 0, to: currentLevelSorted.count, by: batchSize) {
                let batchEnd = min(batchStart + batchSize, currentLevelSorted.count)
                let batch = Array(currentLevelSorted[batchStart..<batchEnd])

                let currentVisited = visited

                // Return both neighbor and edge info
                let discoveredEdges: [(neighbor: String, source: String, target: String, label: String)] = try await database.withTransaction(configuration: .default) { transaction in
                    var edges: [(neighbor: String, source: String, target: String, label: String)] = []

                    let edgeInfos: [EdgeInfo]
                    if direction == .outgoing {
                        edgeInfos = try await self.scanner.batchScanOutgoing(
                            from: batch,
                            edgeLabel: edgeLabel,
                            transaction: transaction
                        )
                    } else {
                        edgeInfos = try await self.scanner.batchScanIncoming(
                            to: batch,
                            edgeLabel: edgeLabel,
                            transaction: transaction
                        )
                    }

                    var seenNeighbors = Set<String>()
                    for edgeInfo in edgeInfos {
                        let neighbor = direction == .outgoing ? edgeInfo.target : edgeInfo.source
                        if !currentVisited.contains(neighbor) && !seenNeighbors.contains(neighbor) {
                            seenNeighbors.insert(neighbor)
                            edges.append((
                                neighbor: neighbor,
                                source: edgeInfo.source,
                                target: edgeInfo.target,
                                label: edgeInfo.edgeLabel
                            ))
                        }
                    }

                    return edges
                }

                for edge in discoveredEdges {
                    if !visited.contains(edge.neighbor) {
                        visited.insert(edge.neighbor)
                        nextLevelSet.insert(edge.neighbor)
                        nodeToEdge[edge.neighbor] = (source: edge.source, target: edge.target, label: edge.label)
                        totalNodesExplored += 1
                    }
                }

                if totalNodesExplored >= maxNodesLimit {
                    break
                }
            }

            // Sort for deterministic ordering
            let nextLevelSorted = nextLevelSet.sorted()

            // Check if we've reached the cursor's depth
            if currentDepth < startDepth {
                // Haven't reached cursor depth yet, continue building state
                currentLevelSorted = nextLevelSorted
                continue
            } else if currentDepth == startDepth && cursor != nil {
                // At cursor depth, start from specified index
                let nodesToProcess = Array(nextLevelSorted.dropFirst(startIndex))

                for node in nodesToProcess {
                    if resultNodes.count >= pageSize {
                        // Page full, create cursor for next page
                        let nextIndex = startIndex + resultNodes.count
                        let nextCursor = TraversalCursor(
                            depth: currentDepth,
                            nodeIndex: nextIndex,
                            parametersHash: paramsHash
                        )
                        return TraversalPage(
                            nodes: resultNodes,
                            edges: resultEdges,
                            depth: currentDepth,
                            nextCursor: nextCursor,
                            isComplete: false,
                            limitReason: nil
                        )
                    }
                    resultNodes.append(node)
                    if let edge = nodeToEdge[node] {
                        resultEdges.append(edge)
                    }
                }

                // Finished this depth, move to next
                currentLevelSorted = nextLevelSorted

            } else {
                // Past cursor depth (or no cursor), process normally
                for (index, node) in nextLevelSorted.enumerated() {
                    if resultNodes.count >= pageSize {
                        // Page full, create cursor for next page
                        let nextCursor = TraversalCursor(
                            depth: currentDepth,
                            nodeIndex: index,
                            parametersHash: paramsHash
                        )
                        return TraversalPage(
                            nodes: resultNodes,
                            edges: resultEdges,
                            depth: currentDepth,
                            nextCursor: nextCursor,
                            isComplete: false,
                            limitReason: nil
                        )
                    }
                    resultNodes.append(node)
                    if let edge = nodeToEdge[node] {
                        resultEdges.append(edge)
                    }
                }

                currentLevelSorted = nextLevelSorted
            }

            // Check limits
            if totalNodesExplored >= maxNodesLimit {
                return TraversalPage(
                    nodes: resultNodes,
                    edges: resultEdges,
                    depth: currentDepth,
                    nextCursor: nil,
                    isComplete: false,
                    limitReason: .maxNodesReached(explored: totalNodesExplored, limit: maxNodesLimit)
                )
            }
        }

        // Traversal complete
        return TraversalPage(
            nodes: resultNodes,
            edges: resultEdges,
            depth: currentDepth,
            nextCursor: nil,
            isComplete: true,
            limitReason: nil
        )
    }
}

// MARK: - TraversalError

/// Errors that can occur during graph traversal
public enum TraversalError: Error, Sendable {
    /// Cursor parameters don't match current query
    case invalidCursor(String)
}
