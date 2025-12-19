// GraphTraverser.swift
// GraphIndex - Graph traversal API for graph indexes (adjacency strategy)
//
// Provides efficient graph traversal operations using graph indexes.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Graph

/// Graph traverser for adjacency index queries
///
/// Provides efficient graph traversal operations using FDB range scans.
///
/// **Key Features**:
/// - 1-hop neighbor queries (single range scan)
/// - Multi-hop BFS traversal
/// - Bidirectional BFS (when index supports it)
/// - Transaction mode selection (strong vs snapshot)
/// - Resumable traversal with continuation tokens
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

    /// Traversal mode for read operations
    public enum TraversalMode: Sendable {
        /// Read-your-writes: sees uncommitted writes in the same transaction
        case strong

        /// Snapshot read: doesn't cause read conflicts, better for large traversals
        case snapshot
    }

    /// Direction for edge traversal
    public enum Direction: Sendable {
        /// Follow edges from source to target
        case outgoing

        /// Follow edges from target to source (requires bidirectional index)
        case incoming
    }

    /// Continuation token for resumable traversal
    public struct ContinuationToken: Sendable, Codable {
        let lastKey: [UInt8]
        let depth: Int
        let visitedCount: Int
    }

    /// Result of a bounded traversal
    public struct TraversalResult: Sendable {
        public let nodes: [String]
        public let edges: [(source: String, target: String, label: String?)]
        public let continuationToken: ContinuationToken?
        public let isComplete: Bool
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let subspace: Subspace

    /// Cached subspace for outgoing edges (computed once at init)
    private let outgoingSubspace: Subspace

    /// Cached subspace for incoming edges (computed once at init)
    private let incomingSubspace: Subspace

    // MARK: - Initialization

    /// Initialize graph traverser
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace
    ) {
        self.database = database
        self.subspace = subspace
        // Cache subspaces at initialization
        // Use integer keys matching GraphIndexMaintainer: 0=out, 1=in
        self.outgoingSubspace = subspace.subspace(Int64(0))
        self.incomingSubspace = subspace.subspace(Int64(1))
    }

    // MARK: - 1-Hop Queries

    /// Find direct neighbors from a node
    ///
    /// Performs a single range scan on the adjacency index.
    /// Returns edges matching the source node and optional label filter.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - label: Optional edge label filter
    ///   - direction: Traversal direction (default: outgoing)
    ///   - mode: Read mode (default: snapshot for large result sets)
    /// - Returns: AsyncThrowingStream of target node IDs
    public func neighbors(
        from nodeID: String,
        label: String? = nil,
        direction: Direction = .outgoing,
        mode: TraversalMode = .snapshot
    ) -> AsyncThrowingStream<String, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await database.withTransaction(configuration: .default) { transaction in
                        let useSnapshot = mode == .snapshot

                        // Build prefix for range scan
                        let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace

                        var prefixElements: [any TupleElement] = []
                        if let label = label {
                            prefixElements.append(label)
                        }
                        prefixElements.append(nodeID)

                        // Note: Don't use subspace.subspace(Tuple(...)) as that treats Tuple as a nested tuple element
                        let prefix = Subspace(prefix: scanSubspace.prefix + Tuple(prefixElements).pack())
                        let (beginKey, endKey) = prefix.range()

                        // Perform range scan
                        let stream = transaction.getRange(
                            beginSelector: .firstGreaterOrEqual(beginKey),
                            endSelector: .firstGreaterOrEqual(endKey),
                            snapshot: useSnapshot
                        )

                        for try await (key, _) in stream {
                            // Extract target node ID from key
                            if let targetID = try self.extractTargetFromKey(key, prefix: prefix) {
                                continuation.yield(targetID)
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
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - label: Optional edge label filter
    ///   - direction: Traversal direction
    ///   - mode: Read mode
    ///   - edgeLoader: Function to load edge data by ID
    /// - Returns: AsyncThrowingStream of edges
    public func neighborsWithEdges(
        from nodeID: String,
        label: String? = nil,
        direction: Direction = .outgoing,
        mode: TraversalMode = .snapshot,
        edgeLoader: @escaping @Sendable (String, String, String?) async throws -> Edge?
    ) -> AsyncThrowingStream<Edge, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    for try await targetID in self.neighbors(from: nodeID, label: label, direction: direction, mode: mode) {
                        if let edge = try await edgeLoader(nodeID, targetID, label) {
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

    /// Traverse graph using BFS
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
    ///   - maxDepth: Maximum traversal depth (default: 3)
    ///   - label: Optional edge label filter
    ///   - direction: Traversal direction
    ///   - mode: Read mode
    ///   - maxNodes: Maximum nodes to visit (default: 10000)
    /// - Returns: AsyncThrowingStream of (depth, nodeID) pairs
    public func traverse(
        from nodeID: String,
        maxDepth: Int = 3,
        label: String? = nil,
        direction: Direction = .outgoing,
        mode: TraversalMode = .snapshot,
        maxNodes: Int = 10000
    ) -> AsyncThrowingStream<(depth: Int, nodeID: String), Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    var visited = Set<String>([nodeID])
                    var currentLevel = Set<String>([nodeID])
                    var depth = 0

                    // Yield starting node at depth 0
                    continuation.yield((depth: 0, nodeID: nodeID))

                    while depth < maxDepth && !currentLevel.isEmpty && visited.count < maxNodes {
                        depth += 1
                        var nextLevel = Set<String>()

                        // Process current level in batches to avoid transaction limits
                        let batchSize = 100
                        let nodes = Array(currentLevel)

                        for batchStart in stride(from: 0, to: nodes.count, by: batchSize) {
                            let batchEnd = min(batchStart + batchSize, nodes.count)
                            let batch = Array(nodes[batchStart..<batchEnd])

                            // Pre-compute scan parameters outside transaction
                            let scanSubspace = direction == .outgoing ? self.outgoingSubspace : self.incomingSubspace
                            let scanParams: [(source: String, beginKey: [UInt8], endKey: [UInt8], prefix: Subspace)] = batch.map { source in
                                var prefixElements: [any TupleElement] = []
                                if let label = label {
                                    prefixElements.append(label)
                                }
                                prefixElements.append(source)
                                let prefix = Subspace(prefix: scanSubspace.prefix + Tuple(prefixElements).pack())
                                let (beginKey, endKey) = prefix.range()
                                return (source, beginKey, endKey, prefix)
                            }

                            // Copy current visited set for filtering (Sendable)
                            let currentVisited = visited

                            // Each batch in its own transaction - returns discovered targets
                            let discoveredTargets: [String] = try await database.withTransaction(configuration: .default) { transaction in
                                var targets: [String] = []

                                for (_, beginKey, endKey, prefix) in scanParams {
                                    let stream = transaction.getRange(
                                        beginSelector: .firstGreaterOrEqual(beginKey),
                                        endSelector: .firstGreaterOrEqual(endKey),
                                        snapshot: mode == .snapshot
                                    )

                                    for try await (key, _) in stream {
                                        if let target = try self.extractTargetFromKey(key, prefix: prefix) {
                                            // Filter using copy of visited set
                                            if !currentVisited.contains(target) && !targets.contains(target) {
                                                targets.append(target)

                                                if currentVisited.count + targets.count >= maxNodes {
                                                    break
                                                }
                                            }
                                        }
                                    }

                                    if currentVisited.count + targets.count >= maxNodes {
                                        break
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

                            if visited.count >= maxNodes {
                                break
                            }
                        }

                        // Yield nodes at current depth
                        for node in nextLevel {
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

    // MARK: - Bounded Traversal

    /// Traverse graph with limits and resumption support
    ///
    /// Returns a bounded result that can be resumed using the continuation token.
    /// Suitable for paginated graph queries in web APIs.
    ///
    /// - Parameters:
    ///   - nodeID: Starting node ID
    ///   - maxDepth: Maximum traversal depth
    ///   - label: Optional edge label filter
    ///   - direction: Traversal direction
    ///   - limit: Maximum nodes to return
    ///   - continuationToken: Token from previous call to resume
    /// - Returns: TraversalResult with nodes and optional continuation token
    public func traverseBounded(
        from nodeID: String,
        maxDepth: Int = 3,
        label: String? = nil,
        direction: Direction = .outgoing,
        limit: Int = 100,
        continuationToken: ContinuationToken? = nil
    ) async throws -> TraversalResult {
        var nodes: [String] = []
        let edges: [(source: String, target: String, label: String?)] = []
        let lastKey: [UInt8]? = nil
        var isComplete = true

        let startDepth = continuationToken?.depth ?? 0
        var visited = Set<String>()

        for try await (depth, nodeID) in traverse(
            from: nodeID,
            maxDepth: maxDepth,
            label: label,
            direction: direction,
            mode: .snapshot,
            maxNodes: limit + 1
        ) {
            if depth < startDepth {
                continue
            }

            if nodes.count >= limit {
                isComplete = false
                break
            }

            nodes.append(nodeID)
            visited.insert(nodeID)
        }

        let token: ContinuationToken? = isComplete ? nil : ContinuationToken(
            lastKey: lastKey ?? [],
            depth: startDepth,
            visitedCount: visited.count
        )

        return TraversalResult(
            nodes: nodes,
            edges: edges,
            continuationToken: token,
            isComplete: isComplete
        )
    }

    // MARK: - Private Methods

    /// Extract target node ID from index key
    private func extractTargetFromKey(_ key: [UInt8], prefix: Subspace) throws -> String? {
        let elements = try prefix.unpack(key)

        // Last element should be the target node ID
        guard !elements.isEmpty, let lastElement = elements[elements.count - 1] else {
            return nil
        }

        if let stringValue = lastElement as? String {
            return stringValue
        } else {
            return String(describing: lastElement)
        }
    }
}
