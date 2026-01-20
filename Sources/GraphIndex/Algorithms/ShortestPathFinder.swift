// ShortestPathFinder.swift
// GraphIndex - Shortest path algorithms for graph indexes
//
// Provides BFS-based shortest path finding with bidirectional optimization.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - ShortestPathFinder

/// Shortest path finder using BFS (for unweighted graphs)
///
/// Provides efficient shortest path computation using:
/// - **Unidirectional BFS**: Standard BFS from source to target
/// - **Bidirectional BFS**: Search from both ends, meeting in the middle
///
/// **Algorithm Complexity**:
/// - Unidirectional BFS: O(V + E)
/// - Bidirectional BFS: O(b^(d/2)) where b=branching factor, d=distance
///
/// **Transaction Strategy**:
/// - Each BFS level uses batch transactions (100 nodes per batch)
/// - Snapshot reads minimize conflicts
/// - Parent tracking enables path reconstruction
///
/// **Reference**: Cormen et al., "Introduction to Algorithms" (CLRS), Ch. 22
///
/// **Usage**:
/// ```swift
/// let finder = ShortestPathFinder<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// let result = try await finder.findShortestPath(
///     from: "alice",
///     to: "bob",
///     edgeLabel: "follows"
/// )
///
/// if let path = result.path {
///     print("Distance: \(path.length)")
///     print("Path: \(path.nodeIDs.joined(separator: " -> "))")
/// }
/// ```
public final class ShortestPathFinder<Edge: Persistable>: Sendable {

    // MARK: - Types

    /// Internal state for BFS search
    private struct SearchState: Sendable {
        var visited: Set<String> = []
        var parent: [String: String] = [:]      // child -> parent for path reconstruction
        var edgeLabel: [String: String] = [:]   // child -> edge label from parent
        var nodesExplored: Int = 0
    }

    /// Direction for neighbor lookup
    public enum Direction: Sendable {
        case outgoing
        case incoming
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let subspace: Subspace

    /// Cached subspace for outgoing edges
    private let outgoingSubspace: Subspace

    /// Cached subspace for incoming edges
    private let incomingSubspace: Subspace

    /// Configuration
    private let configuration: ShortestPathConfiguration

    // MARK: - Initialization

    /// Initialize shortest path finder
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        configuration: ShortestPathConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        // Cache subspaces at initialization
        // Use integer keys matching GraphIndexMaintainer: 0=out, 1=in
        self.outgoingSubspace = subspace.subspace(Int64(0))
        self.incomingSubspace = subspace.subspace(Int64(1))
    }

    // MARK: - Public API

    /// Find shortest path between two nodes
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - target: Target node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - maxDepth: Maximum search depth (overrides configuration)
    ///   - bidirectional: Use bidirectional BFS (overrides configuration)
    /// - Returns: ShortestPathResult containing path or nil if not connected
    public func findShortestPath(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        maxDepth: Int? = nil,
        bidirectional: Bool? = nil
    ) async throws -> ShortestPathResult<Edge> {
        let startTime = DispatchTime.now()
        let effectiveMaxDepth = maxDepth ?? configuration.maxDepth
        let useBidirectional = bidirectional ?? configuration.bidirectional

        // Early termination: source == target
        if source == target {
            let path = GraphPath<Edge>(singleNode: source)
            return ShortestPathResult(
                path: path,
                distance: 0,
                nodesExplored: 1,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        if useBidirectional {
            return try await bidirectionalBFS(
                source: source,
                target: target,
                edgeLabel: edgeLabel,
                maxDepth: effectiveMaxDepth,
                startTime: startTime
            )
        } else {
            return try await unidirectionalBFS(
                source: source,
                target: target,
                edgeLabel: edgeLabel,
                maxDepth: effectiveMaxDepth,
                startTime: startTime
            )
        }
    }

    /// Find all shortest paths between two nodes
    ///
    /// When multiple shortest paths of equal length exist, this method
    /// finds all of them.
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - target: Target node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - maxDepth: Maximum search depth
    /// - Returns: AllShortestPathsResult containing all paths
    public func findAllShortestPaths(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        maxDepth: Int? = nil
    ) async throws -> AllShortestPathsResult<Edge> {
        let startTime = DispatchTime.now()
        let effectiveMaxDepth = maxDepth ?? configuration.maxDepth

        // Early termination: source == target
        if source == target {
            let path = GraphPath<Edge>(singleNode: source)
            return AllShortestPathsResult(
                paths: [path],
                distance: 0,
                nodesExplored: 1,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        return try await allPathsBFS(
            source: source,
            target: target,
            edgeLabel: edgeLabel,
            maxDepth: effectiveMaxDepth,
            startTime: startTime
        )
    }

    /// Check if two nodes are connected within maxDepth hops
    ///
    /// More efficient than finding the actual path when you only need
    /// to know if a connection exists.
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - target: Target node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - maxDepth: Maximum search depth
    /// - Returns: true if connected, false otherwise
    public func isConnected(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        maxDepth: Int? = nil
    ) async throws -> Bool {
        let result = try await findShortestPath(
            from: source,
            to: target,
            edgeLabel: edgeLabel,
            maxDepth: maxDepth
        )
        return result.isConnected
    }

    // MARK: - BFS Implementations

    /// Unidirectional BFS from source to target
    private func unidirectionalBFS(
        source: String,
        target: String,
        edgeLabel: String?,
        maxDepth: Int,
        startTime: DispatchTime
    ) async throws -> ShortestPathResult<Edge> {
        var state = SearchState()
        state.visited.insert(source)
        var currentLevel: [String] = [source]
        var depth = 0

        while depth < maxDepth && !currentLevel.isEmpty {
            depth += 1
            var nextLevel: [String] = []

            // Process in batches to respect transaction limits
            for batchStart in stride(from: 0, to: currentLevel.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, currentLevel.count)
                let batch = Array(currentLevel[batchStart..<batchEnd])

                let neighbors = try await getNeighborsBatch(
                    nodes: batch,
                    edgeLabel: edgeLabel,
                    direction: .outgoing,
                    visited: state.visited
                )

                for (parentNode, targetNode, edge) in neighbors {
                    state.nodesExplored += 1

                    if !state.visited.contains(targetNode) {
                        state.visited.insert(targetNode)
                        state.parent[targetNode] = parentNode
                        state.edgeLabel[targetNode] = edge

                        // Found target - reconstruct path
                        if targetNode == target {
                            let path = reconstructPath(
                                from: source,
                                to: target,
                                parent: state.parent,
                                edgeLabel: state.edgeLabel
                            )
                            return ShortestPathResult(
                                path: path,
                                distance: Double(path.length),
                                nodesExplored: state.nodesExplored,
                                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                            )
                        }

                        nextLevel.append(targetNode)
                    }

                    // Check max nodes limit
                    if state.nodesExplored >= configuration.maxNodesExplored {
                        return .notFound(
                            nodesExplored: state.nodesExplored,
                            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                        )
                    }
                }
            }

            currentLevel = nextLevel
        }

        // No path found
        return .notFound(
            nodesExplored: state.nodesExplored,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    /// Bidirectional BFS from source and target
    ///
    /// Searches from both source (forward) and target (backward),
    /// meeting in the middle. This is O(b^(d/2)) instead of O(b^d).
    private func bidirectionalBFS(
        source: String,
        target: String,
        edgeLabel: String?,
        maxDepth: Int,
        startTime: DispatchTime
    ) async throws -> ShortestPathResult<Edge> {
        // Forward search state
        var forwardVisited: Set<String> = [source]
        var forwardParent: [String: String] = [:]
        var forwardEdge: [String: String] = [:]
        var forwardLevel: [String] = [source]

        // Backward search state
        var backwardVisited: Set<String> = [target]
        var backwardParent: [String: String] = [:]
        var backwardEdge: [String: String] = [:]
        var backwardLevel: [String] = [target]

        var nodesExplored = 0
        var depth = 0

        while depth < maxDepth && (!forwardLevel.isEmpty || !backwardLevel.isEmpty) {
            depth += 1

            // Expand the smaller frontier (optimization)
            let expandForward = forwardLevel.count <= backwardLevel.count

            if expandForward && !forwardLevel.isEmpty {
                var nextLevel: [String] = []

                for batchStart in stride(from: 0, to: forwardLevel.count, by: configuration.batchSize) {
                    let batchEnd = min(batchStart + configuration.batchSize, forwardLevel.count)
                    let batch = Array(forwardLevel[batchStart..<batchEnd])

                    let neighbors = try await getNeighborsBatch(
                        nodes: batch,
                        edgeLabel: edgeLabel,
                        direction: .outgoing,
                        visited: forwardVisited
                    )

                    for (parentNode, targetNode, edge) in neighbors {
                        nodesExplored += 1

                        if !forwardVisited.contains(targetNode) {
                            forwardVisited.insert(targetNode)
                            forwardParent[targetNode] = parentNode
                            forwardEdge[targetNode] = edge

                            // Check if we met the backward search
                            if backwardVisited.contains(targetNode) {
                                let path = reconstructBidirectionalPath(
                                    meetingPoint: targetNode,
                                    source: source,
                                    target: target,
                                    forwardParent: forwardParent,
                                    backwardParent: backwardParent,
                                    forwardEdge: forwardEdge,
                                    backwardEdge: backwardEdge
                                )
                                return ShortestPathResult(
                                    path: path,
                                    distance: Double(path.length),
                                    nodesExplored: nodesExplored,
                                    durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                                )
                            }

                            nextLevel.append(targetNode)
                        }

                        if nodesExplored >= configuration.maxNodesExplored {
                            return .notFound(
                                nodesExplored: nodesExplored,
                                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                            )
                        }
                    }
                }

                forwardLevel = nextLevel
            } else if !backwardLevel.isEmpty {
                var nextLevel: [String] = []

                for batchStart in stride(from: 0, to: backwardLevel.count, by: configuration.batchSize) {
                    let batchEnd = min(batchStart + configuration.batchSize, backwardLevel.count)
                    let batch = Array(backwardLevel[batchStart..<batchEnd])

                    // Search incoming edges (reverse direction)
                    let neighbors = try await getNeighborsBatch(
                        nodes: batch,
                        edgeLabel: edgeLabel,
                        direction: .incoming,
                        visited: backwardVisited
                    )

                    for (parentNode, targetNode, edge) in neighbors {
                        nodesExplored += 1

                        if !backwardVisited.contains(targetNode) {
                            backwardVisited.insert(targetNode)
                            backwardParent[targetNode] = parentNode
                            backwardEdge[targetNode] = edge

                            // Check if we met the forward search
                            if forwardVisited.contains(targetNode) {
                                let path = reconstructBidirectionalPath(
                                    meetingPoint: targetNode,
                                    source: source,
                                    target: target,
                                    forwardParent: forwardParent,
                                    backwardParent: backwardParent,
                                    forwardEdge: forwardEdge,
                                    backwardEdge: backwardEdge
                                )
                                return ShortestPathResult(
                                    path: path,
                                    distance: Double(path.length),
                                    nodesExplored: nodesExplored,
                                    durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                                )
                            }

                            nextLevel.append(targetNode)
                        }

                        if nodesExplored >= configuration.maxNodesExplored {
                            return .notFound(
                                nodesExplored: nodesExplored,
                                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                            )
                        }
                    }
                }

                backwardLevel = nextLevel
            }
        }

        return .notFound(
            nodesExplored: nodesExplored,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    /// BFS that finds all shortest paths (not just one)
    private func allPathsBFS(
        source: String,
        target: String,
        edgeLabel: String?,
        maxDepth: Int,
        startTime: DispatchTime
    ) async throws -> AllShortestPathsResult<Edge> {
        // Track all parents for each node (not just one)
        var visited: Set<String> = [source]
        var parents: [String: [(parent: String, edge: String)]] = [:]
        var currentLevel: [String] = [source]
        var nodesExplored = 0
        var depth = 0
        var foundDepth: Int? = nil

        // BFS level by level
        while depth < maxDepth && !currentLevel.isEmpty {
            depth += 1
            var nextLevel: [String] = []
            var levelNewNodes: Set<String> = []

            for batchStart in stride(from: 0, to: currentLevel.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, currentLevel.count)
                let batch = Array(currentLevel[batchStart..<batchEnd])

                let neighbors = try await getNeighborsBatch(
                    nodes: batch,
                    edgeLabel: edgeLabel,
                    direction: .outgoing,
                    visited: Set()  // Don't filter by visited - we want all paths
                )

                for (parentNode, targetNode, edge) in neighbors {
                    nodesExplored += 1

                    // If we found the target at a previous depth, don't explore further
                    if let found = foundDepth, depth > found {
                        continue
                    }

                    // If this is a new node at this level, track it
                    if !visited.contains(targetNode) {
                        levelNewNodes.insert(targetNode)
                        parents[targetNode, default: []].append((parentNode, edge))

                        if targetNode == target {
                            foundDepth = depth
                        }
                    } else if !currentLevel.contains(targetNode) && levelNewNodes.contains(targetNode) {
                        // Same level, different path - add additional parent
                        parents[targetNode, default: []].append((parentNode, edge))
                    }
                }
            }

            // If we found target at this depth, don't go further
            if foundDepth != nil {
                break
            }

            visited.formUnion(levelNewNodes)
            nextLevel = Array(levelNewNodes)
            currentLevel = nextLevel
        }

        // Reconstruct all paths
        guard foundDepth != nil else {
            return AllShortestPathsResult(
                paths: [],
                distance: nil,
                nodesExplored: nodesExplored,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        let paths = reconstructAllPaths(
            from: source,
            to: target,
            parents: parents
        )

        return AllShortestPathsResult(
            paths: paths,
            distance: paths.first.map { Double($0.length) },
            nodesExplored: nodesExplored,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    // MARK: - Neighbor Queries

    /// Get neighbors for a batch of nodes
    ///
    /// Returns (sourceNode, targetNode, edgeLabel) tuples.
    private func getNeighborsBatch(
        nodes: [String],
        edgeLabel: String?,
        direction: Direction,
        visited: Set<String>
    ) async throws -> [(source: String, target: String, edge: String)] {
        // Pre-compute scan parameters outside transaction (Sendable requirement)
        let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace
        let scanParams: [(source: String, beginKey: [UInt8], endKey: [UInt8], prefix: Subspace)] = nodes.map { source in
            var prefixElements: [any TupleElement] = []
            if let label = edgeLabel {
                prefixElements.append(label)
            }
            prefixElements.append(source)
            let prefix = Subspace(prefix: scanSubspace.prefix + Tuple(prefixElements).pack())
            let (beginKey, endKey) = prefix.range()
            return (source, beginKey, endKey, prefix)
        }

        // Copy visited set for Sendable
        let currentVisited = visited
        let hasEdgeLabel = edgeLabel != nil

        return try await database.withTransaction(configuration: .default) { transaction in
            var results: [(source: String, target: String, edge: String)] = []

            for (source, beginKey, endKey, prefix) in scanParams {
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in stream {
                    if let (target, edge) = try self.extractTargetAndEdge(
                        key: key,
                        prefix: prefix,
                        hasEdgeLabel: hasEdgeLabel,
                        defaultEdge: edgeLabel ?? ""
                    ) {
                        if !currentVisited.contains(target) {
                            results.append((source, target, edge))
                        }
                    }
                }
            }

            return results
        }
    }

    /// Extract target node ID and edge label from index key
    private func extractTargetAndEdge(
        key: [UInt8],
        prefix: Subspace,
        hasEdgeLabel: Bool,
        defaultEdge: String
    ) throws -> (target: String, edge: String)? {
        let elements = try prefix.unpack(key)

        guard !elements.isEmpty else { return nil }

        // Key structure depends on whether edge label is in prefix
        // If hasEdgeLabel: prefix=[label, source], remaining=[target]
        // If !hasEdgeLabel: prefix=[source], remaining=[label, target] or [target]
        let targetIndex = elements.count - 1
        guard let lastElement = elements[targetIndex] else { return nil }

        let target: String
        if let str = lastElement as? String {
            target = str
        } else {
            target = String(describing: lastElement)
        }

        // Extract edge label if not already known
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

        return (target, edge)
    }

    // MARK: - Path Reconstruction

    /// Reconstruct path from source to target using parent pointers
    private func reconstructPath(
        from source: String,
        to target: String,
        parent: [String: String],
        edgeLabel: [String: String]
    ) -> GraphPath<Edge> {
        var nodeIDs: [String] = [target]
        var edgeLabels: [String] = []
        var current = target

        while let p = parent[current] {
            if let edge = edgeLabel[current] {
                edgeLabels.append(edge)
            }
            nodeIDs.append(p)
            current = p
        }

        return GraphPath(
            nodeIDs: nodeIDs.reversed(),
            edgeLabels: edgeLabels.reversed(),
            weights: nil
        )
    }

    /// Reconstruct path when bidirectional BFS meets in the middle
    private func reconstructBidirectionalPath(
        meetingPoint: String,
        source: String,
        target: String,
        forwardParent: [String: String],
        backwardParent: [String: String],
        forwardEdge: [String: String],
        backwardEdge: [String: String]
    ) -> GraphPath<Edge> {
        // Build path from source to meeting point
        var forwardNodes: [String] = [meetingPoint]
        var forwardEdges: [String] = []
        var current = meetingPoint

        while let p = forwardParent[current] {
            if let edge = forwardEdge[current] {
                forwardEdges.append(edge)
            }
            forwardNodes.append(p)
            current = p
        }

        forwardNodes.reverse()
        forwardEdges.reverse()

        // Build path from meeting point to target
        var backwardNodes: [String] = []
        var backwardEdges: [String] = []
        current = meetingPoint

        while let p = backwardParent[current] {
            backwardNodes.append(p)
            if let edge = backwardEdge[current] {
                backwardEdges.append(edge)
            }
            current = p
        }

        // Combine paths
        let nodeIDs = forwardNodes + backwardNodes
        let edgeLabels = forwardEdges + backwardEdges

        return GraphPath(
            nodeIDs: nodeIDs,
            edgeLabels: edgeLabels,
            weights: nil
        )
    }

    /// Reconstruct all paths from source to target
    private func reconstructAllPaths(
        from source: String,
        to target: String,
        parents: [String: [(parent: String, edge: String)]]
    ) -> [GraphPath<Edge>] {
        var paths: [GraphPath<Edge>] = []

        // DFS to enumerate all paths
        func buildPaths(
            current: String,
            pathNodes: [String],
            pathEdges: [String]
        ) {
            if current == source {
                // Found complete path
                let path = GraphPath<Edge>(
                    nodeIDs: pathNodes.reversed(),
                    edgeLabels: pathEdges.reversed(),
                    weights: nil
                )
                paths.append(path)
                return
            }

            guard let nodeParents = parents[current] else { return }

            for (parentNode, edge) in nodeParents {
                buildPaths(
                    current: parentNode,
                    pathNodes: pathNodes + [parentNode],
                    pathEdges: pathEdges + [edge]
                )
            }
        }

        buildPaths(current: target, pathNodes: [target], pathEdges: [])
        return paths
    }
}
