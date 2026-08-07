// ShortestPathFinder.swift
// GraphIndex - Shortest path algorithms for graph indexes
//
// Provides BFS-based shortest path finding with bidirectional optimization.

import DatabaseKit
import DatabaseEngine
import StorageKit

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
/// - The caller provides one snapshot for the complete search
/// - Neighbor ranges are consumed lazily without nested transactions
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
public final class ShortestPathFinder: Sendable {

    // MARK: - Types

    /// Internal state for BFS search
    private struct SearchState: Sendable {
        var visited: Set<GraphIdentity> = []
        var parent: [GraphIdentity: GraphIdentity] = [:]      // child -> parent for path reconstruction
        var edgeLabel: [GraphIdentity: GraphIdentity] = [:]   // child -> edge label from parent
        var nodesExplored: Int = 0
    }

    // MARK: - Properties

    /// Storage snapshot shared by the complete traversal.
    private let snapshot: GraphReadSnapshot

    /// Edge scanner for neighbor lookups (centralizes key structure knowledge)
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: ShortestPathConfiguration

    /// Shared request work budget.
    private let workBudget: GraphAlgorithmWorkBudget?

    // MARK: - Initialization

    /// Initialize shortest path finder
    ///
    /// - Parameters:
    ///   - snapshot: Stable storage snapshot for the complete algorithm
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    ///   - configuration: Algorithm configuration
    package init(
        snapshot: GraphReadSnapshot,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        graphTarget: GraphScanTarget = .all,
        configuration: ShortestPathConfiguration = .default
    ) {
        self.snapshot = snapshot
        self.scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: strategy,
            graphTarget: graphTarget,
            snapshot: snapshot
        )
        self.configuration = configuration
        self.workBudget = snapshot.workBudget
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
        from source: GraphIdentity,
        to target: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        maxDepth: Int? = nil,
        bidirectional: Bool? = nil
    ) async throws -> ShortestPathResult {
        let startTime = snapshot.clock.now()
        let effectiveMaxDepth = maxDepth ?? configuration.maxDepth
        let useBidirectional = bidirectional ?? configuration.bidirectional

        // Early termination: source == target
        if source == target {
            let path = GraphPath(singleNode: source)
            return ShortestPathResult(
                path: path,
                distance: 0,
                nodesExplored: 1,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        if useBidirectional && configuration.maxNodesExplored >= 2 {
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
        from source: GraphIdentity,
        to target: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        maxDepth: Int? = nil
    ) async throws -> AllShortestPathsResult {
        let startTime = snapshot.clock.now()
        let effectiveMaxDepth = maxDepth ?? configuration.maxDepth

        // Early termination: source == target
        if source == target {
            let path = GraphPath(singleNode: source)
            return AllShortestPathsResult(
                paths: [path],
                distance: 0,
                nodesExplored: 1,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
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
    /// - Returns: true if connected, false after a definitive complete search
    /// - Throws: `ShortestPathError.incomplete` when a configured limit makes
    ///   a negative result inconclusive
    public func isConnected(
        from source: GraphIdentity,
        to target: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        maxDepth: Int? = nil
    ) async throws -> Bool {
        let result = try await findShortestPath(
            from: source,
            to: target,
            edgeLabel: edgeLabel,
            maxDepth: maxDepth
        )
        if result.isConnected {
            return true
        }
        guard result.isComplete else {
            guard let limitReason = result.limitReason else {
                throw ShortestPathError.inconsistentResultState
            }
            throw ShortestPathError.incomplete(limitReason)
        }
        return false
    }

    // MARK: - BFS Implementations

    /// Unidirectional BFS from source to target
    private func unidirectionalBFS(
        source: GraphIdentity,
        target: GraphIdentity,
        edgeLabel: GraphIdentity?,
        maxDepth: Int,
        startTime: MonotonicTimestamp
    ) async throws -> ShortestPathResult {
        var state = SearchState()
        state.visited.insert(source)
        state.nodesExplored = 1
        var currentLevel: [GraphIdentity] = [source]
        var depth = 0

        while depth < maxDepth && !currentLevel.isEmpty {
            depth += 1
            var nextLevel: [GraphIdentity] = []

            // Process in batches to respect transaction limits
            for batchStart in stride(from: 0, to: currentLevel.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, currentLevel.count)
                let neighbors = getNeighbors(
                    nodes: currentLevel[batchStart..<batchEnd],
                    edgeLabel: edgeLabel,
                    direction: .outgoing
                )

                var neighborCursor = neighbors.makeCursor()
                while let (parentNode, targetNode, edge) = try await neighborCursor.next() {
                        guard !state.visited.contains(targetNode) else { continue }
                        guard state.nodesExplored < configuration.maxNodesExplored else {
                            return .notFound(
                                nodesExplored: state.nodesExplored,
                                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                                isComplete: false,
                                limitReason: .maxNodesReached(
                                    explored: state.nodesExplored,
                                    limit: configuration.maxNodesExplored
                                )
                            )
                        }

                        state.visited.insert(targetNode)
                        state.nodesExplored += 1
                        state.parent[targetNode] = parentNode
                        state.edgeLabel[targetNode] = edge

                        if targetNode == target {
                            let path = try reconstructPath(
                                from: source,
                                to: target,
                                parent: state.parent,
                                edgeLabel: state.edgeLabel
                            )
                            return ShortestPathResult(
                                path: path,
                                distance: Double(path.length),
                                nodesExplored: state.nodesExplored,
                                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                            )
                        }

                        nextLevel.append(targetNode)
                }
                if let limitReason = neighborCursor.limitReason {
                    return .notFound(
                        nodesExplored: state.nodesExplored,
                        durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                        isComplete: false,
                        limitReason: limitReason
                    )
                }
            }

            currentLevel = nextLevel
        }

        // No path found
        let hitMaximumDepth = depth >= maxDepth && !currentLevel.isEmpty
        return .notFound(
            nodesExplored: state.nodesExplored,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: !hitMaximumDepth,
            limitReason: hitMaximumDepth
                ? .maxDepthReached(depth: depth, limit: maxDepth)
                : nil
        )
    }

    /// Bidirectional BFS from source and target
    ///
    /// Searches from both source (forward) and target (backward),
    /// meeting in the middle. This is O(b^(d/2)) instead of O(b^d).
    private func bidirectionalBFS(
        source: GraphIdentity,
        target: GraphIdentity,
        edgeLabel: GraphIdentity?,
        maxDepth: Int,
        startTime: MonotonicTimestamp
    ) async throws -> ShortestPathResult {
        // Forward search state
        var forwardVisited: Set<GraphIdentity> = [source]
        var forwardParent: [GraphIdentity: GraphIdentity] = [:]
        var forwardEdge: [GraphIdentity: GraphIdentity] = [:]
        var forwardLevel: [GraphIdentity] = [source]

        // Backward search state
        var backwardVisited: Set<GraphIdentity> = [target]
        var backwardParent: [GraphIdentity: GraphIdentity] = [:]
        var backwardEdge: [GraphIdentity: GraphIdentity] = [:]
        var backwardLevel: [GraphIdentity] = [target]

        var nodesExplored = 2
        var depth = 0

        guard nodesExplored <= configuration.maxNodesExplored else {
            return .notFound(
                nodesExplored: configuration.maxNodesExplored,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: false,
                limitReason: .maxNodesReached(
                    explored: configuration.maxNodesExplored,
                    limit: configuration.maxNodesExplored
                )
            )
        }

        while depth < maxDepth && !forwardLevel.isEmpty && !backwardLevel.isEmpty {
            depth += 1

            // Expand the smaller frontier (optimization)
            let expandForward = forwardLevel.count <= backwardLevel.count

            if expandForward && !forwardLevel.isEmpty {
                var nextLevel: [GraphIdentity] = []

                for batchStart in stride(from: 0, to: forwardLevel.count, by: configuration.batchSize) {
                    let batchEnd = min(batchStart + configuration.batchSize, forwardLevel.count)
                    let neighbors = getNeighbors(
                        nodes: forwardLevel[batchStart..<batchEnd],
                        edgeLabel: edgeLabel,
                        direction: .outgoing
                    )

                    var neighborCursor = neighbors.makeCursor()
                    while let (parentNode, targetNode, edge) = try await neighborCursor.next() {
                            guard !forwardVisited.contains(targetNode) else { continue }
                            let isNewNode = !backwardVisited.contains(targetNode)
                            guard !isNewNode || nodesExplored < configuration.maxNodesExplored else {
                                return .notFound(
                                    nodesExplored: nodesExplored,
                                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                                    isComplete: false,
                                    limitReason: .maxNodesReached(
                                        explored: nodesExplored,
                                        limit: configuration.maxNodesExplored
                                    )
                                )
                            }

                            forwardVisited.insert(targetNode)
                            if isNewNode { nodesExplored += 1 }
                            forwardParent[targetNode] = parentNode
                            forwardEdge[targetNode] = edge

                            if backwardVisited.contains(targetNode) {
                                let path = try reconstructBidirectionalPath(
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
                                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                                )
                            }

                            nextLevel.append(targetNode)
                    }
                    if let limitReason = neighborCursor.limitReason {
                        return .notFound(
                            nodesExplored: nodesExplored,
                            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                            isComplete: false,
                            limitReason: limitReason
                        )
                    }
                }

                forwardLevel = nextLevel
            } else if !backwardLevel.isEmpty {
                var nextLevel: [GraphIdentity] = []

                for batchStart in stride(from: 0, to: backwardLevel.count, by: configuration.batchSize) {
                    let batchEnd = min(batchStart + configuration.batchSize, backwardLevel.count)
                    let neighbors = getNeighbors(
                        nodes: backwardLevel[batchStart..<batchEnd],
                        edgeLabel: edgeLabel,
                        direction: .incoming
                    )

                    var neighborCursor = neighbors.makeCursor()
                    while let (parentNode, targetNode, edge) = try await neighborCursor.next() {
                            guard !backwardVisited.contains(targetNode) else { continue }
                            let isNewNode = !forwardVisited.contains(targetNode)
                            guard !isNewNode || nodesExplored < configuration.maxNodesExplored else {
                                return .notFound(
                                    nodesExplored: nodesExplored,
                                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                                    isComplete: false,
                                    limitReason: .maxNodesReached(
                                        explored: nodesExplored,
                                        limit: configuration.maxNodesExplored
                                    )
                                )
                            }

                            backwardVisited.insert(targetNode)
                            if isNewNode { nodesExplored += 1 }
                            backwardParent[targetNode] = parentNode
                            backwardEdge[targetNode] = edge

                            if forwardVisited.contains(targetNode) {
                                let path = try reconstructBidirectionalPath(
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
                                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                                )
                            }

                            nextLevel.append(targetNode)
                    }
                    if let limitReason = neighborCursor.limitReason {
                        return .notFound(
                            nodesExplored: nodesExplored,
                            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                            isComplete: false,
                            limitReason: limitReason
                        )
                    }
                }

                backwardLevel = nextLevel
            }
        }

        let hitMaximumDepth = depth >= maxDepth
            && !forwardLevel.isEmpty
            && !backwardLevel.isEmpty
        return .notFound(
            nodesExplored: nodesExplored,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: !hitMaximumDepth,
            limitReason: hitMaximumDepth
                ? .maxDepthReached(depth: depth, limit: maxDepth)
                : nil
        )
    }

    /// BFS that finds all shortest paths (not just one)
    private func allPathsBFS(
        source: GraphIdentity,
        target: GraphIdentity,
        edgeLabel: GraphIdentity?,
        maxDepth: Int,
        startTime: MonotonicTimestamp
    ) async throws -> AllShortestPathsResult {
        // Track all parents for each node (not just one)
        var visited: Set<GraphIdentity> = [source]
        var parents: [GraphIdentity: [(parent: GraphIdentity, edge: GraphIdentity)]] = [:]
        var currentLevel: [GraphIdentity] = [source]
        var nodesExplored = 1
        var depth = 0
        var foundDepth: Int? = nil
        var limitReason: LimitReason?

        // BFS level by level
        search: while depth < maxDepth && !currentLevel.isEmpty {
            depth += 1
            var nextLevel: [GraphIdentity] = []
            var levelNewNodes: Set<GraphIdentity> = []

            for batchStart in stride(from: 0, to: currentLevel.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, currentLevel.count)
                let neighbors = getNeighbors(
                    nodes: currentLevel[batchStart..<batchEnd],
                    edgeLabel: edgeLabel,
                    direction: .outgoing
                )

                var neighborCursor = neighbors.makeCursor()
                while let (parentNode, targetNode, edge) = try await neighborCursor.next() {
                        if let found = foundDepth, depth > found {
                            continue
                        }

                        if !visited.contains(targetNode) {
                            let isNewAtLevel = !levelNewNodes.contains(targetNode)
                            if isNewAtLevel {
                                guard nodesExplored < configuration.maxNodesExplored else {
                                    limitReason = .maxNodesReached(
                                        explored: nodesExplored,
                                        limit: configuration.maxNodesExplored
                                    )
                                    break search
                                }
                                nodesExplored += 1
                            }

                            levelNewNodes.insert(targetNode)
                            parents[targetNode, default: []].append((parentNode, edge))

                            if targetNode == target {
                                foundDepth = depth
                            }
                        } else if !currentLevel.contains(targetNode) && levelNewNodes.contains(targetNode) {
                            parents[targetNode, default: []].append((parentNode, edge))
                        }
                }
                if let workLimitReason = neighborCursor.limitReason {
                    limitReason = workLimitReason
                    break search
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

        if limitReason == nil,
           foundDepth == nil,
           depth >= maxDepth,
           !currentLevel.isEmpty {
            limitReason = .maxDepthReached(depth: depth, limit: maxDepth)
        }

        // Reconstruct all paths
        guard foundDepth != nil else {
            return AllShortestPathsResult(
                paths: [],
                distance: nil,
                nodesExplored: nodesExplored,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: limitReason == nil,
                limitReason: limitReason
            )
        }

        let reconstruction = try reconstructAllPaths(
            from: source,
            to: target,
            parents: parents
        )
        let paths = reconstruction.paths
        if limitReason == nil {
            limitReason = reconstruction.limitReason
        }

        return AllShortestPathsResult(
            paths: paths,
            distance: paths.first.map { Double($0.length) },
            nodesExplored: nodesExplored,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: limitReason == nil,
            limitReason: limitReason
        )
    }

    // MARK: - Neighbor Queries

    /// Get neighbors for a batch of nodes using GraphEdgeScanner
    ///
    /// Returns (sourceNode, targetNode, edgeLabel) tuples.
    ///
    /// The scanner selects one batch plan and never opens a nested transaction.
    private func getNeighbors(
        nodes: ArraySlice<GraphIdentity>,
        edgeLabel: GraphIdentity?,
        direction: GraphTraversalDirection
    ) -> ShortestPathNeighborScan {
        ShortestPathNeighborScan(
            scanner: scanner,
            snapshot: snapshot,
            nodes: nodes,
            edgeLabel: edgeLabel,
            direction: direction
        )
    }

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }

    // MARK: - Path Reconstruction

    /// Reconstruct path from source to target using parent pointers
    private func reconstructPath(
        from source: GraphIdentity,
        to target: GraphIdentity,
        parent: [GraphIdentity: GraphIdentity],
        edgeLabel: [GraphIdentity: GraphIdentity]
    ) throws -> GraphPath {
        var nodeIDs: [GraphIdentity] = [target]
        var edgeLabels: [GraphIdentity] = []
        var current = target
        var visited: Set<GraphIdentity> = [target]

        while let p = parent[current] {
            guard let edge = edgeLabel[current] else {
                throw ShortestPathError.missingEdgeLabel(node: current)
            }
            guard visited.insert(p).inserted else {
                throw ShortestPathError.parentCycle(node: p)
            }
            edgeLabels.append(edge)
            nodeIDs.append(p)
            current = p
        }
        guard current == source else {
            throw ShortestPathError.pathDoesNotReachSource(
                expected: source,
                actual: current
            )
        }

        return try GraphPath(
            nodeIDs: Array(nodeIDs.reversed()),
            edgeLabels: Array(edgeLabels.reversed()),
            weights: nil
        )
    }

    /// Reconstruct path when bidirectional BFS meets in the middle
    private func reconstructBidirectionalPath(
        meetingPoint: GraphIdentity,
        source: GraphIdentity,
        target: GraphIdentity,
        forwardParent: [GraphIdentity: GraphIdentity],
        backwardParent: [GraphIdentity: GraphIdentity],
        forwardEdge: [GraphIdentity: GraphIdentity],
        backwardEdge: [GraphIdentity: GraphIdentity]
    ) throws -> GraphPath {
        // Build path from source to meeting point
        var forwardNodes: [GraphIdentity] = [meetingPoint]
        var forwardEdges: [GraphIdentity] = []
        var current = meetingPoint
        var visitedForward: Set<GraphIdentity> = [meetingPoint]

        while let p = forwardParent[current] {
            guard let edge = forwardEdge[current] else {
                throw ShortestPathError.missingEdgeLabel(node: current)
            }
            guard visitedForward.insert(p).inserted else {
                throw ShortestPathError.parentCycle(node: p)
            }
            forwardEdges.append(edge)
            forwardNodes.append(p)
            current = p
        }
        guard current == source else {
            throw ShortestPathError.pathDoesNotReachSource(
                expected: source,
                actual: current
            )
        }

        forwardNodes.reverse()
        forwardEdges.reverse()

        // Build path from meeting point to target
        var backwardNodes: [GraphIdentity] = []
        var backwardEdges: [GraphIdentity] = []
        current = meetingPoint
        var visitedBackward: Set<GraphIdentity> = [meetingPoint]

        while let p = backwardParent[current] {
            guard let edge = backwardEdge[current] else {
                throw ShortestPathError.missingEdgeLabel(node: current)
            }
            guard visitedBackward.insert(p).inserted else {
                throw ShortestPathError.parentCycle(node: p)
            }
            backwardNodes.append(p)
            backwardEdges.append(edge)
            current = p
        }
        guard current == target else {
            throw ShortestPathError.pathDoesNotReachTarget(
                expected: target,
                actual: current
            )
        }

        // Combine paths
        let nodeIDs = forwardNodes + backwardNodes
        let edgeLabels = forwardEdges + backwardEdges

        return try GraphPath(
            nodeIDs: nodeIDs,
            edgeLabels: edgeLabels,
            weights: nil
        )
    }

    /// Reconstruct all paths from source to target
    private func reconstructAllPaths(
        from source: GraphIdentity,
        to target: GraphIdentity,
        parents: [GraphIdentity: [(parent: GraphIdentity, edge: GraphIdentity)]]
    ) throws -> (paths: [GraphPath], limitReason: LimitReason?) {
        var paths: [GraphPath] = []
        var limitReason: LimitReason?

        // DFS to enumerate all paths
        func buildPaths(
            current: GraphIdentity,
            pathNodes: [GraphIdentity],
            pathEdges: [GraphIdentity]
        ) throws {
            guard limitReason == nil else { return }
            guard try consumeWork() else {
                limitReason = workBudget?.limitReason
                return
            }

            if current == source {
                // Found complete path
                let path = try GraphPath(
                    nodeIDs: Array(pathNodes.reversed()),
                    edgeLabels: Array(pathEdges.reversed()),
                    weights: nil
                )
                paths.append(path)
                return
            }

            guard let nodeParents = parents[current] else { return }

            for (parentNode, edge) in nodeParents {
                try buildPaths(
                    current: parentNode,
                    pathNodes: pathNodes + [parentNode],
                    pathEdges: pathEdges + [edge]
                )
            }
        }

        try buildPaths(current: target, pathNodes: [target], pathEdges: [])
        return (paths, limitReason)
    }
}

public enum ShortestPathError: Error, Sendable, Equatable {
    case incomplete(LimitReason)
    case inconsistentWorkBudget
    case inconsistentResultState
    case missingEdgeLabel(node: GraphIdentity)
    case parentCycle(node: GraphIdentity)
    case pathDoesNotReachSource(expected: GraphIdentity, actual: GraphIdentity)
    case pathDoesNotReachTarget(expected: GraphIdentity, actual: GraphIdentity)
}
