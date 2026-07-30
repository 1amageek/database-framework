// WeightedShortestPath.swift
// GraphIndex - Dijkstra's shortest path algorithm for weighted graphs
//
// Provides efficient weighted shortest path finding using priority queues.

import DatabaseEngine
import DatabaseKit
import StorageKit

// MARK: - WeightedShortestPathConfiguration

/// Configuration for weighted shortest path computation
public struct WeightedShortestPathConfiguration: Sendable {
    /// Maximum total weight to explore (default: Double.infinity)
    public let maxWeight: Double

    /// Maximum number of nodes to explore (default: 100000)
    public let maxNodes: Int

    /// Batch size for transaction processing (default: 100)
    public let batchSize: Int

    /// Default configuration
    public static let `default` = WeightedShortestPathConfiguration()

    public init(
        maxWeight: Double = .infinity,
        maxNodes: Int = 100000,
        batchSize: Int = 100
    ) {
        self.maxWeight = maxWeight
        self.maxNodes = Swift.max(1, maxNodes)
        self.batchSize = Swift.max(1, batchSize)
    }
}

// MARK: - WeightedPathResult

/// Result of weighted shortest path computation
public struct WeightedPathResult: Sendable {
    /// The path if one exists (nil if no path found)
    public let path: GraphPath?

    /// Total weight of the path (Double.infinity if no path)
    public let totalWeight: Double

    /// Number of nodes explored during search
    public let nodesExplored: Int

    /// Number of edges relaxed
    public let edgesRelaxed: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Whether the search exhausted its valid search space.
    public let isComplete: Bool

    /// Limit that stopped the search, when incomplete.
    public let limitReason: LimitReason?

    /// Whether a path was found
    public var found: Bool { path != nil }

    public init(
        path: GraphPath?,
        totalWeight: Double,
        nodesExplored: Int,
        edgesRelaxed: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.path = path
        self.totalWeight = totalWeight
        self.nodesExplored = nodesExplored
        self.edgesRelaxed = edgesRelaxed
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}

// MARK: - SingleSourceResult

/// Result of single-source shortest paths computation
public struct SingleSourceResult: Sendable {
    /// Source node used for this computation.
    public let source: GraphIdentity

    /// Distances to all reachable nodes
    public let distances: [GraphIdentity: Double]

    /// Parent pointers for path reconstruction
    public let parents: [GraphIdentity: GraphIdentity]

    /// Edge labels used in shortest paths
    public let edgeLabels: [GraphIdentity: GraphIdentity]

    /// Number of nodes explored
    public let nodesExplored: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Whether every reachable path within the configured graph was evaluated.
    public let isComplete: Bool

    /// Limit that stopped the search, when incomplete.
    public let limitReason: LimitReason?

    /// Get the shortest path to a target node
    public func pathTo(_ target: GraphIdentity) throws -> GraphPath? {
        guard distances[target] != nil else { return nil }

        var reversedNodeIDs: [GraphIdentity] = [target]
        var reversedEdgeLabels: [GraphIdentity] = []
        var reversedWeights: [Double] = []
        var current = target
        var visited: Set<GraphIdentity> = [target]

        while let parent = parents[current] {
            guard let edgeLabel = edgeLabels[current] else {
                throw WeightedShortestPathError.missingPathEdgeLabel(node: current)
            }
            guard let currentDistance = distances[current] else {
                throw WeightedShortestPathError.missingPathDistance(node: current)
            }
            guard let parentDistance = distances[parent] else {
                throw WeightedShortestPathError.missingPathDistance(node: parent)
            }
            guard visited.insert(parent).inserted else {
                throw WeightedShortestPathError.pathParentCycle(node: parent)
            }
            reversedNodeIDs.append(parent)
            reversedEdgeLabels.append(edgeLabel)
            reversedWeights.append(currentDistance - parentDistance)
            current = parent
        }
        guard current == source else {
            throw WeightedShortestPathError.pathDoesNotReachSource(
                expected: source,
                actual: current
            )
        }

        return try GraphPath(
            nodeIDs: Array(reversedNodeIDs.reversed()),
            edgeLabels: Array(reversedEdgeLabels.reversed()),
            weights: Array(reversedWeights.reversed())
        )
    }

    public init(
        source: GraphIdentity,
        distances: [GraphIdentity: Double],
        parents: [GraphIdentity: GraphIdentity],
        edgeLabels: [GraphIdentity: GraphIdentity],
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.source = source
        self.distances = distances
        self.parents = parents
        self.edgeLabels = edgeLabels
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}

public enum WeightedShortestPathError: Error, Sendable, CustomStringConvertible {
    case invalidMaximumWeight(Double)
    case negativeWeight(source: GraphIdentity, target: GraphIdentity, edgeLabel: GraphIdentity?, weight: Double)
    case nonFiniteWeight(source: GraphIdentity, target: GraphIdentity, edgeLabel: GraphIdentity?, weight: Double)
    case weightOverflow(source: GraphIdentity, target: GraphIdentity, edgeLabel: GraphIdentity?)
    case missingPathEdgeLabel(node: GraphIdentity)
    case missingPathDistance(node: GraphIdentity)
    case pathParentCycle(node: GraphIdentity)
    case pathDoesNotReachSource(expected: GraphIdentity, actual: GraphIdentity)

    public var description: String {
        switch self {
        case .invalidMaximumWeight(let weight):
            return "Maximum path weight must be non-negative and not NaN: \(weight)"
        case .negativeWeight(let source, let target, let edgeLabel, let weight):
            let label = edgeLabel ?? "<none>"
            return "Dijkstra shortest path does not support negative weights: \(source) -> \(target), label=\(label), weight=\(weight)"
        case .nonFiniteWeight(let source, let target, let edgeLabel, let weight):
            let label = edgeLabel ?? "<none>"
            return "Dijkstra shortest path requires finite edge weights: \(source) -> \(target), label=\(label), weight=\(weight)"
        case .weightOverflow(let source, let target, let edgeLabel):
            let label = edgeLabel ?? "<none>"
            return "Dijkstra path weight overflowed: \(source) -> \(target), label=\(label)"
        case .missingPathEdgeLabel(let node):
            return "Dijkstra path is missing the edge label for node \(node)"
        case .missingPathDistance(let node):
            return "Dijkstra path is missing the distance for node \(node)"
        case .pathParentCycle(let node):
            return "Dijkstra parent pointers contain a cycle at node \(node)"
        case .pathDoesNotReachSource(let expected, let actual):
            return "Dijkstra path ended at \(actual) instead of source \(expected)"
        }
    }
}

// MARK: - WeightedShortestPathFinder

public struct WeightedGraphNeighbor: Sendable {
    public let edge: EdgeInfo
    public let weight: Double

    public init(edge: EdgeInfo, weight: Double) {
        self.edge = edge
        self.weight = weight
    }
}

public protocol WeightedGraphNeighborSource: Sendable {
    func neighbors(
        from source: GraphIdentity,
        edgeLabel: GraphIdentity?
    ) async throws -> [WeightedGraphNeighbor]
}

/// Dijkstra's algorithm for weighted shortest path
///
/// Finds shortest paths in graphs with non-negative edge weights using
/// a priority queue (binary heap) for efficient minimum extraction.
///
/// **Algorithm**: Dijkstra's Algorithm with Binary Heap
/// - Time Complexity: O((V + E) log V)
/// - Space Complexity: O(V) for distance/parent tracking
///
/// **Weight Extraction**:
/// Edge weights are extracted via a closure, allowing flexible weight
/// definitions (e.g., from edge properties, computed values).
///
/// **Limitations**:
/// - Does not support negative weights (use Bellman-Ford instead)
/// - For unweighted graphs, use `ShortestPathFinder` (BFS) instead
///
/// **Transaction Strategy**:
/// - Each relaxation phase uses batch transactions
/// - Snapshot reads for graph traversal
/// - State maintained in memory between batches
///
/// **Reference**: Dijkstra, E.W. "A note on two problems in connexion
///               with graphs" (1959)
///
/// **Usage**:
/// ```swift
/// let finder = WeightedShortestPathFinder<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// // Define weight extractor (e.g., edge has `cost` property)
/// let result = try await finder.findShortestPath(
///     from: "A",
///     to: "D",
///     edgeLabel: "road",
///     weightExtractor: { edge in edge.cost }
/// )
///
/// if let path = result.path {
///     print("Total cost: \(result.totalWeight)")
///     print("Path: \(path.nodeIDs.joined(separator: " -> "))")
/// }
/// ```
public final class WeightedShortestPathFinder: Sendable {

    /// Internal priority queue node
    private struct PriorityNode: Comparable, Sendable {
        let nodeID: GraphIdentity
        let distance: Double

        static func < (lhs: PriorityNode, rhs: PriorityNode) -> Bool {
            lhs.distance < rhs.distance
        }

        static func == (lhs: PriorityNode, rhs: PriorityNode) -> Bool {
            lhs.nodeID == rhs.nodeID && lhs.distance == rhs.distance
        }
    }

    // MARK: - Properties

    /// Indexed neighbor source bound to one storage snapshot.
    private let neighborSource: any WeightedGraphNeighborSource

    /// Configuration
    private let configuration: WeightedShortestPathConfiguration

    /// Shared request work budget.
    private let workBudget: GraphAlgorithmWorkBudget?
    private let clock: MonotonicClock

    // MARK: - Initialization

    /// Initialize weighted shortest path finder
    ///
    /// - Parameters:
    ///   - neighborSource: Weighted neighbor source for one stable snapshot
    ///   - configuration: Algorithm configuration
    public init(
        neighborSource: any WeightedGraphNeighborSource,
        monotonicClock: any StorageMonotonicClock,
        configuration: WeightedShortestPathConfiguration = .default,
        workBudget: GraphAlgorithmWorkBudget? = nil
    ) {
        self.neighborSource = neighborSource
        self.clock = MonotonicClock(source: monotonicClock)
        self.configuration = configuration
        self.workBudget = workBudget
    }

    // MARK: - Public API

    /// Find weighted shortest path between two nodes
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - target: Target node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - maxWeight: Maximum weight to explore (overrides config)
    /// - Returns: WeightedPathResult with path and total weight
    public func findShortestPath(
        from source: GraphIdentity,
        to target: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        maxWeight: Double? = nil
    ) async throws -> WeightedPathResult {
        let startTime = clock.now()
        let effectiveMaxWeight = maxWeight ?? configuration.maxWeight
        guard !effectiveMaxWeight.isNaN, effectiveMaxWeight >= 0 else {
            throw WeightedShortestPathError.invalidMaximumWeight(effectiveMaxWeight)
        }

        // Early termination: source == target
        if source == target {
            let path = GraphPath(singleNode: source)
            return WeightedPathResult(
                path: path,
                totalWeight: 0,
                nodesExplored: 1,
                edgesRelaxed: 0,
                durationNs: clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Initialize Dijkstra state
        var distances: [GraphIdentity: Double] = [source: 0]
        var parents: [GraphIdentity: GraphIdentity] = [:]
        var edgeLabels: [GraphIdentity: GraphIdentity] = [:]
        var visited: Set<GraphIdentity> = []
        var priorityQueue = MinHeap<PriorityNode>()
        priorityQueue.insert(PriorityNode(nodeID: source, distance: 0))

        var nodesExplored = 0
        var edgesRelaxed = 0
        var limitReason: LimitReason?

        // Main Dijkstra loop
        search: while let current = priorityQueue.extractMin() {
            let currentNode = current.nodeID
            let currentDist = current.distance

            // Skip if already visited (stale queue entry)
            if visited.contains(currentNode) {
                continue
            }

            guard nodesExplored < configuration.maxNodes else {
                limitReason = .maxNodesReached(
                    explored: nodesExplored,
                    limit: configuration.maxNodes
                )
                break
            }

            visited.insert(currentNode)
            nodesExplored += 1

            // Early termination: reached target
            if currentNode == target {
                let path = try reconstructPath(
                    from: source,
                    to: target,
                    parents: parents,
                    edgeLabels: edgeLabels,
                    distances: distances
                )
                return WeightedPathResult(
                    path: path,
                    totalWeight: currentDist,
                    nodesExplored: nodesExplored,
                    edgesRelaxed: edgesRelaxed,
                    durationNs: clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                )
            }

            // Get neighbors and relax edges using GraphEdgeScanner
            guard try consumeWork() else {
                limitReason = workBudget?.limitReason
                break
            }
            let neighbors = try await neighborSource.neighbors(
                from: currentNode,
                edgeLabel: edgeLabel
            )
            if let workLimitReason = workBudget?.limitReason {
                limitReason = workLimitReason
                break
            }

            for weightedNeighbor in neighbors {
                guard try consumeWork() else {
                    limitReason = workBudget?.limitReason
                    break search
                }
                let neighbor = weightedNeighbor.edge
                if visited.contains(neighbor.target) {
                    continue
                }
                let weight = weightedNeighbor.weight
                if let workLimitReason = workBudget?.limitReason {
                    limitReason = workLimitReason
                    break search
                }

                guard weight.isFinite else {
                    throw WeightedShortestPathError.nonFiniteWeight(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel,
                        weight: weight
                    )
                }
                guard weight >= 0 else {
                    throw WeightedShortestPathError.negativeWeight(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel,
                        weight: weight
                    )
                }

                let newDist = currentDist + weight
                guard newDist.isFinite else {
                    throw WeightedShortestPathError.weightOverflow(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel
                    )
                }
                edgesRelaxed += 1
                guard newDist <= effectiveMaxWeight else {
                    if case .none = limitReason {
                        limitReason = .maxWeightReached(
                            weight: newDist,
                            limit: effectiveMaxWeight
                        )
                    }
                    continue
                }

                // Relax edge if shorter path found
                let oldDist = distances[neighbor.target] ?? .infinity
                if newDist < oldDist {
                    distances[neighbor.target] = newDist
                    parents[neighbor.target] = currentNode
                    edgeLabels[neighbor.target] = neighbor.edgeLabel
                    priorityQueue.insert(PriorityNode(nodeID: neighbor.target, distance: newDist))
                }
            }
        }

        // No path found
        return WeightedPathResult(
            path: nil,
            totalWeight: .infinity,
            nodesExplored: nodesExplored,
            edgesRelaxed: edgesRelaxed,
            durationNs: clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: limitReason == nil,
            limitReason: limitReason
        )
    }

    /// Find shortest paths from source to all reachable nodes
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - maxWeight: Maximum weight to explore
    /// - Returns: SingleSourceResult with distances to all reachable nodes
    public func findShortestPaths(
        from source: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        maxWeight: Double? = nil
    ) async throws -> SingleSourceResult {
        let startTime = clock.now()
        let effectiveMaxWeight = maxWeight ?? configuration.maxWeight
        guard !effectiveMaxWeight.isNaN, effectiveMaxWeight >= 0 else {
            throw WeightedShortestPathError.invalidMaximumWeight(effectiveMaxWeight)
        }

        // Initialize Dijkstra state
        var distances: [GraphIdentity: Double] = [source: 0]
        var parents: [GraphIdentity: GraphIdentity] = [:]
        var edgeLabels: [GraphIdentity: GraphIdentity] = [:]
        var visited: Set<GraphIdentity> = []
        var priorityQueue = MinHeap<PriorityNode>()
        priorityQueue.insert(PriorityNode(nodeID: source, distance: 0))

        var nodesExplored = 0
        var limitReason: LimitReason?

        // Main Dijkstra loop
        search: while let current = priorityQueue.extractMin() {
            let currentNode = current.nodeID
            let currentDist = current.distance

            // Skip if already visited
            if visited.contains(currentNode) {
                continue
            }

            guard nodesExplored < configuration.maxNodes else {
                limitReason = .maxNodesReached(
                    explored: nodesExplored,
                    limit: configuration.maxNodes
                )
                break
            }

            visited.insert(currentNode)
            nodesExplored += 1

            // Get neighbors and relax edges using GraphEdgeScanner
            guard try consumeWork() else {
                limitReason = workBudget?.limitReason
                break
            }
            let neighbors = try await neighborSource.neighbors(
                from: currentNode,
                edgeLabel: edgeLabel
            )
            if let workLimitReason = workBudget?.limitReason {
                limitReason = workLimitReason
                break
            }

            for weightedNeighbor in neighbors {
                guard try consumeWork() else {
                    limitReason = workBudget?.limitReason
                    break search
                }
                let neighbor = weightedNeighbor.edge
                if visited.contains(neighbor.target) {
                    continue
                }
                let weight = weightedNeighbor.weight
                if let workLimitReason = workBudget?.limitReason {
                    limitReason = workLimitReason
                    break search
                }
                guard weight.isFinite else {
                    throw WeightedShortestPathError.nonFiniteWeight(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel,
                        weight: weight
                    )
                }
                guard weight >= 0 else {
                    throw WeightedShortestPathError.negativeWeight(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel,
                        weight: weight
                    )
                }

                let newDist = currentDist + weight
                guard newDist.isFinite else {
                    throw WeightedShortestPathError.weightOverflow(
                        source: neighbor.source,
                        target: neighbor.target,
                        edgeLabel: neighbor.edgeLabel
                    )
                }
                guard newDist <= effectiveMaxWeight else {
                    if case .none = limitReason {
                        limitReason = .maxWeightReached(
                            weight: newDist,
                            limit: effectiveMaxWeight
                        )
                    }
                    continue
                }

                let oldDist = distances[neighbor.target] ?? .infinity
                if newDist < oldDist {
                    distances[neighbor.target] = newDist
                    parents[neighbor.target] = currentNode
                    edgeLabels[neighbor.target] = neighbor.edgeLabel
                    priorityQueue.insert(PriorityNode(nodeID: neighbor.target, distance: newDist))
                }
            }
        }

        return SingleSourceResult(
            source: source,
            distances: distances,
            parents: parents,
            edgeLabels: edgeLabels,
            nodesExplored: nodesExplored,
            durationNs: clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: limitReason == nil,
            limitReason: limitReason
        )
    }

    // MARK: - Private Methods

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }

    /// Reconstruct path from parent pointers
    private func reconstructPath(
        from source: GraphIdentity,
        to target: GraphIdentity,
        parents: [GraphIdentity: GraphIdentity],
        edgeLabels: [GraphIdentity: GraphIdentity],
        distances: [GraphIdentity: Double]
    ) throws -> GraphPath {
        var reversedNodeIDs: [GraphIdentity] = [target]
        var reversedEdgeLabels: [GraphIdentity] = []
        var reversedWeights: [Double] = []
        var current = target
        var visited: Set<GraphIdentity> = [target]

        while let parent = parents[current] {
            guard let edgeLabel = edgeLabels[current] else {
                throw WeightedShortestPathError.missingPathEdgeLabel(node: current)
            }
            guard let currentDistance = distances[current] else {
                throw WeightedShortestPathError.missingPathDistance(node: current)
            }
            guard let parentDistance = distances[parent] else {
                throw WeightedShortestPathError.missingPathDistance(node: parent)
            }
            guard visited.insert(parent).inserted else {
                throw WeightedShortestPathError.pathParentCycle(node: parent)
            }
            reversedNodeIDs.append(parent)
            reversedEdgeLabels.append(edgeLabel)
            reversedWeights.append(currentDistance - parentDistance)
            current = parent
        }
        guard current == source else {
            throw WeightedShortestPathError.pathDoesNotReachSource(
                expected: source,
                actual: current
            )
        }

        return try GraphPath(
            nodeIDs: Array(reversedNodeIDs.reversed()),
            edgeLabels: Array(reversedEdgeLabels.reversed()),
            weights: Array(reversedWeights.reversed())
        )
    }
}

// MARK: - MinHeap

/// Simple min-heap implementation for priority queue
///
/// **Reference**: CLRS "Introduction to Algorithms", Chapter 6
private struct MinHeap<T: Comparable>: Sendable where T: Sendable {
    private var elements: [T] = []

    var isEmpty: Bool { elements.isEmpty }
    var count: Int { elements.count }

    mutating func insert(_ element: T) {
        elements.append(element)
        siftUp(from: elements.count - 1)
    }

    mutating func extractMin() -> T? {
        guard !elements.isEmpty else { return nil }

        if elements.count == 1 {
            return elements.removeLast()
        }

        let min = elements[0]
        elements[0] = elements.removeLast()
        siftDown(from: 0)
        return min
    }

    private mutating func siftUp(from index: Int) {
        var child = index
        var parent = (child - 1) / 2

        while child > 0 && elements[child] < elements[parent] {
            elements.swapAt(child, parent)
            child = parent
            parent = (child - 1) / 2
        }
    }

    private mutating func siftDown(from index: Int) {
        var parent = index
        let count = elements.count

        while true {
            let leftChild = 2 * parent + 1
            let rightChild = 2 * parent + 2
            var smallest = parent

            if leftChild < count && elements[leftChild] < elements[smallest] {
                smallest = leftChild
            }

            if rightChild < count && elements[rightChild] < elements[smallest] {
                smallest = rightChild
            }

            if smallest == parent {
                break
            }

            elements.swapAt(parent, smallest)
            parent = smallest
        }
    }
}
