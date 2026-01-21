// WeightedShortestPath.swift
// GraphIndex - Dijkstra's shortest path algorithm for weighted graphs
//
// Provides efficient weighted shortest path finding using priority queues.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

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
        self.maxNodes = maxNodes
        self.batchSize = batchSize
    }
}

// MARK: - WeightedPathResult

/// Result of weighted shortest path computation
public struct WeightedPathResult<Edge: Persistable>: Sendable {
    /// The path if one exists (nil if no path found)
    public let path: GraphPath<Edge>?

    /// Total weight of the path (Double.infinity if no path)
    public let totalWeight: Double

    /// Number of nodes explored during search
    public let nodesExplored: Int

    /// Number of edges relaxed
    public let edgesRelaxed: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Whether a path was found
    public var found: Bool { path != nil }

    public init(
        path: GraphPath<Edge>?,
        totalWeight: Double,
        nodesExplored: Int,
        edgesRelaxed: Int,
        durationNs: UInt64
    ) {
        self.path = path
        self.totalWeight = totalWeight
        self.nodesExplored = nodesExplored
        self.edgesRelaxed = edgesRelaxed
        self.durationNs = durationNs
    }
}

// MARK: - SingleSourceResult

/// Result of single-source shortest paths computation
public struct SingleSourceResult<Edge: Persistable>: Sendable {
    /// Distances to all reachable nodes
    public let distances: [String: Double]

    /// Parent pointers for path reconstruction
    public let parents: [String: String]

    /// Edge labels used in shortest paths
    public let edgeLabels: [String: String]

    /// Number of nodes explored
    public let nodesExplored: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Get the shortest path to a target node
    public func pathTo(_ target: String) -> GraphPath<Edge>? {
        guard distances[target] != nil else { return nil }

        var nodeIDs: [String] = [target]
        var current = target

        while let parent = parents[current] {
            nodeIDs.insert(parent, at: 0)
            current = parent
        }

        return GraphPath(
            nodeIDs: nodeIDs,
            edgeLabels: nodeIDs.dropFirst().compactMap { edgeLabels[$0] },
            weights: computeWeights(for: nodeIDs)
        )
    }

    private func computeWeights(for nodeIDs: [String]) -> [Double] {
        guard nodeIDs.count > 1 else { return [] }

        var weights: [Double] = []
        for i in 1..<nodeIDs.count {
            let current = nodeIDs[i]
            let parent = nodeIDs[i - 1]
            let currentDist = distances[current] ?? 0
            let parentDist = distances[parent] ?? 0
            weights.append(currentDist - parentDist)
        }
        return weights
    }

    public init(
        distances: [String: Double],
        parents: [String: String],
        edgeLabels: [String: String],
        nodesExplored: Int,
        durationNs: UInt64
    ) {
        self.distances = distances
        self.parents = parents
        self.edgeLabels = edgeLabels
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
    }
}

// MARK: - WeightedShortestPathFinder

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
public final class WeightedShortestPathFinder<Edge: Persistable>: Sendable {

    // MARK: - Types

    /// Weight extraction closure type
    public typealias WeightExtractor = @Sendable (Edge) -> Double

    /// Internal priority queue node
    private struct PriorityNode: Comparable, Sendable {
        let nodeID: String
        let distance: Double

        static func < (lhs: PriorityNode, rhs: PriorityNode) -> Bool {
            lhs.distance < rhs.distance
        }

        static func == (lhs: PriorityNode, rhs: PriorityNode) -> Bool {
            lhs.nodeID == rhs.nodeID && lhs.distance == rhs.distance
        }
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let subspace: Subspace

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: WeightedShortestPathConfiguration

    // MARK: - Initialization

    /// Initialize weighted shortest path finder
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        configuration: WeightedShortestPathConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(indexSubspace: subspace, strategy: strategy)
    }

    // MARK: - Public API

    /// Find weighted shortest path between two nodes
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - target: Target node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - weightExtractor: Closure to extract weight from edge
    ///   - edgeLoader: Closure to load edge data by (source, target, label)
    ///   - maxWeight: Maximum weight to explore (overrides config)
    /// - Returns: WeightedPathResult with path and total weight
    public func findShortestPath(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        weightExtractor: @escaping WeightExtractor,
        edgeLoader: @escaping @Sendable (String, String, String?) async throws -> Edge?,
        maxWeight: Double? = nil
    ) async throws -> WeightedPathResult<Edge> {
        let startTime = DispatchTime.now()
        let effectiveMaxWeight = maxWeight ?? configuration.maxWeight

        // Early termination: source == target
        if source == target {
            let path = GraphPath<Edge>(singleNode: source)
            return WeightedPathResult(
                path: path,
                totalWeight: 0,
                nodesExplored: 1,
                edgesRelaxed: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Initialize Dijkstra state
        var distances: [String: Double] = [source: 0]
        var parents: [String: String] = [:]
        var edgeLabels: [String: String] = [:]
        var visited: Set<String> = []
        var priorityQueue = MinHeap<PriorityNode>()
        priorityQueue.insert(PriorityNode(nodeID: source, distance: 0))

        var nodesExplored = 0
        var edgesRelaxed = 0

        // Main Dijkstra loop
        while let current = priorityQueue.extractMin() {
            let currentNode = current.nodeID
            let currentDist = current.distance

            // Skip if already visited (stale queue entry)
            if visited.contains(currentNode) {
                continue
            }

            visited.insert(currentNode)
            nodesExplored += 1

            // Early termination: reached target
            if currentNode == target {
                let path = reconstructPath(
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
                    durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                )
            }

            // Check max weight bound
            if currentDist > effectiveMaxWeight {
                continue
            }

            // Check max nodes limit
            if nodesExplored >= configuration.maxNodes {
                break
            }

            // Get neighbors and relax edges using GraphEdgeScanner
            let neighbors = try await database.withTransaction(configuration: .default) { transaction in
                var results: [EdgeInfo] = []
                for try await edgeInfo in self.scanner.scanOutgoing(
                    from: currentNode,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                ) {
                    results.append(edgeInfo)
                }
                return results
            }

            for neighbor in neighbors {
                if visited.contains(neighbor.target) {
                    continue
                }

                // Load edge to get weight
                guard let edge = try await edgeLoader(neighbor.source, neighbor.target, neighbor.edgeLabel) else {
                    continue
                }

                let weight = weightExtractor(edge)

                // Skip negative weights (not supported by Dijkstra)
                guard weight >= 0 else {
                    continue
                }

                let newDist = currentDist + weight
                edgesRelaxed += 1

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
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    /// Find shortest paths from source to all reachable nodes
    ///
    /// - Parameters:
    ///   - source: Source node ID
    ///   - edgeLabel: Optional edge label filter
    ///   - weightExtractor: Closure to extract weight from edge
    ///   - edgeLoader: Closure to load edge data
    ///   - maxWeight: Maximum weight to explore
    /// - Returns: SingleSourceResult with distances to all reachable nodes
    public func findShortestPaths(
        from source: String,
        edgeLabel: String? = nil,
        weightExtractor: @escaping WeightExtractor,
        edgeLoader: @escaping @Sendable (String, String, String?) async throws -> Edge?,
        maxWeight: Double? = nil
    ) async throws -> SingleSourceResult<Edge> {
        let startTime = DispatchTime.now()
        let effectiveMaxWeight = maxWeight ?? configuration.maxWeight

        // Initialize Dijkstra state
        var distances: [String: Double] = [source: 0]
        var parents: [String: String] = [:]
        var edgeLabels: [String: String] = [:]
        var visited: Set<String> = []
        var priorityQueue = MinHeap<PriorityNode>()
        priorityQueue.insert(PriorityNode(nodeID: source, distance: 0))

        var nodesExplored = 0

        // Main Dijkstra loop
        while let current = priorityQueue.extractMin() {
            let currentNode = current.nodeID
            let currentDist = current.distance

            // Skip if already visited
            if visited.contains(currentNode) {
                continue
            }

            visited.insert(currentNode)
            nodesExplored += 1

            // Check bounds
            if currentDist > effectiveMaxWeight {
                continue
            }

            if nodesExplored >= configuration.maxNodes {
                break
            }

            // Get neighbors and relax edges using GraphEdgeScanner
            let neighbors = try await database.withTransaction(configuration: .default) { transaction in
                var results: [EdgeInfo] = []
                for try await edgeInfo in self.scanner.scanOutgoing(
                    from: currentNode,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                ) {
                    results.append(edgeInfo)
                }
                return results
            }

            for neighbor in neighbors {
                if visited.contains(neighbor.target) {
                    continue
                }

                // Load edge to get weight
                guard let edge = try await edgeLoader(neighbor.source, neighbor.target, neighbor.edgeLabel) else {
                    continue
                }

                let weight = weightExtractor(edge)
                guard weight >= 0 else { continue }

                let newDist = currentDist + weight

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
            distances: distances,
            parents: parents,
            edgeLabels: edgeLabels,
            nodesExplored: nodesExplored,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    // MARK: - Private Methods

    /// Reconstruct path from parent pointers
    private func reconstructPath(
        from source: String,
        to target: String,
        parents: [String: String],
        edgeLabels: [String: String],
        distances: [String: Double]
    ) -> GraphPath<Edge> {
        var nodeIDs: [String] = [target]
        var current = target

        while let parent = parents[current] {
            nodeIDs.insert(parent, at: 0)
            current = parent
        }

        // Compute edge labels and weights for the path
        var labels: [String] = []
        var weights: [Double] = []

        for i in 0..<(nodeIDs.count - 1) {
            let from = nodeIDs[i]
            let to = nodeIDs[i + 1]
            let label = edgeLabels[to]

            if let label = label {
                labels.append(label)
            }

            // Calculate weight from distances
            let fromDist = distances[from] ?? 0
            let toDist = distances[to] ?? 0
            weights.append(toDist - fromDist)
        }

        return GraphPath(
            nodeIDs: nodeIDs,
            edgeLabels: labels,
            weights: weights
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
