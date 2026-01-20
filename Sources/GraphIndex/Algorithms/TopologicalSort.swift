// TopologicalSort.swift
// GraphIndex - Topological sorting using Kahn's algorithm
//
// Provides topological ordering for directed acyclic graphs (DAGs).

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - TopologicalSortConfiguration

/// Configuration for topological sorting
public struct TopologicalSortConfiguration: Sendable {
    /// Maximum nodes to process (default: 100000)
    public let maxNodes: Int

    /// Batch size for transaction processing (default: 100)
    public let batchSize: Int

    /// Default configuration
    public static let `default` = TopologicalSortConfiguration()

    public init(
        maxNodes: Int = 100000,
        batchSize: Int = 100
    ) {
        self.maxNodes = maxNodes
        self.batchSize = batchSize
    }
}

// MARK: - TopologicalSortResult

/// Result of topological sort operation
public struct TopologicalSortResult: Sendable {
    /// Topological order (nil if graph has cycle)
    public let order: [String]?

    /// Whether the graph has a cycle (preventing topological order)
    ///
    /// **Important**: This is only definitive when `isComplete` is true.
    /// If `isComplete` is false, the absence of a detected cycle does not
    /// guarantee the graph is acyclic - we may simply have not explored enough.
    public let hasCycle: Bool

    /// Nodes that are part of cycles (if any)
    public let cyclicNodes: Set<String>

    /// Total nodes in the graph
    public let totalNodes: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Whether the result is complete (no limits reached).
    ///
    /// When `false`, the algorithm stopped due to a limit (e.g., maxNodes).
    /// In this case, `hasCycle` may be a false negative (cycle exists but not detected).
    public let isComplete: Bool

    /// Reason for incompleteness (if any).
    ///
    /// Non-nil when `isComplete` is false.
    public let limitReason: LimitReason?

    /// Whether the sort was successful (order available AND complete)
    public var isSuccess: Bool { order != nil && isComplete }

    /// Whether cycle detection is definitive.
    ///
    /// Returns `true` if:
    /// - We detected a cycle (definitive positive), OR
    /// - We completed the full traversal (definitive negative)
    ///
    /// Returns `false` if we hit a limit before completion,
    /// meaning we cannot definitively say whether a cycle exists.
    public var isCycleDefinitive: Bool { isComplete || hasCycle }

    public init(
        order: [String]?,
        hasCycle: Bool,
        cyclicNodes: Set<String>,
        totalNodes: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.order = order
        self.hasCycle = hasCycle
        self.cyclicNodes = cyclicNodes
        self.totalNodes = totalNodes
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}

// MARK: - TopologicalSorter

/// Topological sorting using Kahn's algorithm
///
/// Computes a linear ordering of vertices such that for every directed
/// edge (u, v), vertex u comes before v in the ordering.
///
/// **Algorithm**: Kahn's Algorithm (BFS-based)
/// 1. Compute in-degree for all vertices
/// 2. Initialize queue with vertices having in-degree 0
/// 3. Process queue: for each vertex, decrement in-degree of neighbors
/// 4. Add neighbors with in-degree 0 to queue
/// 5. If all vertices processed, return order. Otherwise, cycle exists.
///
/// **Time Complexity**: O(V + E)
/// **Space Complexity**: O(V) for in-degree and queue storage
///
/// **Transaction Strategy**:
/// - Edge scanning uses batch transactions
/// - In-degree computation in single pass
/// - BFS processing maintains state in memory
///
/// **Reference**: Kahn, A.B. "Topological sorting of large networks"
///               Communications of the ACM (1962)
///
/// **Usage**:
/// ```swift
/// let sorter = TopologicalSorter<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// // Get topological order
/// let result = try await sorter.sort(edgeLabel: "depends_on")
/// if let order = result.order {
///     print("Build order: \(order.joined(separator: " -> "))")
/// } else {
///     print("Circular dependency detected!")
///     print("Cyclic nodes: \(result.cyclicNodes)")
/// }
///
/// // Get all dependencies of a node
/// let deps = try await sorter.dependencies(of: "module_A", edgeLabel: "depends_on")
///
/// // Get all dependents of a node
/// let dependents = try await sorter.dependents(of: "module_A", edgeLabel: "depends_on")
/// ```
public final class TopologicalSorter<Edge: Persistable>: Sendable {

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let subspace: Subspace

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: TopologicalSortConfiguration

    // MARK: - Initialization

    /// Initialize topological sorter
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        configuration: TopologicalSortConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(indexSubspace: subspace)
    }

    // MARK: - Public API

    /// Compute topological order of all nodes
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: TopologicalSortResult with order or cycle information
    public func sort(edgeLabel: String? = nil) async throws -> TopologicalSortResult {
        let (result, _) = try await sortWithGraph(edgeLabel: edgeLabel)
        return result
    }

    /// Internal: Compute topological order and return adjacency list for reuse
    private func sortWithGraph(edgeLabel: String?) async throws -> (TopologicalSortResult, [String: [String]]) {
        let startTime = DispatchTime.now()

        // Step 1: Collect all nodes and compute in-degrees
        let (nodes, inDegree, adjacency) = try await buildGraph(edgeLabel: edgeLabel)

        guard !nodes.isEmpty else {
            return (TopologicalSortResult(
                order: [],
                hasCycle: false,
                cyclicNodes: [],
                totalNodes: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: true,
                limitReason: nil
            ), adjacency)
        }

        // Step 2: Initialize queue with nodes having in-degree 0
        var queue: [String] = []
        var currentInDegree = inDegree

        for node in nodes {
            if (currentInDegree[node] ?? 0) == 0 {
                queue.append(node)
            }
        }

        // Step 3: Process queue using Kahn's algorithm
        // Use index-based iteration to avoid O(n) removeFirst()
        var result: [String] = []
        var queueIndex = 0

        while queueIndex < queue.count {
            let node = queue[queueIndex]
            queueIndex += 1
            result.append(node)

            // Check node limit
            if result.count >= configuration.maxNodes {
                break
            }

            // Decrement in-degree of neighbors
            if let neighbors = adjacency[node] {
                for neighbor in neighbors {
                    if var degree = currentInDegree[neighbor] {
                        degree -= 1
                        currentInDegree[neighbor] = degree

                        if degree == 0 {
                            queue.append(neighbor)
                        }
                    }
                }
            }
        }

        // Step 4: Determine completion status and check for cycles
        //
        // CRITICAL: Distinguish between:
        // 1. maxNodes reached (incomplete traversal, can't determine cycle)
        // 2. Actual cycle (all nodes explored, some couldn't be processed)
        let hitMaxNodes = result.count >= configuration.maxNodes && result.count < nodes.count
        let isComplete = !hitMaxNodes
        let limitReason: LimitReason? = hitMaxNodes
            ? .maxNodesReached(explored: result.count, limit: configuration.maxNodes)
            : nil

        // Only definitively report a cycle if we completed the traversal
        // and still have unprocessed nodes (which must be in cycles)
        let hasCycle = isComplete && result.count != nodes.count
        var cyclicNodes: Set<String> = []

        if hasCycle {
            // Nodes not in result are part of cycles
            // Use Set for O(1) lookup instead of O(n) array.contains
            let processedNodes = Set(result)
            for node in nodes {
                if !processedNodes.contains(node) {
                    cyclicNodes.insert(node)
                }
            }
        }

        return (TopologicalSortResult(
            order: hasCycle ? nil : result,
            hasCycle: hasCycle,
            cyclicNodes: cyclicNodes,
            totalNodes: nodes.count,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: isComplete,
            limitReason: limitReason
        ), adjacency)
    }

    /// Get all dependencies of a node (transitively)
    ///
    /// Returns all nodes that the given node depends on, directly or indirectly.
    ///
    /// - Parameters:
    ///   - nodeID: The node to find dependencies for
    ///   - edgeLabel: Optional edge label filter
    /// - Returns: Array of dependent node IDs in topological order
    public func dependencies(
        of nodeID: String,
        edgeLabel: String? = nil
    ) async throws -> [String] {
        // BFS backwards through incoming edges
        // Use index-based iteration to avoid O(n) removeFirst()
        var visited: Set<String> = [nodeID]
        var queue: [String] = [nodeID]
        var queueIndex = 0
        var result: [String] = []

        while queueIndex < queue.count {
            let current = queue[queueIndex]
            queueIndex += 1

            // Get incoming edges (nodes that current depends on)
            let predecessors = try await getIncomingNeighbors(to: current, edgeLabel: edgeLabel)

            for pred in predecessors {
                if !visited.contains(pred) {
                    visited.insert(pred)
                    queue.append(pred)
                    result.append(pred)
                }
            }

            // Check node limit
            if result.count >= configuration.maxNodes {
                break
            }
        }

        // Return in reverse order (deepest dependencies first)
        return result.reversed()
    }

    /// Get all dependents of a node (transitively)
    ///
    /// Returns all nodes that depend on the given node, directly or indirectly.
    ///
    /// - Parameters:
    ///   - nodeID: The node to find dependents for
    ///   - edgeLabel: Optional edge label filter
    /// - Returns: Array of dependent node IDs in topological order
    public func dependents(
        of nodeID: String,
        edgeLabel: String? = nil
    ) async throws -> [String] {
        // BFS forward through outgoing edges
        // Use index-based iteration to avoid O(n) removeFirst()
        var visited: Set<String> = [nodeID]
        var queue: [String] = [nodeID]
        var queueIndex = 0
        var result: [String] = []

        while queueIndex < queue.count {
            let current = queue[queueIndex]
            queueIndex += 1

            // Get outgoing edges (nodes that depend on current)
            let successors = try await getOutgoingNeighbors(from: current, edgeLabel: edgeLabel)

            for succ in successors {
                if !visited.contains(succ) {
                    visited.insert(succ)
                    queue.append(succ)
                    result.append(succ)
                }
            }

            // Check node limit
            if result.count >= configuration.maxNodes {
                break
            }
        }

        return result
    }

    /// Get the critical path (longest path) through the DAG
    ///
    /// Useful for determining the minimum execution time when nodes
    /// represent tasks with durations.
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: Array of node IDs representing the critical path
    public func criticalPath(edgeLabel: String? = nil) async throws -> [String] {
        // Get topological order and adjacency list in single pass
        let (sortResult, adjacency) = try await sortWithGraph(edgeLabel: edgeLabel)

        guard let order = sortResult.order, !order.isEmpty else {
            return []
        }

        // Compute longest distance to each node
        var distance: [String: Int] = [:]
        var predecessor: [String: String] = [:]

        for node in order {
            distance[node] = 0
        }

        // Process in topological order
        for node in order {
            let currentDist = distance[node] ?? 0

            if let neighbors = adjacency[node] {
                for neighbor in neighbors {
                    let newDist = currentDist + 1
                    if newDist > (distance[neighbor] ?? 0) {
                        distance[neighbor] = newDist
                        predecessor[neighbor] = node
                    }
                }
            }
        }

        // Find the node with maximum distance (end of critical path)
        var maxDist = 0
        var endNode: String?

        for (node, dist) in distance {
            if dist > maxDist {
                maxDist = dist
                endNode = node
            }
        }

        // Reconstruct the critical path
        guard let end = endNode else {
            return order.isEmpty ? [] : [order[0]]
        }

        var path: [String] = [end]
        var current = end

        while let pred = predecessor[current] {
            path.insert(pred, at: 0)
            current = pred
        }

        return path
    }

    // MARK: - Private Methods

    /// Build graph representation from index using GraphEdgeScanner
    private func buildGraph(edgeLabel: String?) async throws -> (
        nodes: Set<String>,
        inDegree: [String: Int],
        adjacency: [String: [String]]
    ) {
        let graphData: (nodes: Set<String>, inDegree: [String: Int], adjacency: [String: [String]]) =
            try await database.withTransaction(configuration: .batch) { transaction in
                var nodes: Set<String> = []
                var inDegree: [String: Int] = [:]
                var adjacency: [String: [String]] = [:]

                for try await edgeInfo in self.scanner.scanAllEdges(
                    edgeLabel: edgeLabel,
                    transaction: transaction
                ) {
                    let from = edgeInfo.source
                    let to = edgeInfo.target

                    // Add nodes
                    nodes.insert(from)
                    nodes.insert(to)

                    // Initialize in-degrees
                    if inDegree[from] == nil {
                        inDegree[from] = 0
                    }
                    inDegree[to, default: 0] += 1

                    // Build adjacency list
                    adjacency[from, default: []].append(to)
                }

                return (nodes, inDegree, adjacency)
            }

        return graphData
    }

    /// Get outgoing neighbors of a node using GraphEdgeScanner
    private func getOutgoingNeighbors(
        from nodeID: String,
        edgeLabel: String?
    ) async throws -> [String] {
        try await database.withTransaction(configuration: .default) { transaction in
            var results: [String] = []
            for try await edgeInfo in self.scanner.scanOutgoing(
                from: nodeID,
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                results.append(edgeInfo.target)
            }
            return results
        }
    }

    /// Get incoming neighbors of a node using GraphEdgeScanner
    private func getIncomingNeighbors(
        to nodeID: String,
        edgeLabel: String?
    ) async throws -> [String] {
        try await database.withTransaction(configuration: .default) { transaction in
            var results: [String] = []
            for try await edgeInfo in self.scanner.scanIncoming(
                to: nodeID,
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                results.append(edgeInfo.source)
            }
            return results
        }
    }
}
