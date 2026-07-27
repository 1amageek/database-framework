// TopologicalSort.swift
// GraphIndex - Topological sorting using Kahn's algorithm
//
// Provides topological ordering for directed acyclic graphs (DAGs).

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseEngine
import StorageKit
import DatabaseKit

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
        self.maxNodes = Swift.max(1, maxNodes)
        self.batchSize = Swift.max(1, batchSize)
    }
}

// MARK: - TopologicalSortResult

/// Result of topological sort operation
public struct TopologicalSortResult: Sendable {
    /// Topological order (nil if graph has cycle)
    public let order: [GraphIdentity]?

    /// Whether the graph has a cycle (preventing topological order)
    ///
    /// **Important**: This is only definitive when `isComplete` is true.
    /// If `isComplete` is false, the absence of a detected cycle does not
    /// guarantee the graph is acyclic - we may simply have not explored enough.
    public let hasCycle: Bool

    /// Nodes that are part of cycles (if any)
    public let cyclicNodes: Set<GraphIdentity>

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
        order: [GraphIdentity]?,
        hasCycle: Bool,
        cyclicNodes: Set<GraphIdentity>,
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
public final class TopologicalSorter: Sendable {

    // MARK: - Properties

    /// Storage snapshot shared by the complete computation.
    private let snapshot: GraphReadSnapshot

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: TopologicalSortConfiguration

    /// Shared request work budget.
    private let workBudget: GraphAlgorithmWorkBudget?

    // MARK: - Initialization

    /// Initialize topological sorter
    ///
    /// - Parameters:
    ///   - snapshot: Stable storage snapshot for the complete computation
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - configuration: Algorithm configuration
    package init(
        snapshot: GraphReadSnapshot,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        scope: GraphScanScope = .all,
        configuration: TopologicalSortConfiguration = .default
    ) {
        self.snapshot = snapshot
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: strategy,
            scope: scope,
            snapshot: snapshot
        )
        self.workBudget = snapshot.workBudget
    }

    // MARK: - Public API

    /// Compute topological order of all nodes
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: TopologicalSortResult with order or cycle information
    public func sort(edgeLabel: GraphIdentity? = nil) async throws -> TopologicalSortResult {
        let (result, _) = try await sortWithGraph(edgeLabel: edgeLabel)
        return result
    }

    /// Internal: Compute topological order and return adjacency list for reuse
    private func sortWithGraph(edgeLabel: GraphIdentity?) async throws -> (TopologicalSortResult, [GraphIdentity: [GraphIdentity]]) {
        let startTime = MonotonicClock.now()

        // Step 1: Collect all nodes and compute in-degrees
        let graph = try await buildGraph(edgeLabel: edgeLabel)
        let nodes = graph.nodes
        let inDegree = graph.inDegree
        let adjacency = graph.adjacency

        guard !nodes.isEmpty else {
            return (TopologicalSortResult(
                order: [],
                hasCycle: false,
                cyclicNodes: [],
                totalNodes: 0,
                durationNs: MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: graph.limitReason == nil,
                limitReason: graph.limitReason
            ), adjacency)
        }

        // Step 2: Initialize queue with nodes having in-degree 0
        var queue: [GraphIdentity] = []
        var currentInDegree = inDegree

        for node in nodes.sorted() {
            guard let degree = currentInDegree[node] else {
                throw TopologicalSortError.inconsistentState(
                    "missing in-degree for graph node"
                )
            }
            if degree == 0 {
                queue.append(node)
            }
        }

        // Step 3: Process queue using Kahn's algorithm
        // Use index-based iteration to avoid O(n) removeFirst()
        var result: [GraphIdentity] = []
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
                    guard var degree = currentInDegree[neighbor] else {
                        throw TopologicalSortError.inconsistentState(
                            "missing in-degree for adjacent node"
                        )
                    }
                    degree -= 1
                    currentInDegree[neighbor] = degree

                    if degree == 0 {
                        queue.append(neighbor)
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
        let isComplete = graph.limitReason == nil && !hitMaxNodes
        let limitReason: LimitReason? = graph.limitReason ?? (hitMaxNodes
            ? .maxNodesReached(explored: result.count, limit: configuration.maxNodes)
            : nil)

        // Only definitively report a cycle if we completed the traversal
        // and still have unprocessed nodes (which must be in cycles)
        let hasCycle = isComplete && result.count != nodes.count
        var cyclicNodes: Set<GraphIdentity> = []

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
            durationNs: MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
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
        of nodeID: GraphIdentity,
        edgeLabel: GraphIdentity? = nil
    ) async throws -> [GraphIdentity] {
        // BFS backwards through incoming edges
        // Use index-based iteration to avoid O(n) removeFirst()
        var visited: Set<GraphIdentity> = [nodeID]
        var queue: [GraphIdentity] = [nodeID]
        var queueIndex = 0
        var result: [GraphIdentity] = []

        while queueIndex < queue.count {
            let current = queue[queueIndex]
            queueIndex += 1

            // Get incoming edges (nodes that current depends on)
            let predecessors = try await getIncomingNeighbors(to: current, edgeLabel: edgeLabel)

            for pred in predecessors {
                if !visited.contains(pred) {
                    guard result.count < configuration.maxNodes else {
                        throw TopologicalSortError.incomplete(
                            .maxNodesReached(
                                explored: result.count,
                                limit: configuration.maxNodes
                            )
                        )
                    }
                    visited.insert(pred)
                    queue.append(pred)
                    result.append(pred)
                }
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
        of nodeID: GraphIdentity,
        edgeLabel: GraphIdentity? = nil
    ) async throws -> [GraphIdentity] {
        // BFS forward through outgoing edges
        // Use index-based iteration to avoid O(n) removeFirst()
        var visited: Set<GraphIdentity> = [nodeID]
        var queue: [GraphIdentity] = [nodeID]
        var queueIndex = 0
        var result: [GraphIdentity] = []

        while queueIndex < queue.count {
            let current = queue[queueIndex]
            queueIndex += 1

            // Get outgoing edges (nodes that depend on current)
            let successors = try await getOutgoingNeighbors(from: current, edgeLabel: edgeLabel)

            for succ in successors {
                if !visited.contains(succ) {
                    guard result.count < configuration.maxNodes else {
                        throw TopologicalSortError.incomplete(
                            .maxNodesReached(
                                explored: result.count,
                                limit: configuration.maxNodes
                            )
                        )
                    }
                    visited.insert(succ)
                    queue.append(succ)
                    result.append(succ)
                }
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
    public func criticalPath(edgeLabel: GraphIdentity? = nil) async throws -> [GraphIdentity] {
        // Get topological order and adjacency list in single pass
        let (sortResult, adjacency) = try await sortWithGraph(edgeLabel: edgeLabel)

        if let limitReason = sortResult.limitReason {
            throw TopologicalSortError.incomplete(limitReason)
        }
        guard sortResult.isComplete else {
            throw TopologicalSortError.inconsistentState(
                "incomplete topological result has no limit reason"
            )
        }
        guard !sortResult.hasCycle else {
            throw TopologicalSortError.cyclicGraph(sortResult.cyclicNodes)
        }
        guard let order = sortResult.order else {
            throw TopologicalSortError.inconsistentState(
                "complete acyclic result has no order"
            )
        }
        guard !order.isEmpty else {
            return []
        }

        // Compute longest distance to each node
        var distance: [GraphIdentity: Int] = [:]
        var predecessor: [GraphIdentity: GraphIdentity] = [:]

        for node in order {
            distance[node] = 0
        }

        // Process in topological order
        for node in order {
            guard let currentDist = distance[node] else {
                throw TopologicalSortError.inconsistentState(
                    "missing critical-path distance"
                )
            }

            if let neighbors = adjacency[node] {
                for neighbor in neighbors {
                    guard let neighborDistance = distance[neighbor] else {
                        throw TopologicalSortError.inconsistentState(
                            "missing adjacent critical-path distance"
                        )
                    }
                    let newDist = currentDist + 1
                    if newDist > neighborDistance {
                        distance[neighbor] = newDist
                        predecessor[neighbor] = node
                    }
                }
            }
        }

        // Find the node with maximum distance (end of critical path)
        var maxDist = 0
        var endNode: GraphIdentity?

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

        var reversedPath: [GraphIdentity] = [end]
        var current = end
        var visited: Set<GraphIdentity> = [end]

        while let pred = predecessor[current] {
            guard visited.insert(pred).inserted else {
                throw TopologicalSortError.inconsistentState(
                    "critical-path predecessor cycle"
                )
            }
            reversedPath.append(pred)
            current = pred
        }

        return Array(reversedPath.reversed())
    }

    // MARK: - Private Methods

    /// Build graph representation from index using GraphEdgeScanner
    private func buildGraph(edgeLabel: GraphIdentity?) async throws -> (
        nodes: Set<GraphIdentity>,
        inDegree: [GraphIdentity: Int],
        adjacency: [GraphIdentity: [GraphIdentity]],
        limitReason: LimitReason?
    ) {
        var nodes: Set<GraphIdentity> = []
        var inDegree: [GraphIdentity: Int] = [:]
        var adjacency: [GraphIdentity: [GraphIdentity]] = [:]
        var limitReason: LimitReason?

        guard try consumeWork() else {
            return (nodes, inDegree, adjacency, workBudget?.limitReason)
        }

        for try await edgeInfo in scanner.scanAllEdges(
            edgeLabel: edgeLabel,
            transaction: snapshot.transaction
        ) {
            guard try consumeWork() else {
                limitReason = workBudget?.limitReason
                break
            }
            let from = edgeInfo.source
            let to = edgeInfo.target

            var missingNodeCount = nodes.contains(from) ? 0 : 1
            if to != from, !nodes.contains(to) {
                missingNodeCount += 1
            }
            guard nodes.count + missingNodeCount <= configuration.maxNodes else {
                limitReason = .maxNodesReached(
                    explored: nodes.count,
                    limit: configuration.maxNodes
                )
                break
            }

            nodes.insert(from)
            nodes.insert(to)

            if inDegree[from] == nil {
                inDegree[from] = 0
            }
            inDegree[to, default: 0] += 1
            adjacency[from, default: []].append(to)
        }

        return (nodes, inDegree, adjacency, limitReason)
    }

    /// Get outgoing neighbors of a node using GraphEdgeScanner
    private func getOutgoingNeighbors(
        from nodeID: GraphIdentity,
        edgeLabel: GraphIdentity?
    ) async throws -> [GraphIdentity] {
        var results: [GraphIdentity] = []
        guard try consumeWork() else {
            throw TopologicalSortError.incomplete(
                try requiredWorkLimitReason()
            )
        }
        for try await edgeInfo in scanner.scanOutgoing(
            from: nodeID,
            edgeLabel: edgeLabel,
            transaction: snapshot.transaction
        ) {
            guard try consumeWork() else {
                throw TopologicalSortError.incomplete(
                    try requiredWorkLimitReason()
                )
            }
            results.append(edgeInfo.target)
        }
        return results
    }

    /// Get incoming neighbors of a node using GraphEdgeScanner
    private func getIncomingNeighbors(
        to nodeID: GraphIdentity,
        edgeLabel: GraphIdentity?
    ) async throws -> [GraphIdentity] {
        var results: [GraphIdentity] = []
        guard try consumeWork() else {
            throw TopologicalSortError.incomplete(
                try requiredWorkLimitReason()
            )
        }
        for try await edgeInfo in scanner.scanIncoming(
            to: nodeID,
            edgeLabel: edgeLabel,
            transaction: snapshot.transaction
        ) {
            guard try consumeWork() else {
                throw TopologicalSortError.incomplete(
                    try requiredWorkLimitReason()
                )
            }
            results.append(edgeInfo.source)
        }
        return results
    }

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }

    private func requiredWorkLimitReason() throws -> LimitReason {
        guard let limitReason = workBudget?.limitReason else {
            throw TopologicalSortError.inconsistentState(
                "work was rejected without a limit reason"
            )
        }
        return limitReason
    }
}

public enum TopologicalSortError: Error, Sendable, Equatable {
    case incomplete(LimitReason)
    case cyclicGraph(Set<GraphIdentity>)
    case inconsistentState(String)
}
