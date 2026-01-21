// CycleDetector.swift
// GraphIndex - Cycle detection using DFS with coloring
//
// Provides efficient cycle detection for directed graphs.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - CycleDetectorConfiguration

/// Configuration for cycle detection
public struct CycleDetectorConfiguration: Sendable {
    /// Maximum cycles to detect (default: 100)
    public let maxCycles: Int

    /// Maximum nodes to explore (default: 100000)
    public let maxNodes: Int

    /// Batch size for transaction processing (default: 100)
    public let batchSize: Int

    /// Default configuration
    public static let `default` = CycleDetectorConfiguration()

    public init(
        maxCycles: Int = 100,
        maxNodes: Int = 100000,
        batchSize: Int = 100
    ) {
        self.maxCycles = maxCycles
        self.maxNodes = maxNodes
        self.batchSize = batchSize
    }
}

// MARK: - CycleDetectionError

/// Errors that can occur during cycle detection
public enum CycleDetectionError: Error, Sendable {
    /// Exploration limit was reached before a definitive answer could be determined.
    ///
    /// This occurs when `wouldCreateCycle()` cannot definitively determine
    /// whether adding an edge would create a cycle because the maxNodes
    /// limit was reached during path exploration.
    ///
    /// **Important**: This is NOT a "no cycle" result. The caller must handle
    /// this case appropriately (e.g., reject the edge insertion to be safe,
    /// or increase the limit and retry).
    case limitReached(message: String, explored: Int, limit: Int)
}

// MARK: - CycleInfo

/// Information about detected cycles
public struct CycleInfo: Sendable {
    /// Whether the graph contains at least one cycle
    ///
    /// **Important**: This is only definitive when `isComplete` is true.
    /// If `isComplete` is false, a `hasCycle == false` result means
    /// "no cycle found yet", not "no cycle exists".
    public let hasCycle: Bool

    /// Detected cycles (list of node IDs forming each cycle)
    public let cycles: [[String]]

    /// Back edges that indicate cycles (from -> to)
    public let backEdges: [(from: String, to: String)]

    /// Number of nodes explored during detection
    public let nodesExplored: Int

    /// Execution time in nanoseconds
    public let durationNs: UInt64

    /// Whether the detection is complete (no limits reached).
    ///
    /// When `false`, the algorithm stopped due to a limit (e.g., maxNodes, maxCycles).
    /// In this case, `hasCycle == false` does NOT mean the graph is acyclic.
    public let isComplete: Bool

    /// Reason for incompleteness (if any).
    ///
    /// Non-nil when `isComplete` is false.
    public let limitReason: LimitReason?

    /// Whether cycle detection is definitive.
    ///
    /// Returns `true` if:
    /// - We detected a cycle (definitive positive), OR
    /// - We completed the full traversal (definitive negative)
    ///
    /// Returns `false` if we hit a limit before completion,
    /// meaning we cannot definitively say whether more cycles exist.
    public var isCycleDefinitive: Bool { isComplete || hasCycle }

    public init(
        hasCycle: Bool,
        cycles: [[String]],
        backEdges: [(from: String, to: String)],
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.hasCycle = hasCycle
        self.cycles = cycles
        self.backEdges = backEdges
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}

// MARK: - CycleCheckResult

/// Lightweight result for cycle existence check
///
/// Use this when you only need to know if a cycle exists,
/// with explicit completeness tracking.
///
/// **Usage**:
/// ```swift
/// let result = try await detector.checkCycle(edgeLabel: "depends_on")
///
/// if result.isDefinitive {
///     if result.hasCycle {
///         print("Graph has a cycle")
///     } else {
///         print("Graph is acyclic (confirmed)")
///     }
/// } else {
///     print("Could not determine: \(result.limitReason)")
/// }
/// ```
public struct CycleCheckResult: Sendable {
    /// Whether at least one cycle was detected.
    ///
    /// When `isComplete == false` and `hasCycle == false`,
    /// this means "no cycle found yet" (not "definitely no cycle").
    public let hasCycle: Bool

    /// Whether the check is complete (all nodes explored).
    public let isComplete: Bool

    /// Reason for incompleteness (if any).
    public let limitReason: LimitReason?

    /// Whether the result is definitive.
    ///
    /// Returns `true` if:
    /// - We found a cycle (definitive positive), OR
    /// - We completed the full traversal (definitive negative)
    public var isDefinitive: Bool {
        hasCycle || isComplete
    }

    public init(hasCycle: Bool, isComplete: Bool, limitReason: LimitReason? = nil) {
        self.hasCycle = hasCycle
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}

// MARK: - CycleDetector

/// Cycle detection using DFS with three-color algorithm
///
/// Detects cycles in directed graphs using depth-first search with
/// node coloring to identify back edges.
///
/// **Algorithm**: DFS Three-Color (White/Gray/Black)
/// - White: Unvisited node
/// - Gray: Node currently being processed (in DFS stack)
/// - Black: Node completely processed (all descendants visited)
///
/// A back edge (edge from gray node to another gray ancestor) indicates a cycle.
///
/// **Time Complexity**: O(V + E)
/// **Space Complexity**: O(V) for color and parent tracking
///
/// **Transaction Strategy**:
/// - DFS uses iterative approach with explicit stack
/// - Neighbor lookups use batch transactions
/// - State maintained in memory
///
/// **Reference**: Cormen et al., "Introduction to Algorithms" (CLRS), Chapter 22
///
/// **Usage**:
/// ```swift
/// let detector = CycleDetector<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// // Check if graph has any cycle
/// let hasCycle = try await detector.hasCycle(edgeLabel: "depends_on")
///
/// // Find all cycles (up to maxCycles)
/// let cycleInfo = try await detector.findCycles(edgeLabel: "depends_on")
/// for cycle in cycleInfo.cycles {
///     print("Cycle: \(cycle.joined(separator: " -> "))")
/// }
///
/// // Check if adding an edge would create a cycle
/// let wouldCycle = try await detector.wouldCreateCycle(
///     from: "A",
///     to: "B",
///     edgeLabel: "depends_on"
/// )
/// ```
public final class CycleDetector<Edge: Persistable>: Sendable {

    // MARK: - Types

    /// Node color for DFS
    private enum Color: Sendable {
        case white  // Unvisited
        case gray   // In progress
        case black  // Completed
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let subspace: Subspace

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: CycleDetectorConfiguration

    // MARK: - Initialization

    /// Initialize cycle detector
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace (same as used by GraphIndexMaintainer)
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        configuration: CycleDetectorConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(indexSubspace: subspace, strategy: strategy)
    }

    // MARK: - Public API

    /// Check if the graph has any cycle
    ///
    /// **Note**: This method returns a simple Bool and loses completeness information.
    /// When `maxNodes` limit is reached before finding a cycle, this method returns `false`
    /// even though a cycle may exist in unexplored parts of the graph.
    ///
    /// For definitive results, use `checkCycle()` instead, which returns completeness info.
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: True if at least one cycle exists
    public func hasCycle(edgeLabel: String? = nil) async throws -> Bool {
        let result = try await findCycles(edgeLabel: edgeLabel, maxCycles: 1)
        return result.hasCycle
    }

    /// Check if the graph has any cycle with explicit completeness tracking
    ///
    /// Unlike `hasCycle()`, this method returns completeness information,
    /// allowing the caller to distinguish between:
    /// - "Definitely no cycle" (isComplete=true, hasCycle=false)
    /// - "No cycle found yet" (isComplete=false, hasCycle=false)
    /// - "Cycle found" (hasCycle=true)
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: CycleCheckResult with cycle existence and completeness
    public func checkCycle(edgeLabel: String? = nil) async throws -> CycleCheckResult {
        let result = try await findCycles(edgeLabel: edgeLabel, maxCycles: 1)
        return CycleCheckResult(
            hasCycle: result.hasCycle,
            isComplete: result.isComplete,
            limitReason: result.limitReason
        )
    }

    /// Find all cycles in the graph
    ///
    /// - Parameters:
    ///   - edgeLabel: Optional edge label filter
    ///   - maxCycles: Maximum number of cycles to find (overrides config)
    /// - Returns: CycleInfo with detected cycles
    public func findCycles(
        edgeLabel: String? = nil,
        maxCycles: Int? = nil
    ) async throws -> CycleInfo {
        let startTime = DispatchTime.now()
        let effectiveMaxCycles = maxCycles ?? configuration.maxCycles

        // Collect all nodes in the graph
        let allNodes = try await collectAllNodes(edgeLabel: edgeLabel)

        guard !allNodes.isEmpty else {
            return CycleInfo(
                hasCycle: false,
                cycles: [],
                backEdges: [],
                nodesExplored: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: true,
                limitReason: nil
            )
        }

        // Initialize DFS state
        var color: [String: Color] = [:]
        var parent: [String: String] = [:]
        var cycles: [[String]] = []
        var backEdges: [(from: String, to: String)] = []
        var nodesExplored = 0

        // Initialize all nodes as white
        for node in allNodes {
            color[node] = .white
        }

        // DFS from each unvisited node
        for startNode in allNodes {
            guard color[startNode] == .white else { continue }
            guard cycles.count < effectiveMaxCycles else { break }

            // Iterative DFS using explicit stack
            var stack: [(node: String, exploring: Bool)] = [(startNode, false)]

            while !stack.isEmpty && cycles.count < effectiveMaxCycles {
                let (node, exploring) = stack.removeLast()

                if exploring {
                    // Finished exploring this node
                    color[node] = .black
                    continue
                }

                guard color[node] == .white else { continue }

                // Mark as gray (being explored)
                color[node] = .gray
                nodesExplored += 1

                // Push marker to finish this node later
                stack.append((node, true))

                // Check node limit
                if nodesExplored >= configuration.maxNodes {
                    break
                }

                // Get neighbors using GraphEdgeScanner
                let neighbors = try await database.withTransaction(configuration: .default) { transaction in
                    var results: [String] = []
                    for try await edgeInfo in self.scanner.scanOutgoing(
                        from: node,
                        edgeLabel: edgeLabel,
                        transaction: transaction
                    ) {
                        results.append(edgeInfo.target)
                    }
                    return results
                }

                for neighbor in neighbors {
                    switch color[neighbor] {
                    case .white:
                        // Tree edge: continue DFS
                        parent[neighbor] = node
                        stack.append((neighbor, false))

                    case .gray:
                        // Back edge: cycle detected!
                        backEdges.append((from: node, to: neighbor))

                        // Reconstruct the cycle
                        if cycles.count < effectiveMaxCycles {
                            let cycle = reconstructCycle(from: node, to: neighbor, parent: parent)
                            cycles.append(cycle)
                        }

                    case .black, .none:
                        // Cross/forward edge: no cycle here
                        break
                    }
                }
            }
        }

        // Determine completion status
        let hitMaxNodes = nodesExplored >= configuration.maxNodes
        let hitMaxCycles = cycles.count >= effectiveMaxCycles
        let isComplete = !hitMaxNodes && !hitMaxCycles

        let limitReason: LimitReason?
        if hitMaxNodes {
            limitReason = .maxNodesReached(explored: nodesExplored, limit: configuration.maxNodes)
        } else if hitMaxCycles {
            limitReason = .maxCyclesReached(found: cycles.count, limit: effectiveMaxCycles)
        } else {
            limitReason = nil
        }

        return CycleInfo(
            hasCycle: !cycles.isEmpty,
            cycles: cycles,
            backEdges: backEdges,
            nodesExplored: nodesExplored,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: isComplete,
            limitReason: limitReason
        )
    }

    /// Check if adding an edge would create a cycle
    ///
    /// This is useful for validating edge insertions in DAG-constrained systems.
    ///
    /// - Parameters:
    ///   - from: Source node of the proposed edge
    ///   - to: Target node of the proposed edge
    ///   - edgeLabel: Optional edge label
    /// - Returns: True if adding the edge would create a cycle
    /// - Throws: `CycleDetectionError.limitReached` if the maxNodes limit is reached
    ///           before a definitive answer can be determined. In this case, the caller
    ///           should either reject the edge (safe approach) or increase the limit.
    public func wouldCreateCycle(
        from: String,
        to: String,
        edgeLabel: String? = nil
    ) async throws -> Bool {
        // Adding edge from -> to creates a cycle if and only if
        // there's already a path from "to" to "from"
        // (because adding from -> to would complete the cycle)
        let (exists, isDefinitive, explored) = try await pathExists(from: to, to: from, edgeLabel: edgeLabel)

        if exists {
            // Path found - adding this edge would definitely create a cycle
            return true
        }

        if !isDefinitive {
            // We hit the limit before finding a path or confirming no path exists
            // We cannot safely say "no cycle" - throw to let caller handle it
            throw CycleDetectionError.limitReached(
                message: "Cannot determine if cycle would be created: exploration limit reached after \(explored) nodes",
                explored: explored,
                limit: configuration.maxNodes
            )
        }

        // Definitively no path exists - adding edge is safe
        return false
    }

    // MARK: - Private Methods

    /// Check if a path exists between two nodes using BFS
    ///
    /// - Returns: A tuple containing:
    ///   - `exists`: Whether a path was found
    ///   - `isDefinitive`: Whether the result is definitive (true if we explored all reachable nodes)
    ///   - `explored`: Number of nodes explored
    ///
    /// Uses index-based iteration to avoid O(n) removeFirst()
    private func pathExists(
        from source: String,
        to target: String,
        edgeLabel: String?
    ) async throws -> (exists: Bool, isDefinitive: Bool, explored: Int) {
        if source == target {
            return (exists: true, isDefinitive: true, explored: 0)
        }

        var visited: Set<String> = [source]
        var frontier: [String] = [source]
        var frontierIndex = 0
        var nodesExplored = 0

        while frontierIndex < frontier.count {
            // Check limit BEFORE processing to ensure we don't exceed it
            if nodesExplored >= configuration.maxNodes {
                // We hit the limit - result is not definitive
                return (exists: false, isDefinitive: false, explored: nodesExplored)
            }

            let current = frontier[frontierIndex]
            frontierIndex += 1
            nodesExplored += 1

            let neighbors = try await database.withTransaction(configuration: .default) { transaction in
                var results: [String] = []
                for try await edgeInfo in self.scanner.scanOutgoing(
                    from: current,
                    edgeLabel: edgeLabel,
                    transaction: transaction
                ) {
                    results.append(edgeInfo.target)
                }
                return results
            }

            for neighbor in neighbors {
                if neighbor == target {
                    return (exists: true, isDefinitive: true, explored: nodesExplored)
                }

                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append(neighbor)
                }
            }
        }

        // Exhausted all reachable nodes without finding target
        // This is a definitive "no path exists"
        return (exists: false, isDefinitive: true, explored: nodesExplored)
    }

    /// Collect all unique nodes in the graph using GraphEdgeScanner
    private func collectAllNodes(edgeLabel: String?) async throws -> Set<String> {
        let nodes: Set<String> = try await database.withTransaction(configuration: .batch) { transaction in
            var collectedNodes: Set<String> = []

            for try await edgeInfo in self.scanner.scanAllEdges(
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                collectedNodes.insert(edgeInfo.source)
                collectedNodes.insert(edgeInfo.target)
            }

            return collectedNodes
        }

        return nodes
    }

    /// Reconstruct cycle from back edge
    ///
    /// Given a back edge from `from` to `to`, reconstruct the cycle
    /// by following parent pointers from `from` back to `to`.
    private func reconstructCycle(
        from: String,
        to: String,
        parent: [String: String]
    ) -> [String] {
        var cycle: [String] = [from]
        var current = from

        // Follow parent pointers back to 'to'
        while current != to {
            guard let p = parent[current] else {
                break
            }
            cycle.insert(p, at: 0)
            current = p
        }

        // Add the back edge target to complete the cycle
        // cycle is now: [to, ..., from]
        // Add 'to' at the end to show it's a cycle
        cycle.append(to)

        return cycle
    }
}
