// GraphPath.swift
// GraphIndex - Path result structures for graph algorithms
//
// Provides data structures for representing paths through graphs,
// including shortest path results and path traversal information.

import Foundation
import Core

// MARK: - GraphPath

/// Represents a path through the graph with all nodes and edges
///
/// A path is an ordered sequence of nodes connected by edges.
/// This structure captures the full path information including
/// optional edge labels and weights for weighted graphs.
///
/// **Design**: Immutable value type for thread safety and Sendable compliance.
///
/// **Reference**: Neo4j Path type, Apache TinkerPop Path
///
/// **Usage**:
/// ```swift
/// let path = GraphPath<Edge>(
///     nodeIDs: ["alice", "bob", "charlie"],
///     edgeLabels: ["follows", "knows"],
///     weights: nil
/// )
///
/// print("Length: \(path.length)")  // 2
/// print("Source: \(path.source)")  // "alice"
/// print("Target: \(path.target)")  // "charlie"
/// ```
public struct GraphPath<T: Persistable>: Sendable {

    // MARK: - Properties

    /// Ordered list of node IDs from source to target
    ///
    /// The first element is the source node, the last is the target.
    /// For a path of length N, this array contains N+1 elements.
    public let nodeIDs: [String]

    /// Edge labels connecting consecutive nodes
    ///
    /// For a path with N nodes, this array contains N-1 edge labels.
    /// `edgeLabels[i]` is the label of the edge from `nodeIDs[i]` to `nodeIDs[i+1]`.
    public let edgeLabels: [String]

    /// Optional edge weights for weighted algorithms
    ///
    /// When present, `weights[i]` is the weight of the edge from
    /// `nodeIDs[i]` to `nodeIDs[i+1]`.
    /// For unweighted graphs, this is nil.
    public let weights: [Double]?

    // MARK: - Computed Properties

    /// Total path length (number of edges / hops)
    ///
    /// A path with N nodes has length N-1.
    /// A single-node path has length 0.
    public var length: Int {
        max(0, nodeIDs.count - 1)
    }

    /// Total weight of the path
    ///
    /// For weighted graphs, returns the sum of edge weights.
    /// For unweighted graphs, returns the path length as a Double.
    public var totalWeight: Double {
        weights?.reduce(0, +) ?? Double(length)
    }

    /// Source node ID (first node in the path)
    ///
    /// Returns nil if the path is empty.
    public var source: String? {
        nodeIDs.first
    }

    /// Target node ID (last node in the path)
    ///
    /// Returns nil if the path is empty.
    public var target: String? {
        nodeIDs.last
    }

    /// Whether the path is empty (contains no nodes)
    public var isEmpty: Bool {
        nodeIDs.isEmpty
    }

    // MARK: - Initialization

    /// Create a new graph path
    ///
    /// - Parameters:
    ///   - nodeIDs: Ordered list of node IDs from source to target
    ///   - edgeLabels: Edge labels connecting consecutive nodes
    ///   - weights: Optional edge weights for weighted algorithms
    public init(nodeIDs: [String], edgeLabels: [String], weights: [Double]?) {
        self.nodeIDs = nodeIDs
        self.edgeLabels = edgeLabels
        self.weights = weights
    }

    /// Create a single-node path (length 0)
    ///
    /// - Parameter nodeID: The single node in the path
    public init(singleNode nodeID: String) {
        self.nodeIDs = [nodeID]
        self.edgeLabels = []
        self.weights = nil
    }

    // MARK: - Query Methods

    /// Check if the path contains a specific node
    ///
    /// - Parameter nodeID: The node ID to search for
    /// - Returns: true if the node is in the path
    public func contains(node nodeID: String) -> Bool {
        nodeIDs.contains(nodeID)
    }

    /// Check if the path contains a specific edge (by consecutive nodes)
    ///
    /// - Parameters:
    ///   - from: Source node of the edge
    ///   - to: Target node of the edge
    /// - Returns: true if the edge exists in the path
    public func containsEdge(from: String, to: String) -> Bool {
        for i in 0..<length {
            if nodeIDs[i] == from && nodeIDs[i + 1] == to {
                return true
            }
        }
        return false
    }

    /// Get the node at a specific position in the path
    ///
    /// - Parameter index: Position (0-based)
    /// - Returns: Node ID at the position, or nil if out of bounds
    public func node(at index: Int) -> String? {
        guard index >= 0 && index < nodeIDs.count else { return nil }
        return nodeIDs[index]
    }

    /// Get the edge label between two consecutive nodes
    ///
    /// - Parameter index: Edge index (0-based, where edge 0 connects nodes 0 and 1)
    /// - Returns: Edge label, or nil if out of bounds
    public func edgeLabel(at index: Int) -> String? {
        guard index >= 0 && index < edgeLabels.count else { return nil }
        return edgeLabels[index]
    }

    /// Get the weight of an edge
    ///
    /// - Parameter index: Edge index (0-based)
    /// - Returns: Edge weight, or nil if no weights or out of bounds
    public func weight(at index: Int) -> Double? {
        guard let weights = weights, index >= 0 && index < weights.count else { return nil }
        return weights[index]
    }

    /// Get a subpath from startIndex to endIndex (inclusive node indices)
    ///
    /// - Parameters:
    ///   - startIndex: Starting node index
    ///   - endIndex: Ending node index
    /// - Returns: Subpath, or nil if indices are invalid
    public func subpath(from startIndex: Int, to endIndex: Int) -> GraphPath<T>? {
        guard startIndex >= 0 && endIndex < nodeIDs.count && startIndex <= endIndex else {
            return nil
        }

        let subNodeIDs = Array(nodeIDs[startIndex...endIndex])
        let subEdgeLabels: [String]
        let subWeights: [Double]?

        if startIndex < endIndex {
            subEdgeLabels = Array(edgeLabels[startIndex..<endIndex])
            subWeights = weights.map { Array($0[startIndex..<endIndex]) }
        } else {
            subEdgeLabels = []
            subWeights = nil
        }

        return GraphPath(nodeIDs: subNodeIDs, edgeLabels: subEdgeLabels, weights: subWeights)
    }

    /// Reverse the path (swap source and target)
    ///
    /// - Returns: A new path with reversed direction
    public func reversed() -> GraphPath<T> {
        GraphPath(
            nodeIDs: nodeIDs.reversed(),
            edgeLabels: edgeLabels.reversed(),
            weights: weights?.reversed()
        )
    }
}

// MARK: - Equatable

extension GraphPath: Equatable {
    public static func == (lhs: GraphPath<T>, rhs: GraphPath<T>) -> Bool {
        lhs.nodeIDs == rhs.nodeIDs &&
        lhs.edgeLabels == rhs.edgeLabels &&
        lhs.weights == rhs.weights
    }
}

// MARK: - Hashable

extension GraphPath: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(nodeIDs)
        hasher.combine(edgeLabels)
        if let weights = weights {
            for weight in weights {
                hasher.combine(weight)
            }
        }
    }
}

// MARK: - CustomStringConvertible

extension GraphPath: CustomStringConvertible {
    public var description: String {
        if nodeIDs.isEmpty {
            return "GraphPath(empty)"
        }

        var parts: [String] = []
        for i in 0..<nodeIDs.count {
            parts.append(nodeIDs[i])
            if i < edgeLabels.count {
                let label = edgeLabels[i]
                if let weights = weights, i < weights.count {
                    parts.append("-[\(label):\(weights[i])]->")
                } else {
                    parts.append("-[\(label)]->")
                }
            }
        }
        return "GraphPath(\(parts.joined()))"
    }
}

// MARK: - ShortestPathResult

/// Result of a shortest path search
///
/// Contains the shortest path (if found), search statistics,
/// and timing information.
///
/// **Usage**:
/// ```swift
/// let result = try await finder.findShortestPath(from: "alice", to: "bob")
///
/// if result.isConnected {
///     print("Distance: \(result.distance!)")
///     print("Path: \(result.path!.nodeIDs.joined(separator: " -> "))")
/// } else {
///     print("No path exists")
/// }
///
/// print("Explored \(result.nodesExplored) nodes in \(result.durationNs / 1_000_000)ms")
/// ```
public struct ShortestPathResult<T: Persistable>: Sendable {

    // MARK: - Properties

    /// The shortest path (nil if no path exists)
    public let path: GraphPath<T>?

    /// Distance (hop count for unweighted, total weight for weighted)
    ///
    /// nil if no path exists.
    public let distance: Double?

    /// Number of nodes explored during the search
    ///
    /// Useful for performance analysis and debugging.
    public let nodesExplored: Int

    /// Search duration in nanoseconds
    public let durationNs: UInt64

    /// Whether the search completed without hitting limits
    ///
    /// When `false`, the algorithm stopped due to a limit (e.g., maxNodesExplored).
    /// In this case, `path == nil` does not definitively mean "no path exists".
    public let isComplete: Bool

    /// Reason for incompleteness (if any)
    ///
    /// Non-nil when `isComplete` is false.
    public let limitReason: LimitReason?

    // MARK: - Computed Properties

    /// Whether source and target are connected
    public var isConnected: Bool {
        path != nil
    }

    /// Whether the result is definitive
    ///
    /// Returns `true` if:
    /// - A path was found (definitive positive), OR
    /// - The search completed without limits (definitive negative)
    ///
    /// Returns `false` if we hit a limit before completion,
    /// meaning we cannot definitively say whether a path exists.
    public var isDefinitive: Bool {
        path != nil || isComplete
    }

    /// Search duration in milliseconds
    public var durationMs: Double {
        Double(durationNs) / 1_000_000
    }

    /// Search duration in seconds
    public var durationSeconds: Double {
        Double(durationNs) / 1_000_000_000
    }

    // MARK: - Initialization

    /// Create a shortest path result
    ///
    /// - Parameters:
    ///   - path: The shortest path (nil if not found)
    ///   - distance: Path distance (nil if not found)
    ///   - nodesExplored: Number of nodes explored during search
    ///   - durationNs: Search duration in nanoseconds
    ///   - isComplete: Whether the search completed without hitting limits
    ///   - limitReason: Reason for incompleteness (if any)
    public init(
        path: GraphPath<T>?,
        distance: Double?,
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.path = path
        self.distance = distance
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }

    /// Create a "not found" result
    ///
    /// - Parameters:
    ///   - nodesExplored: Number of nodes explored
    ///   - durationNs: Search duration
    ///   - isComplete: Whether the search completed without hitting limits
    ///   - limitReason: Reason for incompleteness (if any)
    public static func notFound(
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) -> ShortestPathResult<T> {
        ShortestPathResult(
            path: nil,
            distance: nil,
            nodesExplored: nodesExplored,
            durationNs: durationNs,
            isComplete: isComplete,
            limitReason: limitReason
        )
    }
}

// MARK: - AllShortestPathsResult

/// Result of finding all shortest paths between two nodes
///
/// When multiple shortest paths of equal length exist, this structure
/// captures all of them.
public struct AllShortestPathsResult<T: Persistable>: Sendable {

    /// All shortest paths found (empty if no path exists)
    public let paths: [GraphPath<T>]

    /// Shortest distance (nil if no path exists)
    public let distance: Double?

    /// Number of nodes explored during the search
    public let nodesExplored: Int

    /// Search duration in nanoseconds
    public let durationNs: UInt64

    /// Whether the search completed without hitting limits
    ///
    /// When `false`, the algorithm stopped due to a limit.
    /// In this case, `paths.isEmpty` does not definitively mean "no path exists",
    /// and the path count may be incomplete.
    public let isComplete: Bool

    /// Reason for incompleteness (if any)
    ///
    /// Non-nil when `isComplete` is false.
    public let limitReason: LimitReason?

    /// Whether source and target are connected
    public var isConnected: Bool {
        !paths.isEmpty
    }

    /// Number of shortest paths found
    public var pathCount: Int {
        paths.count
    }

    /// Whether the result is definitive
    ///
    /// Returns `true` if:
    /// - Paths were found (definitive positive), OR
    /// - The search completed without limits (definitive negative)
    ///
    /// Returns `false` if we hit a limit before completion.
    public var isDefinitive: Bool {
        !paths.isEmpty || isComplete
    }

    public init(
        paths: [GraphPath<T>],
        distance: Double?,
        nodesExplored: Int,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.paths = paths
        self.distance = distance
        self.nodesExplored = nodesExplored
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }
}
