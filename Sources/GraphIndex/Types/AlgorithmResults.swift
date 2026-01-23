// AlgorithmResults.swift
// GraphIndex - Result structures for graph algorithms
//
// Provides data structures for PageRank, Community Detection,
// and other graph algorithm results.

import Foundation

// MARK: - PageRank

/// PageRank computation result
///
/// Contains the PageRank scores for all nodes in the graph,
/// along with convergence information and timing statistics.
///
/// **Reference**: Page, Brin et al., "The PageRank Citation Ranking:
/// Bringing Order to the Web" (1999)
///
/// **Usage**:
/// ```swift
/// let result = try await pageRank.compute()
///
/// // Get top 10 nodes by PageRank
/// for (nodeID, score) in result.topK(10) {
///     print("\(nodeID): \(score)")
/// }
///
/// // Check specific node's rank
/// if let score = result.score(for: "important-node") {
///     print("Score: \(score)")
/// }
///
/// // Check convergence
/// print("Converged in \(result.iterations) iterations")
/// print("Final delta: \(result.convergenceDelta)")
/// ```
public struct PageRankResult: Sendable {

    // MARK: - Properties

    /// Node ID to PageRank score mapping
    ///
    /// All scores sum to approximately 1.0 (may vary slightly due to
    /// floating-point precision and dangling node handling).
    public let scores: [String: Double]

    /// Number of iterations until convergence (or max iterations)
    public let iterations: Int

    /// Final convergence delta (L1 norm of score changes in last iteration)
    ///
    /// Lower values indicate better convergence.
    /// Algorithm stops when delta < convergenceThreshold.
    public let convergenceDelta: Double

    /// Computation duration in nanoseconds
    public let durationNs: UInt64

    /// Whether the computation completed without hitting limits
    ///
    /// When `false`, the algorithm stopped due to a limit (e.g., maxNodes in collection phase).
    /// In this case, the scores represent only a partial view of the graph.
    ///
    /// Note: This is different from convergence. Use `hasConverged(threshold:)` to check
    /// whether the algorithm achieved the desired precision.
    public let isComplete: Bool

    /// Reason for incompleteness (if any)
    ///
    /// Non-nil when `isComplete` is false.
    public let limitReason: LimitReason?

    // MARK: - Computed Properties

    /// Number of nodes in the result
    public var nodeCount: Int {
        scores.count
    }

    /// Duration in milliseconds
    public var durationMs: Double {
        Double(durationNs) / 1_000_000
    }

    /// Duration in seconds
    public var durationSeconds: Double {
        Double(durationNs) / 1_000_000_000
    }

    /// Whether the algorithm converged (vs hit max iterations)
    ///
    /// - Parameter threshold: Convergence threshold used in computation
    /// - Returns: true if convergenceDelta < threshold
    public func hasConverged(threshold: Double = 1e-6) -> Bool {
        convergenceDelta < threshold
    }

    // MARK: - Initialization

    public init(
        scores: [String: Double],
        iterations: Int,
        convergenceDelta: Double,
        durationNs: UInt64,
        isComplete: Bool = true,
        limitReason: LimitReason? = nil
    ) {
        self.scores = scores
        self.iterations = iterations
        self.convergenceDelta = convergenceDelta
        self.durationNs = durationNs
        self.isComplete = isComplete
        self.limitReason = limitReason
    }

    // MARK: - Query Methods

    /// Get top K nodes by PageRank score
    ///
    /// - Parameter k: Number of nodes to return
    /// - Returns: Array of (nodeID, score) tuples sorted by score descending
    public func topK(_ k: Int) -> [(nodeID: String, score: Double)] {
        scores
            .sorted { $0.value > $1.value }
            .prefix(k)
            .map { ($0.key, $0.value) }
    }

    /// Get bottom K nodes by PageRank score
    ///
    /// - Parameter k: Number of nodes to return
    /// - Returns: Array of (nodeID, score) tuples sorted by score ascending
    public func bottomK(_ k: Int) -> [(nodeID: String, score: Double)] {
        scores
            .sorted { $0.value < $1.value }
            .prefix(k)
            .map { ($0.key, $0.value) }
    }

    /// Get score for a specific node
    ///
    /// - Parameter nodeID: Node ID to look up
    /// - Returns: PageRank score, or nil if node not in results
    public func score(for nodeID: String) -> Double? {
        scores[nodeID]
    }

    /// Get rank of a specific node (1-based)
    ///
    /// - Parameter nodeID: Node ID to look up
    /// - Returns: Rank (1 = highest score), or nil if node not in results
    public func rank(for nodeID: String) -> Int? {
        guard scores[nodeID] != nil else { return nil }
        let sorted = scores.sorted { $0.value > $1.value }
        for (index, item) in sorted.enumerated() {
            if item.key == nodeID {
                return index + 1
            }
        }
        return nil
    }

    /// Get nodes with score above threshold
    ///
    /// - Parameter threshold: Minimum score threshold
    /// - Returns: Array of (nodeID, score) tuples
    public func nodesAbove(threshold: Double) -> [(nodeID: String, score: Double)] {
        scores
            .filter { $0.value >= threshold }
            .sorted { $0.value > $1.value }
            .map { ($0.key, $0.value) }
    }
}

// MARK: - PageRank Configuration

/// Configuration for PageRank computation
///
/// **Reference Values** (from original paper):
/// - dampingFactor: 0.85 is the standard value
/// - Typical convergence: 50-100 iterations for large graphs
///
/// **Usage**:
/// ```swift
/// let config = PageRankConfiguration(
///     dampingFactor: 0.85,
///     maxIterations: 100,
///     convergenceThreshold: 1e-6,
///     batchSize: 1000
/// )
///
/// let result = try await computer.compute(configuration: config)
/// ```
public struct PageRankConfiguration: Sendable {

    /// Damping factor (probability of following a link vs random jump)
    ///
    /// Standard value is 0.85 (from original paper).
    /// Higher values give more weight to link structure.
    /// Lower values distribute rank more evenly.
    ///
    /// Valid range: 0.0 to 1.0
    public let dampingFactor: Double

    /// Maximum iterations before stopping
    ///
    /// Typical convergence occurs in 50-100 iterations.
    /// Set higher for very large or slowly-converging graphs.
    public let maxIterations: Int

    /// Convergence threshold
    ///
    /// Algorithm stops when the L1 norm of score changes falls below this value.
    /// Smaller values give more accurate results but take longer.
    ///
    /// Typical values: 1e-6 to 1e-8
    public let convergenceThreshold: Double

    /// Batch size for transaction operations
    ///
    /// Larger batches are more efficient but may hit FDB limits.
    /// Default: 1000 nodes per batch.
    public let batchSize: Int

    /// Default configuration
    ///
    /// - dampingFactor: 0.85
    /// - maxIterations: 100
    /// - convergenceThreshold: 1e-6
    /// - batchSize: 1000
    public static let `default` = PageRankConfiguration(
        dampingFactor: 0.85,
        maxIterations: 100,
        convergenceThreshold: 1e-6,
        batchSize: 1000
    )

    /// Fast configuration (fewer iterations, lower precision)
    ///
    /// - dampingFactor: 0.85
    /// - maxIterations: 20
    /// - convergenceThreshold: 1e-4
    /// - batchSize: 2000
    public static let fast = PageRankConfiguration(
        dampingFactor: 0.85,
        maxIterations: 20,
        convergenceThreshold: 1e-4,
        batchSize: 2000
    )

    /// High precision configuration
    ///
    /// - dampingFactor: 0.85
    /// - maxIterations: 200
    /// - convergenceThreshold: 1e-10
    /// - batchSize: 500
    public static let highPrecision = PageRankConfiguration(
        dampingFactor: 0.85,
        maxIterations: 200,
        convergenceThreshold: 1e-10,
        batchSize: 500
    )

    public init(
        dampingFactor: Double = 0.85,
        maxIterations: Int = 100,
        convergenceThreshold: Double = 1e-6,
        batchSize: Int = 1000
    ) {
        // Clamp damping factor to valid range
        self.dampingFactor = Swift.min(1.0, Swift.max(0.0, dampingFactor))
        self.maxIterations = Swift.max(1, maxIterations)
        self.convergenceThreshold = Swift.max(0, convergenceThreshold)
        self.batchSize = Swift.max(1, batchSize)
    }
}

// MARK: - Community Detection

/// Community detection result using Label Propagation Algorithm
///
/// Contains the community assignments for all nodes, along with
/// community membership lists and statistics.
///
/// **Reference**: Raghavan et al., "Near linear time algorithm to detect
/// community structures in large-scale networks" (2007)
///
/// **Usage**:
/// ```swift
/// let result = try await detector.detect()
///
/// // Get number of communities
/// print("Found \(result.communityCount) communities")
///
/// // Get community members for a node
/// let members = result.communityMembers(for: "alice")
/// print("Alice's community has \(members.count) members")
///
/// // Get largest communities
/// for community in result.largestCommunities(k: 5) {
///     print("Community \(community.label): \(community.memberCount) members")
/// }
/// ```
public struct CommunityResult: Sendable {

    // MARK: - Properties

    /// Node ID to community label mapping
    ///
    /// Each node is assigned to exactly one community.
    /// Community labels are strings (typically derived from node IDs).
    public let assignments: [String: String]

    /// Community label to member node IDs mapping
    ///
    /// Inverse of `assignments` for efficient community lookups.
    public let communities: [String: [String]]

    /// Number of iterations until convergence (or max iterations)
    public let iterations: Int

    /// Computation duration in nanoseconds
    public let durationNs: UInt64

    /// Modularity score (measure of community quality)
    ///
    /// Range: -0.5 to 1.0. Higher is better.
    /// nil if modularity was not computed.
    ///
    /// **Reference**: Newman, M.E.J., "Modularity and community structure
    /// in networks" (2006)
    public let modularity: Double?

    // MARK: - Computed Properties

    /// Number of communities found
    public var communityCount: Int {
        communities.count
    }

    /// Total number of nodes
    public var nodeCount: Int {
        assignments.count
    }

    /// Duration in milliseconds
    public var durationMs: Double {
        Double(durationNs) / 1_000_000
    }

    /// Average community size
    public var averageCommunitySize: Double {
        guard communityCount > 0 else { return 0 }
        return Double(nodeCount) / Double(communityCount)
    }

    /// Size of the largest community
    public var largestCommunitySize: Int {
        communities.values.map(\.count).max() ?? 0
    }

    /// Size of the smallest community
    public var smallestCommunitySize: Int {
        communities.values.map(\.count).min() ?? 0
    }

    // MARK: - Initialization

    public init(
        assignments: [String: String],
        communities: [String: [String]],
        iterations: Int,
        durationNs: UInt64,
        modularity: Double? = nil
    ) {
        self.assignments = assignments
        self.communities = communities
        self.iterations = iterations
        self.durationNs = durationNs
        self.modularity = modularity
    }

    /// Create from assignments only (computes communities mapping)
    public init(
        assignments: [String: String],
        iterations: Int,
        durationNs: UInt64,
        modularity: Double? = nil
    ) {
        self.assignments = assignments
        self.iterations = iterations
        self.durationNs = durationNs
        self.modularity = modularity

        // Build communities from assignments
        var communitiesBuilder: [String: [String]] = [:]
        for (nodeID, communityLabel) in assignments {
            communitiesBuilder[communityLabel, default: []].append(nodeID)
        }
        self.communities = communitiesBuilder
    }

    // MARK: - Query Methods

    /// Get community label for a node
    ///
    /// - Parameter nodeID: Node ID to look up
    /// - Returns: Community label, or nil if node not in results
    public func community(for nodeID: String) -> String? {
        assignments[nodeID]
    }

    /// Get all members of a node's community
    ///
    /// - Parameter nodeID: Node ID to look up
    /// - Returns: Array of node IDs in the same community (including the node itself)
    public func communityMembers(for nodeID: String) -> [String] {
        guard let label = assignments[nodeID] else { return [] }
        return communities[label] ?? []
    }

    /// Get members of a community by label
    ///
    /// - Parameter label: Community label
    /// - Returns: Array of member node IDs
    public func members(ofCommunity label: String) -> [String] {
        communities[label] ?? []
    }

    /// Get size of a community
    ///
    /// - Parameter label: Community label
    /// - Returns: Number of members, or 0 if community not found
    public func size(ofCommunity label: String) -> Int {
        communities[label]?.count ?? 0
    }

    /// Check if two nodes are in the same community
    ///
    /// - Parameters:
    ///   - nodeA: First node ID
    ///   - nodeB: Second node ID
    /// - Returns: true if both nodes are in the same community
    public func inSameCommunity(_ nodeA: String, _ nodeB: String) -> Bool {
        guard let labelA = assignments[nodeA],
              let labelB = assignments[nodeB] else {
            return false
        }
        return labelA == labelB
    }

    /// Get the K largest communities
    ///
    /// - Parameter k: Number of communities to return
    /// - Returns: Array of (label, memberCount) tuples sorted by size descending
    public func largestCommunities(k: Int) -> [(label: String, memberCount: Int)] {
        communities
            .map { ($0.key, $0.value.count) }
            .sorted { $0.1 > $1.1 }
            .prefix(k)
            .map { ($0.0, $0.1) }
    }

    /// Get communities with at least minSize members
    ///
    /// - Parameter minSize: Minimum community size
    /// - Returns: Dictionary of community label to members
    public func communitiesWithMinSize(_ minSize: Int) -> [String: [String]] {
        communities.filter { $0.value.count >= minSize }
    }

    /// Get distribution of community sizes
    ///
    /// - Returns: Dictionary of size to count of communities with that size
    public func sizeDistribution() -> [Int: Int] {
        var distribution: [Int: Int] = [:]
        for members in communities.values {
            distribution[members.count, default: 0] += 1
        }
        return distribution
    }
}

// MARK: - Community Detection Configuration

/// Configuration for community detection
///
/// **Usage**:
/// ```swift
/// let config = CommunityDetectionConfiguration(
///     maxIterations: 100,
///     batchSize: 500,
///     computeModularity: true
/// )
///
/// let result = try await detector.detect(configuration: config)
/// ```
public struct CommunityDetectionConfiguration: Sendable {

    /// Maximum iterations before stopping
    ///
    /// Label propagation typically converges in 5-15 iterations.
    /// Higher values allow for slower-converging graphs.
    public let maxIterations: Int

    /// Batch size for transaction operations
    ///
    /// Default: 500 nodes per batch (smaller than PageRank due to
    /// more complex neighbor lookups).
    public let batchSize: Int

    /// Whether to compute modularity score
    ///
    /// Computing modularity requires an additional pass over all edges,
    /// which can be expensive for large graphs.
    public let computeModularity: Bool

    /// Minimum community size to keep
    ///
    /// Communities smaller than this will be merged into larger neighbors.
    /// Set to 1 to keep all communities.
    public let minCommunitySize: Int

    /// Random seed for deterministic execution
    ///
    /// When set, the algorithm will produce the same results on repeated
    /// runs with the same input data. This is useful for:
    /// - Reproducible experiments
    /// - Testing
    /// - Debugging
    ///
    /// When `nil`, uses system random (non-deterministic).
    public let seed: UInt64?

    /// Default configuration
    ///
    /// - maxIterations: 100
    /// - batchSize: 500
    /// - computeModularity: false
    /// - minCommunitySize: 1
    public static let `default` = CommunityDetectionConfiguration(
        maxIterations: 100,
        batchSize: 500,
        computeModularity: false,
        minCommunitySize: 1
    )

    /// Fast configuration
    ///
    /// - maxIterations: 20
    /// - batchSize: 1000
    /// - computeModularity: false
    /// - minCommunitySize: 1
    public static let fast = CommunityDetectionConfiguration(
        maxIterations: 20,
        batchSize: 1000,
        computeModularity: false,
        minCommunitySize: 1
    )

    /// Configuration with modularity computation
    ///
    /// - maxIterations: 100
    /// - batchSize: 500
    /// - computeModularity: true
    /// - minCommunitySize: 1
    public static let withModularity = CommunityDetectionConfiguration(
        maxIterations: 100,
        batchSize: 500,
        computeModularity: true,
        minCommunitySize: 1
    )

    public init(
        maxIterations: Int = 100,
        batchSize: Int = 500,
        computeModularity: Bool = false,
        minCommunitySize: Int = 1,
        seed: UInt64? = nil
    ) {
        self.maxIterations = Swift.max(1, maxIterations)
        self.batchSize = Swift.max(1, batchSize)
        self.computeModularity = computeModularity
        self.minCommunitySize = Swift.max(1, minCommunitySize)
        self.seed = seed
    }

    /// Create a deterministic configuration with a specific seed
    ///
    /// - Parameter seed: Random seed for reproducible results
    /// - Returns: Configuration with deterministic behavior
    public func withSeed(_ seed: UInt64) -> CommunityDetectionConfiguration {
        CommunityDetectionConfiguration(
            maxIterations: maxIterations,
            batchSize: batchSize,
            computeModularity: computeModularity,
            minCommunitySize: minCommunitySize,
            seed: seed
        )
    }
}

// MARK: - Shortest Path Configuration

/// Configuration for shortest path algorithms
public struct ShortestPathConfiguration: Sendable {

    /// Maximum search depth
    ///
    /// Limits how far the algorithm will search from the source.
    /// Higher values can find longer paths but take more time.
    public let maxDepth: Int

    /// Whether to use bidirectional BFS
    ///
    /// Bidirectional BFS searches from both source and target,
    /// meeting in the middle. This is significantly faster for
    /// long paths in sparse graphs.
    ///
    /// Default: true
    public let bidirectional: Bool

    /// Batch size for transaction operations
    public let batchSize: Int

    /// Maximum nodes to explore before giving up
    ///
    /// Prevents runaway searches in very large graphs.
    public let maxNodesExplored: Int

    /// Default configuration
    public static let `default` = ShortestPathConfiguration(
        maxDepth: 10,
        bidirectional: true,
        batchSize: 100,
        maxNodesExplored: 100_000
    )

    /// Fast configuration (smaller search space)
    public static let fast = ShortestPathConfiguration(
        maxDepth: 5,
        bidirectional: true,
        batchSize: 200,
        maxNodesExplored: 10_000
    )

    /// Thorough configuration (larger search space)
    public static let thorough = ShortestPathConfiguration(
        maxDepth: 20,
        bidirectional: true,
        batchSize: 100,
        maxNodesExplored: 500_000
    )

    public init(
        maxDepth: Int = 10,
        bidirectional: Bool = true,
        batchSize: Int = 100,
        maxNodesExplored: Int = 100_000
    ) {
        self.maxDepth = Swift.max(1, maxDepth)
        self.bidirectional = bidirectional
        self.batchSize = Swift.max(1, batchSize)
        self.maxNodesExplored = Swift.max(1, maxNodesExplored)
    }
}
