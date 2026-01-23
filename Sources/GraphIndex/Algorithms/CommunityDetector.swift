// CommunityDetector.swift
// GraphIndex - Community detection using Label Propagation Algorithm
//
// Provides community detection for graph indexes.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - Seeded Random Number Generator

/// A seeded random number generator for deterministic shuffling
///
/// Uses xorshift128+ algorithm for fast, high-quality pseudo-random numbers.
/// Reference: Vigna, S. (2017). "Further scramblings of Marsaglia's xorshift generators"
private struct SeededRandomNumberGenerator: RandomNumberGenerator {
    private var state: (UInt64, UInt64)

    init(seed: UInt64) {
        // Initialize state using SplitMix64 to expand the seed
        var s = seed
        func splitMix64() -> UInt64 {
            s &+= 0x9e3779b97f4a7c15
            var z = s
            z = (z ^ (z >> 30)) &* 0xbf58476d1ce4e5b9
            z = (z ^ (z >> 27)) &* 0x94d049bb133111eb
            return z ^ (z >> 31)
        }
        state = (splitMix64(), splitMix64())
    }

    mutating func next() -> UInt64 {
        var s1 = state.0
        let s0 = state.1
        let result = s0 &+ s1
        state.0 = s0
        s1 ^= s1 << 23
        state.1 = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5)
        return result
    }
}

// MARK: - CommunityDetector

/// Community detection using Label Propagation Algorithm (LPA)
///
/// Detects communities in a graph by iteratively propagating labels
/// between connected nodes until convergence.
///
/// **Algorithm**: Label Propagation
/// ```
/// 1. Initialize: Each node gets a unique label
/// 2. Repeat until convergence:
///    - For each node (in random order):
///      - Adopt the most common label among neighbors
/// 3. Nodes with the same label form a community
/// ```
///
/// **Time Complexity**: O(E * iterations), typically converges in 5-15 iterations
/// **Space Complexity**: O(V) for label storage
///
/// **Transaction Strategy**:
/// - Each iteration processes nodes in batches
/// - Uses `.batch` transaction configuration
/// - Randomizes node order for better convergence
///
/// **Reference**: Raghavan et al., "Near linear time algorithm to detect
/// community structures in large-scale networks" (2007)
///
/// **Usage**:
/// ```swift
/// let detector = CommunityDetector<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// let result = try await detector.detect(edgeLabel: "friends")
///
/// print("Found \(result.communityCount) communities")
///
/// for community in result.largestCommunities(k: 5) {
///     print("Community \(community.label): \(community.memberCount) members")
/// }
/// ```
public final class CommunityDetector<Edge: Persistable>: Sendable {

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Index subspace
    private let subspace: Subspace

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: CommunityDetectionConfiguration

    // MARK: - Initialization

    /// Initialize community detector
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        configuration: CommunityDetectionConfiguration = .default
    ) {
        self.database = database
        self.subspace = subspace
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(indexSubspace: subspace, strategy: strategy)
    }

    // MARK: - Public API

    /// Detect communities in the graph
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: CommunityResult with node assignments
    public func detect(edgeLabel: String? = nil) async throws -> CommunityResult {
        let startTime = DispatchTime.now()

        // Step 1: Collect all nodes
        let nodes = try await collectAllNodes(edgeLabel: edgeLabel)

        guard !nodes.isEmpty else {
            return CommunityResult(
                assignments: [:],
                iterations: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Step 2: Initialize each node with its own unique label
        var labels: [String: String] = [:]
        for node in nodes {
            labels[node] = node  // Initial label = node ID
        }

        // Step 3: Iterate until convergence
        var iteration = 0
        var changed = true

        // Create seeded RNG if seed is provided
        var seededRNG: SeededRandomNumberGenerator? = configuration.seed.map { SeededRandomNumberGenerator(seed: $0) }

        while iteration < configuration.maxIterations && changed {
            iteration += 1
            changed = false

            // Shuffle nodes for randomization (important for LPA convergence)
            // Use seeded RNG for deterministic shuffling if seed is provided
            //
            // Note: When using seeded RNG, we sort nodes first to ensure deterministic
            // ordering regardless of Set iteration order (which is unspecified in Swift).
            let shuffled: [String]
            if var rng = seededRNG {
                var nodeArray = Array(nodes).sorted()  // Sort for deterministic base order
                nodeArray.shuffle(using: &rng)
                seededRNG = rng  // Update the RNG state
                shuffled = nodeArray
            } else {
                shuffled = Array(nodes).shuffled()
            }

            // Process nodes synchronously (update labels immediately after each node)
            // This is the standard LPA approach that avoids oscillation on small cliques.
            // Processing order is randomized to prevent bias.
            for node in shuffled {
                let newLabel = try await computeSingleNodeUpdate(
                    node: node,
                    currentLabels: labels,
                    edgeLabel: edgeLabel,
                    rng: &seededRNG
                )

                if let label = newLabel, labels[node] != label {
                    labels[node] = label
                    changed = true
                }
            }
        }

        // Step 4: Apply minimum community size filter if needed
        if configuration.minCommunitySize > 1 {
            labels = try await applyMinCommunitySize(
                labels: labels,
                edgeLabel: edgeLabel
            )
        }

        // Step 5: Build community mapping
        var communities: [String: [String]] = [:]
        for (node, label) in labels {
            communities[label, default: []].append(node)
        }

        // Step 6: Optionally compute modularity
        var modularity: Double? = nil
        if configuration.computeModularity {
            modularity = try await computeModularity(
                labels: labels,
                edgeLabel: edgeLabel
            )
        }

        return CommunityResult(
            assignments: labels,
            communities: communities,
            iterations: iteration,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            modularity: modularity
        )
    }

    /// Detect community for a single node
    ///
    /// Runs a localized version of LPA starting from the given node
    /// and its neighborhood.
    ///
    /// - Parameters:
    ///   - node: Node to find community for
    ///   - maxHops: Maximum hops from node to consider (default: 3)
    ///   - edgeLabel: Optional edge label filter
    /// - Returns: Set of node IDs in the same community
    public func detectLocalCommunity(
        for node: String,
        maxHops: Int = 3,
        edgeLabel: String? = nil
    ) async throws -> Set<String> {
        // Collect local neighborhood
        var neighborhood: Set<String> = [node]
        var frontier: Set<String> = [node]

        for _ in 0..<maxHops {
            var nextFrontier: Set<String> = []

            for currentNode in frontier {
                let neighbors = try await getNeighbors(
                    of: currentNode,
                    edgeLabel: edgeLabel
                )
                for neighbor in neighbors where !neighborhood.contains(neighbor) {
                    nextFrontier.insert(neighbor)
                    neighborhood.insert(neighbor)
                }
            }

            frontier = nextFrontier
            if frontier.isEmpty { break }
        }

        // Run LPA on local neighborhood
        var labels: [String: String] = [:]
        for n in neighborhood {
            labels[n] = n
        }

        var changed = true
        var iteration = 0

        // Create seeded RNG if seed is provided
        var seededRNG: SeededRandomNumberGenerator? = configuration.seed.map { SeededRandomNumberGenerator(seed: $0) }

        while changed && iteration < configuration.maxIterations {
            iteration += 1
            changed = false

            // Shuffle with determinism support
            // Note: Sort before shuffle for deterministic results (Set order is unspecified)
            let shuffledNeighborhood: [String]
            if var rng = seededRNG {
                var nodeArray = Array(neighborhood).sorted()
                nodeArray.shuffle(using: &rng)
                seededRNG = rng
                shuffledNeighborhood = nodeArray
            } else {
                shuffledNeighborhood = Array(neighborhood).shuffled()
            }

            for n in shuffledNeighborhood {
                let neighbors = try await getNeighbors(of: n, edgeLabel: edgeLabel)
                    .filter { neighborhood.contains($0) }

                guard !neighbors.isEmpty else { continue }

                var labelCounts: [String: Int] = [:]
                for neighbor in neighbors {
                    if let label = labels[neighbor] {
                        labelCounts[label, default: 0] += 1
                    }
                }

                let maxCount = labelCounts.values.max() ?? 0
                var candidates = labelCounts.filter { $0.value == maxCount }.map { $0.key }

                // Sort for deterministic selection when using seeded RNG
                if seededRNG != nil {
                    candidates.sort()
                }

                let newLabel: String?
                if var generator = seededRNG {
                    newLabel = candidates.randomElement(using: &generator)
                    seededRNG = generator
                } else {
                    newLabel = candidates.randomElement()
                }

                if let label = newLabel, label != labels[n] {
                    labels[n] = label
                    changed = true
                }
            }
        }

        // Return nodes with the same label as the input node
        guard let nodeLabel = labels[node] else {
            return [node]
        }

        return Set(labels.filter { $0.value == nodeLabel }.map { $0.key })
    }

    // MARK: - Private Methods

    /// Collect all unique nodes from the graph using GraphEdgeScanner
    private func collectAllNodes(edgeLabel: String?) async throws -> Set<String> {
        try await database.withTransaction(configuration: .batch) { transaction in
            var nodes: Set<String> = []
            var edgeCount = 0

            for try await edgeInfo in self.scanner.scanAllEdges(
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                nodes.insert(edgeInfo.source)
                nodes.insert(edgeInfo.target)
                edgeCount += 1
            }

            #if DEBUG
            print("[CommunityDetector] collectAllNodes: found \(nodes.count) nodes from \(edgeCount) edges")
            #endif

            return nodes
        }
    }

    /// Compute label updates for a batch of nodes
    private func computeLabelUpdates(
        nodes: [String],
        currentLabels: [String: String],
        edgeLabel: String?,
        rng: inout SeededRandomNumberGenerator?
    ) async throws -> [(node: String, label: String)] {
        var updates: [(node: String, label: String)] = []

        for node in nodes {
            let neighbors = try await getNeighbors(of: node, edgeLabel: edgeLabel)

            guard !neighbors.isEmpty else { continue }

            // Count label frequencies among neighbors
            var labelCounts: [String: Int] = [:]
            for neighbor in neighbors {
                if let label = currentLabels[neighbor] {
                    labelCounts[label, default: 0] += 1
                }
            }

            // Find most common label (deterministic tie-breaking when seeded)
            let maxCount = labelCounts.values.max() ?? 0
            var candidates = labelCounts.filter { $0.value == maxCount }.map { $0.key }

            // Sort candidates for deterministic selection when using seeded RNG
            if rng != nil {
                candidates.sort()
            }

            let newLabel: String?
            if var generator = rng {
                newLabel = candidates.randomElement(using: &generator)
                rng = generator  // Update the RNG state
            } else {
                newLabel = candidates.randomElement()
            }

            if let label = newLabel {
                updates.append((node, label))
            }
        }

        return updates
    }

    /// Compute label update for a single node (for synchronous LPA)
    ///
    /// Returns the new label for the node, or nil if no update is needed
    /// (e.g., when the node has no neighbors).
    private func computeSingleNodeUpdate(
        node: String,
        currentLabels: [String: String],
        edgeLabel: String?,
        rng: inout SeededRandomNumberGenerator?
    ) async throws -> String? {
        let neighbors = try await getNeighbors(of: node, edgeLabel: edgeLabel)

        guard !neighbors.isEmpty else { return nil }

        // Count label frequencies among neighbors
        var labelCounts: [String: Int] = [:]
        for neighbor in neighbors {
            if let label = currentLabels[neighbor] {
                labelCounts[label, default: 0] += 1
            }
        }

        // Find most common label (deterministic tie-breaking when seeded)
        let maxCount = labelCounts.values.max() ?? 0
        var candidates = labelCounts.filter { $0.value == maxCount }.map { $0.key }

        // Sort candidates for deterministic selection when using seeded RNG
        if rng != nil {
            candidates.sort()
        }

        let newLabel: String?
        if var generator = rng {
            newLabel = candidates.randomElement(using: &generator)
            rng = generator  // Update the RNG state
        } else {
            newLabel = candidates.randomElement()
        }

        return newLabel
    }

    /// Get all neighbors of a node (both directions) using GraphEdgeScanner
    private func getNeighbors(
        of nodeID: String,
        edgeLabel: String?
    ) async throws -> [String] {
        try await database.withTransaction(configuration: .default) { transaction in
            var neighbors: Set<String> = []

            // Outgoing neighbors
            for try await edgeInfo in self.scanner.scanOutgoing(
                from: nodeID,
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                neighbors.insert(edgeInfo.target)
            }

            // Incoming neighbors
            for try await edgeInfo in self.scanner.scanIncoming(
                to: nodeID,
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                neighbors.insert(edgeInfo.source)
            }

            #if DEBUG
            if neighbors.isEmpty {
                print("[CommunityDetector] getNeighbors(\(nodeID)): NO NEIGHBORS FOUND")
            }
            #endif

            return Array(neighbors)
        }
    }

    /// Apply minimum community size filter by merging small communities
    private func applyMinCommunitySize(
        labels: [String: String],
        edgeLabel: String?
    ) async throws -> [String: String] {
        var result = labels

        // Build community sizes
        var communitySizes: [String: Int] = [:]
        for (_, label) in labels {
            communitySizes[label, default: 0] += 1
        }

        // Find small communities
        let smallCommunities = communitySizes.filter { $0.value < configuration.minCommunitySize }

        // For each node in a small community, assign to largest neighbor community
        for (node, label) in result {
            guard smallCommunities[label] != nil else { continue }

            let neighbors = try await getNeighbors(of: node, edgeLabel: edgeLabel)
            var neighborLabels: [String: Int] = [:]

            for neighbor in neighbors {
                if let neighborLabel = result[neighbor],
                   smallCommunities[neighborLabel] == nil {
                    let size = communitySizes[neighborLabel] ?? 0
                    neighborLabels[neighborLabel, default: 0] = max(neighborLabels[neighborLabel, default: 0], size)
                }
            }

            // Assign to the largest neighbor community
            if let (largestLabel, _) = neighborLabels.max(by: { $0.value < $1.value }) {
                result[node] = largestLabel
            }
        }

        return result
    }

    /// Compute modularity score for the community assignment using GraphEdgeScanner
    ///
    /// Modularity Q = (1/2m) * Σ [A_ij - (k_i * k_j)/(2m)] * δ(c_i, c_j)
    /// where m = total edges, A = adjacency, k = degree, c = community
    private func computeModularity(
        labels: [String: String],
        edgeLabel: String?
    ) async throws -> Double {
        // Collect edges using GraphEdgeScanner
        let edges: [(source: String, target: String)] = try await database.withTransaction(configuration: .batch) { transaction in
            var collectedEdges: [(source: String, target: String)] = []

            for try await edgeInfo in self.scanner.scanAllEdges(
                edgeLabel: edgeLabel,
                transaction: transaction
            ) {
                collectedEdges.append((edgeInfo.source, edgeInfo.target))
            }

            return collectedEdges
        }

        // Process outside transaction to avoid Sendable issues
        var totalEdges = 0
        var degrees: [String: Int] = [:]
        var inCommunityEdges = 0

        for (source, target) in edges {
            totalEdges += 1
            degrees[source, default: 0] += 1
            degrees[target, default: 0] += 1

            // Check if same community
            if let sourceLabel = labels[source],
               let targetLabel = labels[target],
               sourceLabel == targetLabel {
                inCommunityEdges += 1
            }
        }

        guard totalEdges > 0 else { return 0 }

        let m = Double(totalEdges)

        // Compute expected in-community edges based on degree distribution
        var expectedInCommunity = 0.0

        // Group nodes by community
        var communities: [String: [String]] = [:]
        for (node, label) in labels {
            communities[label, default: []].append(node)
        }

        for (_, members) in communities {
            var communityDegreeSum = 0
            for member in members {
                communityDegreeSum += degrees[member] ?? 0
            }
            expectedInCommunity += Double(communityDegreeSum * communityDegreeSum) / (4.0 * m)
        }

        let modularity = (Double(inCommunityEdges) / (2.0 * m)) - (expectedInCommunity / (2.0 * m))

        return modularity
    }
}
