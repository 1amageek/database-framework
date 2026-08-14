// CommunityDetector.swift
// GraphIndex - Community detection using Label Propagation Algorithm
//
// Provides community detection for graph indexes.

import DatabaseKit
import DatabaseEngine
import StorageKit

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
/// **Snapshot Strategy**:
/// - One caller-owned snapshot covers collection and every iteration
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
public final class CommunityDetector: Sendable {
    private enum WorkLimited<Value: Sendable>: Sendable {
        case value(Value)
        case incomplete(LimitReason)
    }

    // MARK: - Properties

    /// Storage snapshot shared by the complete computation.
    private let snapshot: GraphReadSnapshot

    /// Edge scanner for neighbor lookups
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: CommunityDetectionConfiguration

    /// Shared request work budget.
    private let workBudget: GraphAlgorithmWorkBudget?

    // MARK: - Initialization

    /// Initialize community detector
    ///
    /// - Parameters:
    ///   - snapshot: Stable storage snapshot for the complete computation
    ///   - subspace: Index subspace
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    ///   - configuration: Algorithm configuration
    @_spi(DatabaseExecution)
    public init(
        snapshot: GraphReadSnapshot,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        graphTarget: GraphScanTarget = .all,
        configuration: CommunityDetectionConfiguration = .default
    ) {
        self.snapshot = snapshot
        self.configuration = configuration
        self.scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: strategy,
            graphTarget: graphTarget,
            snapshot: snapshot
        )
        self.workBudget = snapshot.workBudget
    }

    // MARK: - Public API

    /// Detect communities in the graph
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: CommunityResult with node assignments
    public func detect(edgeLabel: GraphIdentity? = nil) async throws -> CommunityResult {
        let startTime = snapshot.clock.now()

        // Step 1: Collect all nodes
        let collection = try await collectAllNodes(edgeLabel: edgeLabel)
        let nodes = collection.nodes
        let graph = collection.graph

        if let limitReason = collection.limitReason {
            return CommunityResult(
                assignments: [:],
                iterations: 0,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: false,
                limitReason: limitReason
            )
        }

        guard !nodes.isEmpty else {
            return CommunityResult(
                assignments: [:],
                iterations: 0,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Step 2: Initialize each node with its own unique label
        var labels: [GraphIdentity: GraphIdentity] = [:]
        for node in nodes {
            labels[node] = node  // Initial label = node ID
        }

        // Step 3: Iterate until convergence
        var iteration = 0
        var changed = true
        var limitReason: LimitReason?

        // Create seeded RNG if seed is provided
        var seededRNG: SeededRandomNumberGenerator? = configuration.seed.map { SeededRandomNumberGenerator(seed: $0) }

        iterationLoop: while iteration < configuration.maxIterations && changed {
            iteration += 1
            changed = false

            // Shuffle nodes for randomization (important for LPA convergence)
            // Use seeded RNG for deterministic shuffling if seed is provided
            //
            // Note: When using seeded RNG, we sort nodes first to ensure deterministic
            // ordering regardless of Set iteration order (which is unspecified in Swift).
            let shuffled: [GraphIdentity]
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
                let newLabel: GraphIdentity?
                switch try computeSingleNodeUpdate(
                        node: node,
                        currentLabels: labels,
                        graph: graph,
                        rng: &seededRNG
                    ) {
                case .value(let value):
                    newLabel = value
                case .incomplete(let reason):
                    limitReason = reason
                    break iterationLoop
                }

                if let label = newLabel, labels[node] != label {
                    labels[node] = label
                    changed = true
                }
            }
        }

        // Step 4: Apply minimum community size filter if needed
        if limitReason == nil && configuration.minCommunitySize > 1 {
            switch try applyMinCommunitySize(
                    labels: labels,
                    graph: graph
                ) {
            case .value(let filteredLabels):
                labels = filteredLabels
            case .incomplete(let reason):
                limitReason = reason
            }
        }

        // Step 5: Build community mapping
        var communities: [GraphIdentity: [GraphIdentity]] = [:]
        for (node, label) in labels {
            communities[label, default: []].append(node)
        }

        // Step 6: Optionally compute modularity
        var modularity: Double? = nil
        if limitReason == nil && configuration.computeModularity {
            switch try computeModularity(
                    labels: labels,
                    graph: graph
                ) {
            case .value(let value):
                modularity = value
            case .incomplete(let reason):
                modularity = nil
                limitReason = reason
            }
        }

        let converged = limitReason == nil && !changed
        let finalLimitReason = limitReason ?? (converged
            ? nil
            : .maxIterationsReached(
                iterations: iteration,
                limit: configuration.maxIterations
            ))
        return CommunityResult(
            assignments: labels,
            communities: communities,
            iterations: iteration,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            modularity: modularity,
            isComplete: converged,
            limitReason: finalLimitReason
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
        for node: GraphIdentity,
        maxHops: Int = 3,
        edgeLabel: GraphIdentity? = nil
    ) async throws -> Set<GraphIdentity> {
        let load = try await MaterializedGraphSnapshotBuilder.load(
            scanner: scanner,
            edgeLabel: edgeLabel,
            snapshot: snapshot
        )
        if let limitReason = load.limitReason {
            throw CommunityDetectionError.incomplete(limitReason)
        }
        let graph = load.graph

        // Collect local neighborhood
        var neighborhood: Set<GraphIdentity> = [node]
        var frontier: Set<GraphIdentity> = [node]

        for _ in 0..<maxHops {
            var nextFrontier: Set<GraphIdentity> = []

            for currentNode in frontier {
                let neighbors: Set<GraphIdentity>
                switch try getNeighbors(of: currentNode, graph: graph) {
                case .value(let value): neighbors = value
                case .incomplete(let reason):
                    throw CommunityDetectionError.incomplete(reason)
                }
                for neighbor in neighbors where !neighborhood.contains(neighbor) {
                    nextFrontier.insert(neighbor)
                    neighborhood.insert(neighbor)
                }
            }

            frontier = nextFrontier
            if frontier.isEmpty { break }
        }

        // Run LPA on local neighborhood
        var labels: [GraphIdentity: GraphIdentity] = [:]
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
            let shuffledNeighborhood: [GraphIdentity]
            if var rng = seededRNG {
                var nodeArray = Array(neighborhood).sorted()
                nodeArray.shuffle(using: &rng)
                seededRNG = rng
                shuffledNeighborhood = nodeArray
            } else {
                shuffledNeighborhood = Array(neighborhood).shuffled()
            }

            for n in shuffledNeighborhood {
                let neighbors: Set<GraphIdentity>
                switch try getNeighbors(of: n, graph: graph) {
                case .value(let value): neighbors = value
                case .incomplete(let reason):
                    throw CommunityDetectionError.incomplete(reason)
                }

                var labelCounts: [GraphIdentity: Int] = [:]
                for neighbor in neighbors where neighborhood.contains(neighbor) {
                    guard let label = labels[neighbor] else {
                        throw CommunityDetectionError.inconsistentState(
                            "local neighbor is missing its label"
                        )
                    }
                    labelCounts[label, default: 0] += 1
                }

                guard let maxCount = labelCounts.values.max() else { continue }
                var candidates = labelCounts.filter { $0.value == maxCount }.map { $0.key }

                // Sort for deterministic selection when using seeded RNG
                if seededRNG != nil {
                    candidates.sort()
                }

                let newLabel: GraphIdentity
                if var generator = seededRNG {
                    guard let selected = candidates.randomElement(using: &generator) else {
                        throw CommunityDetectionError.inconsistentState(
                            "local label candidate selection is empty"
                        )
                    }
                    newLabel = selected
                    seededRNG = generator
                } else {
                    guard let selected = candidates.randomElement() else {
                        throw CommunityDetectionError.inconsistentState(
                            "local label candidate selection is empty"
                        )
                    }
                    newLabel = selected
                }

                if newLabel != labels[n] {
                    labels[n] = newLabel
                    changed = true
                }
            }
        }

        // Return nodes with the same label as the input node
        guard let nodeLabel = labels[node] else {
            throw CommunityDetectionError.inconsistentState(
                "local community lost the source node label"
            )
        }
        guard !changed else {
            throw CommunityDetectionError.incomplete(
                .maxIterationsReached(
                    iterations: iteration,
                    limit: configuration.maxIterations
                )
            )
        }

        return Set(labels.filter { $0.value == nodeLabel }.map { $0.key })
    }

    // MARK: - Private Methods

    private struct NodeCollection: Sendable {
        let nodes: Set<GraphIdentity>
        let graph: MaterializedGraphSnapshot
        let limitReason: LimitReason?
    }

    /// Collect all unique nodes from the graph using GraphEdgeScanner
    private func collectAllNodes(edgeLabel: GraphIdentity?) async throws -> NodeCollection {
        let load = try await MaterializedGraphSnapshotBuilder.load(
            scanner: scanner,
            edgeLabel: edgeLabel,
            snapshot: snapshot
        )
        return NodeCollection(
            nodes: load.graph.nodes,
            graph: load.graph,
            limitReason: load.limitReason
        )
    }

    /// Compute label update for a single node (for synchronous LPA)
    ///
    /// Returns the new label for the node, or nil if no update is needed
    /// (e.g., when the node has no neighbors).
    private func computeSingleNodeUpdate(
        node: GraphIdentity,
        currentLabels: [GraphIdentity: GraphIdentity],
        graph: MaterializedGraphSnapshot,
        rng: inout SeededRandomNumberGenerator?
    ) throws -> WorkLimited<GraphIdentity?> {
        let neighbors: Set<GraphIdentity>
        switch try getNeighbors(of: node, graph: graph) {
        case .value(let value): neighbors = value
        case .incomplete(let reason): return .incomplete(reason)
        }

        guard !neighbors.isEmpty else { return .value(nil) }

        // Count label frequencies among neighbors
        var labelCounts: [GraphIdentity: Int] = [:]
        for neighbor in neighbors {
            guard let label = currentLabels[neighbor] else {
                throw CommunityDetectionError.inconsistentState(
                    "neighbor is missing its current label"
                )
            }
            labelCounts[label, default: 0] += 1
        }

        // Find most common label (deterministic tie-breaking when seeded)
        guard let maxCount = labelCounts.values.max() else {
            throw CommunityDetectionError.inconsistentState(
                "non-empty neighborhood produced no label counts"
            )
        }
        var candidates = labelCounts.filter { $0.value == maxCount }.map { $0.key }

        // Sort candidates for deterministic selection when using seeded RNG
        if rng != nil {
            candidates.sort()
        }

        let newLabel: GraphIdentity
        if var generator = rng {
            guard let selected = candidates.randomElement(using: &generator) else {
                throw CommunityDetectionError.inconsistentState(
                    "label candidate selection is empty"
                )
            }
            newLabel = selected
            rng = generator  // Update the RNG state
        } else {
            guard let selected = candidates.randomElement() else {
                throw CommunityDetectionError.inconsistentState(
                    "label candidate selection is empty"
                )
            }
            newLabel = selected
        }

        return .value(newLabel)
    }

    /// Get all neighbors of a node (both directions) using GraphEdgeScanner
    private func getNeighbors(
        of nodeID: GraphIdentity,
        graph: MaterializedGraphSnapshot
    ) throws -> WorkLimited<Set<GraphIdentity>> {
        var neighbors: Set<GraphIdentity> = []

        if let reason = try workLimitReason() { return .incomplete(reason) }
        for edgeInfo in graph.outgoingNeighbors(of: nodeID) {
            if let reason = try workLimitReason() { return .incomplete(reason) }
            neighbors.insert(edgeInfo.target)
        }

        if let reason = try workLimitReason() { return .incomplete(reason) }
        for edgeInfo in graph.incomingNeighbors(of: nodeID) {
            if let reason = try workLimitReason() { return .incomplete(reason) }
            neighbors.insert(edgeInfo.source)
        }

        return .value(neighbors)
    }

    /// Apply minimum community size filter by merging small communities
    private func applyMinCommunitySize(
        labels: [GraphIdentity: GraphIdentity],
        graph: MaterializedGraphSnapshot
    ) throws -> WorkLimited<[GraphIdentity: GraphIdentity]> {
        var result = labels

        // Build community sizes
        var communitySizes: [GraphIdentity: Int] = [:]
        for (_, label) in labels {
            communitySizes[label, default: 0] += 1
        }

        // Find small communities
        let smallCommunities = communitySizes.filter { $0.value < configuration.minCommunitySize }

        // For each node in a small community, assign to largest neighbor community
        let orderedNodes = configuration.seed == nil
            ? Array(result.keys)
            : result.keys.sorted()
        for node in orderedNodes {
            guard let label = result[node] else {
                throw CommunityDetectionError.inconsistentState(
                    "community assignment key is missing its label"
                )
            }
            guard smallCommunities[label] != nil else { continue }

            let neighbors: Set<GraphIdentity>
            switch try getNeighbors(of: node, graph: graph) {
            case .value(let value): neighbors = value
            case .incomplete(let reason): return .incomplete(reason)
            }
            var neighborLabels: [GraphIdentity: Int] = [:]

            for neighbor in neighbors {
                if let neighborLabel = result[neighbor],
                   smallCommunities[neighborLabel] == nil {
                    guard let size = communitySizes[neighborLabel] else {
                        throw CommunityDetectionError.inconsistentState(
                            "community size is missing for a neighbor label"
                        )
                    }
                    neighborLabels[neighborLabel, default: 0] = max(neighborLabels[neighborLabel, default: 0], size)
                }
            }

            // Assign to the largest neighbor community
            let largestLabel = neighborLabels
                .sorted { lhs, rhs in
                    if lhs.value != rhs.value { return lhs.value > rhs.value }
                    return lhs.key < rhs.key
                }
                .first?
                .key
            if let largestLabel {
                result[node] = largestLabel
            }
        }

        return .value(result)
    }

    /// Compute modularity score for the community assignment using GraphEdgeScanner
    ///
    /// Modularity Q = (1/2m) * Σ [A_ij - (k_i * k_j)/(2m)] * δ(c_i, c_j)
    /// where m = total edges, A = adjacency, k = degree, c = community
    private func computeModularity(
        labels: [GraphIdentity: GraphIdentity],
        graph: MaterializedGraphSnapshot
    ) throws -> WorkLimited<Double> {
        var totalEdges = 0
        var degrees: [GraphIdentity: Int] = [:]
        var inCommunityEdges = 0

        for edge in graph.edges {
            if let reason = try workLimitReason() { return .incomplete(reason) }
            let source = edge.source
            let target = edge.target
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

        guard totalEdges > 0 else { return .value(0) }

        let m = Double(totalEdges)

        // Compute expected in-community edges based on degree distribution
        var expectedInCommunity = 0.0

        // Group nodes by community
        var communities: [GraphIdentity: [GraphIdentity]] = [:]
        for (node, label) in labels {
            communities[label, default: []].append(node)
        }

        for label in communities.keys.sorted() {
            guard let members = communities[label] else {
                throw CommunityDetectionError.inconsistentState(
                    "community key is missing its members"
                )
            }
            var communityDegreeSum = 0
            for member in members.sorted() {
                guard let degree = degrees[member] else {
                    throw CommunityDetectionError.inconsistentState(
                        "community member is missing its graph degree"
                    )
                }
                communityDegreeSum += degree
            }
            expectedInCommunity += Double(communityDegreeSum * communityDegreeSum) / (4.0 * m)
        }

        let modularity = (Double(inCommunityEdges) / (2.0 * m)) - (expectedInCommunity / (2.0 * m))

        return .value(modularity)
    }

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }

    private func workLimitReason(
        _ units: UInt64 = 1
    ) throws -> LimitReason? {
        guard try consumeWork(units) else {
            guard let reason = workBudget?.limitReason else {
                throw CommunityDetectionError.inconsistentState(
                    "work budget rejected work without a limit reason"
                )
            }
            return reason
        }
        return nil
    }
}

public enum CommunityDetectionError: Error, Sendable {
    case incomplete(LimitReason)
    case inconsistentState(String)
}
