// PageRankComputer.swift
// GraphIndex - PageRank algorithm implementation
//
// Provides PageRank computation using power iteration.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - PageRankComputer

/// PageRank computation for graph indexes
///
/// Computes PageRank scores for all nodes in a graph using the
/// power iteration method.
///
/// **Algorithm**: Power Iteration with Damping Factor
/// ```
/// PR(v) = (1-d)/N + d * Σ PR(u)/out(u) for all u linking to v
/// ```
/// where d = damping factor (typically 0.85), N = total nodes
///
/// **Time Complexity**: O(E * iterations)
/// **Space Complexity**: O(V) for score storage
///
/// **Transaction Strategy**:
/// - Each iteration processes nodes in batches
/// - Uses `.batch` transaction configuration (30s timeout)
/// - Stores intermediate results in memory for resumability
///
/// **Reference**: Page, Brin et al., "The PageRank Citation Ranking:
/// Bringing Order to the Web" (1999)
///
/// **Usage**:
/// ```swift
/// let computer = PageRankComputer<Edge>(
///     database: database,
///     subspace: indexSubspace
/// )
///
/// let result = try await computer.compute(edgeLabel: "follows")
///
/// // Get top 10 nodes
/// for (nodeID, score) in result.topK(10) {
///     print("\(nodeID): \(score)")
/// }
/// ```
public final class PageRankComputer<Edge: Persistable>: Sendable {

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Edge scanner for neighbor lookups (centralizes key structure knowledge)
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: PageRankConfiguration

    // MARK: - Initialization

    /// Initialize PageRank computer
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace
    ///   - configuration: Algorithm configuration
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        configuration: PageRankConfiguration = .default
    ) {
        self.database = database
        self.scanner = GraphEdgeScanner(indexSubspace: subspace)
        self.configuration = configuration
    }

    // MARK: - Public API

    /// Compute PageRank for all nodes in the graph
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: PageRankResult with scores for all nodes
    public func compute(edgeLabel: String? = nil) async throws -> PageRankResult {
        let startTime = DispatchTime.now()

        // Step 1: Collect all nodes and their out-degrees
        let (nodes, outDegrees) = try await collectNodesAndDegrees(edgeLabel: edgeLabel)

        guard !nodes.isEmpty else {
            return PageRankResult(
                scores: [:],
                iterations: 0,
                convergenceDelta: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Step 2: Initialize scores uniformly
        let n = Double(nodes.count)
        var scores: [String: Double] = [:]
        for node in nodes {
            scores[node] = 1.0 / n
        }

        // Step 3: Power iteration
        let d = configuration.dampingFactor
        var iteration = 0
        var delta = Double.infinity

        while iteration < configuration.maxIterations && delta > configuration.convergenceThreshold {
            iteration += 1
            var newScores: [String: Double] = [:]

            // Initialize with teleportation probability
            for node in nodes {
                newScores[node] = (1.0 - d) / n
            }

            // Distribute scores along edges in batches
            let nodeArray = Array(nodes)
            for batchStart in stride(from: 0, to: nodeArray.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, nodeArray.count)
                let batch = Array(nodeArray[batchStart..<batchEnd])

                // Fetch incoming edges for each node in the batch
                let contributions = try await computeContributions(
                    nodes: batch,
                    scores: scores,
                    outDegrees: outDegrees,
                    edgeLabel: edgeLabel,
                    dampingFactor: d
                )

                // Accumulate contributions
                for (node, contribution) in contributions {
                    newScores[node, default: (1.0 - d) / n] += contribution
                }
            }

            // Compute convergence delta (L1 norm)
            delta = 0
            for node in nodes {
                delta += abs((newScores[node] ?? 0) - (scores[node] ?? 0))
            }

            scores = newScores
        }

        return PageRankResult(
            scores: scores,
            iterations: iteration,
            convergenceDelta: delta,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    /// Compute PageRank for a single node (personalized PageRank)
    ///
    /// Starts from a specific node with probability 1.0 and computes
    /// the stationary distribution.
    ///
    /// - Parameters:
    ///   - startNode: Starting node for personalized PageRank
    ///   - edgeLabel: Optional edge label filter
    /// - Returns: PageRankResult with scores relative to startNode
    public func computePersonalized(
        from startNode: String,
        edgeLabel: String? = nil
    ) async throws -> PageRankResult {
        let startTime = DispatchTime.now()

        // Step 1: Collect all nodes and their out-degrees
        let (nodes, outDegrees) = try await collectNodesAndDegrees(edgeLabel: edgeLabel)

        guard nodes.contains(startNode) else {
            return PageRankResult(
                scores: [startNode: 1.0],
                iterations: 0,
                convergenceDelta: 0,
                durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        // Step 2: Initialize - all probability on start node
        var scores: [String: Double] = [:]
        for node in nodes {
            scores[node] = node == startNode ? 1.0 : 0.0
        }

        // Step 3: Power iteration with personalized teleportation
        let d = configuration.dampingFactor
        var iteration = 0
        var delta = Double.infinity

        while iteration < configuration.maxIterations && delta > configuration.convergenceThreshold {
            iteration += 1
            var newScores: [String: Double] = [:]

            // Teleportation goes back to start node (not uniform)
            for node in nodes {
                newScores[node] = node == startNode ? (1.0 - d) : 0.0
            }

            // Distribute scores along edges
            let nodeArray = Array(nodes)
            for batchStart in stride(from: 0, to: nodeArray.count, by: configuration.batchSize) {
                let batchEnd = min(batchStart + configuration.batchSize, nodeArray.count)
                let batch = Array(nodeArray[batchStart..<batchEnd])

                let contributions = try await computeContributions(
                    nodes: batch,
                    scores: scores,
                    outDegrees: outDegrees,
                    edgeLabel: edgeLabel,
                    dampingFactor: d
                )

                for (node, contribution) in contributions {
                    newScores[node, default: 0] += contribution
                }
            }

            // Compute convergence delta
            delta = 0
            for node in nodes {
                delta += abs((newScores[node] ?? 0) - (scores[node] ?? 0))
            }

            scores = newScores
        }

        return PageRankResult(
            scores: scores,
            iterations: iteration,
            convergenceDelta: delta,
            durationNs: DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
        )
    }

    // MARK: - Private Methods

    /// Collect all nodes and compute their out-degrees using GraphEdgeScanner
    private func collectNodesAndDegrees(
        edgeLabel: String?
    ) async throws -> (nodes: Set<String>, outDegrees: [String: Int]) {
        // Use GraphEdgeScanner to scan all edges
        let edges: [EdgeInfo] = try await database.withTransaction(configuration: .batch) { transaction in
            var collectedEdges: [EdgeInfo] = []
            for try await edge in self.scanner.scanAllEdges(edgeLabel: edgeLabel, transaction: transaction) {
                collectedEdges.append(edge)
            }
            return collectedEdges
        }

        // Process edges outside transaction to avoid Sendable issues
        var nodes: Set<String> = []
        var outDegrees: [String: Int] = [:]

        for edge in edges {
            nodes.insert(edge.source)
            nodes.insert(edge.target)
            outDegrees[edge.source, default: 0] += 1
        }

        return (nodes, outDegrees)
    }

    /// Compute PageRank contributions for a batch of target nodes using GraphEdgeScanner
    ///
    /// **Performance Note (Adjacency Strategy)**:
    /// - When `edgeLabel` is specified: O(degree) per node via prefix scan
    /// - When `edgeLabel` is nil: O(E) full scan of incoming edge subspace + filter
    private func computeContributions(
        nodes: [String],
        scores: [String: Double],
        outDegrees: [String: Int],
        edgeLabel: String?,
        dampingFactor: Double
    ) async throws -> [(node: String, contribution: Double)] {
        // Use GraphEdgeScanner to get incoming edges for all target nodes
        let incomingEdges: [EdgeInfo] = try await database.withTransaction(configuration: .batch) { transaction in
            try await self.scanner.batchScanIncoming(
                to: nodes,
                edgeLabel: edgeLabel,
                transaction: transaction
            )
        }

        // Accumulate contributions per target node
        var contributionsByTarget: [String: Double] = [:]

        for edge in incomingEdges {
            let targetNode = edge.target
            let sourceNode = edge.source

            let sourceScore = scores[sourceNode] ?? 0
            let sourceOutDegree = outDegrees[sourceNode] ?? 1

            contributionsByTarget[targetNode, default: 0] += dampingFactor * sourceScore / Double(sourceOutDegree)
        }

        // Convert to result format
        return contributionsByTarget.compactMap { (node, contribution) in
            contribution > 0 ? (node, contribution) : nil
        }
    }
}
