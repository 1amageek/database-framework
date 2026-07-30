// PageRankComputer.swift
// GraphIndex - PageRank algorithm implementation
//
// Provides PageRank computation using power iteration.

import DatabaseKit
import DatabaseEngine
import StorageKit

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
/// **Snapshot Strategy**:
/// - One caller-owned snapshot covers collection and every iteration
/// - Intermediate scores remain in memory
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
public final class PageRankComputer: Sendable {

    // MARK: - Properties

    /// Storage snapshot shared by the complete computation.
    private let snapshot: GraphReadSnapshot

    /// Edge scanner for neighbor lookups (centralizes key structure knowledge)
    private let scanner: GraphEdgeScanner

    /// Configuration
    private let configuration: PageRankConfiguration

    /// Shared request work budget.
    private let workBudget: GraphAlgorithmWorkBudget?

    // MARK: - Initialization

    /// Initialize PageRank computer
    ///
    /// - Parameters:
    ///   - snapshot: Stable storage snapshot for the complete computation
    ///   - subspace: Index subspace
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    ///   - configuration: Algorithm configuration
    package init(
        snapshot: GraphReadSnapshot,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        scope: GraphScanScope = .all,
        configuration: PageRankConfiguration = .default
    ) {
        self.snapshot = snapshot
        self.scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: strategy,
            scope: scope,
            snapshot: snapshot
        )
        self.configuration = configuration
        self.workBudget = snapshot.workBudget
    }

    // MARK: - Public API

    /// Compute PageRank for all nodes in the graph
    ///
    /// - Parameter edgeLabel: Optional edge label filter
    /// - Returns: PageRankResult with scores for all nodes
    public func compute(edgeLabel: GraphIdentity? = nil) async throws -> PageRankResult {
        let startTime = snapshot.clock.now()

        // Step 1: Collect all nodes and their out-degrees
        let collection = try await collectNodesAndDegrees(edgeLabel: edgeLabel)
        let nodes = collection.nodes
        let outDegrees = collection.outDegrees
        let graph = collection.graph

        if let limitReason = collection.limitReason {
            return PageRankResult(
                scores: [:],
                iterations: 0,
                convergenceDelta: 0,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: false,
                limitReason: limitReason
            )
        }

        guard !nodes.isEmpty else {
            return PageRankResult(
                scores: [:],
                iterations: 0,
                convergenceDelta: 0,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            )
        }

        let orderedNodes = nodes.sorted()

        // Step 2: Initialize scores uniformly
        let n = Double(nodes.count)
        var scores: [GraphIdentity: Double] = [:]
        for node in orderedNodes {
            scores[node] = 1.0 / n
        }

        // Step 3: Power iteration
        let d = configuration.dampingFactor
        var iteration = 0
        var delta = Double.infinity

        while iteration < configuration.maxIterations && delta > configuration.convergenceThreshold {
            var newScores: [GraphIdentity: Double] = [:]

            // Calculate dangling rank sum (nodes with no outgoing edges)
            // Dangling nodes distribute their rank uniformly to all nodes
            // Reference: Page, Brin et al., "The PageRank Citation Ranking" (1999)
            var danglingRankSum = 0.0
            for node in orderedNodes {
                guard let outDegree = outDegrees[node],
                      let score = scores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                if outDegree == 0 {
                    danglingRankSum += score
                }
            }
            let danglingContribution = d * danglingRankSum / n

            // Initialize with teleportation probability + dangling contribution
            for node in orderedNodes {
                newScores[node] = (1.0 - d) / n + danglingContribution
            }

            let contributionBatch = try computeContributions(
                nodes: orderedNodes,
                scores: scores,
                outDegrees: outDegrees,
                graph: graph,
                dampingFactor: d
            )

            if let limitReason = contributionBatch.limitReason {
                return PageRankResult(
                    scores: scores,
                    iterations: iteration,
                    convergenceDelta: delta.isFinite ? delta : 0,
                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                    isComplete: false,
                    limitReason: limitReason
                )
            }

            for (node, contribution) in contributionBatch.contributions {
                guard let score = newScores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                newScores[node] = score + contribution
            }

            // Compute convergence delta (L1 norm)
            delta = 0
            for node in orderedNodes {
                guard let newScore = newScores[node],
                      let oldScore = scores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                delta += abs(newScore - oldScore)
            }

            scores = newScores
            iteration += 1
        }

        let converged = delta <= configuration.convergenceThreshold
        return PageRankResult(
            scores: scores,
            iterations: iteration,
            convergenceDelta: delta,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: converged,
            limitReason: converged
                ? nil
                : .maxIterationsReached(
                    iterations: iteration,
                    limit: configuration.maxIterations
                )
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
        from startNode: GraphIdentity,
        edgeLabel: GraphIdentity? = nil
    ) async throws -> PageRankResult {
        let startTime = snapshot.clock.now()

        // Step 1: Collect all nodes and their out-degrees
        let collection = try await collectNodesAndDegrees(edgeLabel: edgeLabel)
        let nodes = collection.nodes
        let outDegrees = collection.outDegrees
        let graph = collection.graph

        if let limitReason = collection.limitReason {
            return PageRankResult(
                scores: [:],
                iterations: 0,
                convergenceDelta: 0,
                durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                isComplete: false,
                limitReason: limitReason
            )
        }

        guard nodes.contains(startNode) else {
            throw PageRankError.personalizationSourceNotFound(startNode)
        }

        let orderedNodes = nodes.sorted()

        // Step 2: Initialize - all probability on start node
        var scores: [GraphIdentity: Double] = [:]
        for node in orderedNodes {
            scores[node] = node == startNode ? 1.0 : 0.0
        }

        // Step 3: Power iteration with personalized teleportation
        let d = configuration.dampingFactor
        var iteration = 0
        var delta = Double.infinity

        while iteration < configuration.maxIterations && delta > configuration.convergenceThreshold {
            var newScores: [GraphIdentity: Double] = [:]

            // Calculate dangling rank sum (nodes with no outgoing edges)
            // In personalized PageRank, dangling nodes teleport back to start node
            // Reference: Haveliwala, "Topic-Sensitive PageRank" (2002)
            var danglingRankSum = 0.0
            for node in orderedNodes {
                guard let outDegree = outDegrees[node],
                      let score = scores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                if outDegree == 0 {
                    danglingRankSum += score
                }
            }
            let danglingContribution = d * danglingRankSum

            // Teleportation goes back to start node (not uniform)
            // Dangling contribution also goes to start node
            for node in orderedNodes {
                if node == startNode {
                    newScores[node] = (1.0 - d) + danglingContribution
                } else {
                    newScores[node] = 0.0
                }
            }

            let contributionBatch = try computeContributions(
                nodes: orderedNodes,
                scores: scores,
                outDegrees: outDegrees,
                graph: graph,
                dampingFactor: d
            )

            if let limitReason = contributionBatch.limitReason {
                return PageRankResult(
                    scores: scores,
                    iterations: iteration,
                    convergenceDelta: delta.isFinite ? delta : 0,
                    durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
                    isComplete: false,
                    limitReason: limitReason
                )
            }

            for (node, contribution) in contributionBatch.contributions {
                guard let score = newScores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                newScores[node] = score + contribution
            }

            // Compute convergence delta
            delta = 0
            for node in orderedNodes {
                guard let newScore = newScores[node],
                      let oldScore = scores[node] else {
                    throw PageRankError.inconsistentNodeState(node)
                }
                delta += abs(newScore - oldScore)
            }

            scores = newScores
            iteration += 1
        }

        let converged = delta <= configuration.convergenceThreshold
        return PageRankResult(
            scores: scores,
            iterations: iteration,
            convergenceDelta: delta,
            durationNs: snapshot.clock.now().uptimeNanoseconds - startTime.uptimeNanoseconds,
            isComplete: converged,
            limitReason: converged
                ? nil
                : .maxIterationsReached(
                    iterations: iteration,
                    limit: configuration.maxIterations
                )
        )
    }

    // MARK: - Private Methods

    /// Collect all nodes and compute their out-degrees using GraphEdgeScanner
    private struct NodeCollection: Sendable {
        let nodes: Set<GraphIdentity>
        let outDegrees: [GraphIdentity: Int]
        let graph: MaterializedGraphSnapshot
        let limitReason: LimitReason?
    }

    private struct ContributionBatch: Sendable {
        let contributions: [GraphIdentity: Double]
        let limitReason: LimitReason?
    }

    private func collectNodesAndDegrees(
        edgeLabel: GraphIdentity?
    ) async throws -> NodeCollection {
        let load = try await MaterializedGraphSnapshotBuilder.load(
            scanner: scanner,
            edgeLabel: edgeLabel,
            snapshot: snapshot
        )
        let graph = load.graph
        var outDegrees: [GraphIdentity: Int] = [:]
        outDegrees.reserveCapacity(graph.nodes.count)
        for node in graph.nodes {
            outDegrees[node] = 0
        }
        for (node, edges) in graph.outgoing {
            outDegrees[node] = edges.count
        }

        return NodeCollection(
            nodes: graph.nodes,
            outDegrees: outDegrees,
            graph: graph,
            limitReason: load.limitReason
        )
    }

    /// Compute PageRank contributions for a batch of target nodes using GraphEdgeScanner
    ///
    /// **Performance Note (Adjacency Strategy)**:
    /// - When `edgeLabel` is specified: O(degree) per node via prefix scan
    /// - When `edgeLabel` is nil: O(E) full scan of incoming edge subspace + filter
    private func computeContributions(
        nodes: [GraphIdentity],
        scores: [GraphIdentity: Double],
        outDegrees: [GraphIdentity: Int],
        graph: MaterializedGraphSnapshot,
        dampingFactor: Double
    ) throws -> ContributionBatch {
        var contributionsByTarget: [GraphIdentity: Double] = [:]

        for node in nodes {
            guard try consumeWork() else {
                return ContributionBatch(
                    contributions: [:],
                    limitReason: workBudget?.limitReason
                )
            }

            for edge in graph.incomingNeighbors(of: node) {
                guard try consumeWork() else {
                    return ContributionBatch(
                        contributions: [:],
                        limitReason: workBudget?.limitReason
                    )
                }

                guard let sourceScore = scores[edge.source],
                      let sourceOutDegree = outDegrees[edge.source],
                      sourceOutDegree > 0 else {
                    throw PageRankError.inconsistentNodeState(edge.source)
                }
                contributionsByTarget[edge.target, default: 0] +=
                    dampingFactor * sourceScore / Double(sourceOutDegree)
            }
        }

        return ContributionBatch(
            contributions: contributionsByTarget,
            limitReason: nil
        )
    }

    private func consumeWork(_ units: UInt64 = 1) throws -> Bool {
        try workBudget?.consume(units) ?? true
    }
}

public enum PageRankError: Error, Sendable {
    case personalizationSourceNotFound(GraphIdentity)
    case inconsistentNodeState(GraphIdentity)
}
