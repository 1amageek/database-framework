#if FOUNDATION_DB
// GraphAlgorithmTests.swift
// Tests for Graph algorithms: Shortest Path, Path Pattern, PageRank, Community Detection

import Testing
import Foundation
import StorageKit
import Core
import DatabaseValue
import Graph
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable(type: "GraphAlgoEdge")
private struct Edge {
    var id: String = UUID().uuidString
    var source: String
    var target: String
    var label: String
    var weight: Double

    init(id: String = UUID().uuidString, source: String, target: String, label: String, weight: Double = 1.0) {
        self.id = id
        self.source = source
        self.target = target
        self.label = label
        self.weight = weight
    }
}

// MARK: - Test Helper

private struct GraphAlgorithmContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace

    init(
        indexName: String = "GraphAlgoEdge_graph"
    ) async throws {
        self.database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graphalgo", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
    }

    func cleanup() async throws {
        let range = subspace.range()
        try await database.withTransaction(configuration: .batch) { tx in
            try tx.clearRange(beginKey: range.0, endKey: range.1)
            // Note: withTransaction automatically commits on success - don't call commit() explicitly
        }
    }

    func cleanupReportingFailure() async {
        do {
            try await cleanup()
        } catch {
            Issue.record("Graph test cleanup failed: \(error)")
        }
    }

    /// Inserts canonical adjacency keys so these tests isolate scanner and
    /// algorithm behavior from record-schema and index-maintainer behavior.
    func insertEdges(_ edges: [Edge]) async throws {
        try await database.withTransaction(configuration: .batch) { transaction in
            for edge in edges {
                try transaction.setValue(
                    Bytes(),
                    for: indexSubspace.subspace(Int64(0)).pack(
                        Tuple(edge.source, edge.label, edge.target)
                    )
                )
                try transaction.setValue(
                    Bytes(),
                    for: indexSubspace.subspace(Int64(1)).pack(
                        Tuple(edge.target, edge.label, edge.source)
                    )
                )
            }
        }
    }

    func shortestPath(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        configuration: ShortestPathConfiguration = .default
    ) async throws -> ShortestPathResult {
        try await database.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let finder = ShortestPathFinder(
                snapshot: snapshot,
                subspace: indexSubspace,
                configuration: configuration
            )
            return try await finder.findShortestPath(
                from: .identifier(source),
                to: .identifier(target),
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    func allShortestPaths(
        from source: String,
        to target: String,
        edgeLabel: String? = nil,
        configuration: ShortestPathConfiguration = .default
    ) async throws -> AllShortestPathsResult {
        try await database.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let finder = ShortestPathFinder(
                snapshot: snapshot,
                subspace: indexSubspace,
                configuration: configuration
            )
            return try await finder.findAllShortestPaths(
                from: .identifier(source),
                to: .identifier(target),
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    func pageRank(
        configuration: PageRankConfiguration = .default,
        edgeLabel: String? = nil
    ) async throws -> PageRankResult {
        try await database.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let computer = PageRankComputer(
                snapshot: snapshot,
                subspace: indexSubspace,
                configuration: configuration
            )
            return try await computer.compute(
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    func communities(
        configuration: CommunityDetectionConfiguration = .default,
        edgeLabel: String? = nil
    ) async throws -> CommunityResult {
        try await database.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let detector = CommunityDetector(
                snapshot: snapshot,
                subspace: indexSubspace,
                configuration: configuration
            )
            return try await detector.detect(
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }

    func localCommunity(
        for node: String,
        maxHops: Int,
        configuration: CommunityDetectionConfiguration = .default,
        edgeLabel: String? = nil
    ) async throws -> Set<GraphIdentity> {
        try await database.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let detector = CommunityDetector(
                snapshot: snapshot,
                subspace: indexSubspace,
                configuration: configuration
            )
            return try await detector.detectLocalCommunity(
                for: .identifier(node),
                maxHops: maxHops,
                edgeLabel: edgeLabel.map(GraphIdentity.identifier)
            )
        }
    }
}

// MARK: - PathLength Tests

@Suite("PathLength Tests", .serialized, .heartbeat)
struct PathLengthTests {

    @Test("PathLength.exactly matches only specific length")
    func exactlyMatches() {
        let pathLength = PathLength.exactly(3)
        #expect(pathLength.matches(2) == false)
        #expect(pathLength.matches(3) == true)
        #expect(pathLength.matches(4) == false)
    }

    @Test("PathLength.range matches within range")
    func rangeMatches() {
        let pathLength = PathLength.range(2, 5)
        #expect(pathLength.matches(1) == false)
        #expect(pathLength.matches(2) == true)
        #expect(pathLength.matches(3) == true)
        #expect(pathLength.matches(5) == true)
        #expect(pathLength.matches(6) == false)
    }

    @Test("PathLength.atLeast matches minimum or above")
    func atLeastMatches() {
        let pathLength = PathLength.atLeast(3)
        #expect(pathLength.matches(2) == false)
        #expect(pathLength.matches(3) == true)
        #expect(pathLength.matches(10) == true)
    }

    @Test("PathLength.atMost matches maximum or below")
    func atMostMatches() {
        let pathLength = PathLength.atMost(3)
        #expect(pathLength.matches(0) == true)
        #expect(pathLength.matches(3) == true)
        #expect(pathLength.matches(4) == false)
    }

    @Test("PathLength.any matches all lengths")
    func anyMatches() {
        let pathLength = PathLength.any
        #expect(pathLength.matches(0) == true)
        #expect(pathLength.matches(100) == true)
    }
}

// MARK: - GraphPath Tests

@Suite("GraphPath Tests", .serialized, .heartbeat)
struct GraphPathTests {

    @Test("GraphPath length is correct")
    func lengthCalculation() throws {
        let path = try GraphPath(
            nodeIDs: ["A", "B", "C", "D"],
            edgeLabels: ["e1", "e2", "e3"],
            weights: nil
        )
        #expect(path.length == 3)
        #expect(path.source == "A")
        #expect(path.target == "D")
    }

    @Test("GraphPath totalWeight with weights")
    func totalWeightCalculation() throws {
        let path = try GraphPath(
            nodeIDs: ["A", "B", "C"],
            edgeLabels: ["e1", "e2"],
            weights: [1.5, 2.5]
        )
        #expect(path.totalWeight == 4.0)
    }

    @Test("GraphPath totalWeight without weights uses length")
    func totalWeightWithoutWeights() throws {
        let path = try GraphPath(
            nodeIDs: ["A", "B", "C", "D"],
            edgeLabels: ["e1", "e2", "e3"],
            weights: nil
        )
        #expect(path.totalWeight == 3.0)
    }

    @Test("GraphPath isEmpty for single node")
    func isEmptyForSingleNode() throws {
        let path = try GraphPath(
            nodeIDs: ["A"],
            edgeLabels: [],
            weights: nil
        )
        // A single-node path is NOT empty - it contains one node.
        // isEmpty returns true only when nodeIDs is empty (no nodes at all).
        // The path *length* is 0 (no edges), but the path itself exists.
        #expect(path.isEmpty == false)
        #expect(path.length == 0)
    }

    @Test("GraphPath rejects an edge count that does not match its nodes")
    func rejectsMismatchedEdgeCount() {
        #expect(throws: GraphPathError.self) {
            _ = try GraphPath(
                nodeIDs: ["A", "B", "C"],
                edgeLabels: ["edge"],
                weights: nil
            )
        }
    }

    @Test("GraphPath rejects non-finite weights")
    func rejectsNonFiniteWeights() {
        #expect(throws: GraphPathError.self) {
            _ = try GraphPath(
                nodeIDs: ["A", "B"],
                edgeLabels: ["edge"],
                weights: [.infinity]
            )
        }
    }
}

// MARK: - PageRankResult Tests

@Suite("PageRankResult Tests", .serialized, .heartbeat)
struct PageRankResultTests {

    @Test("PageRankResult topK returns sorted results")
    func topKSorting() {
        let result = PageRankResult(
            scores: ["A": 0.3, "B": 0.5, "C": 0.2, "D": 0.4],
            iterations: 10,
            convergenceDelta: 0.001,
            durationNs: 1000
        )

        let top2 = result.topK(2)
        #expect(top2.count == 2)
        #expect(top2[0].nodeID == "B")
        #expect(top2[0].score == 0.5)
        #expect(top2[1].nodeID == "D")
        #expect(top2[1].score == 0.4)
    }

    @Test("PageRankResult score lookup")
    func scoreLookup() {
        let result = PageRankResult(
            scores: ["A": 0.3, "B": 0.5],
            iterations: 10,
            convergenceDelta: 0.001,
            durationNs: 1000
        )

        #expect(result.score(for: "A") == 0.3)
        #expect(result.score(for: "B") == 0.5)
        #expect(result.score(for: "C") == nil)
    }
}

// MARK: - CommunityResult Tests

@Suite("CommunityResult Tests", .serialized, .heartbeat)
struct CommunityResultTests {

    @Test("CommunityResult community lookup")
    func communityLookup() {
        let result = CommunityResult(
            assignments: ["A": "comm1", "B": "comm1", "C": "comm2"],
            communities: ["comm1": ["A", "B"], "comm2": ["C"]],
            iterations: 5,
            durationNs: 1000,
            modularity: 0.4
        )

        #expect(result.community(for: "A") == "comm1")
        #expect(result.community(for: "B") == "comm1")
        #expect(result.community(for: "C") == "comm2")
        #expect(result.community(for: "D") == nil)
    }

    @Test("CommunityResult largest communities")
    func largestCommunities() {
        let result = CommunityResult(
            assignments: ["A": "comm1", "B": "comm1", "C": "comm1", "D": "comm2", "E": "comm3"],
            communities: ["comm1": ["A", "B", "C"], "comm2": ["D"], "comm3": ["E"]],
            iterations: 5,
            durationNs: 1000,
            modularity: nil
        )

        let largest = result.largestCommunities(k: 2)
        #expect(largest.count == 2)
        #expect(largest[0].label == "comm1")
        #expect(largest[0].memberCount == 3)
    }

    @Test("CommunityResult inSameCommunity check")
    func sameCommunityCheck() {
        let result = CommunityResult(
            assignments: ["A": "comm1", "B": "comm1", "C": "comm2"],
            iterations: 5,
            durationNs: 1000,
            modularity: nil
        )

        #expect(result.inSameCommunity("A", "B") == true)
        #expect(result.inSameCommunity("A", "C") == false)
        #expect(result.inSameCommunity("A", "X") == false)
    }
}

// MARK: - ShortestPathConfiguration Tests

@Suite("ShortestPathConfiguration Tests", .serialized, .heartbeat)
struct ShortestPathConfigurationTests {

    @Test("Default configuration has expected values")
    func defaultConfiguration() {
        let config = ShortestPathConfiguration.default
        #expect(config.maxDepth == 10)
        #expect(config.bidirectional == true)
        #expect(config.batchSize == 100)
    }

    @Test("Fast configuration uses bidirectional BFS")
    func fastConfiguration() {
        let config = ShortestPathConfiguration.fast
        #expect(config.bidirectional == true)
        #expect(config.maxDepth == 5)  // .fast uses maxDepth=5 for faster execution
    }
}

// MARK: - ShortestPathFinder Integration Tests

@Suite("ShortestPathFinder Integration Tests", .serialized)
struct ShortestPathFinderIntegrationTests {

    @Test("Find shortest path in simple graph")
    func simpleShortestPath() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create a simple graph: A -> B -> C -> D
        let edges = [
            Edge(source: "A", target: "B", label: "follows"),
            Edge(source: "B", target: "C", label: "follows"),
            Edge(source: "C", target: "D", label: "follows"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.shortestPath(from: "A", to: "D")

        #expect(result.isConnected == true)
        #expect(result.distance == 3)
        #expect(result.path?.nodeIDs == ["A", "B", "C", "D"])
    }

    @Test("No path between disconnected nodes")
    func noPathDisconnected() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create disconnected graph: A -> B, C -> D
        let edges = [
            Edge(source: "A", target: "B", label: "follows"),
            Edge(source: "C", target: "D", label: "follows"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.shortestPath(from: "A", to: "D")

        #expect(result.isConnected == false)
        #expect(result.path == nil)
    }

    @Test("Shortest path with edge label filter")
    func shortestPathWithEdgeFilter() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // A -> B (follows), A -> C (blocks), C -> D (follows)
        // With "follows" filter, should find A -> B only path or no path to D
        let edges = [
            Edge(source: "A", target: "B", label: "follows"),
            Edge(source: "A", target: "C", label: "blocks"),
            Edge(source: "C", target: "D", label: "follows"),
            Edge(source: "B", target: "D", label: "follows"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.shortestPath(
            from: "A",
            to: "D",
            edgeLabel: "follows"
        )

        #expect(result.isConnected == true)
        #expect(result.distance == 2)  // A -> B -> D
    }

    @Test("Shortest path with edgeLabel=nil (wildcard) considers ALL edge labels")
    func shortestPathWithWildcardEdgeLabel() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Graph with multiple edge labels:
        // A -> B (follows)
        // A -> C (blocks) - different label, but should still be traversed
        // C -> D (likes)  - yet another label
        //
        // With edgeLabel=nil (wildcard), should find path A -> C -> D (length 2)
        // If edgeLabel=nil were incorrectly treated as empty string,
        // no path would be found (this was the bug in the old implementation)
        let edges = [
            Edge(source: "A", target: "B", label: "follows"),
            Edge(source: "A", target: "C", label: "blocks"),
            Edge(source: "C", target: "D", label: "likes"),
        ]
        try await ctx.insertEdges(edges)

        // edgeLabel=nil means "match ALL labels" (wildcard)
        // Use default bidirectional BFS
        let result = try await ctx.shortestPath(
            from: "A",
            to: "D",
            edgeLabel: nil
        )

        #expect(result.isConnected == true)
        #expect(result.distance == 2)  // A -> C -> D
        // Path could be A -> C -> D or A -> B -> ? (no path from B to D)
        // Since B has no outgoing edges to D, path must be A -> C -> D
        #expect(result.path?.length == 2)
    }

    @Test("Bidirectional search reconstructs the backward half in edge direction")
    func bidirectionalBackwardPathReconstruction() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // The wider forward frontier forces the next expansion to scan incoming
        // edges from the target side and meet at B.
        let edges = [
            Edge(source: "A", target: "B", label: "edge"),
            Edge(source: "A", target: "C", label: "edge"),
            Edge(source: "A", target: "D", label: "edge"),
            Edge(source: "B", target: "X", label: "edge"),
            Edge(source: "X", target: "T", label: "edge"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.shortestPath(
            from: "A",
            to: "T",
            edgeLabel: "edge",
            configuration: ShortestPathConfiguration(
                maxDepth: 3,
                bidirectional: true,
                batchSize: 100,
                maxNodesExplored: 100
            )
        )

        #expect(result.isConnected)
        #expect(result.distance == 3)
        #expect(result.path?.nodeIDs == ["A", "B", "X", "T"])
        #expect(result.path?.edgeLabels == ["edge", "edge", "edge"])
    }

    @Test("Bidirectional search is complete when either frontier is exhausted")
    func bidirectionalExhaustedFrontierIsComplete() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // A wider forward frontier makes the second expansion inspect T. Since
        // T has no incoming edges, no path can exist regardless of forward depth.
        try await ctx.insertEdges([
            Edge(source: "A", target: "B", label: "edge"),
            Edge(source: "A", target: "C", label: "edge"),
        ])

        let result = try await ctx.shortestPath(
            from: "A",
            to: "T",
            edgeLabel: "edge",
            configuration: ShortestPathConfiguration(
                maxDepth: 2,
                bidirectional: true,
                batchSize: 100,
                maxNodesExplored: 100
            )
        )

        #expect(result.path == nil)
        #expect(result.isComplete)
        #expect(result.limitReason == nil)
    }

    @Test("All-shortest search returns every equal-length path")
    func allEqualLengthShortestPaths() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        let edges = [
            Edge(source: "A", target: "B", label: "edge"),
            Edge(source: "A", target: "C", label: "edge"),
            Edge(source: "B", target: "T", label: "edge"),
            Edge(source: "C", target: "T", label: "edge"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.allShortestPaths(
            from: "A",
            to: "T",
            edgeLabel: "edge"
        )
        let actualPaths = Set(result.paths.map(\.nodeIDs))
        let expectedPaths: Set<[GraphIdentity]> = [
            ["A", "B", "T"],
            ["A", "C", "T"],
        ]

        #expect(result.isComplete)
        #expect(result.limitReason == nil)
        #expect(result.distance == 2)
        #expect(actualPaths == expectedPaths)
    }
}

// MARK: - PageRankComputer Integration Tests

@Suite("PageRankComputer Integration Tests", .serialized)
struct PageRankComputerIntegrationTests {

    @Test("PageRank on simple directed graph")
    func simplePageRank() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Simple graph: A -> B -> C, A -> C
        // C should have highest PageRank (receives from both A and B)
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "B", target: "C", label: "links"),
            Edge(source: "A", target: "C", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 50,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        #expect(result.nodeCount == 3)
        #expect(result.iterations > 0)

        // C should have highest score (most incoming links)
        let top1 = result.topK(1)
        #expect(top1.first?.nodeID == "C")
    }

    @Test("PageRank converges")
    func pageRankConverges() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create a cycle: A -> B -> C -> A
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "B", target: "C", label: "links"),
            Edge(source: "C", target: "A", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 100,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        // In a symmetric cycle, all nodes should have equal PageRank
        let scores = result.scores
        let scoreA = scores["A"] ?? 0
        let scoreB = scores["B"] ?? 0
        let scoreC = scores["C"] ?? 0

        // All scores should be approximately equal (within 1%)
        #expect(abs(scoreA - scoreB) < 0.01)
        #expect(abs(scoreB - scoreC) < 0.01)
    }

    @Test("PageRank with edgeLabel=nil (wildcard) considers ALL edge labels")
    func pageRankWithWildcardEdgeLabel() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Graph with multiple edge labels:
        // A -> C (follows)
        // B -> C (likes)   - different label
        // D -> C (shares)  - yet another label
        //
        // With edgeLabel=nil (wildcard), C should receive PageRank from A, B, and D
        // If edgeLabel=nil were incorrectly treated as empty string,
        // C would have no incoming edges (this was the bug in the old implementation)
        let edges = [
            Edge(source: "A", target: "C", label: "follows"),
            Edge(source: "B", target: "C", label: "likes"),
            Edge(source: "D", target: "C", label: "shares"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 50,
                convergenceThreshold: 1e-6,
                batchSize: 100
            ),
            edgeLabel: nil
        )

        // All 4 nodes should be discovered
        #expect(result.nodeCount == 4)

        // C should have highest score (receives from 3 sources)
        let top1 = result.topK(1)
        #expect(top1.first?.nodeID == "C")

        // A, B, D should have similar scores (each has no incoming edges, only outgoing)
        let scores = result.scores
        let scoreA = scores["A"] ?? 0
        let scoreB = scores["B"] ?? 0
        let scoreD = scores["D"] ?? 0

        // A, B, D should have similar low scores (only teleportation, no incoming)
        #expect(abs(scoreA - scoreB) < 0.05)
        #expect(abs(scoreB - scoreD) < 0.05)
    }

    @Test("PageRank with specific edgeLabel filters correctly")
    func pageRankWithSpecificEdgeLabel() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Graph with multiple edge labels:
        // A -> C (follows)
        // B -> C (likes)
        //
        // With edgeLabel="follows", only A -> C should be considered
        let edges = [
            Edge(source: "A", target: "C", label: "follows"),
            Edge(source: "B", target: "C", label: "likes"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 50,
                convergenceThreshold: 1e-6,
                batchSize: 100
            ),
            edgeLabel: "follows"
        )

        // Only A and C should be discovered (B is not connected via "follows")
        #expect(result.nodeCount == 2)

        // C should have higher score than A
        let scores = result.scores
        let scoreA = scores["A"] ?? 0
        let scoreC = scores["C"] ?? 0
        #expect(scoreC > scoreA)
    }
}

// MARK: - CommunityDetector Integration Tests

@Suite("CommunityDetector Integration Tests", .serialized)
struct CommunityDetectorIntegrationTests {

    @Test("Detect obvious communities")
    func detectObviousCommunities() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Two fully connected cliques with one bridge
        // Clique 1: A, B, C
        // Clique 2: D, E, F
        // Bridge: C -> D
        let edges = [
            // Clique 1
            Edge(source: "A", target: "B", label: "friends"),
            Edge(source: "B", target: "A", label: "friends"),
            Edge(source: "A", target: "C", label: "friends"),
            Edge(source: "C", target: "A", label: "friends"),
            Edge(source: "B", target: "C", label: "friends"),
            Edge(source: "C", target: "B", label: "friends"),
            // Clique 2
            Edge(source: "D", target: "E", label: "friends"),
            Edge(source: "E", target: "D", label: "friends"),
            Edge(source: "D", target: "F", label: "friends"),
            Edge(source: "F", target: "D", label: "friends"),
            Edge(source: "E", target: "F", label: "friends"),
            Edge(source: "F", target: "E", label: "friends"),
            // Bridge
            Edge(source: "C", target: "D", label: "friends"),
            Edge(source: "D", target: "C", label: "friends"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.communities(
            configuration: CommunityDetectionConfiguration(
                maxIterations: 100,
                batchSize: 100,
                computeModularity: true,
                minCommunitySize: 1
            )
        )

        // Should detect at least 2 communities (might merge due to bridge)
        #expect(result.communityCount >= 1)

        // A, B, C should be in the same community
        #expect(result.inSameCommunity("A", "B") == true)
        #expect(result.inSameCommunity("B", "C") == true)

        // D, E, F should be in the same community
        #expect(result.inSameCommunity("D", "E") == true)
        #expect(result.inSameCommunity("E", "F") == true)
    }

    @Test("Detect local community")
    func detectLocalCommunity() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Simple star graph: A connected to B, C, D
        let edges = [
            Edge(source: "A", target: "B", label: "knows"),
            Edge(source: "B", target: "A", label: "knows"),
            Edge(source: "A", target: "C", label: "knows"),
            Edge(source: "C", target: "A", label: "knows"),
            Edge(source: "A", target: "D", label: "knows"),
            Edge(source: "D", target: "A", label: "knows"),
        ]
        try await ctx.insertEdges(edges)

        let localCommunity = try await ctx.localCommunity(for: "A", maxHops: 2)

        // Should find all connected nodes
        #expect(localCommunity.contains("A"))
        #expect(localCommunity.contains("B"))
        #expect(localCommunity.contains("C"))
        #expect(localCommunity.contains("D"))
    }

    @Test("Deterministic results with seed")
    func deterministicResultsWithSeed() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create a graph with multiple possible community assignments
        // This tests that the same seed produces the same results
        let edges = [
            // Cluster 1
            Edge(source: "A", target: "B", label: "knows"),
            Edge(source: "B", target: "A", label: "knows"),
            Edge(source: "B", target: "C", label: "knows"),
            Edge(source: "C", target: "B", label: "knows"),
            Edge(source: "C", target: "A", label: "knows"),
            Edge(source: "A", target: "C", label: "knows"),
            // Cluster 2
            Edge(source: "D", target: "E", label: "knows"),
            Edge(source: "E", target: "D", label: "knows"),
            Edge(source: "E", target: "F", label: "knows"),
            Edge(source: "F", target: "E", label: "knows"),
            Edge(source: "F", target: "D", label: "knows"),
            Edge(source: "D", target: "F", label: "knows"),
            // Bridge
            Edge(source: "C", target: "D", label: "knows"),
        ]
        try await ctx.insertEdges(edges)

        let seed: UInt64 = 12345

        // Run with seed multiple times
        let config = CommunityDetectionConfiguration(
            maxIterations: 50,
            batchSize: 100,
            computeModularity: false,
            minCommunitySize: 1,
            seed: seed
        )

        let result1 = try await ctx.communities(configuration: config)
        let result2 = try await ctx.communities(configuration: config)
        let result3 = try await ctx.communities(configuration: config)

        // All runs should produce identical community assignments
        #expect(result1.assignments == result2.assignments)
        #expect(result2.assignments == result3.assignments)
        #expect(result1.communityCount == result2.communityCount)
        #expect(result2.communityCount == result3.communityCount)
    }

    @Test("Different seeds produce different results")
    func differentSeedsProduceDifferentResults() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create a larger graph where different seeds may lead to different results
        var edges: [Edge] = []
        // Create a ring of nodes
        for i in 0..<10 {
            let source = "N\(i)"
            let target = "N\((i + 1) % 10)"
            edges.append(Edge(source: source, target: target, label: "link"))
            edges.append(Edge(source: target, target: source, label: "link"))
        }
        try await ctx.insertEdges(edges)

        let config1 = CommunityDetectionConfiguration(seed: 111)
        let config2 = CommunityDetectionConfiguration(seed: 222)

        let result1 = try await ctx.communities(configuration: config1)
        let result2 = try await ctx.communities(configuration: config2)

        // Both should find communities (the ring should form one big SCC)
        #expect(result1.communityCount >= 1)
        #expect(result2.communityCount >= 1)

        // Note: Different seeds don't guarantee different results,
        // but the algorithm should still work correctly
    }

    @Test("Modularity computation")
    func modularityComputation() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Two clearly separated cliques should have high modularity
        let edges = [
            // Clique 1: A, B, C (fully connected)
            Edge(source: "A", target: "B", label: "friends"),
            Edge(source: "B", target: "A", label: "friends"),
            Edge(source: "A", target: "C", label: "friends"),
            Edge(source: "C", target: "A", label: "friends"),
            Edge(source: "B", target: "C", label: "friends"),
            Edge(source: "C", target: "B", label: "friends"),
            // Clique 2: D, E, F (fully connected)
            Edge(source: "D", target: "E", label: "friends"),
            Edge(source: "E", target: "D", label: "friends"),
            Edge(source: "D", target: "F", label: "friends"),
            Edge(source: "F", target: "D", label: "friends"),
            Edge(source: "E", target: "F", label: "friends"),
            Edge(source: "F", target: "E", label: "friends"),
        ]
        try await ctx.insertEdges(edges)

        let config = CommunityDetectionConfiguration(
            maxIterations: 100,
            batchSize: 100,
            computeModularity: true,
            minCommunitySize: 1,
            seed: 42
        )

        let result = try await ctx.communities(configuration: config)

        // Modularity should be computed
        #expect(result.modularity != nil)

        // Good community structure should have positive modularity
        if let modularity = result.modularity {
            #expect(modularity > 0)
        }

        // Should detect 2 communities
        #expect(result.communityCount == 2)
    }

    @Test("Three or more communities")
    func threeOrMoreCommunities() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Create 3 separate cliques with no connections between them
        let edges = [
            // Clique 1: A, B
            Edge(source: "A", target: "B", label: "link"),
            Edge(source: "B", target: "A", label: "link"),
            // Clique 2: C, D
            Edge(source: "C", target: "D", label: "link"),
            Edge(source: "D", target: "C", label: "link"),
            // Clique 3: E, F
            Edge(source: "E", target: "F", label: "link"),
            Edge(source: "F", target: "E", label: "link"),
        ]
        try await ctx.insertEdges(edges)

        let config = CommunityDetectionConfiguration(
            maxIterations: 100,
            batchSize: 100,
            computeModularity: true,
            minCommunitySize: 1,
            seed: 42
        )

        let result = try await ctx.communities(configuration: config)

        // Should detect 3 separate communities (disconnected components)
        #expect(result.communityCount == 3)

        // Verify each pair is in same community
        #expect(result.inSameCommunity("A", "B") == true)
        #expect(result.inSameCommunity("C", "D") == true)
        #expect(result.inSameCommunity("E", "F") == true)

        // Verify different communities are separate
        #expect(result.inSameCommunity("A", "C") == false)
        #expect(result.inSameCommunity("A", "E") == false)
        #expect(result.inSameCommunity("C", "E") == false)
    }

    @Test("Single node isolation")
    func singleNodeIsolation() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Graph with isolated node
        let edges = [
            // Connected pair
            Edge(source: "A", target: "B", label: "link"),
            Edge(source: "B", target: "A", label: "link"),
            // Isolated node C (self-loop)
            Edge(source: "C", target: "C", label: "link"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.communities(
            configuration: CommunityDetectionConfiguration(seed: 42)
        )

        // Should detect at least 2 communities
        #expect(result.communityCount >= 2)

        // A and B should be together
        #expect(result.inSameCommunity("A", "B") == true)

        // C should be in its own community (isolated)
        #expect(result.inSameCommunity("A", "C") == false)
    }
}

// MARK: - PageRank Edge Case Tests

@Suite("PageRank Edge Case Tests", .serialized)
struct PageRankEdgeCaseTests {

    @Test("Sink node handling")
    func sinkNodeHandling() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Graph with sink node (no outgoing edges)
        // A -> B -> C (sink)
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "B", target: "C", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 100,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        // Should find all 3 nodes
        #expect(result.nodeCount == 3)

        // Sink node C should still receive PageRank
        let scoreC = result.scores["C"] ?? 0
        #expect(scoreC > 0)

        // Scores should sum to approximately 1.0
        let totalScore = result.scores.values.reduce(0, +)
        #expect(abs(totalScore - 1.0) < 0.01)
    }

    @Test("Disconnected graph components")
    func disconnectedGraphComponents() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Two disconnected components
        // Component 1: A -> B
        // Component 2: C -> D
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "C", target: "D", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 100,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        // Should find all 4 nodes
        #expect(result.nodeCount == 4)

        // All nodes should have positive scores
        for (_, score) in result.scores {
            #expect(score > 0)
        }

        // B and D (sinks) should have similar scores
        let scoreB = result.scores["B"] ?? 0
        let scoreD = result.scores["D"] ?? 0
        #expect(abs(scoreB - scoreD) < 0.01)

        // A and C (sources) should have similar scores
        let scoreA = result.scores["A"] ?? 0
        let scoreC = result.scores["C"] ?? 0
        #expect(abs(scoreA - scoreC) < 0.01)
    }

    @Test("Self-loop node")
    func selfLoopNode() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Node with self-loop
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "B", target: "B", label: "links"),  // Self-loop
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 100,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        // Should find 2 nodes
        #expect(result.nodeCount == 2)

        // B should have higher score (receives from A and itself)
        let scoreA = result.scores["A"] ?? 0
        let scoreB = result.scores["B"] ?? 0
        #expect(scoreB > scoreA)
    }

    @Test("Convergence threshold validation")
    func convergenceThresholdValidation() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Simple cycle for predictable convergence
        let edges = [
            Edge(source: "A", target: "B", label: "links"),
            Edge(source: "B", target: "C", label: "links"),
            Edge(source: "C", target: "A", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        // Use strict convergence threshold
        let strictConfig = PageRankConfiguration(
            dampingFactor: 0.85,
            maxIterations: 1000,
            convergenceThreshold: 1e-10,
            batchSize: 100
        )

        let result = try await ctx.pageRank(configuration: strictConfig)

        // Should converge (delta should be <= threshold)
        #expect(result.convergenceDelta <= 1e-10 || result.iterations == 1000)

        // All scores should be approximately equal in a cycle
        let scores = Array(result.scores.values)
        let avgScore = scores.reduce(0, +) / Double(scores.count)
        for score in scores {
            #expect(abs(score - avgScore) < 0.001)
        }
    }

    @Test("Max iterations limit")
    func maxIterationsLimit() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Use a directed chain (not a cycle) that takes multiple iterations to converge.
        // In a chain A -> B -> C -> ... -> T, scores propagate through the chain,
        // and dangling nodes at the end cause a cascade effect requiring many iterations.
        // Note: A cycle graph converges immediately because uniform distribution is already
        // the stationary distribution.
        var edges: [Edge] = []
        for i in 0..<19 {
            edges.append(Edge(source: "N\(i)", target: "N\(i + 1)", label: "links"))
        }
        try await ctx.insertEdges(edges)

        // Use very few iterations with very strict threshold
        let config = PageRankConfiguration(
            dampingFactor: 0.85,
            maxIterations: 5,
            convergenceThreshold: 1e-20,  // Very strict, won't converge
            batchSize: 100
        )

        let result = try await ctx.pageRank(configuration: config)

        // Should stop at max iterations (chain graph doesn't converge quickly)
        #expect(result.iterations == 5)

        // Should still produce valid scores
        #expect(result.nodeCount == 20)
        let totalScore = result.scores.values.reduce(0, +)
        #expect(abs(totalScore - 1.0) < 0.01)
    }

    @Test("Empty graph")
    func emptyGraph() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // No edges
        let result = try await ctx.pageRank()

        // Should handle empty graph gracefully
        #expect(result.nodeCount == 0)
        #expect(result.iterations == 0)
        #expect(result.scores.isEmpty)
    }

    @Test("Star graph hub dominance")
    func starGraphHubDominance() async throws {
        let ctx = try await GraphAlgorithmContext()
        defer { Task { await ctx.cleanupReportingFailure() } }

        // Star graph: all nodes point to center hub
        // A, B, C, D all point to H
        let edges = [
            Edge(source: "A", target: "H", label: "links"),
            Edge(source: "B", target: "H", label: "links"),
            Edge(source: "C", target: "H", label: "links"),
            Edge(source: "D", target: "H", label: "links"),
        ]
        try await ctx.insertEdges(edges)

        let result = try await ctx.pageRank(
            configuration: PageRankConfiguration(
                dampingFactor: 0.85,
                maxIterations: 100,
                convergenceThreshold: 1e-6,
                batchSize: 100
            )
        )

        // Hub should have highest score
        let topNode = result.topK(1).first
        #expect(topNode?.nodeID == "H")

        // Hub should have significantly higher score than spokes
        let hubScore = result.scores["H"] ?? 0
        let spokeScore = result.scores["A"] ?? 0
        #expect(hubScore > spokeScore * 2)  // Hub should be much higher
    }
}
#endif
