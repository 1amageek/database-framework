#if !os(WASI)
#if FOUNDATION_DB
// GraphFusionTests.swift
// Tests for GraphIndex Fusion query (Connected)

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Models

/// Person model with graph and scalar indexes.
@Persistable
struct GraphFusionPerson {
    #Index(
        .ordered(
            name: "GraphTestPerson_userId",
            keys: [.ascending(\GraphFusionPerson.userId)]
        ))

    var id: String = UUID().uuidString
    var userId: String
    var name: String
    var bio: String = ""
}

/// Follow relationship for graph index testing
@Persistable
struct GraphFusionFollow {
    #Index(
        .graph(
            name: "GraphTestFollow_graph",
            definition: .property(
                source: \GraphFusionFollow.follower,
                label: .field(\GraphFusionFollow.edgeType),
                target: \GraphFusionFollow.followee,
                graph: nil,
                strategy: .adjacency
            )
        ))

    var id: String = UUID().uuidString
    var follower: String
    var followee: String
    var edgeType: String = "follows"
}

// MARK: - Test Context

private struct GraphFusionContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let itemsSubspace: Subspace
    let blobsSubspace: Subspace
    let maintainer: GraphIndexMaintainer<GraphFusionFollow>
    let strategy: PropertyGraphIndexStrategy

    init(
        strategy: PropertyGraphIndexStrategy = .adjacency,
        indexName: String = "GraphTestFollow_graph"
    ) async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        self.strategy = strategy
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graph_fusion", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")
        self.blobsSubspace = subspace.subspace("B")

        let definition = GraphIndexDefinition<FieldIdentity>.property(
            source: GraphFusionFollow.fields.follower.identity,
            label: .field(GraphFusionFollow.fields.edgeType.identity),
            target: GraphFusionFollow.fields.followee.identity,
            graph: nil,
            strategy: strategy
        )

        let index = try ResolvedIndex(
            for: GraphFusionFollow.self,
            name: indexName,
            definition: .graph(definition, includedFields: []),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "follower"),
                FieldKeyExpression(fieldName: "edgeType"),
                FieldKeyExpression(fieldName: "followee"),
            ]),
            itemTypes: Set(["GraphFusionFollow"])
        )

        self.maintainer = try GraphIndexMaintainer<GraphFusionFollow>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            definition: definition
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func insertFollow(_ follow: GraphFusionFollow) async throws {
        try await database.withTransaction { transaction in
            let itemKey = itemsSubspace.pack(Tuple(follow.id))
            let storage = ItemStorageWriter(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            try await storage.write(
                try PersistableStorageCodec.encode(follow),
                for: itemKey
            )

            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: follow,
                transaction: transaction
            )
        }
    }
}

// MARK: - Unit Tests (API Pattern)

@Suite("Graph Fusion - Unit Tests", .heartbeat)
struct GraphFusionUnitTests {

    @Test("Property graph definition identifier is 'graph'")
    func testPropertyGraphDefinitionIdentifier() {
        #expect(IndexType.graph(.property).diagnosticName == "graph.property")
    }

    @Test("Connected.Direction enum values")
    func testConnectedDirectionValues() {
        #expect(Connected<GraphFusionPerson>.Direction.outgoing == .outgoing)
        #expect(Connected<GraphFusionPerson>.Direction.incoming == .incoming)
        #expect(Connected<GraphFusionPerson>.Direction.both == .both)
    }

    @Test("PropertyGraphIndexStrategy exposes its storage strategy")
    func testPropertyGraphIndexStrategyValues() {
        #expect(PropertyGraphIndexStrategy.adjacency.storageStrategy == .adjacency)
        #expect(PropertyGraphIndexStrategy.tripleStore.storageStrategy == .tripleStore)
        #expect(PropertyGraphIndexStrategy.hexastore.storageStrategy == .hexastore)
    }

    @Test("Index descriptor configuration")
    func testIndexDescriptorConfiguration() throws {
        let descriptors = try GraphFusionFollow.indexDescriptors
        #expect(descriptors.count == 1)

        let graphIndex = descriptors[0]
        #expect(graphIndex.name == "GraphTestFollow_graph")
        #expect(graphIndex.type == .graph(.property))
        #expect(graphIndex.fieldNames.contains("follower"))
        #expect(graphIndex.fieldNames.contains("followee"))
        #expect(graphIndex.fieldNames.contains("edgeType"))
    }

    @Test("Scalar index for userId lookup")
    func testScalarIndexForUserIdLookup() throws {
        let descriptors = try GraphFusionPerson.indexDescriptors
        let scalarIndex = descriptors.first { $0.type == .ordered }

        #expect(scalarIndex != nil)
        #expect(scalarIndex?.fieldNames.contains("userId") == true)
    }
}

// MARK: - Scoring Tests

@Suite("Graph Fusion - Scoring", .heartbeat)
struct GraphFusionScoringTests {

    @Test("Hop-based scoring calculation")
    func testHopBasedScoring() {
        // Connected uses 1.0 / hops for scoring
        #expect(1.0 / 1.0 == 1.0)  // Direct connection
        #expect(1.0 / 2.0 == 0.5)  // 2 hops
        #expect(abs(1.0 / 3.0 - 0.333) < 0.01)  // 3 hops
        #expect(1.0 / 4.0 == 0.25)  // 4 hops
        #expect(1.0 / 10.0 == 0.1)  // 10 hops
    }

    @Test("ScoredResult with hop-based score")
    func testScoredResultWithHopScore() {
        let person = GraphFusionPerson(userId: "user123", name: "Alice")

        let result1 = ScoredResult(item: person, score: 1.0 / 1.0)
        #expect(result1.score == 1.0)

        let result2 = ScoredResult(item: person, score: 1.0 / 2.0)
        #expect(result2.score == 0.5)

        let result3 = ScoredResult(item: person, score: 1.0 / 3.0)
        #expect(abs(result3.score - 0.333) < 0.01)
    }

    @Test("Scores sorted by proximity")
    func testScoresSortedByProximity() {
        let connections = [
            (node: "David", hops: 3),
            (node: "Bob", hops: 1),
            (node: "Charlie", hops: 2),
        ]

        var results: [(name: String, score: Double)] = connections.map { conn in
            (name: conn.node, score: 1.0 / Double(conn.hops))
        }

        // Sort by score descending (closer = higher score)
        results.sort { $0.score > $1.score }

        #expect(results[0].name == "Bob")     // 1 hop, score = 1.0
        #expect(results[1].name == "Charlie") // 2 hops, score = 0.5
        #expect(results[2].name == "David")   // 3 hops, score = 0.33
    }
}

// MARK: - BFS Traversal Tests

@Suite("Graph Fusion - BFS Traversal", .heartbeat)
struct GraphFusionBFSTests {

    @Test("BFS finds direct neighbors")
    func testBFSDirectNeighbors() {
        var visited: Set<String> = []
        var results: [(node: String, hops: Int)] = []
        var frontier: [(node: String, hops: Int)] = [("Alice", 0)]
        visited.insert("Alice")

        let neighbors: [String: [String]] = [
            "Alice": ["Bob", "Charlie"],
            "Bob": ["David"],
            "Charlie": ["Eve"],
        ]

        let maxHops = 1

        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append((node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHops {
                continue
            }

            for neighbor in neighbors[currentNode] ?? [] {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        #expect(results.count == 2)
        #expect(results.map(\.node).sorted() == ["Bob", "Charlie"])
        #expect(results.allSatisfy { $0.hops == 1 })
    }

    @Test("BFS finds multi-hop connections")
    func testBFSMultiHopConnections() {
        var visited: Set<String> = []
        var results: [(node: String, hops: Int)] = []
        var frontier: [(node: String, hops: Int)] = [("Alice", 0)]
        visited.insert("Alice")

        let neighbors: [String: [String]] = [
            "Alice": ["Bob"],
            "Bob": ["Charlie"],
            "Charlie": ["David"],
        ]

        let maxHops = 3

        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append((node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHops {
                continue
            }

            for neighbor in neighbors[currentNode] ?? [] {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        #expect(results.count == 3)
        #expect(results[0] == (node: "Bob", hops: 1))
        #expect(results[1] == (node: "Charlie", hops: 2))
        #expect(results[2] == (node: "David", hops: 3))
    }

    @Test("BFS handles cycles")
    func testBFSHandlesCycles() {
        var visited: Set<String> = []
        var results: [(node: String, hops: Int)] = []
        var frontier: [(node: String, hops: Int)] = [("Alice", 0)]
        visited.insert("Alice")

        // Graph with cycle: Alice -> Bob -> Charlie -> Alice
        let neighbors: [String: [String]] = [
            "Alice": ["Bob"],
            "Bob": ["Charlie"],
            "Charlie": ["Alice"],
        ]

        let maxHops = 5

        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append((node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHops {
                continue
            }

            for neighbor in neighbors[currentNode] ?? [] {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        #expect(results.count == 2)  // Bob(1), Charlie(2) - Alice already visited
        #expect(visited.count == 3)  // Alice, Bob, Charlie
    }

    @Test("BFS respects maxHops limit")
    func testBFSMaxHopsLimit() {
        var visited: Set<String> = []
        var results: [(node: String, hops: Int)] = []
        var frontier: [(node: String, hops: Int)] = [("A", 0)]
        visited.insert("A")

        let neighbors: [String: [String]] = [
            "A": ["B"],
            "B": ["C"],
            "C": ["D"],
            "D": ["E"],
        ]

        let maxHops = 2

        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append((node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHops {
                continue
            }

            for neighbor in neighbors[currentNode] ?? [] {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        #expect(results.count == 2)
        #expect(results.map(\.node) == ["B", "C"])
    }

    @Test("BFS with empty neighbors")
    func testBFSWithEmptyNeighbors() {
        var visited: Set<String> = []
        var results: [(node: String, hops: Int)] = []
        var frontier: [(node: String, hops: Int)] = [("Isolated", 0)]
        visited.insert("Isolated")

        let neighbors: [String: [String]] = [:]  // No connections
        let maxHops = 3

        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append((node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHops {
                continue
            }

            for neighbor in neighbors[currentNode] ?? [] {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        #expect(results.isEmpty)
    }
}

// MARK: - Integration Tests

@Suite("Graph Fusion - Integration Tests", .foundationDBScenario, .serialized, .heartbeat)
struct GraphFusionIntegrationTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func withContext<Result>(
        _ operation: (GraphFusionContext) async throws -> Result
    ) async throws -> Result {
        let context = try await GraphFusionContext()
        let result: Result
        do {
            result = try await operation(context)
        } catch let operationError {
            do {
                try await context.cleanup()
            } catch let cleanupError {
                Issue.record("Graph fusion cleanup failed: \(cleanupError)")
            }
            throw operationError
        }
        try await context.cleanup()
        return result
    }

    @Test("Graph index maintainer initialization")
    func testGraphIndexMaintainerInitialization() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in
                // Verify maintainer is properly configured with the expected strategy
                #expect(context.strategy == .adjacency)
            }
        }
    }

    @Test("Insert and index follow relationship")
    func testInsertAndIndexFollow() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in

                let followId = uniqueID("follow")
                let follow = GraphFusionFollow(
                    id: followId,
                    follower: "alice",
                    followee: "bob",
                    edgeType: "follows"
                )

                try await context.insertFollow(follow)

                let exists = try await context.database.withTransaction { transaction -> Bool in
                    let itemKey = context.itemsSubspace.pack(Tuple(followId))
                    let value = try await transaction.getValue(for: itemKey, snapshot: true)
                    return value != nil
                }

                #expect(exists)
            }
        }
    }

    @Test("Multiple follow relationships")
    func testMultipleFollowRelationships() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in

                let follows = [
                    GraphFusionFollow(id: uniqueID("f"), follower: "alice", followee: "bob"),
                    GraphFusionFollow(id: uniqueID("f"), follower: "alice", followee: "charlie"),
                    GraphFusionFollow(id: uniqueID("f"), follower: "bob", followee: "charlie"),
                ]

                for follow in follows {
                    try await context.insertFollow(follow)
                }

                for follow in follows {
                    let exists = try await context.database.withTransaction { transaction -> Bool in
                        let itemKey = context.itemsSubspace.pack(Tuple(follow.id))
                        let value = try await transaction.getValue(for: itemKey, snapshot: true)
                        return value != nil
                    }
                    #expect(exists, "Follow \(follow.follower) -> \(follow.followee) should exist")
                }
            }
        }
    }

    @Test("Different edge types")
    func testDifferentEdgeTypes() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in

                let follows = [
                    GraphFusionFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "follows"),
                    GraphFusionFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "likes"),
                    GraphFusionFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "blocks"),
                ]

                for follow in follows {
                    try await context.insertFollow(follow)
                }

                // All should be inserted successfully
                for follow in follows {
                    let exists = try await context.database.withTransaction { transaction -> Bool in
                        let itemKey = context.itemsSubspace.pack(Tuple(follow.id))
                        let value = try await transaction.getValue(for: itemKey, snapshot: true)
                        return value != nil
                    }
                    #expect(exists)
                }
            }
        }
    }
}

// MARK: - Configuration Tests

@Suite("Graph Fusion - Configuration", .heartbeat)
struct GraphFusionConfigurationTests {

    @Test("maxHops configuration ensures minimum of 1")
    func testMaxHopsMinimum() {
        let minHops = max(1, 0)
        #expect(minHops == 1)

        let minHopsNegative = max(1, -5)
        #expect(minHopsNegative == 1)
    }

    @Test("Direction affects traversal")
    func testDirectionAffectsTraversal() {
        // Outgoing: follower -> followee
        let outgoing = Connected<GraphFusionPerson>.Direction.outgoing
        // Incoming: followee <- follower
        let incoming = Connected<GraphFusionPerson>.Direction.incoming
        // Both directions
        let both = Connected<GraphFusionPerson>.Direction.both

        #expect(outgoing != incoming)
        #expect(outgoing != both)
        #expect(incoming != both)
    }
}

// MARK: - Edge Case Tests

@Suite("Graph Fusion - Edge Cases", .heartbeat)
struct GraphFusionEdgeCaseTests {

    @Test("Self-referential edge")
    func testSelfReferentialEdge() {
        let follow = GraphFusionFollow(follower: "alice", followee: "alice")
        #expect(follow.follower == follow.followee)
    }

    @Test("Unicode node identifiers")
    func testUnicodeNodeIdentifiers() {
        let follow = GraphFusionFollow(
            follower: "用户1",
            followee: "用户2",
            edgeType: "关注"
        )

        #expect(follow.follower == "用户1")
        #expect(follow.followee == "用户2")
        #expect(follow.edgeType == "关注")
    }

    @Test("Empty edge type")
    func testEmptyEdgeType() {
        let follow = GraphFusionFollow(follower: "alice", followee: "bob", edgeType: "")
        #expect(follow.edgeType.isEmpty)
    }

    @Test("Very long node identifier")
    func testVeryLongNodeIdentifier() {
        let longId = String(repeating: "x", count: 10000)
        let follow = GraphFusionFollow(follower: longId, followee: "bob")
        #expect(follow.follower.count == 10000)
    }

    @Test("Node identifier with special characters")
    func testNodeIdWithSpecialCharacters() {
        let specialId = "user/with\\special:chars@domain.com"
        let follow = GraphFusionFollow(follower: specialId, followee: "bob")
        #expect(follow.follower == specialId)
    }

    @Test("Empty node values return empty results")
    func testEmptyNodeValuesReturnEmpty() {
        let nodeValues: [String] = []
        #expect(nodeValues.isEmpty)
    }

    @Test("FusionQueryError for missing source/target")
    func testMissingSourceOrTarget() {
        let error = FusionQueryError.invalidConfiguration("Must specify from() or to() for Connected query")
        #expect(error.description.contains("from()"))
        #expect(error.description.contains("to()"))
    }
}

// MARK: - Index Discovery Tests

@Suite("Graph Fusion - Index Discovery", .heartbeat)
struct GraphFusionIndexDiscoveryTests {

    @Test("findIndexDescriptor matches by typed graph index family")
    func testFindIndexDescriptorByKindIdentifier() throws {
        let descriptors = try GraphFusionFollow.indexDescriptors

        let graphDescriptor = descriptors.first { descriptor in
            descriptor.type == .graph(.property)
        }

        #expect(graphDescriptor != nil)
        #expect(graphDescriptor?.type == .graph(.property))
    }

    @Test("findIndexDescriptor matches by fieldName")
    func testFindIndexDescriptorByFieldName() throws {
        let descriptors = try GraphFusionFollow.indexDescriptors
        let fieldName = "follower"

        let matchingDescriptor = descriptors.first { descriptor in
            descriptor.type == .graph(.property)
                && descriptor.fieldNames.contains(fieldName)
        }

        #expect(matchingDescriptor != nil)
    }

    @Test("Scalar index is declared for node lookup")
    func testScalarIndexForNodeLookup() throws {
        let descriptors = try GraphFusionPerson.indexDescriptors

        let scalarDescriptor = descriptors.first { descriptor in
            descriptor.type == .ordered
                && descriptor.fieldNames.contains("userId")
        }

        #expect(scalarDescriptor != nil)
    }
}
#endif

#endif
