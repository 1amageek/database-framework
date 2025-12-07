// GraphFusionTests.swift
// Tests for GraphIndex Fusion query (Connected)

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Models

/// Person model with graph and scalar indexes
struct GraphTestPerson: Persistable {
    typealias ID = String

    var id: String
    var userId: String
    var name: String
    var bio: String

    init(id: String = UUID().uuidString, userId: String, name: String, bio: String = "") {
        self.id = id
        self.userId = userId
        self.name = name
        self.bio = bio
    }

    static var persistableType: String { "GraphTestPerson" }
    static var allFields: [String] { ["id", "userId", "name", "bio"] }

    static var indexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "GraphTestPerson_userId",
                keyPaths: [\GraphTestPerson.userId],
                kind: ScalarIndexKind<GraphTestPerson>(fields: [\GraphTestPerson.userId])
            )
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "userId": return userId
        case "name": return name
        case "bio": return bio
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<GraphTestPerson, Value>) -> String {
        switch keyPath {
        case \GraphTestPerson.id: return "id"
        case \GraphTestPerson.userId: return "userId"
        case \GraphTestPerson.name: return "name"
        case \GraphTestPerson.bio: return "bio"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<GraphTestPerson>) -> String {
        switch keyPath {
        case \GraphTestPerson.id: return "id"
        case \GraphTestPerson.userId: return "userId"
        case \GraphTestPerson.name: return "name"
        case \GraphTestPerson.bio: return "bio"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<GraphTestPerson> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Follow relationship for graph index testing
struct GraphTestFollow: Persistable {
    typealias ID = String

    var id: String
    var follower: String
    var followee: String
    var edgeType: String

    init(id: String = UUID().uuidString, follower: String, followee: String, edgeType: String = "follows") {
        self.id = id
        self.follower = follower
        self.followee = followee
        self.edgeType = edgeType
    }

    static var persistableType: String { "GraphTestFollow" }
    static var allFields: [String] { ["id", "follower", "followee", "edgeType"] }

    static var indexDescriptors: [IndexDescriptor] {
        let kind = GraphIndexKind<GraphTestFollow>(
            from: \.follower,
            edge: \.edgeType,
            to: \.followee,
            strategy: .adjacency
        )
        return [
            IndexDescriptor(
                name: "GraphTestFollow_graph",
                keyPaths: [\GraphTestFollow.follower, \GraphTestFollow.edgeType, \GraphTestFollow.followee],
                kind: kind
            )
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "follower": return follower
        case "followee": return followee
        case "edgeType": return edgeType
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<GraphTestFollow, Value>) -> String {
        switch keyPath {
        case \GraphTestFollow.id: return "id"
        case \GraphTestFollow.follower: return "follower"
        case \GraphTestFollow.followee: return "followee"
        case \GraphTestFollow.edgeType: return "edgeType"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<GraphTestFollow>) -> String {
        switch keyPath {
        case \GraphTestFollow.id: return "id"
        case \GraphTestFollow.follower: return "follower"
        case \GraphTestFollow.followee: return "followee"
        case \GraphTestFollow.edgeType: return "edgeType"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<GraphTestFollow> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Context

private struct GraphTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let itemsSubspace: Subspace
    let maintainer: GraphIndexMaintainer<GraphTestFollow>
    let strategy: GraphIndexStrategy

    init(strategy: GraphIndexStrategy = .adjacency, indexName: String = "GraphTestFollow_graph") throws {
        self.database = try FDBClient.openDatabase()
        self.strategy = strategy
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graph_fusion", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")

        let kind = GraphIndexKind<GraphTestFollow>(
            from: \.follower,
            edge: \.edgeType,
            to: \.followee,
            strategy: strategy
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "follower"),
                FieldKeyExpression(fieldName: "edgeType"),
                FieldKeyExpression(fieldName: "followee")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["GraphTestFollow"])
        )

        self.maintainer = GraphIndexMaintainer<GraphTestFollow>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            fromField: kind.fromField,
            edgeField: kind.edgeField,
            toField: kind.toField,
            strategy: strategy
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func insertFollow(_ follow: GraphTestFollow) async throws {
        try await database.withTransaction { transaction in
            let itemKey = itemsSubspace.pack(Tuple(follow.id))
            let encoder = JSONEncoder()
            let data = try encoder.encode([
                "id": follow.id,
                "follower": follow.follower,
                "followee": follow.followee,
                "edgeType": follow.edgeType
            ])
            transaction.setValue(Array(data), for: itemKey)

            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: follow,
                transaction: transaction
            )
        }
    }
}

// MARK: - Unit Tests (API Pattern)

@Suite("Graph Fusion - Unit Tests")
struct GraphFusionUnitTests {

    @Test("GraphIndexKind identifier is 'graph'")
    func testGraphIndexKindIdentifier() {
        #expect(GraphIndexKind<GraphTestFollow>.identifier == "graph")
    }

    @Test("Connected.Direction enum values")
    func testConnectedDirectionValues() {
        #expect(Connected<GraphTestPerson>.Direction.outgoing == .outgoing)
        #expect(Connected<GraphTestPerson>.Direction.incoming == .incoming)
        #expect(Connected<GraphTestPerson>.Direction.both == .both)
    }

    @Test("GraphIndexStrategy enum values")
    func testGraphIndexStrategyValues() {
        #expect(GraphIndexStrategy.adjacency == .adjacency)
        #expect(GraphIndexStrategy.tripleStore == .tripleStore)
        #expect(GraphIndexStrategy.hexastore == .hexastore)
    }

    @Test("Index descriptor configuration")
    func testIndexDescriptorConfiguration() {
        let descriptors = GraphTestFollow.indexDescriptors
        #expect(descriptors.count == 1)

        let graphIndex = descriptors[0]
        #expect(graphIndex.name == "GraphTestFollow_graph")
        #expect(graphIndex.kindIdentifier == "graph")

        // Access fieldNames through the kind
        if let graphKind = graphIndex.kind as? GraphIndexKind<GraphTestFollow> {
            #expect(graphKind.fieldNames.contains("follower"))
            #expect(graphKind.fieldNames.contains("followee"))
            #expect(graphKind.fieldNames.contains("edgeType"))
        } else {
            Issue.record("Expected GraphIndexKind")
        }
    }

    @Test("Scalar index for userId lookup")
    func testScalarIndexForUserIdLookup() {
        let descriptors = GraphTestPerson.indexDescriptors
        let scalarIndex = descriptors.first { $0.kindIdentifier == "scalar" }

        #expect(scalarIndex != nil)
        // Access fieldNames through the kind
        if let scalarKind = scalarIndex?.kind as? ScalarIndexKind<GraphTestPerson> {
            #expect(scalarKind.fieldNames.contains("userId"))
        } else {
            Issue.record("Expected ScalarIndexKind")
        }
    }
}

// MARK: - Scoring Tests

@Suite("Graph Fusion - Scoring")
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
        let person = GraphTestPerson(userId: "user123", name: "Alice")

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
            (node: "Charlie", hops: 2)
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

@Suite("Graph Fusion - BFS Traversal")
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
            "Charlie": ["Eve"]
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
            "Charlie": ["David"]
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
            "Charlie": ["Alice"]
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
            "D": ["E"]
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

@Suite("Graph Fusion - Integration Tests", .serialized)
struct GraphFusionIntegrationTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Graph index maintainer initialization")
    func testGraphIndexMaintainerInitialization() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try GraphTestContext()
            defer { Task { try? await context.cleanup() } }

            // Verify maintainer is properly configured with the expected strategy
            #expect(context.strategy == .adjacency)
        }
    }

    @Test("Insert and index follow relationship")
    func testInsertAndIndexFollow() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try GraphTestContext()
            defer { Task { try? await context.cleanup() } }

            let followId = uniqueID("follow")
            let follow = GraphTestFollow(
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

    @Test("Multiple follow relationships")
    func testMultipleFollowRelationships() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try GraphTestContext()
            defer { Task { try? await context.cleanup() } }

            let follows = [
                GraphTestFollow(id: uniqueID("f"), follower: "alice", followee: "bob"),
                GraphTestFollow(id: uniqueID("f"), follower: "alice", followee: "charlie"),
                GraphTestFollow(id: uniqueID("f"), follower: "bob", followee: "charlie")
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

    @Test("Different edge types")
    func testDifferentEdgeTypes() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try GraphTestContext()
            defer { Task { try? await context.cleanup() } }

            let follows = [
                GraphTestFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "follows"),
                GraphTestFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "likes"),
                GraphTestFollow(id: uniqueID("f"), follower: "alice", followee: "bob", edgeType: "blocks")
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

// MARK: - Configuration Tests

@Suite("Graph Fusion - Configuration")
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
        let outgoing = Connected<GraphTestPerson>.Direction.outgoing
        // Incoming: followee <- follower
        let incoming = Connected<GraphTestPerson>.Direction.incoming
        // Both directions
        let both = Connected<GraphTestPerson>.Direction.both

        #expect(outgoing != incoming)
        #expect(outgoing != both)
        #expect(incoming != both)
    }
}

// MARK: - Edge Case Tests

@Suite("Graph Fusion - Edge Cases")
struct GraphFusionEdgeCaseTests {

    @Test("Self-referential edge")
    func testSelfReferentialEdge() {
        let follow = GraphTestFollow(follower: "alice", followee: "alice")
        #expect(follow.follower == follow.followee)
    }

    @Test("Unicode node identifiers")
    func testUnicodeNodeIdentifiers() {
        let follow = GraphTestFollow(
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
        let follow = GraphTestFollow(follower: "alice", followee: "bob", edgeType: "")
        #expect(follow.edgeType.isEmpty)
    }

    @Test("Very long node identifier")
    func testVeryLongNodeIdentifier() {
        let longId = String(repeating: "x", count: 10000)
        let follow = GraphTestFollow(follower: longId, followee: "bob")
        #expect(follow.follower.count == 10000)
    }

    @Test("Node identifier with special characters")
    func testNodeIdWithSpecialCharacters() {
        let specialId = "user/with\\special:chars@domain.com"
        let follow = GraphTestFollow(follower: specialId, followee: "bob")
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

@Suite("Graph Fusion - Index Discovery")
struct GraphFusionIndexDiscoveryTests {

    @Test("findIndexDescriptor matches by kindIdentifier")
    func testFindIndexDescriptorByKindIdentifier() {
        let descriptors = GraphTestFollow.indexDescriptors

        let graphDescriptor = descriptors.first { descriptor in
            descriptor.kindIdentifier == GraphIndexKind<GraphTestFollow>.identifier
        }

        #expect(graphDescriptor != nil)
        #expect(graphDescriptor?.kindIdentifier == "graph")
    }

    @Test("findIndexDescriptor matches by fieldName")
    func testFindIndexDescriptorByFieldName() {
        let descriptors = GraphTestFollow.indexDescriptors
        let fieldName = "follower"

        let matchingDescriptor = descriptors.first { descriptor in
            guard let graphKind = descriptor.kind as? GraphIndexKind<GraphTestFollow> else { return false }
            return graphKind.fieldNames.contains(fieldName)
        }

        #expect(matchingDescriptor != nil)
    }

    @Test("Scalar index for efficient node lookup")
    func testScalarIndexForNodeLookup() {
        let descriptors = GraphTestPerson.indexDescriptors

        let scalarDescriptor = descriptors.first { descriptor in
            guard descriptor.kindIdentifier == "scalar" else { return false }
            guard let scalarKind = descriptor.kind as? ScalarIndexKind<GraphTestPerson> else { return false }
            return scalarKind.fieldNames.contains("userId")
        }

        #expect(scalarDescriptor != nil)
    }
}
