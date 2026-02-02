// NamedGraphKeyTests.swift
// Key generation tests for Named Graph (Quad) support
//
// Layer 2: GraphIndexMaintainer key structure verification (FDB required)
// Verifies that graph field is appended at the END of every index key tuple.

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model (Quad)

struct TestQuad: Persistable {
    typealias ID = String

    var id: String
    var subject: String
    var predicate: String
    var object: String
    var graph: String

    init(
        id: String = UUID().uuidString,
        subject: String,
        predicate: String,
        object: String,
        graph: String
    ) {
        self.id = id
        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.graph = graph
    }

    static var persistableType: String { "TestQuad" }
    static var allFields: [String] { ["id", "subject", "predicate", "object", "graph"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "subject": return subject
        case "predicate": return predicate
        case "object": return object
        case "graph": return graph
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestQuad, Value>) -> String {
        switch keyPath {
        case \TestQuad.id: return "id"
        case \TestQuad.subject: return "subject"
        case \TestQuad.predicate: return "predicate"
        case \TestQuad.object: return "object"
        case \TestQuad.graph: return "graph"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestQuad>) -> String {
        switch keyPath {
        case \TestQuad.id: return "id"
        case \TestQuad.subject: return "subject"
        case \TestQuad.predicate: return "predicate"
        case \TestQuad.object: return "object"
        case \TestQuad.graph: return "graph"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestQuad> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct QuadTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: GraphIndexMaintainer<TestQuad>
    let strategy: GraphIndexStrategy

    init(
        strategy: GraphIndexStrategy,
        graphField: String? = "graph",
        indexName: String = "TestQuad_graph"
    ) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "namedgraph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.strategy = strategy

        let index = Index(
            name: indexName,
            kind: GraphIndexKind<TestQuad>(
                fromField: "subject",
                edgeField: "predicate",
                toField: "object",
                graphField: graphField,
                strategy: strategy
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["TestQuad"])
        )

        self.maintainer = GraphIndexMaintainer<TestQuad>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            fromField: "subject",
            edgeField: "predicate",
            toField: "object",
            graphField: graphField,
            strategy: strategy
        )
    }

    func insert(_ quad: TestQuad) async throws {
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as TestQuad?,
                newItem: quad,
                transaction: transaction
            )
        }
    }

    func delete(_ quad: TestQuad) async throws {
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: quad,
                newItem: nil,
                transaction: transaction
            )
        }
    }

    /// Scan all keys in a strategy subspace and unpack tuples
    func scanKeysInSubspace(key: Int64) async throws -> [[any TupleElement]] {
        let targetSubspace = indexSubspace.subspace(key)
        return try await database.withTransaction { transaction -> [[any TupleElement]] in
            let (begin, end) = targetSubspace.range()
            var result: [[any TupleElement]] = []
            for try await (key, _) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                let unpacked = try targetSubspace.unpack(key)
                let elements = try Tuple.unpack(from: unpacked.pack())
                result.append(elements)
            }
            return result
        }
    }

    /// Count total entries across given subspace keys
    func countEntries(keys: [Int64]) async throws -> Int {
        var total = 0
        for key in keys {
            let targetSubspace = indexSubspace.subspace(key)
            try await database.withTransaction { transaction in
                let (begin, end) = targetSubspace.range()
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    total += 1
                }
            }
        }
        return total
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

// MARK: - TripleStore Strategy Key Tests

@Suite("NamedGraph TripleStore Key Tests", .tags(.fdb), .serialized)
struct NamedGraphTripleStoreKeyTests {

    @Test("TripleStore with graph produces 3 entries")
    func testTripleStoreKeyCountWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let count = try await ctx.countEntries(keys: [2, 3, 4])
        #expect(count == 3)

        try await ctx.cleanup()
    }

    @Test("SPO key has graph at end: [from, edge, to, graph]")
    func testTripleStoreSPOKeyHasGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let spoKeys = try await ctx.scanKeysInSubspace(key: 2)
        #expect(spoKeys.count == 1)
        let tuple = spoKeys[0]
        #expect(tuple.count == 4)
        #expect(tuple[0] as? String == "Alice")
        #expect(tuple[1] as? String == "knows")
        #expect(tuple[2] as? String == "Bob")
        #expect(tuple[3] as? String == "g1")

        try await ctx.cleanup()
    }

    @Test("POS key has graph at end: [edge, to, from, graph]")
    func testTripleStorePOSKeyHasGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let posKeys = try await ctx.scanKeysInSubspace(key: 3)
        #expect(posKeys.count == 1)
        let tuple = posKeys[0]
        #expect(tuple.count == 4)
        #expect(tuple[0] as? String == "knows")
        #expect(tuple[1] as? String == "Bob")
        #expect(tuple[2] as? String == "Alice")
        #expect(tuple[3] as? String == "g1")

        try await ctx.cleanup()
    }

    @Test("OSP key has graph at end: [to, from, edge, graph]")
    func testTripleStoreOSPKeyHasGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let ospKeys = try await ctx.scanKeysInSubspace(key: 4)
        #expect(ospKeys.count == 1)
        let tuple = ospKeys[0]
        #expect(tuple.count == 4)
        #expect(tuple[0] as? String == "Bob")
        #expect(tuple[1] as? String == "Alice")
        #expect(tuple[2] as? String == "knows")
        #expect(tuple[3] as? String == "g1")

        try await ctx.cleanup()
    }

    @Test("Delete removes all 3 entries")
    func testTripleStoreDeleteWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let countBefore = try await ctx.countEntries(keys: [2, 3, 4])
        #expect(countBefore == 3)

        try await ctx.delete(quad)

        let countAfter = try await ctx.countEntries(keys: [2, 3, 4])
        #expect(countAfter == 0)

        try await ctx.cleanup()
    }

    @Test("Same triple in different graphs produces 6 entries")
    func testTripleStoreSameTripleDifferentGraphs() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore)

        let q1 = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        let q2 = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g2")
        try await ctx.insert(q1)
        try await ctx.insert(q2)

        let count = try await ctx.countEntries(keys: [2, 3, 4])
        #expect(count == 6)

        // SPO subspace should have 2 entries (one per graph)
        let spoKeys = try await ctx.scanKeysInSubspace(key: 2)
        #expect(spoKeys.count == 2)
        let graphs = Set(spoKeys.compactMap { $0[3] as? String })
        #expect(graphs == Set(["g1", "g2"]))

        try await ctx.cleanup()
    }
}

// MARK: - Hexastore Strategy Key Tests

@Suite("NamedGraph Hexastore Key Tests", .tags(.fdb), .serialized)
struct NamedGraphHexastoreKeyTests {

    @Test("Hexastore with graph produces 6 entries")
    func testHexastoreKeyCountWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .hexastore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let count = try await ctx.countEntries(keys: [2, 3, 4, 5, 6, 7])
        #expect(count == 6)

        try await ctx.cleanup()
    }

    @Test("All 6 permutations have graph at end")
    func testHexastoreAllKeysHaveGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .hexastore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        // SPO=2, POS=3, OSP=4, SOP=5, PSO=6, OPS=7
        for key in [Int64(2), Int64(3), Int64(4), Int64(5), Int64(6), Int64(7)] {
            let keys = try await ctx.scanKeysInSubspace(key: key)
            #expect(keys.count == 1, "Subspace \(key) should have 1 entry")
            let tuple = keys[0]
            #expect(tuple.count == 4, "Subspace \(key) tuple should have 4 elements")
            #expect(tuple[3] as? String == "g1", "Subspace \(key) last element should be graph")
        }

        try await ctx.cleanup()
    }

    @Test("Delete removes all 6 entries")
    func testHexastoreDeleteWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .hexastore)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)
        try await ctx.delete(quad)

        let count = try await ctx.countEntries(keys: [2, 3, 4, 5, 6, 7])
        #expect(count == 0)

        try await ctx.cleanup()
    }

    @Test("Same triple in different graphs produces 12 entries")
    func testHexastoreSameTripleDifferentGraphs() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .hexastore)

        let q1 = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        let q2 = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g2")
        try await ctx.insert(q1)
        try await ctx.insert(q2)

        let count = try await ctx.countEntries(keys: [2, 3, 4, 5, 6, 7])
        #expect(count == 12)

        try await ctx.cleanup()
    }
}

// MARK: - Adjacency Strategy Key Tests

@Suite("NamedGraph Adjacency Key Tests", .tags(.fdb), .serialized)
struct NamedGraphAdjacencyKeyTests {

    @Test("Adjacency with graph produces 2 entries")
    func testAdjacencyKeyCountWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .adjacency)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let count = try await ctx.countEntries(keys: [0, 1])
        #expect(count == 2)

        try await ctx.cleanup()
    }

    @Test("Out key has graph at end: [edge, from, to, graph]")
    func testAdjacencyOutKeyHasGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .adjacency)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let outKeys = try await ctx.scanKeysInSubspace(key: 0)
        #expect(outKeys.count == 1)
        let tuple = outKeys[0]
        #expect(tuple.count == 4)
        #expect(tuple[0] as? String == "knows")
        #expect(tuple[1] as? String == "Alice")
        #expect(tuple[2] as? String == "Bob")
        #expect(tuple[3] as? String == "g1")

        try await ctx.cleanup()
    }

    @Test("In key has graph at end: [edge, to, from, graph]")
    func testAdjacencyInKeyHasGraphAtEnd() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .adjacency)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        let inKeys = try await ctx.scanKeysInSubspace(key: 1)
        #expect(inKeys.count == 1)
        let tuple = inKeys[0]
        #expect(tuple.count == 4)
        #expect(tuple[0] as? String == "knows")
        #expect(tuple[1] as? String == "Bob")
        #expect(tuple[2] as? String == "Alice")
        #expect(tuple[3] as? String == "g1")

        try await ctx.cleanup()
    }

    @Test("Delete removes all 2 entries")
    func testAdjacencyDeleteWithGraph() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .adjacency)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)
        try await ctx.delete(quad)

        let count = try await ctx.countEntries(keys: [0, 1])
        #expect(count == 0)

        try await ctx.cleanup()
    }
}

// MARK: - Backward Compatibility Tests

@Suite("NamedGraph Backward Compatibility Key Tests", .tags(.fdb), .serialized)
struct NamedGraphBackwardCompatibilityKeyTests {

    @Test("TripleStore without graph produces 3-element keys")
    func testTripleStoreWithoutGraphProduces3ElementKeys() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .tripleStore, graphField: nil)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        for key in [Int64(2), Int64(3), Int64(4)] {
            let keys = try await ctx.scanKeysInSubspace(key: key)
            #expect(keys.count == 1)
            #expect(keys[0].count == 3, "Without graphField, tuple should have 3 elements, not 4")
        }

        try await ctx.cleanup()
    }

    @Test("Hexastore without graph produces 3-element keys")
    func testHexastoreWithoutGraphProduces3ElementKeys() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .hexastore, graphField: nil)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        for key in [Int64(2), Int64(3), Int64(4), Int64(5), Int64(6), Int64(7)] {
            let keys = try await ctx.scanKeysInSubspace(key: key)
            #expect(keys.count == 1)
            #expect(keys[0].count == 3, "Without graphField, tuple should have 3 elements, not 4")
        }

        try await ctx.cleanup()
    }

    @Test("Adjacency without graph produces 3-element keys")
    func testAdjacencyWithoutGraphProduces3ElementKeys() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try QuadTestContext(strategy: .adjacency, graphField: nil)

        let quad = TestQuad(subject: "Alice", predicate: "knows", object: "Bob", graph: "g1")
        try await ctx.insert(quad)

        for key in [Int64(0), Int64(1)] {
            let keys = try await ctx.scanKeysInSubspace(key: key)
            #expect(keys.count == 1)
            #expect(keys[0].count == 3, "Without graphField, tuple should have 3 elements, not 4")
        }

        try await ctx.cleanup()
    }
}
