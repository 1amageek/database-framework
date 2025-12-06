// GraphIndexBehaviorTests.swift
// Integration tests for GraphIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

struct TestEdge: Persistable {
    typealias ID = String

    var id: String
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

    static var persistableType: String { "TestEdge" }
    static var allFields: [String] { ["id", "source", "target", "label", "weight"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "source": return source
        case "target": return target
        case "label": return label
        case "weight": return weight
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestEdge, Value>) -> String {
        switch keyPath {
        case \TestEdge.id: return "id"
        case \TestEdge.source: return "source"
        case \TestEdge.target: return "target"
        case \TestEdge.label: return "label"
        case \TestEdge.weight: return "weight"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestEdge>) -> String {
        switch keyPath {
        case \TestEdge.id: return "id"
        case \TestEdge.source: return "source"
        case \TestEdge.target: return "target"
        case \TestEdge.label: return "label"
        case \TestEdge.weight: return "weight"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestEdge> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: GraphIndexMaintainer<TestEdge>
    let kind: GraphIndexKind<TestEdge>
    let strategy: GraphIndexStrategy

    init(strategy: GraphIndexStrategy = .adjacency, indexName: String = "TestEdge_graph") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.strategy = strategy

        self.kind = GraphIndexKind<TestEdge>(
            from: \.source,
            edge: \.label,
            to: \.target,
            strategy: strategy
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "source"),
                FieldKeyExpression(fieldName: "target"),
                FieldKeyExpression(fieldName: "label")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["TestEdge"])
        )

        self.maintainer = GraphIndexMaintainer<TestEdge>(
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

    /// Count entries in a specific subspace key
    private func countEntries(key: Int64) async throws -> Int {
        let targetSubspace = indexSubspace.subspace(key)
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = targetSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func countOutgoingEdges() async throws -> Int {
        // adjacency uses key 0 for out, tripleStore uses key 2 for spo
        return try await countEntries(key: strategy == .adjacency ? 0 : 2)
    }

    func countIncomingEdges() async throws -> Int {
        // adjacency uses key 1 for in
        return try await countEntries(key: 1)
    }

    func getOutgoingNeighbors(from source: String, label: String) async throws -> [String] {
        // For adjacency: [out=0]/[edge]/[from]/[to]
        let outSubspace = indexSubspace.subspace(Int64(0))
        let prefixSubspace = outSubspace.subspace(label).subspace(source)
        return try await database.withTransaction { transaction -> [String] in
            let (begin, end) = prefixSubspace.range()
            var targets: [String] = []
            for try await (key, _) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                let unpacked = try prefixSubspace.unpack(key)
                let elements = try Tuple.unpack(from: unpacked.pack())
                if let target = elements.first as? String {
                    targets.append(target)
                }
            }
            return targets
        }
    }

    func getIncomingNeighbors(to target: String, label: String) async throws -> [String] {
        // For adjacency: [in=1]/[edge]/[to]/[from]
        let inSubspace = indexSubspace.subspace(Int64(1))
        let prefixSubspace = inSubspace.subspace(label).subspace(target)
        return try await database.withTransaction { transaction -> [String] in
            let (begin, end) = prefixSubspace.range()
            var sources: [String] = []
            for try await (key, _) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                let unpacked = try prefixSubspace.unpack(key)
                let elements = try Tuple.unpack(from: unpacked.pack())
                if let source = elements.first as? String {
                    sources.append(source)
                }
            }
            return sources
        }
    }
}

// MARK: - Adjacency Strategy Tests

@Suite("GraphIndex Adjacency Strategy Tests", .tags(.fdb), .serialized)
struct GraphIndexAdjacencyTests {

    @Test("Insert creates outgoing edge entry")
    func testInsertCreatesOutgoingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let outCount = try await ctx.countOutgoingEdges()
        #expect(outCount == 1, "Should have 1 outgoing edge entry")

        try await ctx.cleanup()
    }

    @Test("Insert creates incoming edge entry")
    func testInsertCreatesIncomingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let inCount = try await ctx.countIncomingEdges()
        #expect(inCount == 1, "Should have 1 incoming edge entry")

        try await ctx.cleanup()
    }

    @Test("Multiple edges from same source")
    func testMultipleEdgesFromSameSource() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edges = [
            TestEdge(source: "alice", target: "bob", label: "follows"),
            TestEdge(source: "alice", target: "charlie", label: "follows"),
            TestEdge(source: "alice", target: "dave", label: "follows")
        ]

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        let neighbors = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        #expect(neighbors.count == 3, "Alice should follow 3 people")
        #expect(neighbors.contains("bob"))
        #expect(neighbors.contains("charlie"))
        #expect(neighbors.contains("dave"))

        try await ctx.cleanup()
    }

    @Test("Multiple edges to same target")
    func testMultipleEdgesToSameTarget() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edges = [
            TestEdge(source: "alice", target: "dave", label: "follows"),
            TestEdge(source: "bob", target: "dave", label: "follows"),
            TestEdge(source: "charlie", target: "dave", label: "follows")
        ]

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        let followers = try await ctx.getIncomingNeighbors(to: "dave", label: "follows")
        #expect(followers.count == 3, "Dave should have 3 followers")
        #expect(followers.contains("alice"))
        #expect(followers.contains("bob"))
        #expect(followers.contains("charlie"))

        try await ctx.cleanup()
    }

    @Test("Delete removes edge entries")
    func testDeleteRemovesEdges() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let outCountBefore = try await ctx.countOutgoingEdges()
        #expect(outCountBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge,
                newItem: nil,
                transaction: transaction
            )
        }

        let outCountAfter = try await ctx.countOutgoingEdges()
        let inCountAfter = try await ctx.countIncomingEdges()
        #expect(outCountAfter == 0, "Should have 0 outgoing edges after delete")
        #expect(inCountAfter == 0, "Should have 0 incoming edges after delete")

        try await ctx.cleanup()
    }

    @Test("Different labels create separate edges")
    func testDifferentLabels() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .adjacency)

        let edges = [
            TestEdge(id: "e1", source: "alice", target: "bob", label: "follows"),
            TestEdge(id: "e2", source: "alice", target: "bob", label: "blocks"),
            TestEdge(id: "e3", source: "alice", target: "bob", label: "likes")
        ]

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        let followsNeighbors = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        let blocksNeighbors = try await ctx.getOutgoingNeighbors(from: "alice", label: "blocks")
        let likesNeighbors = try await ctx.getOutgoingNeighbors(from: "alice", label: "likes")

        #expect(followsNeighbors == ["bob"])
        #expect(blocksNeighbors == ["bob"])
        #expect(likesNeighbors == ["bob"])

        try await ctx.cleanup()
    }
}

// MARK: - TripleStore Strategy Tests

@Suite("GraphIndex TripleStore Strategy Tests", .tags(.fdb), .serialized)
struct GraphIndexTripleStoreTests {

    @Test("TripleStore creates 3 index entries")
    func testTripleStoreCreates3Entries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .tripleStore)

        let edge = TestEdge(source: "alice", target: "bob", label: "knows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Count entries in each subspace (spo=2, pos=3, osp=4)
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4)] {
            let subspace = ctx.indexSubspace.subspace(key)
            try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    totalCount += 1
                }
            }
        }

        #expect(totalCount == 3, "TripleStore should create 3 index entries (SPO, POS, OSP)")

        try await ctx.cleanup()
    }

    @Test("TripleStore delete removes all 3 entries")
    func testTripleStoreDeleteRemovesAllEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .tripleStore)

        let edge = TestEdge(source: "alice", target: "bob", label: "knows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge,
                newItem: nil,
                transaction: transaction
            )
        }

        // Count entries in each subspace
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4)] {
            let subspace = ctx.indexSubspace.subspace(key)
            try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    totalCount += 1
                }
            }
        }

        #expect(totalCount == 0, "TripleStore delete should remove all 3 entries")

        try await ctx.cleanup()
    }
}

// MARK: - Hexastore Strategy Tests

@Suite("GraphIndex Hexastore Strategy Tests", .tags(.fdb), .serialized)
struct GraphIndexHexastoreTests {

    @Test("Hexastore creates 6 index entries")
    func testHexastoreCreates6Entries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .hexastore)

        let edge = TestEdge(source: "alice", target: "bob", label: "knows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Count entries in all 6 subspaces (spo=2, pos=3, osp=4, sop=5, pso=6, ops=7)
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4), Int64(5), Int64(6), Int64(7)] {
            let subspace = ctx.indexSubspace.subspace(key)
            try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    totalCount += 1
                }
            }
        }

        #expect(totalCount == 6, "Hexastore should create 6 index entries")

        try await ctx.cleanup()
    }

    @Test("Hexastore delete removes all 6 entries")
    func testHexastoreDeleteRemovesAllEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .hexastore)

        let edge = TestEdge(source: "alice", target: "bob", label: "knows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge,
                newItem: nil,
                transaction: transaction
            )
        }

        // Count entries
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4), Int64(5), Int64(6), Int64(7)] {
            let subspace = ctx.indexSubspace.subspace(key)
            try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    totalCount += 1
                }
            }
        }

        #expect(totalCount == 0, "Hexastore delete should remove all 6 entries")

        try await ctx.cleanup()
    }
}
