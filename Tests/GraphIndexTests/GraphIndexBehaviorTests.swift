// GraphIndexBehaviorTests.swift
// Integration tests for GraphIndex (Adjacency) behavior with FDB

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
    let maintainer: AdjacencyIndexMaintainer<TestEdge>
    let kind: AdjacencyIndexKind<TestEdge>

    init(bidirectional: Bool = true, indexName: String = "TestEdge_adjacency") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = AdjacencyIndexKind<TestEdge>(
            source: \.source,
            target: \.target,
            label: \.label,
            bidirectional: bidirectional
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

        self.maintainer = AdjacencyIndexMaintainer<TestEdge>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            sourceField: kind.sourceField,
            targetField: kind.targetField,
            labelField: kind.labelField,
            bidirectional: kind.bidirectional
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countOutgoingEdges() async throws -> Int {
        let outSubspace = indexSubspace.subspace("adj")
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = outSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func countIncomingEdges() async throws -> Int {
        let inSubspace = indexSubspace.subspace("adj_in")
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = inSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func getOutgoingNeighbors(from source: String, label: String) async throws -> [String] {
        let outSubspace = indexSubspace.subspace("adj")
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
        let inSubspace = indexSubspace.subspace("adj_in")
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

// MARK: - Behavior Tests

@Suite("GraphIndex Behavior Tests", .tags(.fdb), .serialized)
struct GraphIndexBehaviorTests {

    // MARK: - Insert Tests (Bidirectional)

    @Test("Insert creates outgoing edge entry")
    func testInsertCreatesOutgoingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

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

    @Test("Insert creates incoming edge entry (bidirectional)")
    func testInsertCreatesIncomingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let inCount = try await ctx.countIncomingEdges()
        #expect(inCount == 1, "Should have 1 incoming edge entry (bidirectional)")

        try await ctx.cleanup()
    }

    @Test("Insert without bidirectional does not create incoming edge")
    func testInsertUnidirectionalNoIncoming() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: false)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let outCount = try await ctx.countOutgoingEdges()
        let inCount = try await ctx.countIncomingEdges()

        #expect(outCount == 1, "Should have 1 outgoing edge")
        #expect(inCount == 0, "Should have 0 incoming edges (unidirectional)")

        try await ctx.cleanup()
    }

    @Test("Multiple edges from same source")
    func testMultipleEdgesFromSameSource() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

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
        let ctx = try TestContext(bidirectional: true)

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

    // MARK: - Delete Tests

    @Test("Delete removes outgoing edge entry")
    func testDeleteRemovesOutgoingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

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
        #expect(outCountAfter == 0, "Should have 0 outgoing edges after delete")

        try await ctx.cleanup()
    }

    @Test("Delete removes incoming edge entry (bidirectional)")
    func testDeleteRemovesIncomingEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let edge = TestEdge(source: "alice", target: "bob", label: "follows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let inCountBefore = try await ctx.countIncomingEdges()
        #expect(inCountBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge,
                newItem: nil,
                transaction: transaction
            )
        }

        let inCountAfter = try await ctx.countIncomingEdges()
        #expect(inCountAfter == 0, "Should have 0 incoming edges after delete")

        try await ctx.cleanup()
    }

    @Test("Delete specific edge among multiple")
    func testDeleteSpecificEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let edge1 = TestEdge(id: "e1", source: "alice", target: "bob", label: "follows")
        let edge2 = TestEdge(id: "e2", source: "alice", target: "charlie", label: "follows")

        // Insert both
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: edge1, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: edge2, transaction: transaction)
        }

        let neighborsBefore = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        #expect(neighborsBefore.count == 2)

        // Delete edge1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge1,
                newItem: nil,
                transaction: transaction
            )
        }

        let neighborsAfter = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        #expect(neighborsAfter.count == 1)
        #expect(neighborsAfter.contains("charlie"))
        #expect(!neighborsAfter.contains("bob"))

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update edge target moves edge")
    func testUpdateEdgeTarget() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let edge = TestEdge(id: "e1", source: "alice", target: "bob", label: "follows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        let neighborsBefore = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        #expect(neighborsBefore.contains("bob"))

        // Update target from bob to charlie
        let updatedEdge = TestEdge(id: "e1", source: "alice", target: "charlie", label: "follows")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: edge,
                newItem: updatedEdge,
                transaction: transaction
            )
        }

        let neighborsAfter = try await ctx.getOutgoingNeighbors(from: "alice", label: "follows")
        #expect(neighborsAfter.count == 1)
        #expect(neighborsAfter.contains("charlie"))
        #expect(!neighborsAfter.contains("bob"))

        // Verify incoming edges also updated
        let bobFollowers = try await ctx.getIncomingNeighbors(to: "bob", label: "follows")
        let charlieFollowers = try await ctx.getIncomingNeighbors(to: "charlie", label: "follows")
        #expect(bobFollowers.isEmpty, "Bob should have no followers")
        #expect(charlieFollowers.contains("alice"), "Charlie should be followed by Alice")

        try await ctx.cleanup()
    }

    // MARK: - Label Tests

    @Test("Different labels create separate edges")
    func testDifferentLabels() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

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

    // MARK: - Self-Loop Tests

    @Test("Self-loop edge is supported")
    func testSelfLoopEdge() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let selfLoop = TestEdge(source: "alice", target: "alice", label: "self-reference")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestEdge?,
                newItem: selfLoop,
                transaction: transaction
            )
        }

        let outgoing = try await ctx.getOutgoingNeighbors(from: "alice", label: "self-reference")
        let incoming = try await ctx.getIncomingNeighbors(to: "alice", label: "self-reference")

        #expect(outgoing.contains("alice"), "Self-loop should appear in outgoing")
        #expect(incoming.contains("alice"), "Self-loop should appear in incoming")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem creates edge entries")
    func testScanItemCreatesEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(bidirectional: true)

        let edges = [
            TestEdge(id: "e1", source: "alice", target: "bob", label: "follows"),
            TestEdge(id: "e2", source: "bob", target: "charlie", label: "follows")
        ]

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.scanItem(
                    edge,
                    id: Tuple(edge.id),
                    transaction: transaction
                )
            }
        }

        let outCount = try await ctx.countOutgoingEdges()
        let inCount = try await ctx.countIncomingEdges()

        #expect(outCount == 2, "Should have 2 outgoing edge entries")
        #expect(inCount == 2, "Should have 2 incoming edge entries (bidirectional)")

        try await ctx.cleanup()
    }
}
