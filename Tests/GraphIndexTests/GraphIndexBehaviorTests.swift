#if FOUNDATION_DB
// GraphIndexBehaviorTests.swift
// Integration tests for GraphIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct GraphIndexEdge {
    var id: String = UUID().uuidString
    var source: String
    var target: String
    var label: String
    var weight: Double = 1.0

}

// MARK: - Test Helper

private struct GraphIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: GraphIndexMaintainer<GraphIndexEdge>
    let strategy: PropertyGraphIndexStrategy

    init(
        strategy: PropertyGraphIndexStrategy = .adjacency,
        indexName: String = "TestEdge_graph"
    ) async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "graph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.strategy = strategy

        let definition = propertyGraphIndexDefinition(
            source: GraphIndexEdge.fields.source.identity,
            label: GraphIndexEdge.fields.label.identity,
            target: GraphIndexEdge.fields.target.identity,
            strategy: strategy
        )

        let index = try ResolvedIndex(
            for: GraphIndexEdge.self,
            name: indexName,
            definition: .graph(definition, includedFields: []),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "source"),
                FieldKeyExpression(fieldName: "target"),
                FieldKeyExpression(fieldName: "label"),
            ]),
            itemTypes: Set(["GraphIndexEdge"])
        )

        self.maintainer = try GraphIndexMaintainer<GraphIndexEdge>(
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

    /// Count entries in a specific subspace key
    private func countEntries(key: Int64) async throws -> Int {
        let targetSubspace = indexSubspace.subspace(key)
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = targetSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
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
        let scanner = GraphEdgeScanner(
            indexSubspace: indexSubspace,
            strategy: .adjacency
        )
        return try await database.withTransaction { transaction -> [String] in
            let edges = try await scanner.scanAllOutgoing(
                from: .identifier(source),
                edgeLabel: .identifier(label),
                transaction: transaction
            )
            return edges.compactMap { edge in
                edge.target.identifier
            }
        }
    }

    func getIncomingNeighbors(to target: String, label: String) async throws -> [String] {
        let scanner = GraphEdgeScanner(
            indexSubspace: indexSubspace,
            strategy: .adjacency
        )
        return try await database.withTransaction { transaction -> [String] in
            let edges = try await scanner.scanAllIncoming(
                to: .identifier(target),
                edgeLabel: .identifier(label),
                transaction: transaction
            )
            return edges.compactMap { edge in
                edge.source.identifier
            }
        }
    }
}

// MARK: - Adjacency Strategy Tests

@Suite("GraphIndex Adjacency Strategy Tests", .tags(.fdb), .serialized, .foundationDBScenario, .heartbeat)
struct GraphIndexAdjacencyTests {

    @Test("Insert creates outgoing edge entry")
    func testInsertCreatesOutgoingEdge() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "follows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edges = [
            GraphIndexEdge(source: "alice", target: "bob", label: "follows"),
            GraphIndexEdge(source: "alice", target: "charlie", label: "follows"),
            GraphIndexEdge(source: "alice", target: "dave", label: "follows"),
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edges = [
            GraphIndexEdge(source: "alice", target: "dave", label: "follows"),
            GraphIndexEdge(source: "bob", target: "dave", label: "follows"),
            GraphIndexEdge(source: "charlie", target: "dave", label: "follows"),
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "follows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .adjacency)

        let edges = [
            GraphIndexEdge(id: "e1", source: "alice", target: "bob", label: "follows"),
            GraphIndexEdge(id: "e2", source: "alice", target: "bob", label: "blocks"),
            GraphIndexEdge(id: "e3", source: "alice", target: "bob", label: "likes"),
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

@Suite("GraphIndex TripleStore Strategy Tests", .tags(.fdb), .serialized, .foundationDBScenario, .heartbeat)
struct GraphIndexTripleStoreTests {

    @Test("TripleStore creates 3 index entries")
    func testTripleStoreCreates3Entries() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .tripleStore)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "knows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Count entries in each subspace (spo=2, pos=3, osp=4)
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4)] {
            let subspace = ctx.indexSubspace.subspace(key)
            totalCount += try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        #expect(totalCount == 3, "TripleStore should create 3 index entries (SPO, POS, OSP)")

        try await ctx.cleanup()
    }

    @Test("TripleStore delete removes all 3 entries")
    func testTripleStoreDeleteRemovesAllEntries() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .tripleStore)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "knows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
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
            totalCount += try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        #expect(totalCount == 0, "TripleStore delete should remove all 3 entries")

        try await ctx.cleanup()
    }
}

// MARK: - Hexastore Strategy Tests

@Suite("GraphIndex Hexastore Strategy Tests", .tags(.fdb), .serialized, .foundationDBScenario, .heartbeat)
struct GraphIndexHexastoreTests {

    @Test("Hexastore creates 6 index entries")
    func testHexastoreCreates6Entries() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .hexastore)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "knows")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
                newItem: edge,
                transaction: transaction
            )
        }

        // Count entries in all 6 subspaces (spo=2, pos=3, osp=4, sop=5, pso=6, ops=7)
        var totalCount = 0
        for key in [Int64(2), Int64(3), Int64(4), Int64(5), Int64(6), Int64(7)] {
            let subspace = ctx.indexSubspace.subspace(key)
            totalCount += try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        #expect(totalCount == 6, "Hexastore should create 6 index entries")

        try await ctx.cleanup()
    }

    @Test("Hexastore delete removes all 6 entries")
    func testHexastoreDeleteRemovesAllEntries() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await GraphIndexContext(strategy: .hexastore)

        let edge = GraphIndexEdge(source: "alice", target: "bob", label: "knows")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as GraphIndexEdge?,
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
            totalCount += try await ctx.database.withTransaction { transaction in
                let (begin, end) = subspace.range()
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        #expect(totalCount == 0, "Hexastore delete should remove all 6 entries")

        try await ctx.cleanup()
    }
}
#endif
