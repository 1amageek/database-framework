// GraphIndexPerformanceTests.swift
// Performance benchmarks for GraphIndex operations

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Benchmark Context

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: GraphIndexMaintainer<BenchmarkEdge>
    let strategy: GraphIndexStrategy

    init(strategy: GraphIndexStrategy = .adjacency) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("bench", "graph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace("edges")
        self.strategy = strategy

        let kind = GraphIndexKind<BenchmarkEdge>(
            from: \.source,
            edge: \.label,
            to: \.target,
            strategy: strategy
        )

        let index = Index(
            name: "edges",
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "source"),
                FieldKeyExpression(fieldName: "label"),
                FieldKeyExpression(fieldName: "target")
            ]),
            subspaceKey: "edges",
            itemTypes: Set(["BenchmarkEdge"])
        )

        self.maintainer = GraphIndexMaintainer<BenchmarkEdge>(
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
}

// MARK: - Benchmark Model

private struct BenchmarkEdge: Persistable {
    typealias ID = String

    var id: String
    var source: String
    var target: String
    var label: String

    init(id: String = UUID().uuidString, source: String, target: String, label: String = "follows") {
        self.id = id
        self.source = source
        self.target = target
        self.label = label
    }

    static var persistableType: String { "BenchmarkEdge" }
    static var allFields: [String] { ["id", "source", "target", "label"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "source": return source
        case "target": return target
        case "label": return label
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkEdge, Value>) -> String {
        switch keyPath {
        case \BenchmarkEdge.id: return "id"
        case \BenchmarkEdge.source: return "source"
        case \BenchmarkEdge.target: return "target"
        case \BenchmarkEdge.label: return "label"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkEdge>) -> String {
        switch keyPath {
        case \BenchmarkEdge.id: return "id"
        case \BenchmarkEdge.source: return "source"
        case \BenchmarkEdge.target: return "target"
        case \BenchmarkEdge.label: return "label"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BenchmarkEdge> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Data

private func generateSocialGraph(nodeCount: Int, avgDegree: Int) -> [BenchmarkEdge] {
    var edges: [BenchmarkEdge] = []
    let nodes = (0..<nodeCount).map { "user\($0)" }

    for node in nodes {
        // Each node follows avgDegree random other nodes
        let targets = nodes.shuffled().prefix(avgDegree)
        for target in targets where target != node {
            edges.append(BenchmarkEdge(source: node, target: target, label: "follows"))
        }
    }

    return edges
}

// MARK: - Benchmark Helper

private struct BenchmarkResult {
    let operation: String
    let count: Int
    let durationMs: Double
    let throughput: Double

    var description: String {
        String(format: "%@ (%d items): %.2f ms (%.0f ops/s)",
               operation, count, durationMs, throughput)
    }
}

private func measure<T>(_ operation: () async throws -> T) async throws -> (result: T, durationMs: Double) {
    let start = DispatchTime.now()
    let result = try await operation()
    let end = DispatchTime.now()
    let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
    return (result, Double(nanos) / 1_000_000)
}

// MARK: - Performance Tests

@Suite("GraphIndex Performance Tests", .tags(.fdb, .performance), .serialized)
struct GraphIndexPerformanceTests {

    // MARK: - Bulk Insert Tests

    @Test("Bulk insert performance - adjacency strategy")
    func testBulkInsertAdjacency() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        let edges = generateSocialGraph(nodeCount: 50, avgDegree: 4)

        let (_, durationMs) = try await measure {
            let batchSize = 50
            for batch in stride(from: 0, to: edges.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, edges.count)
                let batchEdges = Array(edges[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for edge in batchEdges {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: edge,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        let throughput = Double(edges.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert (adjacency)",
            count: edges.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 30000, "Bulk insert should complete within 30s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - tripleStore strategy")
    func testBulkInsertTripleStore() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .tripleStore)

        let edges = generateSocialGraph(nodeCount: 50, avgDegree: 4)

        let (_, durationMs) = try await measure {
            let batchSize = 50
            for batch in stride(from: 0, to: edges.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, edges.count)
                let batchEdges = Array(edges[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for edge in batchEdges {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: edge,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        let throughput = Double(edges.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert (tripleStore)",
            count: edges.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 30000, "Bulk insert should complete within 30s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - hexastore strategy")
    func testBulkInsertHexastore() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .hexastore)

        let edges = generateSocialGraph(nodeCount: 50, avgDegree: 4)

        let (_, durationMs) = try await measure {
            let batchSize = 50
            for batch in stride(from: 0, to: edges.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, edges.count)
                let batchEdges = Array(edges[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for edge in batchEdges {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: edge,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        let throughput = Double(edges.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert (hexastore)",
            count: edges.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 60000, "Bulk insert should complete within 60s")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("Outgoing neighbors query performance")
    func testOutgoingNeighborsQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Insert edges: user0 follows user1-9
        var edges: [BenchmarkEdge] = []
        for i in 1..<10 {
            edges.append(BenchmarkEdge(source: "user0", target: "user\(i)", label: "follows"))
        }
        // Add more edges from other users
        for i in 1..<50 {
            edges.append(BenchmarkEdge(source: "user\(i)", target: "user\((i + 1) % 50)", label: "follows"))
        }

        let batchSize = 50
        for batch in stride(from: 0, to: edges.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, edges.count)
            let batchEdges = Array(edges[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for edge in batchEdges {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: edge,
                        transaction: transaction
                    )
                }
            }
        }

        // Query outgoing edges from user0
        let (_, durationMs) = try await measure {
            let outSubspace = ctx.indexSubspace.subspace(Int64(0))
            let prefixSubspace = outSubspace.subspace("follows").subspace("user0")

            return try await ctx.database.withTransaction { transaction in
                let (begin, end) = prefixSubspace.range()
                var count = 0
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    count += 1
                }
                return count
            }
        }

        print(String(format: "Outgoing neighbors query: %.2f ms", durationMs))
        #expect(durationMs < 5000, "Query should complete within 5s")

        try await ctx.cleanup()
    }

    @Test("Incoming neighbors query performance")
    func testIncomingNeighborsQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Insert edges: user1-9 follow user0
        var edges: [BenchmarkEdge] = []
        for i in 1..<10 {
            edges.append(BenchmarkEdge(source: "user\(i)", target: "user0", label: "follows"))
        }
        // Add more edges
        for i in 1..<50 {
            edges.append(BenchmarkEdge(source: "user\(i)", target: "user\((i + 1) % 50)", label: "follows"))
        }

        let batchSize = 50
        for batch in stride(from: 0, to: edges.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, edges.count)
            let batchEdges = Array(edges[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for edge in batchEdges {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: edge,
                        transaction: transaction
                    )
                }
            }
        }

        // Query incoming edges to user0
        let (_, durationMs) = try await measure {
            let inSubspace = ctx.indexSubspace.subspace(Int64(1))
            let prefixSubspace = inSubspace.subspace("follows").subspace("user0")

            return try await ctx.database.withTransaction { transaction in
                let (begin, end) = prefixSubspace.range()
                var count = 0
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    count += 1
                }
                return count
            }
        }

        print(String(format: "Incoming neighbors query: %.2f ms", durationMs))
        #expect(durationMs < 5000, "Query should complete within 5s")

        try await ctx.cleanup()
    }

    // MARK: - Strategy Comparison Tests

    @Test("Strategy comparison - write cost")
    func testStrategyWriteCost() async throws {
        try await FDBTestSetup.shared.initialize()

        let strategies: [GraphIndexStrategy] = [.adjacency, .tripleStore, .hexastore]
        let edgeCount = 100

        for strategy in strategies {
            let ctx = try BenchmarkContext(strategy: strategy)
            let edges = generateSocialGraph(nodeCount: 20, avgDegree: 5)
                .prefix(edgeCount)
                .map { $0 }

            let (_, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    for edge in edges {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: edge,
                            transaction: transaction
                        )
                    }
                }
            }

            let throughput = Double(edges.count) / (durationMs / 1000)
            print(BenchmarkResult(
                operation: "Insert (\(strategy))",
                count: edges.count,
                durationMs: durationMs,
                throughput: throughput
            ).description)

            try await ctx.cleanup()
        }
    }

    // MARK: - Update/Delete Tests

    @Test("Update performance")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Insert 50 edges
        let edges = (0..<50).map { i in
            BenchmarkEdge(id: "e\(i)", source: "user0", target: "user\(i + 1)", label: "follows")
        }

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        // Update all edges (change target)
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for (i, edge) in edges.enumerated() {
                    let updated = BenchmarkEdge(
                        id: edge.id,
                        source: edge.source,
                        target: "updated_user\(i + 1)",
                        label: edge.label
                    )
                    try await ctx.maintainer.updateIndex(
                        oldItem: edge,
                        newItem: updated,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(edges.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Update",
            count: edges.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        try await ctx.cleanup()
    }

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Insert 50 edges
        let edges = (0..<50).map { i in
            BenchmarkEdge(id: "e\(i)", source: "user0", target: "user\(i + 1)", label: "follows")
        }

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        // Delete all edges
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for edge in edges {
                    try await ctx.maintainer.updateIndex(
                        oldItem: edge,
                        newItem: nil,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(edges.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Delete",
            count: edges.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        try await ctx.cleanup()
    }

    // MARK: - Traversal Tests

    @Test("GraphTraverser 1-hop performance")
    func testTraverser1Hop() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Insert star graph: user0 follows user1-20
        let edges = (1...20).map { i in
            BenchmarkEdge(source: "user0", target: "user\(i)", label: "follows")
        }

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        let traverser = GraphTraverser<BenchmarkEdge>(
            database: ctx.database,
            subspace: ctx.indexSubspace
        )

        var neighborCount = 0
        let (_, durationMs) = try await measure {
            neighborCount = 0
            for try await _ in traverser.neighbors(from: "user0", label: "follows", direction: .outgoing) {
                neighborCount += 1
            }
        }

        print(String(format: "1-hop traversal (%d neighbors): %.2f ms", neighborCount, durationMs))
        #expect(neighborCount == 20, "Should find 20 neighbors")
        #expect(durationMs < 5000, "Traversal should complete within 5s")

        try await ctx.cleanup()
    }

    @Test("GraphTraverser multi-hop performance")
    func testTraverserMultiHop() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        // Create chain: user0 → user1 → user2 → ... → user9
        var edges: [BenchmarkEdge] = []
        for i in 0..<9 {
            edges.append(BenchmarkEdge(source: "user\(i)", target: "user\(i + 1)", label: "follows"))
        }
        // Add some branches
        edges.append(BenchmarkEdge(source: "user1", target: "user10", label: "follows"))
        edges.append(BenchmarkEdge(source: "user2", target: "user11", label: "follows"))
        edges.append(BenchmarkEdge(source: "user2", target: "user12", label: "follows"))

        try await ctx.database.withTransaction { transaction in
            for edge in edges {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: edge,
                    transaction: transaction
                )
            }
        }

        let traverser = GraphTraverser<BenchmarkEdge>(
            database: ctx.database,
            subspace: ctx.indexSubspace
        )

        var nodeCount = 0
        let (_, durationMs) = try await measure {
            nodeCount = 0
            for try await _ in traverser.traverse(from: "user0", maxDepth: 3, label: "follows") {
                nodeCount += 1
            }
        }

        print(String(format: "Multi-hop traversal (depth=3, %d nodes): %.2f ms", nodeCount, durationMs))
        #expect(nodeCount > 0, "Should find some nodes")
        #expect(durationMs < 10000, "Traversal should complete within 10s")

        try await ctx.cleanup()
    }

    // MARK: - Scale Tests

    @Test("Scale test - 500 edges")
    func testScale500Edges() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(strategy: .adjacency)

        let edges = generateSocialGraph(nodeCount: 100, avgDegree: 5)

        // Insert
        let batchSize = 50
        let (_, insertDuration) = try await measure {
            for batch in stride(from: 0, to: edges.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, edges.count)
                let batchEdges = Array(edges[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for edge in batchEdges {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: edge,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        print(String(format: "Insert %d edges: %.2f ms (%.0f ops/s)",
                    edges.count, insertDuration, Double(edges.count) / (insertDuration / 1000)))

        // Query random user's neighbors
        let (_, queryDuration) = try await measure {
            let outSubspace = ctx.indexSubspace.subspace(Int64(0))
            let prefixSubspace = outSubspace.subspace("follows").subspace("user50")

            return try await ctx.database.withTransaction { transaction in
                let (begin, end) = prefixSubspace.range()
                var count = 0
                for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    count += 1
                }
                return count
            }
        }

        print(String(format: "Query neighbors: %.2f ms", queryDuration))

        try await ctx.cleanup()
    }
}
