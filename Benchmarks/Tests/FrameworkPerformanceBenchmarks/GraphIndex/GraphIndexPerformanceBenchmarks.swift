#if FOUNDATION_DB
// GraphIndexPerformanceBenchmarks.swift
// Performance benchmarks for GraphIndex operations

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
@_spi(DatabaseExecution) @testable import GraphIndex

// MARK: - Benchmark Context

private struct BenchmarkContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: GraphIndexMaintainer<BenchmarkEdge>
    let strategy: PropertyGraphIndexStrategy

    init(strategy: PropertyGraphIndexStrategy = .adjacency) async throws {
        self.database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("bench", "graph", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace("edges")
        self.strategy = strategy

        let definition = benchmarkPropertyGraphIndexDefinition(
            source: BenchmarkEdge.fields.source.identity,
            label: BenchmarkEdge.fields.label.identity,
            target: BenchmarkEdge.fields.target.identity,
            strategy: strategy
        )

        let index = try ResolvedIndex(
            for: BenchmarkEdge.self,
            name: "edges",
            definition: .graph(definition, includedFields: []),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "source"),
                FieldKeyExpression(fieldName: "label"),
                FieldKeyExpression(fieldName: "target"),
            ]),
            itemTypes: Set(["BenchmarkEdge"])
        )

        self.maintainer = try GraphIndexMaintainer<BenchmarkEdge>(
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
}

// MARK: - Benchmark Model

@Persistable
private struct BenchmarkEdge {
    var id: String = UUID().uuidString
    var source: String
    var target: String
    var label: String

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

@Suite("GraphIndex Performance Tests", .tags(.fdb, .performance), .serialized, .foundationDBBenchmark, .heartbeat)
struct GraphIndexPerformanceBenchmarks {

    // MARK: - Bulk Insert Tests

    @Test("Bulk insert performance - adjacency strategy")
    func testBulkInsertAdjacency() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .tripleStore)

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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .hexastore)

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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        print(String(format: "Outgoing neighbors query: %.2f ms", durationMs))
        #expect(durationMs < 5000, "Query should complete within 5s")

        try await ctx.cleanup()
    }

    @Test("Incoming neighbors query performance")
    func testIncomingNeighborsQuery() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        print(String(format: "Incoming neighbors query: %.2f ms", durationMs))
        #expect(durationMs < 5000, "Query should complete within 5s")

        try await ctx.cleanup()
    }

    // MARK: - Strategy Comparison Tests

    @Test("Strategy comparison - write cost")
    func testStrategyWriteCost() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        let strategies: [PropertyGraphIndexStrategy] = [
            .adjacency,
            .tripleStore,
            .hexastore,
        ]
        let edgeCount = 100

        for strategy in strategies {
            let ctx = try await BenchmarkContext(strategy: strategy)
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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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

    // MARK: - Scale Tests

    @Test("Scale test - 500 edges")
    func testScale500Edges() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(strategy: .adjacency)

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
                return try await transaction.collectRange(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).count
            }
        }

        print(String(format: "Query neighbors: %.2f ms", queryDuration))

        try await ctx.cleanup()
    }
}
#endif
