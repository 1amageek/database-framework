// RankIndexPerformanceTests.swift
// Performance benchmarks for RankIndex operations

import Testing
import Foundation
import FoundationDB
import Core
import Rank
import TestSupport
@testable import DatabaseEngine
@testable import RankIndex

// MARK: - Benchmark Context

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: RankIndexMaintainer<BenchmarkPlayer, Int64>

    init() throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("bench", "rank", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace("score_rank")

        let kind = RankIndexKind<BenchmarkPlayer, Int64>(field: \.score)
        let index = Index(
            name: "score_rank",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "score_rank",
            itemTypes: Set(["BenchmarkPlayer"])
        )

        self.maintainer = RankIndexMaintainer<BenchmarkPlayer, Int64>(
            index: index,
            bucketSize: kind.bucketSize,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

// MARK: - Benchmark Player Model

private struct BenchmarkPlayer: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var score: Int64

    init(id: String = UUID().uuidString, name: String, score: Int64) {
        self.id = id
        self.name = name
        self.score = score
    }

    static var persistableType: String { "BenchmarkPlayer" }
    static var allFields: [String] { ["id", "name", "score"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkPlayer, Value>) -> String {
        switch keyPath {
        case \BenchmarkPlayer.id: return "id"
        case \BenchmarkPlayer.name: return "name"
        case \BenchmarkPlayer.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkPlayer>) -> String {
        switch keyPath {
        case \BenchmarkPlayer.id: return "id"
        case \BenchmarkPlayer.name: return "name"
        case \BenchmarkPlayer.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BenchmarkPlayer> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
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

@Suite("RankIndex Performance Tests", .tags(.fdb, .performance), .serialized)
struct RankIndexPerformanceTests {

    // MARK: - Bulk Insert Tests

    @Test("Bulk insert performance - 100 players")
    func testBulkInsert100() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let players = (0..<100).map { i in
            BenchmarkPlayer(name: "Player\(i)", score: Int64.random(in: 0...10000))
        }

        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for player in players {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as BenchmarkPlayer?,
                        newItem: player,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(players.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert",
            count: players.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 5000, "Bulk insert of 100 players should complete within 5s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - 1000 players")
    func testBulkInsert1000() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let players = (0..<1000).map { i in
            BenchmarkPlayer(name: "Player\(i)", score: Int64.random(in: 0...100000))
        }

        // Insert in batches to avoid transaction size limits
        let batchSize = 100
        let (_, durationMs) = try await measure {
            for batch in stride(from: 0, to: players.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, players.count)
                let batchPlayers = Array(players[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for player in batchPlayers {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil as BenchmarkPlayer?,
                            newItem: player,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        let throughput = Double(players.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert",
            count: players.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 30000, "Bulk insert of 1000 players should complete within 30s")

        try await ctx.cleanup()
    }

    // MARK: - Top-K Query Tests

    @Test("Top-K query performance - varying K values")
    func testTopKPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 players
        let playerCount = 500
        let players = (0..<playerCount).map { i in
            BenchmarkPlayer(name: "Player\(i)", score: Int64.random(in: 0...50000))
        }

        let batchSize = 100
        for batch in stride(from: 0, to: players.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, players.count)
            let batchPlayers = Array(players[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for player in batchPlayers {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as BenchmarkPlayer?,
                        newItem: player,
                        transaction: transaction
                    )
                }
            }
        }

        // Test various K values
        let kValues = [10, 50, 100, 250]

        for k in kValues {
            let (results, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getTopK(k: k, transaction: transaction)
                }
            }

            print(String(format: "Top-%d query (%d players): %.2f ms, returned %d results",
                        k, playerCount, durationMs, results.count))

            #expect(results.count == min(k, playerCount), "Should return min(k, total) results")
            #expect(durationMs < 5000, "Top-\(k) query should complete within 5s")
        }

        try await ctx.cleanup()
    }

    @Test("Top-K ordering verification")
    func testTopKOrdering() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert players with known scores
        let knownScores: [Int64] = [100, 500, 200, 1000, 800, 300, 900, 400, 600, 700]
        let players = knownScores.enumerated().map { i, score in
            BenchmarkPlayer(id: "p\(i)", name: "Player\(i)", score: score)
        }

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BenchmarkPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        let top5 = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getTopK(k: 5, transaction: transaction)
        }

        // Verify descending order
        let scores = top5.map { $0.score }
        #expect(scores == [1000, 900, 800, 700, 600], "Should return top 5 in descending order")

        try await ctx.cleanup()
    }

    // MARK: - Rank Lookup Tests

    @Test("Rank lookup performance - varying ranks")
    func testRankLookupPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 players with scores 1-500
        let playerCount = 500
        let players = (1...playerCount).map { i in
            BenchmarkPlayer(id: "p\(i)", name: "Player\(i)", score: Int64(i * 10))
        }

        let batchSize = 100
        for batch in stride(from: 0, to: players.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, players.count)
            let batchPlayers = Array(players[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for player in batchPlayers {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as BenchmarkPlayer?,
                        newItem: player,
                        transaction: transaction
                    )
                }
            }
        }

        // Test rank lookup for various scores
        let testScores: [(score: Int64, expectedRank: Int64)] = [
            (5000, 0),    // Highest score
            (2500, 250),  // Middle score
            (10, 499)     // Lowest score
        ]

        for (score, expectedRank) in testScores {
            let (rank, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getRank(score: score, transaction: transaction)
                }
            }

            print(String(format: "Rank lookup (score %d, rank %d): %.2f ms",
                        score, rank, durationMs))

            #expect(rank == expectedRank, "Score \(score) should have rank \(expectedRank)")
            #expect(durationMs < 5000, "Rank lookup should complete within 5s")
        }

        try await ctx.cleanup()
    }

    // MARK: - Count Query Tests

    @Test("Count query performance (O(1))")
    func testCountPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 players
        let playerCount = 500
        let players = (0..<playerCount).map { i in
            BenchmarkPlayer(name: "Player\(i)", score: Int64.random(in: 0...50000))
        }

        let batchSize = 100
        for batch in stride(from: 0, to: players.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, players.count)
            let batchPlayers = Array(players[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for player in batchPlayers {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as BenchmarkPlayer?,
                        newItem: player,
                        transaction: transaction
                    )
                }
            }
        }

        // Measure count query (should be O(1))
        var totalDuration: Double = 0
        let iterations = 10

        for _ in 0..<iterations {
            let (count, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getCount(transaction: transaction)
                }
            }
            totalDuration += durationMs
            #expect(count == Int64(playerCount), "Count should match player count")
        }

        let avgDuration = totalDuration / Double(iterations)
        print(String(format: "Count query (O(1)): %.2f ms average over %d iterations",
                    avgDuration, iterations))

        #expect(avgDuration < 100, "Count query should be fast (O(1))")

        try await ctx.cleanup()
    }

    // MARK: - Percentile Query Tests

    @Test("Percentile query performance")
    func testPercentilePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 players with scores 1-500
        let playerCount = 500
        let players = (1...playerCount).map { i in
            BenchmarkPlayer(id: "p\(i)", name: "Player\(i)", score: Int64(i))
        }

        let batchSize = 100
        for batch in stride(from: 0, to: players.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, players.count)
            let batchPlayers = Array(players[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for player in batchPlayers {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as BenchmarkPlayer?,
                        newItem: player,
                        transaction: transaction
                    )
                }
            }
        }

        // Test various percentiles
        let percentiles = [0.5, 0.75, 0.90, 0.95, 0.99]

        for p in percentiles {
            let (score, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getPercentile(p, transaction: transaction)
                }
            }

            print(String(format: "Percentile %.2f query: %.2f ms, score = %@",
                        p, durationMs, score.map { String($0) } ?? "nil"))

            #expect(score != nil, "Percentile should return a score")
            #expect(durationMs < 5000, "Percentile query should complete within 5s")
        }

        try await ctx.cleanup()
    }

    // MARK: - Update/Delete Tests

    @Test("Update performance")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 100 players
        let players = (0..<100).map { i in
            BenchmarkPlayer(id: "p\(i)", name: "Player\(i)", score: Int64(i * 100))
        }

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BenchmarkPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Update scores
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for player in players {
                    let updated = BenchmarkPlayer(
                        id: player.id,
                        name: player.name,
                        score: player.score + 1000  // Boost score
                    )
                    try await ctx.maintainer.updateIndex(
                        oldItem: player,
                        newItem: updated,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(players.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Update",
            count: players.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        // Verify count unchanged
        let count = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(transaction: transaction)
        }
        #expect(count == Int64(players.count), "Count should be unchanged after updates")

        try await ctx.cleanup()
    }

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 100 players
        let players = (0..<100).map { i in
            BenchmarkPlayer(id: "p\(i)", name: "Player\(i)", score: Int64(i * 100))
        }

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BenchmarkPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Delete all
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for player in players {
                    try await ctx.maintainer.updateIndex(
                        oldItem: player,
                        newItem: nil as BenchmarkPlayer?,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(players.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Delete",
            count: players.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        // Verify count is 0
        let count = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(transaction: transaction)
        }
        #expect(count == 0, "Count should be 0 after deletes")

        try await ctx.cleanup()
    }

    // MARK: - Ties Handling Tests

    @Test("Ties handling performance")
    func testTiesPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 100 players with only 10 distinct scores (many ties)
        let distinctScores: [Int64] = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        let players = (0..<100).map { i in
            BenchmarkPlayer(
                id: "p\(i)",
                name: "Player\(i)",
                score: distinctScores[i % distinctScores.count]
            )
        }

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BenchmarkPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Top-K with ties
        let (top20, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 20, transaction: transaction)
            }
        }

        print(String(format: "Top-20 with ties: %.2f ms", durationMs))

        #expect(top20.count == 20, "Should return 20 results")
        // All top 20 should have score 1000 (10 players) or 900 (10 players)
        #expect(top20[0].score == 1000, "First should be highest score")

        // Rank lookup for tied score
        let (rank1000, rankDuration) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getRank(score: 1000, transaction: transaction)
            }
        }

        print(String(format: "Rank lookup for score 1000 (10 ties): %.2f ms, rank = %d",
                    rankDuration, rank1000))

        #expect(rank1000 == 0, "Score 1000 should be rank 0 (no one above)")

        try await ctx.cleanup()
    }

    // MARK: - Scale Tests

    @Test("Scale test - 2000 players")
    func testScale2000() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let playerCount = 2000
        let players = (0..<playerCount).map { i in
            BenchmarkPlayer(name: "Player\(i)", score: Int64.random(in: 0...1_000_000))
        }

        // Insert
        let batchSize = 100
        let (_, insertDuration) = try await measure {
            for batch in stride(from: 0, to: players.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, players.count)
                let batchPlayers = Array(players[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for player in batchPlayers {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil as BenchmarkPlayer?,
                            newItem: player,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        print(String(format: "Insert %d players: %.2f ms (%.0f ops/s)",
                    playerCount, insertDuration, Double(playerCount) / (insertDuration / 1000)))

        // Top-100 query
        let (top100, top100Duration) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 100, transaction: transaction)
            }
        }

        print(String(format: "Top-100 (%d players): %.2f ms", playerCount, top100Duration))
        #expect(top100.count == 100, "Should return 100 results")

        // Count query
        let (count, countDuration) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getCount(transaction: transaction)
            }
        }

        print(String(format: "Count (%d players): %.2f ms", playerCount, countDuration))
        #expect(count == Int64(playerCount), "Count should match")

        try await ctx.cleanup()
    }
}
