// LeaderboardIndexPerformanceTests.swift
// Performance tests for LeaderboardIndex

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import LeaderboardIndex

// MARK: - Test Model

private struct PerfGameScore: Persistable {
    typealias ID = String

    var id: String
    var playerId: String
    var score: Int64
    var region: String

    init(id: String = UUID().uuidString, playerId: String, score: Int64, region: String = "global") {
        self.id = id
        self.playerId = playerId
        self.score = score
        self.region = region
    }

    static var persistableType: String { "PerfGameScore" }
    static var allFields: [String] { ["id", "playerId", "score", "region"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "playerId": return playerId
        case "score": return score
        case "region": return region
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PerfGameScore, Value>) -> String {
        switch keyPath {
        case \PerfGameScore.id: return "id"
        case \PerfGameScore.playerId: return "playerId"
        case \PerfGameScore.score: return "score"
        case \PerfGameScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PerfGameScore>) -> String {
        switch keyPath {
        case \PerfGameScore.id: return "id"
        case \PerfGameScore.playerId: return "playerId"
        case \PerfGameScore.score: return "score"
        case \PerfGameScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PerfGameScore> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Performance Test Helper

private struct PerfTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let maintainer: TimeWindowLeaderboardIndexMaintainer<PerfGameScore, Int64>
    let indexName: String

    init(testName: String, window: LeaderboardWindowType = .daily, windowCount: Int = 7) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.indexName = "PerfGameScore_leaderboard_score"
        self.subspace = Subspace(prefix: Tuple("test", "leaderboard_perf", String(testId), testName).pack())

        let indexSubspace = subspace.subspace("I").subspace(indexName)

        let kind = TimeWindowLeaderboardIndexKind<PerfGameScore, Int64>(
            scoreField: \.score,
            groupBy: [],
            window: window,
            windowCount: windowCount
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "score"),
            keyPaths: [\PerfGameScore.score],
            subspaceKey: indexName,
            itemTypes: Set(["PerfGameScore"])
        )

        self.maintainer = TimeWindowLeaderboardIndexMaintainer<PerfGameScore, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            window: window,
            windowCount: windowCount
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}

// MARK: - Benchmark Helper

private func benchmark(_ name: String, iterations: Int = 1, operation: () async throws -> Void) async throws -> (totalMs: Double, perIterationMs: Double) {
    let start = DispatchTime.now()
    for _ in 0..<iterations {
        try await operation()
    }
    let end = DispatchTime.now()
    let totalNs = Double(end.uptimeNanoseconds - start.uptimeNanoseconds)
    let totalMs = totalNs / 1_000_000
    let perIterationMs = totalMs / Double(iterations)
    return (totalMs, perIterationMs)
}

// MARK: - Insert Performance Tests

@Suite("LeaderboardIndex Insert Performance", .tags(.fdb), .serialized)
struct LeaderboardIndexInsertPerformanceTests {

    @Test("Bulk insert performance - 100 records")
    func testBulkInsert100Records() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "bulk_insert_100")

        let scores = (0..<100).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64.random(in: 0...10000),
                region: "global"
            )
        }

        let (totalMs, _) = try await benchmark("Insert 100 records") {
            try await ctx.database.withTransaction { transaction in
                for score in scores {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfGameScore?,
                        newItem: score,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 100 records: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 100.0 / (totalMs / 1000))) records/s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - 1000 records")
    func testBulkInsert1000Records() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "bulk_insert_1000")

        let scores = (0..<1000).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64.random(in: 0...100000),
                region: ["us", "eu", "asia"][i % 3]
            )
        }

        let (totalMs, _) = try await benchmark("Insert 1000 records") {
            try await ctx.database.withTransaction { transaction in
                for score in scores {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfGameScore?,
                        newItem: score,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 1000 records: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 1000.0 / (totalMs / 1000))) records/s")

        try await ctx.cleanup()
    }

    @Test("Sequential insert performance")
    func testSequentialInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "sequential_insert")

        let scores = (0..<100).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64(i * 100),
                region: "global"
            )
        }

        var totalMs: Double = 0
        for score in scores {
            let (ms, _) = try await benchmark("Insert single") {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfGameScore?,
                        newItem: score,
                        transaction: transaction
                    )
                }
            }
            totalMs += ms
        }

        print("Sequential insert 100 records: \(String(format: "%.2f", totalMs))ms")
        print("Average per insert: \(String(format: "%.2f", totalMs / 100))ms")

        try await ctx.cleanup()
    }
}

// MARK: - Query Performance Tests

@Suite("LeaderboardIndex Query Performance", .tags(.fdb), .serialized)
struct LeaderboardIndexQueryPerformanceTests {

    @Test("Top-K query performance")
    func testTopKQueryPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "topk_query")

        // Setup: Insert 1000 records
        let scores = (0..<1000).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64.random(in: 0...100000),
                region: "global"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Top 10
        var top10Results: [(pk: Tuple, score: Int64)]!
        let (top10Ms, _) = try await benchmark("Top 10 query", iterations: 100) {
            top10Results = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 10, transaction: transaction)
            }
        }

        #expect(top10Results.count == 10)
        print("Top 10 query (100 iterations): \(String(format: "%.2f", top10Ms))ms")
        print("Per query: \(String(format: "%.3f", top10Ms / 100))ms")

        // Benchmark: Top 100
        var top100Results: [(pk: Tuple, score: Int64)]!
        let (top100Ms, _) = try await benchmark("Top 100 query", iterations: 100) {
            top100Results = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 100, transaction: transaction)
            }
        }

        #expect(top100Results.count == 100)
        print("Top 100 query (100 iterations): \(String(format: "%.2f", top100Ms))ms")
        print("Per query: \(String(format: "%.3f", top100Ms / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Rank lookup performance")
    func testRankLookupPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "rank_lookup")

        // Setup: Insert 1000 records with sequential scores
        let scores = (0..<1000).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64(i * 10),  // 0, 10, 20, ... 9990
                region: "global"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Rank #1 (highest score)
        var rank1: Int?
        let (rank1Ms, _) = try await benchmark("Rank #1 lookup", iterations: 100) {
            rank1 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getRank(pk: Tuple("game-999"), transaction: transaction)
            }
        }

        #expect(rank1 == 1)
        print("Rank #1 lookup (100 iterations): \(String(format: "%.2f", rank1Ms))ms")
        print("Per lookup: \(String(format: "%.3f", rank1Ms / 100))ms")

        // Benchmark: Rank #500 (middle)
        var rank500: Int?
        let (rank500Ms, _) = try await benchmark("Rank #500 lookup", iterations: 100) {
            rank500 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getRank(pk: Tuple("game-500"), transaction: transaction)
            }
        }

        #expect(rank500 == 500)
        print("Rank #500 lookup (100 iterations): \(String(format: "%.2f", rank500Ms))ms")
        print("Per lookup: \(String(format: "%.3f", rank500Ms / 100))ms")

        // Benchmark: Rank #1000 (lowest)
        var rank1000: Int?
        let (rank1000Ms, _) = try await benchmark("Rank #1000 lookup", iterations: 100) {
            rank1000 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getRank(pk: Tuple("game-0"), transaction: transaction)
            }
        }

        #expect(rank1000 == 1000)
        print("Rank #1000 lookup (100 iterations): \(String(format: "%.2f", rank1000Ms))ms")
        print("Per lookup: \(String(format: "%.3f", rank1000Ms / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Available windows query performance")
    func testAvailableWindowsPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "available_windows")

        // Setup: Insert records
        let scores = (0..<100).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64.random(in: 0...10000),
                region: "global"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Get available windows
        var windows: [Int64]!
        let (windowsMs, _) = try await benchmark("Get available windows", iterations: 100) {
            windows = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getAvailableWindows(transaction: transaction)
            }
        }

        #expect(!windows.isEmpty)
        print("Get available windows (100 iterations): \(String(format: "%.2f", windowsMs))ms")
        print("Per query: \(String(format: "%.3f", windowsMs / 100))ms")

        try await ctx.cleanup()
    }
}

// MARK: - Update Performance Tests

@Suite("LeaderboardIndex Update Performance", .tags(.fdb), .serialized)
struct LeaderboardIndexUpdatePerformanceTests {

    @Test("Score update performance")
    func testScoreUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "score_update")

        // Setup: Insert 100 records
        var scores = (0..<100).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64(i * 100),
                region: "global"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Update all scores
        let (updateMs, _) = try await benchmark("Update 100 scores") {
            try await ctx.database.withTransaction { transaction in
                for i in 0..<100 {
                    let oldScore = scores[i]
                    var newScore = oldScore
                    newScore.score = oldScore.score + 500

                    try await ctx.maintainer.updateIndex(
                        oldItem: oldScore,
                        newItem: newScore,
                        transaction: transaction
                    )

                    scores[i] = newScore
                }
            }
        }

        print("Update 100 scores: \(String(format: "%.2f", updateMs))ms")
        print("Per update: \(String(format: "%.3f", updateMs / 100))ms")

        // Verify
        let top = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getTopK(k: 1, transaction: transaction)
        }
        #expect(top[0].score == 10400) // 99 * 100 + 500

        try await ctx.cleanup()
    }

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "delete")

        // Setup: Insert 100 records
        let scores = (0..<100).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64(i * 100),
                region: "global"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Delete all
        let (deleteMs, _) = try await benchmark("Delete 100 records") {
            try await ctx.database.withTransaction { transaction in
                for score in scores {
                    try await ctx.maintainer.updateIndex(
                        oldItem: score,
                        newItem: nil as PerfGameScore?,
                        transaction: transaction
                    )
                }
            }
        }

        print("Delete 100 records: \(String(format: "%.2f", deleteMs))ms")
        print("Per delete: \(String(format: "%.3f", deleteMs / 100))ms")

        // Verify
        let top = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getTopK(k: 10, transaction: transaction)
        }
        #expect(top.isEmpty)

        try await ctx.cleanup()
    }
}

// MARK: - Scale Tests

@Suite("LeaderboardIndex Scale Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexScaleTests {

    @Test("Large leaderboard - 10000 entries")
    func testLargeLeaderboard() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "large_leaderboard")

        // Insert 10000 records in batches
        let batchSize = 500
        let totalRecords = 10000

        var insertMs: Double = 0
        for batch in stride(from: 0, to: totalRecords, by: batchSize) {
            let scores = (batch..<min(batch + batchSize, totalRecords)).map { i in
                PerfGameScore(
                    id: "game-\(i)",
                    playerId: "player-\(i)",
                    score: Int64.random(in: 0...1_000_000),
                    region: ["us", "eu", "asia", "other"][i % 4]
                )
            }

            let (batchMs, _) = try await benchmark("Insert batch") {
                try await ctx.database.withTransaction { transaction in
                    for score in scores {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil as PerfGameScore?,
                            newItem: score,
                            transaction: transaction
                        )
                    }
                }
            }
            insertMs += batchMs
        }

        print("Insert \(totalRecords) records: \(String(format: "%.2f", insertMs))ms")
        print("Throughput: \(String(format: "%.0f", Double(totalRecords) / (insertMs / 1000))) records/s")

        // Query performance at scale
        var top10: [(pk: Tuple, score: Int64)]!
        let (top10Ms, _) = try await benchmark("Top 10 from 10K", iterations: 50) {
            top10 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 10, transaction: transaction)
            }
        }

        #expect(top10.count == 10)
        print("Top 10 from 10K entries (50 iterations): \(String(format: "%.2f", top10Ms))ms")
        print("Per query: \(String(format: "%.3f", top10Ms / 50))ms")

        // Top 1000
        var top1000: [(pk: Tuple, score: Int64)]!
        let (top1000Ms, _) = try await benchmark("Top 1000 from 10K", iterations: 10) {
            top1000 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 1000, transaction: transaction)
            }
        }

        #expect(top1000.count == 1000)
        print("Top 1000 from 10K entries (10 iterations): \(String(format: "%.2f", top1000Ms))ms")
        print("Per query: \(String(format: "%.3f", top1000Ms / 10))ms")

        try await ctx.cleanup()
    }

    @Test("ScanItem performance")
    func testScanItemPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "scan_item")

        let scores = (0..<1000).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64.random(in: 0...100000),
                region: "global"
            )
        }

        let (scanMs, _) = try await benchmark("ScanItem 1000 records") {
            try await ctx.database.withTransaction { transaction in
                for score in scores {
                    try await ctx.maintainer.scanItem(
                        score,
                        id: Tuple(score.id),
                        transaction: transaction
                    )
                }
            }
        }

        print("ScanItem 1000 records: \(String(format: "%.2f", scanMs))ms")
        print("Per scanItem: \(String(format: "%.3f", scanMs / 1000))ms")
        print("Throughput: \(String(format: "%.0f", 1000.0 / (scanMs / 1000))) records/s")

        try await ctx.cleanup()
    }

    @Test("Ties handling performance")
    func testTiesHandlingPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "ties")

        // Insert 1000 records with only 10 distinct scores (many ties)
        let scores = (0..<1000).map { i in
            PerfGameScore(
                id: "game-\(i)",
                playerId: "player-\(i)",
                score: Int64(i % 10) * 100,  // 0, 100, 200, ... 900 repeating
                region: "global"
            )
        }

        let (insertMs, _) = try await benchmark("Insert 1000 with ties") {
            try await ctx.database.withTransaction { transaction in
                for score in scores {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfGameScore?,
                        newItem: score,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 1000 with ties: \(String(format: "%.2f", insertMs))ms")

        // Query top 100 (should have many entries with same score)
        var top100: [(pk: Tuple, score: Int64)]!
        let (queryMs, _) = try await benchmark("Top 100 with ties", iterations: 100) {
            top100 = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getTopK(k: 100, transaction: transaction)
            }
        }

        #expect(top100.count == 100)
        print("Top 100 with ties (100 iterations): \(String(format: "%.2f", queryMs))ms")
        print("Per query: \(String(format: "%.3f", queryMs / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Window types performance comparison")
    func testWindowTypesPerformance() async throws {
        try await FDBTestSetup.shared.initialize()

        // Test different window types
        let windowTypes: [(LeaderboardWindowType, String)] = [
            (.hourly, "hourly"),
            (.daily, "daily"),
            (.weekly, "weekly")
        ]

        for (windowType, name) in windowTypes {
            let ctx = try PerfTestContext(testName: "window_\(name)", window: windowType)

            let scores = (0..<100).map { i in
                PerfGameScore(
                    id: "game-\(i)",
                    playerId: "player-\(i)",
                    score: Int64.random(in: 0...10000),
                    region: "global"
                )
            }

            let (insertMs, _) = try await benchmark("Insert 100 (\(name))") {
                try await ctx.database.withTransaction { transaction in
                    for score in scores {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil as PerfGameScore?,
                            newItem: score,
                            transaction: transaction
                        )
                    }
                }
            }

            let (queryMs, _) = try await benchmark("Top 10 (\(name))", iterations: 100) {
                _ = try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getTopK(k: 10, transaction: transaction)
                }
            }

            print("\(name.uppercased()) window:")
            print("  Insert 100: \(String(format: "%.2f", insertMs))ms")
            print("  Top 10 query (100 iterations): \(String(format: "%.2f", queryMs))ms")
            print("  Per query: \(String(format: "%.3f", queryMs / 100))ms")

            try await ctx.cleanup()
        }
    }
}
