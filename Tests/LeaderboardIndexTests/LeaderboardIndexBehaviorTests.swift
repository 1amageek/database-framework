// LeaderboardIndexBehaviorTests.swift
// Comprehensive tests for LeaderboardIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import LeaderboardIndex

// MARK: - Test Model

struct TestGameScore: Persistable {
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

    static var persistableType: String { "TestGameScore" }
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

    static func fieldName<Value>(for keyPath: KeyPath<TestGameScore, Value>) -> String {
        switch keyPath {
        case \TestGameScore.id: return "id"
        case \TestGameScore.playerId: return "playerId"
        case \TestGameScore.score: return "score"
        case \TestGameScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestGameScore>) -> String {
        switch keyPath {
        case \TestGameScore.id: return "id"
        case \TestGameScore.playerId: return "playerId"
        case \TestGameScore.score: return "score"
        case \TestGameScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestGameScore> {
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
    let maintainer: TimeWindowLeaderboardIndexMaintainer<TestGameScore, Int64>
    let kind: TimeWindowLeaderboardIndexKind<TestGameScore, Int64>

    init(
        indexName: String = "TestGameScore_leaderboard_score",
        window: LeaderboardWindowType = .daily,
        windowCount: Int = 7
    ) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "leaderboard", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = TimeWindowLeaderboardIndexKind<TestGameScore, Int64>(
            scoreField: \.score,
            groupBy: [],
            window: window,
            windowCount: windowCount
        )

        // Build expression for score field
        let rootExpression: KeyExpression = FieldKeyExpression(fieldName: "score")

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: rootExpression,
            keyPaths: [\TestGameScore.score],  // Required for extractScore()
            subspaceKey: indexName,
            itemTypes: Set(["TestGameScore"])
        )

        self.maintainer = TimeWindowLeaderboardIndexMaintainer<TestGameScore, Int64>(
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

    func getTopK(k: Int, grouping: [any TupleElement]? = nil) async throws -> [(pk: Tuple, score: Int64)] {
        try await database.withTransaction { transaction in
            try await maintainer.getTopK(k: k, grouping: grouping, transaction: transaction)
        }
    }

    func getRank(pk: Tuple, grouping: [any TupleElement]? = nil) async throws -> Int? {
        try await database.withTransaction { transaction in
            try await maintainer.getRank(pk: pk, grouping: grouping, transaction: transaction)
        }
    }

    func getAvailableWindows() async throws -> [Int64] {
        try await database.withTransaction { transaction in
            try await maintainer.getAvailableWindows(transaction: transaction)
        }
    }
}

// MARK: - Insert Tests

@Suite("LeaderboardIndex Insert Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexInsertTests {

    @Test("Insert adds to leaderboard")
    func testInsertAddsToLeaderboard() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 1, "Should have 1 entry after insert")
        #expect(top[0].score == 1000, "Score should be 1000")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create leaderboard order")
    func testMultipleInsertsCreateOrder() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 500),
            TestGameScore(id: "g2", playerId: "player2", score: 1500),
            TestGameScore(id: "g3", playerId: "player3", score: 1000),
            TestGameScore(id: "g4", playerId: "player4", score: 2000)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 4, "Should have 4 entries")

        // Verify descending order
        #expect(top[0].score == 2000, "First should be 2000")
        #expect(top[1].score == 1500, "Second should be 1500")
        #expect(top[2].score == 1000, "Third should be 1000")
        #expect(top[3].score == 500, "Fourth should be 500")

        try await ctx.cleanup()
    }
}

// MARK: - Delete Tests

@Suite("LeaderboardIndex Delete Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexDeleteTests {

    @Test("Delete removes from leaderboard")
    func testDeleteRemovesFromLeaderboard() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let topBefore = try await ctx.getTopK(k: 10)
        #expect(topBefore.count == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: score,
                newItem: nil as TestGameScore?,
                transaction: transaction
            )
        }

        let topAfter = try await ctx.getTopK(k: 10)
        #expect(topAfter.count == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    @Test("Delete one maintains others")
    func testDeleteOneMaintainsOthers() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 1000),
            TestGameScore(id: "g2", playerId: "player2", score: 2000),
            TestGameScore(id: "g3", playerId: "player3", score: 3000)
        ]

        // Insert all
        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Delete middle score
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: scores[1],  // 2000
                newItem: nil as TestGameScore?,
                transaction: transaction
            )
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 2, "Should have 2 entries")
        #expect(top[0].score == 3000, "First should be 3000")
        #expect(top[1].score == 1000, "Second should be 1000")

        try await ctx.cleanup()
    }
}

// MARK: - Update Tests

@Suite("LeaderboardIndex Update Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexUpdateTests {

    @Test("Update score changes rank")
    func testUpdateScoreChangesRank() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 1000),
            TestGameScore(id: "g2", playerId: "player2", score: 2000),
            TestGameScore(id: "g3", playerId: "player3", score: 3000)
        ]

        // Insert all
        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Update player1 score from 1000 to 5000 (should become #1)
        let updatedScore = TestGameScore(id: "g1", playerId: "player1", score: 5000)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: scores[0],
                newItem: updatedScore,
                transaction: transaction
            )
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 3, "Should still have 3 entries")
        #expect(top[0].score == 5000, "First should now be 5000")
        #expect(top[1].score == 3000, "Second should be 3000")
        #expect(top[2].score == 2000, "Third should be 2000")

        // Verify primary key in first position
        let firstPK = top[0].pk[0] as? String
        #expect(firstPK == "g1", "First place should be g1")

        try await ctx.cleanup()
    }

    @Test("Update non-score field keeps position")
    func testUpdateNonScoreFieldKeepsPosition() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let oldScore = TestGameScore(id: "g1", playerId: "player1", score: 1000, region: "us")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: oldScore,
                transaction: transaction
            )
        }

        // Update region (non-indexed field)
        let newScore = TestGameScore(id: "g1", playerId: "player1", score: 1000, region: "eu")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: oldScore,
                newItem: newScore,
                transaction: transaction
            )
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 1, "Should still have 1 entry")
        #expect(top[0].score == 1000, "Score should remain 1000")

        try await ctx.cleanup()
    }
}

// MARK: - TopK Tests

@Suite("LeaderboardIndex TopK Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexTopKTests {

    @Test("getTopK returns correct count")
    func testGetTopKReturnsCorrectCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 10 scores
        try await ctx.database.withTransaction { transaction in
            for i in 1...10 {
                let score = TestGameScore(id: "g\(i)", playerId: "player\(i)", score: Int64(i * 100))
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Get top 5
        let top5 = try await ctx.getTopK(k: 5)
        #expect(top5.count == 5, "Should return exactly 5")

        // Scores should be 1000, 900, 800, 700, 600
        #expect(top5[0].score == 1000)
        #expect(top5[1].score == 900)
        #expect(top5[2].score == 800)
        #expect(top5[3].score == 700)
        #expect(top5[4].score == 600)

        try await ctx.cleanup()
    }

    @Test("getTopK returns all when k > count")
    func testGetTopKReturnsAllWhenKGreaterThanCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 3 scores
        try await ctx.database.withTransaction { transaction in
            for i in 1...3 {
                let score = TestGameScore(id: "g\(i)", playerId: "player\(i)", score: Int64(i * 100))
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Get top 100 (more than available)
        let top = try await ctx.getTopK(k: 100)
        #expect(top.count == 3, "Should return all 3 entries")

        try await ctx.cleanup()
    }

    @Test("getTopK empty when no entries")
    func testGetTopKEmptyWhenNoEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let top = try await ctx.getTopK(k: 10)
        #expect(top.isEmpty, "Should be empty")

        try await ctx.cleanup()
    }
}

// MARK: - Rank Tests

@Suite("LeaderboardIndex Rank Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexRankTests {

    @Test("getRank returns correct position")
    func testGetRankReturnsCorrectPosition() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 100),
            TestGameScore(id: "g2", playerId: "player2", score: 500),
            TestGameScore(id: "g3", playerId: "player3", score: 1000)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Get ranks (1-based)
        let rank1 = try await ctx.getRank(pk: Tuple("g3"))  // 1000 -> #1
        let rank2 = try await ctx.getRank(pk: Tuple("g2"))  // 500 -> #2
        let rank3 = try await ctx.getRank(pk: Tuple("g1"))  // 100 -> #3

        #expect(rank1 == 1, "Score 1000 should be rank 1")
        #expect(rank2 == 2, "Score 500 should be rank 2")
        #expect(rank3 == 3, "Score 100 should be rank 3")

        try await ctx.cleanup()
    }

    @Test("getRank returns nil for non-existent")
    func testGetRankReturnsNilForNonExistent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let rank = try await ctx.getRank(pk: Tuple("nonexistent"))
        #expect(rank == nil, "Should return nil for non-existent entry")

        try await ctx.cleanup()
    }
}

// MARK: - Ties Tests

@Suite("LeaderboardIndex Ties Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexTiesTests {

    @Test("Ties are handled correctly")
    func testTiesHandled() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Multiple players with same score
        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 1000),
            TestGameScore(id: "g2", playerId: "player2", score: 1000),
            TestGameScore(id: "g3", playerId: "player3", score: 1000),
            TestGameScore(id: "g4", playerId: "player4", score: 500)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 4, "Should have 4 entries")

        // All 1000 scores should be before 500
        let scores1000 = top.prefix(3)
        let score500 = top.last

        for entry in scores1000 {
            #expect(entry.score == 1000, "First 3 should be 1000")
        }
        #expect(score500?.score == 500, "Last should be 500")

        try await ctx.cleanup()
    }
}

// MARK: - Window Tests

@Suite("LeaderboardIndex Window Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexWindowTests {

    @Test("Available windows are tracked")
    func testAvailableWindowsTracked() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let windows = try await ctx.getAvailableWindows()
        #expect(!windows.isEmpty, "Should have at least one window")

        try await ctx.cleanup()
    }

    @Test("Window uses correct duration")
    func testWindowUsesCorrectDuration() async throws {
        try await FDBTestSetup.shared.initialize()

        // Test hourly window
        let hourlyCtx = try TestContext(indexName: "hourly_test", window: .hourly)

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)

        try await hourlyCtx.database.withTransaction { transaction in
            try await hourlyCtx.maintainer.updateIndex(
                oldItem: nil as TestGameScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let windows = try await hourlyCtx.getAvailableWindows()
        #expect(!windows.isEmpty)

        // Window ID should be timestamp / 3600 (hourly)
        let expectedWindowId = Int64(Date().timeIntervalSince1970) / 3600
        #expect(windows.contains(expectedWindowId), "Should contain current hourly window")

        try await hourlyCtx.cleanup()
    }
}

// MARK: - ScanItem Tests

@Suite("LeaderboardIndex ScanItem Tests", .tags(.fdb), .serialized)
struct LeaderboardIndexScanItemTests {

    @Test("scanItem adds to leaderboard")
    func testScanItemAddsToLeaderboard() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 1000),
            TestGameScore(id: "g2", playerId: "player2", score: 2000)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.scanItem(
                    score,
                    id: Tuple(score.id),
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 2, "Should have 2 entries")
        #expect(top[0].score == 2000, "First should be 2000")
        #expect(top[1].score == 1000, "Second should be 1000")

        try await ctx.cleanup()
    }
}

// MARK: - Edge Cases Tests

@Suite("LeaderboardIndex Edge Cases", .tags(.fdb), .serialized)
struct LeaderboardIndexEdgeCasesTests {

    @Test("Large scores work correctly")
    func testLargeScores() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: Int64.max - 100),
            TestGameScore(id: "g2", playerId: "player2", score: Int64.max - 200),
            TestGameScore(id: "g3", playerId: "player3", score: Int64.max - 50)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 3)
        #expect(top[0].score == Int64.max - 50, "Highest score should be first")

        try await ctx.cleanup()
    }

    @Test("Zero scores work correctly")
    func testZeroScores() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: 0),
            TestGameScore(id: "g2", playerId: "player2", score: 100),
            TestGameScore(id: "g3", playerId: "player3", score: 0)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 3)
        #expect(top[0].score == 100, "100 should be first")
        #expect(top[1].score == 0, "Second should be 0")
        #expect(top[2].score == 0, "Third should be 0")

        try await ctx.cleanup()
    }

    @Test("Negative scores work correctly")
    func testNegativeScores() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            TestGameScore(id: "g1", playerId: "player1", score: -100),
            TestGameScore(id: "g2", playerId: "player2", score: 100),
            TestGameScore(id: "g3", playerId: "player3", score: -50)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let top = try await ctx.getTopK(k: 10)
        #expect(top.count == 3)
        #expect(top[0].score == 100, "100 should be first")
        #expect(top[1].score == -50, "-50 should be second")
        #expect(top[2].score == -100, "-100 should be third")

        try await ctx.cleanup()
    }

    @Test("Large number of entries")
    func testLargeNumberOfEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 100 scores
        try await ctx.database.withTransaction { transaction in
            for i in 1...100 {
                let score = TestGameScore(id: "g\(i)", playerId: "player\(i)", score: Int64(i * 10))
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestGameScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        // Get top 10
        let top10 = try await ctx.getTopK(k: 10)
        #expect(top10.count == 10, "Should return exactly 10")
        #expect(top10[0].score == 1000, "Top score should be 1000")
        #expect(top10[9].score == 910, "10th score should be 910")

        // Get top 100
        let top100 = try await ctx.getTopK(k: 100)
        #expect(top100.count == 100, "Should return all 100")

        try await ctx.cleanup()
    }

    @Test("computeIndexKeys returns expected keys")
    func testComputeIndexKeys() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = TestGameScore(id: "g1", playerId: "player1", score: 1000)
        let keys = try await ctx.maintainer.computeIndexKeys(for: score, id: Tuple("g1"))

        #expect(!keys.isEmpty, "Should have at least one index key")

        try await ctx.cleanup()
    }
}
