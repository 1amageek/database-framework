#if FOUNDATION_DB
// RankIndexBehaviorTests.swift
// Integration tests for RankIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import RankIndex

// MARK: - Test Model

@Persistable
struct RankedPlayer {
    var id: String = UUID().uuidString
    var name: String
    var score: Int64
}

// MARK: - Rank Index Context

private struct RankIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: RankIndexMaintainer<RankedPlayer, Int64>

    init(indexName: String = "RankedPlayer_score") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "rank", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: score
        let index = try ResolvedIndex(
            for: RankedPlayer.self,
            name: indexName,
            definition: rankIndexDefinition(fieldNumber: 3),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            itemTypes: Set(["RankedPlayer"])
        )

        self.maintainer = RankIndexMaintainer<RankedPlayer, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getTopK(k: Int) async throws -> [(score: Int64, primaryKey: [any TupleElement])] {
        try await database.withTransaction { transaction in
            try await maintainer.getTopK(
                k: k,
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget(),
                    monotonicClock: TestProcessMonotonicClock()
                )
            )
        }
    }

    func getRank(score: Int64) async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getRank(score: score, transaction: transaction)
        }
    }

    func getCount() async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getCount(
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget(),
                    monotonicClock: TestProcessMonotonicClock()
                )
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("RankIndex Behavior Tests", .tags(.fdb), .foundationDBScenario, .heartbeat)
struct RankIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to ranking")
    func testInsertAddsToRanking() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let player = RankedPlayer(id: "p1", name: "Alice", score: 1000)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RankedPlayer?,
                newItem: player,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount()
        #expect(count == 1, "Should have 1 entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create leaderboard")
    func testMultipleInserts() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let players = [
            RankedPlayer(id: "p1", name: "Alice", score: 1000),
            RankedPlayer(id: "p2", name: "Bob", score: 1500),
            RankedPlayer(id: "p3", name: "Charlie", score: 800),
        ]

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RankedPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount()
        #expect(count == 3, "Should have 3 entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from ranking")
    func testDeleteRemovesFromRanking() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let player = RankedPlayer(id: "p1", name: "Alice", score: 1000)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RankedPlayer?,
                newItem: player,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.getCount()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: player,
                newItem: nil as RankedPlayer?,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.getCount()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update changes rank")
    func testUpdateChangesRank() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let player = RankedPlayer(id: "p1", name: "Alice", score: 500)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RankedPlayer?,
                newItem: player,
                transaction: transaction
            )
        }

        // Update with higher score
        let updatedPlayer = RankedPlayer(id: "p1", name: "Alice", score: 1500)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: player,
                newItem: updatedPlayer,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount()
        #expect(count == 1, "Should still have 1 entry")

        let topK = try await ctx.getTopK(k: 1)
        #expect(topK.first?.score == 1500, "Top score should be 1500")

        try await ctx.cleanup()
    }

    // MARK: - Top-K Tests

    @Test("getTopK rejects a negative count")
    func testGetTopKRejectsNegativeCount() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        await #expect(throws: RankScannerError.negativeIndex(-1)) {
            try await ctx.getTopK(k: -1)
        }

        try await ctx.cleanup()
    }

    @Test("getTopN returns top items")
    func testGetTopNReturnsTopItems() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let players = [
            RankedPlayer(id: "p1", name: "Low", score: 100),
            RankedPlayer(id: "p2", name: "Medium", score: 500),
            RankedPlayer(id: "p3", name: "High", score: 1000),
            RankedPlayer(id: "p4", name: "VeryHigh", score: 2000),
        ]

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RankedPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Get top 2
        let top2 = try await ctx.getTopK(k: 2)
        #expect(top2.count == 2, "Should return 2 items")
        #expect(top2[0].score == 2000, "First should be highest (2000)")
        #expect(top2[1].score == 1000, "Second should be 1000")

        // Get top 10 (more than available)
        let top10 = try await ctx.getTopK(k: 10)
        #expect(top10.count == 4, "Should return all 4 items")

        try await ctx.cleanup()
    }

    // MARK: - Rank Query Tests

    @Test("getRank returns correct position")
    func testGetRankReturnsCorrectPosition() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let players = [
            RankedPlayer(id: "p1", name: "Third", score: 100),
            RankedPlayer(id: "p2", name: "Second", score: 500),
            RankedPlayer(id: "p3", name: "First", score: 1000),
        ]

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RankedPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Rank of highest score (1000) should be 0 (no one above)
        let rank1000 = try await ctx.getRank(score: 1000)
        #expect(rank1000 == 0, "Score 1000 should be rank 0")

        // Rank of middle score (500) should be 1 (one person above)
        let rank500 = try await ctx.getRank(score: 500)
        #expect(rank500 == 1, "Score 500 should be rank 1")

        // Rank of lowest score (100) should be 2 (two people above)
        let rank100 = try await ctx.getRank(score: 100)
        #expect(rank100 == 2, "Score 100 should be rank 2")

        try await ctx.cleanup()
    }

    // MARK: - Ties Tests

    @Test("Ties handled correctly")
    func testTiesHandledCorrectly() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        // Multiple players with same score
        let players = [
            RankedPlayer(id: "p1", name: "Alice", score: 1000),
            RankedPlayer(id: "p2", name: "Bob", score: 1000),
            RankedPlayer(id: "p3", name: "Charlie", score: 1000),
            RankedPlayer(id: "p4", name: "Low", score: 500),
        ]

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RankedPlayer?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount()
        #expect(count == 4, "Should have 4 entries including ties")

        // All 1000 scores should have same rank (0)
        let rank1000 = try await ctx.getRank(score: 1000)
        #expect(rank1000 == 0, "Score 1000 should be rank 0 (no one above)")

        // Score 500 should have rank 3 (three players above)
        let rank500 = try await ctx.getRank(score: 500)
        #expect(rank500 == 3, "Score 500 should be rank 3")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to ranking")
    func testScanItemAddsToRanking() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await RankIndexContext()

        let players = [
            RankedPlayer(id: "p1", name: "Alice", score: 1000),
            RankedPlayer(id: "p2", name: "Bob", score: 500),
        ]

        try await ctx.database.withTransaction { transaction in
            for player in players {
                try await ctx.maintainer.scanItem(
                    player,
                    id: Tuple(player.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount()
        #expect(count == 2, "Should have 2 entries after scanItem")

        let topK = try await ctx.getTopK(k: 2)
        #expect(topK[0].score == 1000, "Top score should be 1000")

        try await ctx.cleanup()
    }
}
#endif
