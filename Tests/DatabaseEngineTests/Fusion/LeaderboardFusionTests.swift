// LeaderboardFusionTests.swift
// Tests for LeaderboardIndex Fusion query (Leaderboard)

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import LeaderboardIndex

// MARK: - Test Model

/// Game score model with time-windowed leaderboard index
@Persistable
struct LeaderboardTestScore {
    #Directory<LeaderboardTestScore>("test", "leaderboard")
    var id: String = UUID().uuidString
    var playerId: String = ""
    var playerName: String = ""
    var score: Int64 = 0
    var region: String = "global"

    #Index<LeaderboardTestScore>(TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>(
        scoreField: \.score,
        window: .daily,
        windowCount: 7
    ))

    #Index<LeaderboardTestScore>(TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>(
        scoreField: \.score,
        groupBy: [\.region],
        window: .daily,
        windowCount: 7
    ))
}

// MARK: - Unit Tests (No FDB)

@Suite("Leaderboard - Unit Tests")
struct LeaderboardUnitTests {

    @Test("TimeWindowLeaderboardIndexKind identifier")
    func testLeaderboardIndexKindIdentifier() {
        let identifier = TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>.identifier
        #expect(identifier == "time_window_leaderboard")
    }

    @Test("LeaderboardWindowType durations")
    func testWindowDurations() {
        #expect(LeaderboardWindowType.hourly.durationSeconds == 3600)
        #expect(LeaderboardWindowType.daily.durationSeconds == 86400)
        #expect(LeaderboardWindowType.weekly.durationSeconds == 604800)
        #expect(LeaderboardWindowType.monthly.durationSeconds == 2592000)
        #expect(LeaderboardWindowType.custom(duration: 7200).durationSeconds == 7200)
    }

    @Test("LeaderboardWindowType equality")
    func testWindowEquality() {
        #expect(LeaderboardWindowType.hourly == LeaderboardWindowType.hourly)
        #expect(LeaderboardWindowType.hourly != LeaderboardWindowType.daily)
        #expect(LeaderboardWindowType.custom(duration: 100) == LeaderboardWindowType.custom(duration: 100))
        #expect(LeaderboardWindowType.custom(duration: 100) != LeaderboardWindowType.custom(duration: 200))
    }

    @Test("ScoredResult initialization")
    func testScoredResultInitialization() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000)
        let result = ScoredResult(item: score, score: 0.75)

        #expect(result.score == 0.75)
        #expect(result.item.playerName == "Alice")
        #expect(result.item.score == 1000)
    }

    @Test("Rank-based scoring formula")
    func testRankBasedScoring() {
        // Formula: score = 1.0 - (rank / (count - 1))
        func fusionScore(rank: Int, count: Int) -> Double {
            guard count > 1 else { return 1.0 }
            return 1.0 - Double(rank) / Double(count - 1)
        }

        #expect(fusionScore(rank: 0, count: 10) == 1.0)      // 1st place
        #expect(fusionScore(rank: 9, count: 10) == 0.0)      // Last place
        #expect(abs(fusionScore(rank: 4, count: 10) - 0.556) < 0.01)  // Middle

        // Single item case
        #expect(fusionScore(rank: 0, count: 1) == 1.0)
    }

    @Test("Scores sorted by game score descending")
    func testScoresSorting() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 500),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 1000),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 750)
        ]

        let sorted = scores.sorted { $0.score > $1.score }

        #expect(sorted[0].playerName == "Bob")
        #expect(sorted[1].playerName == "Charlie")
        #expect(sorted[2].playerName == "Alice")
    }

    @Test("Filter by region")
    func testFilterByRegion() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000, region: "asia"),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 800, region: "europe"),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 900, region: "asia")
        ]

        let asiaScores = scores.filter { $0.region == "asia" }
        #expect(asiaScores.count == 2)
        #expect(Set(asiaScores.map(\.playerName)) == Set(["Alice", "Charlie"]))
    }

    @Test("Group scores by region")
    func testGroupByRegion() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000, region: "asia"),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 800, region: "europe"),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 900, region: "asia")
        ]

        let grouped = Dictionary(grouping: scores, by: \.region)

        #expect(grouped["asia"]?.count == 2)
        #expect(grouped["europe"]?.count == 1)
    }
}

// MARK: - Edge Case Tests

@Suite("Leaderboard - Edge Cases")
struct LeaderboardEdgeCaseTests {

    @Test("Zero score")
    func testZeroScore() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Newbie", score: 0)
        #expect(score.score == 0)
    }

    @Test("Negative score")
    func testNegativeScore() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Penalty", score: -100)
        #expect(score.score == -100)
    }

    @Test("Int64 boundary scores")
    func testInt64BoundaryScores() {
        let maxScore = LeaderboardTestScore(playerId: "p1", playerName: "Max", score: Int64.max)
        let minScore = LeaderboardTestScore(playerId: "p2", playerName: "Min", score: Int64.min)

        #expect(maxScore.score == Int64.max)
        #expect(minScore.score == Int64.min)
    }

    @Test("Unicode in player name and region")
    func testUnicodeSupport() {
        let score = LeaderboardTestScore(
            playerId: "p1",
            playerName: "日本人プレイヤー",
            score: 1000,
            region: "日本"
        )
        #expect(score.playerName == "日本人プレイヤー")
        #expect(score.region == "日本")
    }

    @Test("Ties in scores")
    func testTiedScores() {
        let scores = (0..<10).map { i in
            LeaderboardTestScore(playerId: "p\(i)", playerName: "Player\(i)", score: 1000)
        }

        #expect(scores.allSatisfy { $0.score == 1000 })

        // Fusion scores should still distribute (based on order)
        let count = scores.count
        for (i, _) in scores.enumerated() {
            let fusion = count > 1 ? 1.0 - Double(i) / Double(count - 1) : 1.0
            #expect(fusion >= 0.0 && fusion <= 1.0)
        }
    }

    @Test("k larger than result count")
    func testKLargerThanResults() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 900)
        ]

        let k = 100
        let topK = Array(scores.prefix(k))

        #expect(topK.count == 2)  // Returns all available, not k
    }
}

// MARK: - Integration Tests

@Suite("Leaderboard - Integration Tests", .tags(.fdb), .serialized)
struct LeaderboardIntegrationTests {

    private func createContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([LeaderboardTestScore.self])
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "leaderboard"])
    }

    @Test("Insert and retrieve scores via FDBContext")
    func testInsertAndRetrieve() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let score = LeaderboardTestScore(
            playerId: "player1",
            playerName: "Alice",
            score: 1000,
            region: "asia"
        )
        let scoreId = score.id

        context.insert(score)
        try await context.save()

        // Fetch back
        let fetched = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == scoreId)
            .first()
        #expect(fetched != nil)
        #expect(fetched?.playerName == "Alice")
        #expect(fetched?.score == 1000)
    }

    @Test("Multiple scores indexed correctly")
    func testMultipleScoresIndexed() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 900),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 800)
        ]

        for score in scores {
            context.insert(score)
        }
        try await context.save()

        // Verify all inserted
        for score in scores {
            let fetched = try await context.fetch(LeaderboardTestScore.self)
                .where(\.id == score.id)
                .first()
            #expect(fetched != nil, "Score for \(score.playerName) should exist")
        }
    }

    @Test("Scores with different regions")
    func testScoresWithDifferentRegions() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let asiaScore = LeaderboardTestScore(
            playerId: "p1",
            playerName: "Alice",
            score: 1000,
            region: "asia"
        )
        let europeScore = LeaderboardTestScore(
            playerId: "p2",
            playerName: "Bob",
            score: 900,
            region: "europe"
        )
        let asiaId = asiaScore.id
        let europeId = europeScore.id

        context.insert(asiaScore)
        context.insert(europeScore)
        try await context.save()

        let fetchedAsia = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == asiaId)
            .first()
        let fetchedEurope = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == europeId)
            .first()

        #expect(fetchedAsia?.region == "asia")
        #expect(fetchedEurope?.region == "europe")
    }

    @Test("Update score")
    func testUpdateScore() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        var score = LeaderboardTestScore(
            playerId: "p1",
            playerName: "Alice",
            score: 500
        )
        let scoreId = score.id

        context.insert(score)
        try await context.save()

        // Update score
        score.score = 1500
        context.insert(score)
        try await context.save()

        let fetched = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == scoreId)
            .first()
        #expect(fetched?.score == 1500)
    }

    @Test("Delete score")
    func testDeleteScore() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let score = LeaderboardTestScore(
            playerId: "p1",
            playerName: "ToDelete",
            score: 100
        )
        let scoreId = score.id

        context.insert(score)
        try await context.save()

        // Verify exists
        let beforeDelete = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == scoreId)
            .first()
        #expect(beforeDelete != nil)

        // Delete
        context.delete(score)
        try await context.save()

        // Verify deleted
        let afterDelete = try await context.fetch(LeaderboardTestScore.self)
            .where(\.id == scoreId)
            .first()
        #expect(afterDelete == nil)
    }
}

// MARK: - Index Descriptor Tests

@Suite("Leaderboard - Index Descriptors")
struct LeaderboardIndexDescriptorTests {

    @Test("Index descriptors are correctly defined")
    func testIndexDescriptors() {
        let descriptors = LeaderboardTestScore.indexDescriptors

        // Should have at least the leaderboard indexes
        let leaderboardIndexes = descriptors.filter {
            $0.kindIdentifier == "time_window_leaderboard"
        }

        #expect(leaderboardIndexes.count >= 1, "Should have at least one leaderboard index")
    }

    @Test("Index descriptor has correct field names")
    func testIndexDescriptorFieldNames() {
        let descriptors = LeaderboardTestScore.indexDescriptors

        let scoreIndex = descriptors.first { descriptor in
            guard let kind = descriptor.kind as? TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64> else {
                return false
            }
            return kind.fieldNames.contains("score") && !kind.fieldNames.contains("region")
        }

        #expect(scoreIndex != nil, "Should have a score-only leaderboard index")
    }

    @Test("Grouped index has groupBy field")
    func testGroupedIndexDescriptor() {
        let descriptors = LeaderboardTestScore.indexDescriptors

        let regionIndex = descriptors.first { descriptor in
            guard let kind = descriptor.kind as? TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64> else {
                return false
            }
            return kind.fieldNames.contains("region")
        }

        #expect(regionIndex != nil, "Should have a region-grouped leaderboard index")
    }
}

// MARK: - Error Tests

@Suite("Leaderboard - Error Handling")
struct LeaderboardErrorTests {

    @Test("FusionQueryError descriptions")
    func testFusionQueryErrorDescriptions() {
        let error = FusionQueryError.indexNotFound(
            type: "LeaderboardTestScore",
            field: "unknownField",
            kind: "leaderboard"
        )

        #expect(error.description.contains("leaderboard"))
        #expect(error.description.contains("unknownField"))
        #expect(error.description.contains("LeaderboardTestScore"))
    }
}
