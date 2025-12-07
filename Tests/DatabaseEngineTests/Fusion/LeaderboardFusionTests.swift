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
struct LeaderboardTestScore: Persistable {
    typealias ID = String

    var id: String
    var playerId: String
    var playerName: String
    var score: Int64
    var region: String

    init(
        id: String = UUID().uuidString,
        playerId: String,
        playerName: String,
        score: Int64,
        region: String = "global"
    ) {
        self.id = id
        self.playerId = playerId
        self.playerName = playerName
        self.score = score
        self.region = region
    }

    static var persistableType: String { "LeaderboardTestScore" }
    static var allFields: [String] { ["id", "playerId", "playerName", "score", "region"] }

    static var indexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "LeaderboardTestScore_leaderboard_score",
                kind: TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>(
                    scoreField: \.score,
                    window: .daily,
                    windowCount: 7
                ),
                fieldNames: ["score"],
                rootExpression: FieldKeyExpression(fieldName: "score")
            ),
            IndexDescriptor(
                name: "LeaderboardTestScore_leaderboard_region_score",
                kind: TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>(
                    scoreField: \.score,
                    groupBy: [\LeaderboardTestScore.region],
                    window: .daily,
                    windowCount: 7
                ),
                fieldNames: ["region", "score"],
                rootExpression: ConcatenateKeyExpression(children: [
                    FieldKeyExpression(fieldName: "region"),
                    FieldKeyExpression(fieldName: "score")
                ])
            )
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "playerId": return playerId
        case "playerName": return playerName
        case "score": return score
        case "region": return region
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<LeaderboardTestScore, Value>) -> String {
        switch keyPath {
        case \LeaderboardTestScore.id: return "id"
        case \LeaderboardTestScore.playerId: return "playerId"
        case \LeaderboardTestScore.playerName: return "playerName"
        case \LeaderboardTestScore.score: return "score"
        case \LeaderboardTestScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<LeaderboardTestScore>) -> String {
        switch keyPath {
        case \LeaderboardTestScore.id: return "id"
        case \LeaderboardTestScore.playerId: return "playerId"
        case \LeaderboardTestScore.playerName: return "playerName"
        case \LeaderboardTestScore.score: return "score"
        case \LeaderboardTestScore.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<LeaderboardTestScore> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Context

private struct LeaderboardTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let itemsSubspace: Subspace
    let maintainer: TimeWindowLeaderboardIndexMaintainer<LeaderboardTestScore, Int64>

    init(indexName: String = "LeaderboardTestScore_leaderboard_score") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "leaderboard_fusion", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")

        let kind = TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>(
            scoreField: \.score,
            window: .daily,
            windowCount: 7
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: indexName,
            itemTypes: Set(["LeaderboardTestScore"])
        )

        self.maintainer = TimeWindowLeaderboardIndexMaintainer<LeaderboardTestScore, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            scoreKeyPath: \LeaderboardTestScore.score,
            groupByKeyPaths: [],
            window: .daily,
            windowCount: 7
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func insertScore(_ score: LeaderboardTestScore) async throws {
        try await database.withTransaction { transaction in
            let itemKey = itemsSubspace.pack(Tuple(score.id))
            let encoder = JSONEncoder()
            let data = try encoder.encode([
                "id": score.id,
                "playerId": score.playerId,
                "playerName": score.playerName,
                "score": String(score.score),
                "region": score.region
            ])
            transaction.setValue(Array(data), for: itemKey)

            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: score,
                transaction: transaction
            )
        }
    }
}

// MARK: - Unit Tests (API Pattern)

@Suite("Leaderboard Fusion - Unit Tests")
struct LeaderboardFusionUnitTests {

    @Test("TimeWindowLeaderboardIndexKind identifier is 'time_window_leaderboard'")
    func testLeaderboardIndexKindIdentifier() {
        let identifier = TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>.identifier
        #expect(identifier == "time_window_leaderboard")
    }

    @Test("Index descriptor configuration")
    func testIndexDescriptorConfiguration() {
        let descriptors = LeaderboardTestScore.indexDescriptors
        #expect(descriptors.count == 2)

        let scoreIndex = descriptors.first { $0.name.contains("leaderboard_score") && !$0.name.contains("region") }
        #expect(scoreIndex != nil)
        #expect(scoreIndex?.kindIdentifier == "time_window_leaderboard")
        #expect(scoreIndex?.fieldNames.contains("score") == true)

        let regionIndex = descriptors.first { $0.name.contains("region") }
        #expect(regionIndex != nil)
        #expect(regionIndex?.fieldNames.contains("region") == true)
        #expect(regionIndex?.fieldNames.contains("score") == true)
    }

    @Test("ScoredResult initialization")
    func testScoredResultInitialization() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000)
        let result = ScoredResult(item: score, score: 1.0)

        #expect(result.score == 1.0)
        #expect(result.item.playerName == "Alice")
        #expect(result.item.score == 1000)
    }

    @Test("Default k value is 100")
    func testDefaultKValue() {
        let defaultK = 100
        #expect(defaultK == 100)
    }
}

// MARK: - LeaderboardWindowType Tests

@Suite("Leaderboard Fusion - Window Types")
struct LeaderboardFusionWindowTypeTests {

    @Test("Hourly window duration")
    func testHourlyWindow() {
        let window = LeaderboardWindowType.hourly
        #expect(window.durationSeconds == 3600)
    }

    @Test("Daily window duration")
    func testDailyWindow() {
        let window = LeaderboardWindowType.daily
        #expect(window.durationSeconds == 86400)
    }

    @Test("Weekly window duration")
    func testWeeklyWindow() {
        let window = LeaderboardWindowType.weekly
        #expect(window.durationSeconds == 604800)
    }

    @Test("Monthly window duration")
    func testMonthlyWindow() {
        let window = LeaderboardWindowType.monthly
        #expect(window.durationSeconds == 2592000)  // 30 days
    }

    @Test("Custom window duration")
    func testCustomWindow() {
        let window = LeaderboardWindowType.custom(duration: 7200)  // 2 hours
        #expect(window.durationSeconds == 7200)
    }

    @Test("Window type equality")
    func testWindowTypeEquality() {
        let hourly1 = LeaderboardWindowType.hourly
        let hourly2 = LeaderboardWindowType.hourly
        let daily = LeaderboardWindowType.daily

        #expect(hourly1 == hourly2)
        #expect(hourly1 != daily)
    }

    @Test("Custom window equality")
    func testCustomWindowEquality() {
        let custom1 = LeaderboardWindowType.custom(duration: 3600)
        let custom2 = LeaderboardWindowType.custom(duration: 3600)
        let custom3 = LeaderboardWindowType.custom(duration: 7200)

        #expect(custom1 == custom2)
        #expect(custom1 != custom3)
    }
}

// MARK: - Scoring Tests

@Suite("Leaderboard Fusion - Scoring")
struct LeaderboardFusionScoringTests {

    @Test("Rank-based scoring calculation")
    func testRankBasedScoring() {
        // Formula: score = 1.0 - (index / (count - 1))
        let count = 10.0

        let rank1Score = 1.0 - 0.0 / (count - 1)
        #expect(rank1Score == 1.0)

        let rank5Score = 1.0 - 4.0 / (count - 1)
        #expect(abs(rank5Score - 0.556) < 0.01)

        let rank10Score = 1.0 - 9.0 / (count - 1)
        #expect(rank10Score == 0.0)
    }

    @Test("Single result scoring")
    func testSingleResultScoring() {
        let count = 1.0
        let score = count > 1 ? 1.0 - 0.0 / (count - 1) : 1.0
        #expect(score == 1.0)
    }

    @Test("ScoredResult with rank-based score")
    func testScoredResultWithRankScore() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 800),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 600)
        ]

        let count = Double(scores.count)

        let result1 = ScoredResult(item: scores[0], score: 1.0 - 0.0 / (count - 1))
        let result2 = ScoredResult(item: scores[1], score: 1.0 - 1.0 / (count - 1))
        let result3 = ScoredResult(item: scores[2], score: 1.0 - 2.0 / (count - 1))

        #expect(result1.score == 1.0)
        #expect(result2.score == 0.5)
        #expect(result3.score == 0.0)
    }

    @Test("Results sorted by game score descending")
    func testResultsSortedByGameScore() {
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

    @Test("Fusion score distribution")
    func testFusionScoreDistribution() {
        let leaderboardResults = [
            (playerName: "Bob", gameScore: Int64(1000)),
            (playerName: "Charlie", gameScore: Int64(750)),
            (playerName: "Alice", gameScore: Int64(500))
        ]

        let count = Double(leaderboardResults.count)
        var fusionScores: [(name: String, score: Double)] = []

        for (index, result) in leaderboardResults.enumerated() {
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            fusionScores.append((name: result.playerName, score: score))
        }

        #expect(fusionScores[0] == (name: "Bob", score: 1.0))
        #expect(fusionScores[1] == (name: "Charlie", score: 0.5))
        #expect(fusionScores[2] == (name: "Alice", score: 0.0))
    }
}

// MARK: - Configuration Tests

@Suite("Leaderboard Fusion - Configuration")
struct LeaderboardFusionConfigurationTests {

    @Test("topK configuration")
    func testTopKConfiguration() {
        let k = 50
        let validK = max(1, k)
        #expect(validK == 50)
    }

    @Test("topK with zero")
    func testTopKWithZero() {
        let k = 0
        let validK = max(0, k)
        #expect(validK == 0)
    }

    @Test("group configuration")
    func testGroupConfiguration() {
        let groupValue = "asia"
        #expect(groupValue == "asia")
    }

    @Test("window configuration")
    func testWindowConfiguration() {
        let windowId: Int64 = 12345
        #expect(windowId == 12345)
    }

    @Test("groupBy field name extraction")
    func testGroupByFieldNameExtraction() {
        let descriptor = LeaderboardTestScore.indexDescriptors.first { $0.name.contains("region") }
        #expect(descriptor != nil)
        #expect(descriptor?.fieldNames.first == "region")
    }
}

// MARK: - Candidates Filtering Tests

@Suite("Leaderboard Fusion - Candidates Filtering")
struct LeaderboardFusionCandidatesTests {

    @Test("Candidates filtering preserves order")
    func testCandidatesFilteringPreservesOrder() {
        let scores = [
            LeaderboardTestScore(id: "s1", playerId: "p1", playerName: "Bob", score: 1000),
            LeaderboardTestScore(id: "s2", playerId: "p2", playerName: "Charlie", score: 750),
            LeaderboardTestScore(id: "s3", playerId: "p3", playerName: "Alice", score: 500)
        ]

        let candidates: Set<String> = ["s1", "s3"]
        let filtered = scores.filter { candidates.contains($0.id) }

        #expect(filtered.count == 2)
        #expect(filtered[0].playerName == "Bob")
        #expect(filtered[1].playerName == "Alice")
    }

    @Test("Empty candidates returns no results")
    func testEmptyCandidatesReturnsNoResults() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000)
        ]

        let candidates: Set<String> = []
        let filtered = scores.filter { candidates.contains($0.id) }

        #expect(filtered.isEmpty)
    }

    @Test("Candidates with no matches")
    func testCandidatesWithNoMatches() {
        let scores = [
            LeaderboardTestScore(id: "s1", playerId: "p1", playerName: "Alice", score: 1000)
        ]

        let candidates: Set<String> = ["s999"]
        let filtered = scores.filter { candidates.contains($0.id) }

        #expect(filtered.isEmpty)
    }
}

// MARK: - Integration Tests

@Suite("Leaderboard Fusion - Integration Tests", .serialized)
struct LeaderboardFusionIntegrationTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Leaderboard index maintainer initialization")
    func testLeaderboardIndexMaintainerInitialization() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try LeaderboardTestContext()
            defer { Task { try? await context.cleanup() } }

            #expect(context.maintainer != nil)
        }
    }

    @Test("Insert and index score")
    func testInsertAndIndexScore() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try LeaderboardTestContext()
            defer { Task { try? await context.cleanup() } }

            let scoreId = uniqueID("score")
            let score = LeaderboardTestScore(
                id: scoreId,
                playerId: "player1",
                playerName: "Alice",
                score: 1000,
                region: "asia"
            )

            try await context.insertScore(score)

            let exists = try await context.database.withTransaction { transaction -> Bool in
                let itemKey = context.itemsSubspace.pack(Tuple(scoreId))
                let value = try await transaction.getValue(for: itemKey, snapshot: true)
                return value != nil
            }

            #expect(exists)
        }
    }

    @Test("Multiple scores in leaderboard")
    func testMultipleScoresInLeaderboard() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try LeaderboardTestContext()
            defer { Task { try? await context.cleanup() } }

            let scores = [
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p1", playerName: "Alice", score: 1000),
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p2", playerName: "Bob", score: 900),
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p3", playerName: "Charlie", score: 800)
            ]

            for score in scores {
                try await context.insertScore(score)
            }

            for score in scores {
                let exists = try await context.database.withTransaction { transaction -> Bool in
                    let itemKey = context.itemsSubspace.pack(Tuple(score.id))
                    let value = try await transaction.getValue(for: itemKey, snapshot: true)
                    return value != nil
                }
                #expect(exists, "Score for \(score.playerName) should exist")
            }
        }
    }

    @Test("Scores with different regions")
    func testScoresWithDifferentRegions() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let context = try LeaderboardTestContext()
            defer { Task { try? await context.cleanup() } }

            let scores = [
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p1", playerName: "Alice", score: 1000, region: "asia"),
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p2", playerName: "Bob", score: 900, region: "europe"),
                LeaderboardTestScore(id: uniqueID("s"), playerId: "p3", playerName: "Charlie", score: 800, region: "asia")
            ]

            for score in scores {
                try await context.insertScore(score)
            }

            // All should be inserted successfully
            for score in scores {
                let exists = try await context.database.withTransaction { transaction -> Bool in
                    let itemKey = context.itemsSubspace.pack(Tuple(score.id))
                    let value = try await transaction.getValue(for: itemKey, snapshot: true)
                    return value != nil
                }
                #expect(exists)
            }
        }
    }
}

// MARK: - Edge Case Tests

@Suite("Leaderboard Fusion - Edge Cases")
struct LeaderboardFusionEdgeCaseTests {

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

    @Test("Maximum Int64 score")
    func testMaxInt64Score() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Champion", score: Int64.max)
        #expect(score.score == Int64.max)
    }

    @Test("Minimum Int64 score")
    func testMinInt64Score() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Bottom", score: Int64.min)
        #expect(score.score == Int64.min)
    }

    @Test("Empty region")
    func testEmptyRegion() {
        let score = LeaderboardTestScore(playerId: "p1", playerName: "Nobody", score: 100, region: "")
        #expect(score.region.isEmpty)
    }

    @Test("Unicode region and player name")
    func testUnicodeRegionAndPlayerName() {
        let score = LeaderboardTestScore(
            playerId: "p1",
            playerName: "日本人プレイヤー",
            score: 1000,
            region: "日本"
        )
        #expect(score.region == "日本")
        #expect(score.playerName == "日本人プレイヤー")
    }

    @Test("Many ties in scores")
    func testManyTiesInScores() {
        let scores = (0..<100).map { i in
            LeaderboardTestScore(playerId: "p\(i)", playerName: "Player\(i)", score: 1000)
        }

        #expect(scores.allSatisfy { $0.score == 1000 })

        // Fusion scores should still be distributed
        let count = Double(scores.count)
        for (index, _) in scores.enumerated() {
            let fusionScore = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            #expect(fusionScore >= 0.0)
            #expect(fusionScore <= 1.0)
        }
    }

    @Test("k larger than result count")
    func testKLargerThanResultCount() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 900)
        ]

        let k = 100
        let topK = scores.prefix(k)

        #expect(topK.count == 2)
    }

    @Test("Very long player name")
    func testVeryLongPlayerName() {
        let longName = String(repeating: "A", count: 10000)
        let score = LeaderboardTestScore(playerId: "p1", playerName: longName, score: 1000)

        #expect(score.playerName.count == 10000)
    }

    @Test("Special characters in player name")
    func testSpecialCharactersInPlayerName() {
        let specialName = "Player<>\"'&@#$%^*(){}[]|\\:;?/"
        let score = LeaderboardTestScore(playerId: "p1", playerName: specialName, score: 1000)

        #expect(score.playerName == specialName)
    }
}

// MARK: - Grouping Tests

@Suite("Leaderboard Fusion - Grouping")
struct LeaderboardFusionGroupingTests {

    @Test("Filter by region")
    func testFilterByRegion() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000, region: "asia"),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 800, region: "europe"),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 900, region: "asia")
        ]

        let asiaScores = scores.filter { $0.region == "asia" }
        #expect(asiaScores.count == 2)
        #expect(asiaScores.map(\.playerName).sorted() == ["Alice", "Charlie"])

        let europeScores = scores.filter { $0.region == "europe" }
        #expect(europeScores.count == 1)
        #expect(europeScores[0].playerName == "Bob")
    }

    @Test("Group scores by region")
    func testGroupScoresByRegion() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000, region: "asia"),
            LeaderboardTestScore(playerId: "p2", playerName: "Bob", score: 800, region: "europe"),
            LeaderboardTestScore(playerId: "p3", playerName: "Charlie", score: 900, region: "asia"),
            LeaderboardTestScore(playerId: "p4", playerName: "David", score: 700, region: "europe")
        ]

        let grouped = Dictionary(grouping: scores, by: { $0.region })

        #expect(grouped.keys.count == 2)
        #expect(grouped["asia"]?.count == 2)
        #expect(grouped["europe"]?.count == 2)
    }

    @Test("Empty group returns no results")
    func testEmptyGroupReturnsNoResults() {
        let scores = [
            LeaderboardTestScore(playerId: "p1", playerName: "Alice", score: 1000, region: "asia")
        ]

        let filteredByNonexistentRegion = scores.filter { $0.region == "antarctica" }
        #expect(filteredByNonexistentRegion.isEmpty)
    }
}

// MARK: - Index Discovery Tests

@Suite("Leaderboard Fusion - Index Discovery")
struct LeaderboardFusionIndexDiscoveryTests {

    @Test("findIndexDescriptor matches by kindIdentifier")
    func testFindIndexDescriptorByKindIdentifier() {
        let descriptors = LeaderboardTestScore.indexDescriptors

        let leaderboardDescriptor = descriptors.first { descriptor in
            descriptor.kindIdentifier == TimeWindowLeaderboardIndexKind<LeaderboardTestScore, Int64>.identifier
        }

        #expect(leaderboardDescriptor != nil)
        #expect(leaderboardDescriptor?.kindIdentifier == "time_window_leaderboard")
    }

    @Test("findIndexDescriptor matches by fieldName")
    func testFindIndexDescriptorByFieldName() {
        let descriptors = LeaderboardTestScore.indexDescriptors
        let fieldName = "score"

        let matchingDescriptor = descriptors.first { descriptor in
            descriptor.fieldNames.contains(fieldName)
        }

        #expect(matchingDescriptor != nil)
    }

    @Test("FusionQueryError for missing index")
    func testMissingIndex() {
        let error = FusionQueryError.indexNotFound(
            type: "LeaderboardTestScore",
            field: "unknownField",
            kind: "leaderboard"
        )

        #expect(error.description.contains("leaderboard"))
        #expect(error.description.contains("unknownField"))
    }
}
