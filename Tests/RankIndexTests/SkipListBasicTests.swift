// SkipListBasicTests.swift
// Basic functionality tests for Skip List implementation

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import RankIndex

// MARK: - Test Models (One per test for isolation)

@Persistable
struct SkipListTestPlayer1 {
    #Directory<SkipListTestPlayer1>("test", "skip_insert_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer1, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer2 {
    #Directory<SkipListTestPlayer2>("test", "skip_topk_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer2, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer3 {
    #Directory<SkipListTestPlayer3>("test", "skip_update_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer3, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer4 {
    #Directory<SkipListTestPlayer4>("test", "skip_delete_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer4, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer5 {
    #Directory<SkipListTestPlayer5>("test", "skip_empty_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer5, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer6 {
    #Directory<SkipListTestPlayer6>("test", "skip_duplicate_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer6, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer7 {
    #Directory<SkipListTestPlayer7>("test", "skip_rank_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer7, Int64>(field: \.score, bucketSize: 100))
}

@Persistable
struct SkipListTestPlayer8 {
    #Directory<SkipListTestPlayer8>("test", "skip_span_test")
    var id: String = UUID().uuidString
    var name: String = ""
    var score: Int64 = 0
    #Index(RankIndexKind<SkipListTestPlayer8, Int64>(field: \.score, bucketSize: 100))
}

// MARK: - Tests

@Suite("Skip List Basic Tests", .serialized)
struct SkipListBasicTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test("Insert creates entries at all levels")
    func testInsertCreatesEntries() async throws {
        let schema = Schema([SkipListTestPlayer1.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let players = [
            SkipListTestPlayer1(name: "Alice", score: 1000),
            SkipListTestPlayer1(name: "Bob", score: 800),
            SkipListTestPlayer1(name: "Carol", score: 600),
        ]

        for player in players {
            context.insert(player)
        }
        try await context.save()

        // Verify count
        let results = try await context.rank(SkipListTestPlayer1.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(results.count == 3, "Should have 3 players")
    }

    @Test("getTopK returns highest scores")
    func testGetTopK() async throws {
        let schema = Schema([SkipListTestPlayer2.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let players = [
            SkipListTestPlayer2(name: "Alice", score: 1000),
            SkipListTestPlayer2(name: "Bob", score: 800),
            SkipListTestPlayer2(name: "Carol", score: 600),
            SkipListTestPlayer2(name: "Dave", score: 400),
            SkipListTestPlayer2(name: "Eve", score: 200),
        ]

        for player in players {
            context.insert(player)
        }
        try await context.save()

        // Get top 3
        let results = try await context.rank(SkipListTestPlayer2.self)
            .by(\.score)
            .top(3)
            .execute()

        #expect(results.count == 3, "Should return top 3")
        #expect(results[0].item.name == "Alice", "First should be Alice (1000)")
        #expect(results[0].item.score == 1000)
        #expect(results[0].rank == 0, "Alice should be rank 0")

        #expect(results[1].item.name == "Bob", "Second should be Bob (800)")
        #expect(results[1].item.score == 800)
        #expect(results[1].rank == 1, "Bob should be rank 1")

        #expect(results[2].item.name == "Carol", "Third should be Carol (600)")
        #expect(results[2].item.score == 600)
        #expect(results[2].rank == 2, "Carol should be rank 2")
    }

    @Test("Update changes rank correctly")
    func testUpdate() async throws {
        let schema = Schema([SkipListTestPlayer3.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert initial data
        let alice = SkipListTestPlayer3(name: "Alice", score: 500)
        let bob = SkipListTestPlayer3(name: "Bob", score: 1000)

        context.insert(alice)
        context.insert(bob)
        try await context.save()

        // Verify initial ranks
        let before = try await context.rank(SkipListTestPlayer3.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(before[0].item.name == "Bob", "Bob should be first (1000)")
        #expect(before[1].item.name == "Alice", "Alice should be second (500)")

        // Update Alice's score to be highest (delete + insert)
        var updatedAlice = alice
        updatedAlice.score = 2000
        context.delete(alice)
        context.insert(updatedAlice)
        try await context.save()

        // Verify ranks changed
        let after = try await context.rank(SkipListTestPlayer3.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(after[0].item.name == "Alice", "Alice should now be first (2000)")
        #expect(after[0].item.score == 2000)
        #expect(after[1].item.name == "Bob", "Bob should now be second (1000)")
    }

    @Test("Delete removes entry completely")
    func testDelete() async throws {
        let schema = Schema([SkipListTestPlayer4.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert data
        let alice = SkipListTestPlayer4(name: "Alice", score: 1000)
        let bob = SkipListTestPlayer4(name: "Bob", score: 800)

        context.insert(alice)
        context.insert(bob)
        try await context.save()

        // Verify before delete
        let before = try await context.rank(SkipListTestPlayer4.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(before.count == 2)

        // Delete Alice
        context.delete(alice)
        try await context.save()

        // Verify after delete
        let after = try await context.rank(SkipListTestPlayer4.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(after.count == 1)
        #expect(after[0].item.name == "Bob", "Only Bob should remain")
    }

    @Test("Empty index returns empty results")
    func testEmptyIndex() async throws {
        let schema = Schema([SkipListTestPlayer5.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        let results = try await context.rank(SkipListTestPlayer5.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(results.isEmpty, "Should return empty array")
    }

    @Test("Handles duplicate scores correctly")
    func testDuplicateScores() async throws {
        let schema = Schema([SkipListTestPlayer6.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert players with same score
        let players = [
            SkipListTestPlayer6(name: "Alice", score: 1000),
            SkipListTestPlayer6(name: "Bob", score: 1000),
            SkipListTestPlayer6(name: "Carol", score: 1000),
        ]

        for player in players {
            context.insert(player)
        }
        try await context.save()

        // All should be retrievable
        let results = try await context.rank(SkipListTestPlayer6.self)
            .by(\.score)
            .top(10)
            .execute()

        #expect(results.count == 3, "Should retrieve all 3 players")

        // All have same score
        for result in results {
            #expect(result.item.score == 1000)
        }

        // Names should be sorted by primary key (id)
        let names = results.map { $0.item.name }.sorted()
        #expect(names == ["Alice", "Bob", "Carol"])
    }

    @Test("getRank single entry")
    func testGetRankSingleEntry() async throws {
        // Simplified test: insert one entry and get its rank
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "skiplist_single", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("single_rank")

        let index = Index(
            name: "single_rank",
            kind: RankIndexKind<SkipListTestPlayer7, Int64>(field: \.score),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "single_rank",
            itemTypes: Set(["SkipListTestPlayer7"])
        )

        let maintainer = SkipListIndexMaintainer<SkipListTestPlayer7, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert single entry
        let player = SkipListTestPlayer7(name: "Only", score: 500)
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as SkipListTestPlayer7?,
                newItem: player,
                transaction: transaction
            )
        }

        // Check count
        let count = try await database.withTransaction { transaction in
            try await maintainer.getCount(transaction: transaction)
        }
        #expect(count == 1, "Should have 1 entry")


        // Get rank (should be 0)
        let rank = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 500, transaction: transaction)
        }
        #expect(rank == 0, "Single entry should have rank 0")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("getRank with two entries")
    func testGetRankTwoEntries() async throws {
        // Simplified test: two entries to debug span accumulation
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "skiplist_two", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("two_rank")

        let index = Index(
            name: "two_rank",
            kind: RankIndexKind<SkipListTestPlayer7, Int64>(field: \.score),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "two_rank",
            itemTypes: Set(["SkipListTestPlayer7"])
        )

        let maintainer = SkipListIndexMaintainer<SkipListTestPlayer7, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert two entries
        let player1 = SkipListTestPlayer7(name: "Low", score: 100)
        let player2 = SkipListTestPlayer7(name: "High", score: 200)

        for player in [player1, player2] {
            try await database.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil as SkipListTestPlayer7?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Check count
        let count = try await database.withTransaction { transaction in
            try await maintainer.getCount(transaction: transaction)
        }
        #expect(count == 2, "Should have 2 entries")

        // Rank of 200 (highest) should be 0
        let rank200 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 200, transaction: transaction)
        }
        #expect(rank200 == 0, "Score 200 should be rank 0, got \(rank200)")

        // Rank of 100 (lowest) should be 1
        let rank100 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 100, transaction: transaction)
        }
        #expect(rank100 == 1, "Score 100 should be rank 1, got \(rank100)")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("getRank returns correct descending rank")
    func testGetRankDescendingOrder() async throws {
        // Setup: Create SkipListIndexMaintainer directly
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "skiplist_rank", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("SkipListTestPlayer7_rank_score")

        let index = Index(
            name: "SkipListTestPlayer7_rank_score",
            kind: RankIndexKind<SkipListTestPlayer7, Int64>(field: \.score),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "SkipListTestPlayer7_rank_score",
            itemTypes: Set(["SkipListTestPlayer7"])
        )

        let maintainer = SkipListIndexMaintainer<SkipListTestPlayer7, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Clear any existing data before test
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }

        // Insert test data with various scores
        // IMPORTANT: Insert each player in separate transaction to ensure
        // proper span counter maintenance
        let players = [
            SkipListTestPlayer7(name: "Fifth", score: 100),
            SkipListTestPlayer7(name: "Fourth", score: 200),
            SkipListTestPlayer7(name: "Third", score: 600),
            SkipListTestPlayer7(name: "Second", score: 800),
            SkipListTestPlayer7(name: "First", score: 1000),
        ]

        for player in players {
            try await database.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil as SkipListTestPlayer7?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Verify count
        let count = try await database.withTransaction { transaction in
            try await maintainer.getCount(transaction: transaction)
        }
        #expect(count == 5, "Should have 5 entries, got \(count)")

        // Rank 0 = highest score (1000)
        let rank1000 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 1000, transaction: transaction)
        }
        #expect(rank1000 == 0, "Score 1000 should be rank 0 (highest)")

        // Rank 1 = second highest (800)
        let rank800 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 800, transaction: transaction)
        }
        #expect(rank800 == 1, "Score 800 should be rank 1")

        // Rank 2 = middle (600)
        let rank600 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 600, transaction: transaction)
        }
        #expect(rank600 == 2, "Score 600 should be rank 2")

        // Rank 3 = fourth (200)
        let rank200 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 200, transaction: transaction)
        }
        #expect(rank200 == 3, "Score 200 should be rank 3")

        // Rank 4 = lowest (100)
        let rank100 = try await database.withTransaction { transaction in
            try await maintainer.getRank(score: 100, transaction: transaction)
        }
        #expect(rank100 == 4, "Score 100 should be rank 4 (lowest)")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Span Counter Accuracy Tests

    @Test("Span counter accuracy with 100 entries")
    func testSpanCounterAccuracy() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "skiplist_span", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("span_rank")

        let index = Index(
            name: "span_rank",
            kind: RankIndexKind<SkipListTestPlayer8, Int64>(field: \.score),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "span_rank",
            itemTypes: Set(["SkipListTestPlayer8"])
        )

        let maintainer = SkipListIndexMaintainer<SkipListTestPlayer8, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Clear any existing data before test
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }

        // Insert 100 entries
        let entryCount = 100
        for i in 0..<entryCount {
            var player = SkipListTestPlayer8()
            player.name = "Player\(i)"
            player.score = Int64(i * 10)  // 0, 10, 20, ..., 990

            try await database.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil as SkipListTestPlayer8?,
                    newItem: player,
                    transaction: transaction
                )
            }
        }

        // Verify total count
        let totalCount = try await database.withTransaction { transaction in
            try await maintainer.getCount(transaction: transaction)
        }
        #expect(totalCount == Int64(entryCount), "Should have \(entryCount) entries, got \(totalCount)")

        // Verify span counter accuracy for each level
        // Each level's span sum should equal the total count
        let levelStats = try await database.withTransaction { transaction in
            try await maintainer.validateSpanIntegrity(transaction: transaction)
        }

        // Verify that we have stats for all levels
        #expect(!levelStats.isEmpty, "Should have stats for at least one level")

        // All span sums should equal the total count (validated inside validateSpanIntegrity)
        for (level, stats) in levelStats.sorted(by: { $0.key < $1.key }) {
            #expect(
                stats.spanSum == totalCount,
                "Level \(level) span sum (\(stats.spanSum)) should equal total count (\(totalCount)), but has \(stats.entries) entries"
            )
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
