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
}
