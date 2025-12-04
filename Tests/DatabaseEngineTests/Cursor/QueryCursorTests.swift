// QueryCursorTests.swift
// DatabaseEngine Tests - QueryCursor integration tests
//
// Tests cursor-based pagination including:
// - Basic pagination with batch sizes
// - Limit handling across pages
// - Stream and collect APIs
// - Statistics tracking

import Testing
import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine
@testable import TestSupport

@Suite("QueryCursor Tests", .serialized)
struct QueryCursorTests {

    // MARK: - Test Model

    @Persistable
    struct PaginatedUser {
        #Directory<PaginatedUser>("test", "cursor", "users")
        var id: String = ULID().ulidString
        var name: String
        var age: Int
        var score: Double
    }

    // MARK: - Setup

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()

        let schema = Schema([PaginatedUser.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema)
    }

    private func cleanup(container: FDBContainer) async throws {
        let context = container.newContext()
        try await context.deleteAll(PaginatedUser.self)
        try await context.save()
    }

    private func seedUsers(context: FDBContext, count: Int) async throws -> [PaginatedUser] {
        var users: [PaginatedUser] = []
        for i in 0..<count {
            let user = PaginatedUser(
                name: "User \(String(format: "%03d", i))",
                age: 20 + (i % 50),
                score: Double(i) * 1.5
            )
            context.insert(user)
            users.append(user)
        }
        try await context.save()
        return users
    }

    // MARK: - Basic Pagination Tests

    @Test("Cursor returns correct batch size")
    func cursorReturnsBatchSize() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 25)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(10)
                .build()

            let firstPage = try await cursor.next()
            #expect(firstPage.items.count == 10)
            #expect(firstPage.hasMore == true)

            let secondPage = try await cursor.next()
            #expect(secondPage.items.count == 10)
            #expect(secondPage.hasMore == true)

            let thirdPage = try await cursor.next()
            #expect(thirdPage.items.count == 5)
            #expect(thirdPage.hasMore == false)
        }
    }

    @Test("Cursor respects total limit")
    func cursorRespectsLimit() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 50)

            let cursor = try context.cursor(PaginatedUser.self)
                .limit(15)  // Total limit
                .batchSize(10)  // Per-page
                .build()

            let firstPage = try await cursor.next()
            #expect(firstPage.items.count == 10)
            #expect(firstPage.hasMore == true)

            let secondPage = try await cursor.next()
            #expect(secondPage.items.count == 5)  // Only 5 remaining from limit
            #expect(secondPage.hasMore == false)
            #expect(secondPage.noNextReason == .returnLimitReached)
        }
    }

    @Test("Cursor with empty result set")
    func cursorWithEmptyResults() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(10)
                .build()

            let result = try await cursor.next()
            #expect(result.isEmpty == true)
            #expect(result.hasMore == false)
        }
    }

    // MARK: - Stream API Tests

    @Test("Stream yields all items")
    func streamYieldsAllItems() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            let seeded = try await seedUsers(context: context, count: 25)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(7)  // Non-divisible batch size
                .build()

            var streamed: [PaginatedUser] = []
            for try await user in cursor.stream() {
                streamed.append(user)
            }

            #expect(streamed.count == seeded.count)
        }
    }

    // MARK: - Collect API Tests

    @Test("Collect returns all items")
    func collectReturnsAllItems() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            let seeded = try await seedUsers(context: context, count: 18)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(5)
                .build()

            let collected = try await cursor.collect()

            #expect(collected.count == seeded.count)
        }
    }

    @Test("Collect respects limit")
    func collectRespectsLimit() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 50)

            let cursor = try context.cursor(PaginatedUser.self)
                .limit(20)
                .batchSize(7)
                .build()

            let collected = try await cursor.collect()

            #expect(collected.count == 20)
        }
    }

    // MARK: - Statistics Tests

    @Test("Statistics track items and pages correctly")
    func statisticsTrackCorrectly() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 25)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(10)
                .build()

            // Initial state
            var stats = cursor.statistics
            #expect(stats.itemsReturned == 0)
            #expect(stats.pagesReturned == 0)
            #expect(stats.isExhausted == false)

            // After first page
            _ = try await cursor.next()
            stats = cursor.statistics
            #expect(stats.itemsReturned == 10)
            #expect(stats.pagesReturned == 1)
            #expect(stats.isExhausted == false)

            // After second page
            _ = try await cursor.next()
            stats = cursor.statistics
            #expect(stats.itemsReturned == 20)
            #expect(stats.pagesReturned == 2)

            // After last page
            _ = try await cursor.next()
            stats = cursor.statistics
            #expect(stats.itemsReturned == 25)
            #expect(stats.pagesReturned == 3)
            #expect(stats.isExhausted == true)
        }
    }

    @Test("Exhausted cursor returns empty")
    func exhaustedCursorReturnsEmpty() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 5)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(10)
                .build()

            // First call exhausts cursor
            let first = try await cursor.next()
            #expect(first.items.count == 5)
            #expect(first.hasMore == false)

            // Subsequent calls return empty
            let second = try await cursor.next()
            #expect(second.isEmpty == true)
            #expect(second.noNextReason == .sourceExhausted)
        }
    }

    // MARK: - Edge Cases

    @Test("Batch size larger than data")
    func batchSizeLargerThanData() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 5)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(100)  // Much larger than data
                .build()

            let result = try await cursor.next()
            #expect(result.items.count == 5)
            #expect(result.hasMore == false)
        }
    }

    @Test("Batch size of 1")
    func batchSizeOfOne() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 3)

            let cursor = try context.cursor(PaginatedUser.self)
                .batchSize(1)
                .build()

            var totalItems = 0
            var pageCount = 0

            while true {
                let result = try await cursor.next()
                totalItems += result.items.count
                pageCount += 1
                if !result.hasMore {
                    break
                }
            }

            #expect(totalItems == 3)
            #expect(pageCount == 3)
        }
    }

    @Test("Limit of 0 returns nothing")
    func limitOfZeroReturnsNothing() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()
            _ = try await seedUsers(context: context, count: 10)

            let cursor = try context.cursor(PaginatedUser.self)
                .limit(0)
                .batchSize(5)
                .build()

            let result = try await cursor.next()
            #expect(result.isEmpty == true)
            #expect(result.noNextReason == .returnLimitReached)
        }
    }
}
