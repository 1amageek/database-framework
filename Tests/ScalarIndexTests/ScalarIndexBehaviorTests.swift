#if FOUNDATION_DB
// ScalarIndexBehaviorTests.swift
// Integration tests for ScalarIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// MARK: - Test Model

@Persistable
struct ScalarIndexedUser {
    var id: String
    var email: String
    var age: Int64
    var city: String

}

// MARK: - Scalar Index Context

private struct ScalarIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: ScalarIndexMaintainer<ScalarIndexedUser>

    init(indexName: String = "ScalarIndexedUser_email") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "scalar", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = Index(
            name: indexName,
            kind: scalarIndexMetadata(
                fields: [FieldIdentity(name: "email", number: 2)]
            ),
            rootExpression: FieldKeyExpression(fieldName: "email"),
            subspaceKey: indexName,
            itemTypes: Set(["ScalarIndexedUser"])
        )

        self.maintainer = ScalarIndexMaintainer<ScalarIndexedUser>(
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

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    func getIndexKeys() async throws -> [ByteString] {
        try await database.withTransaction { transaction -> [ByteString] in
            let (begin, end) = indexSubspace.range()
            var keys: [ByteString] = []
            for (key, _) in try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true
            ) {
                keys.append(key)
            }
            return keys
        }
    }
}

// MARK: - Behavior Tests

@Suite("ScalarIndex Behavior Tests", .tags(.fdb), .serialized, .heartbeat)
struct ScalarIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert creates index entry")
    func testInsertCreatesIndexEntry() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after insert")

        try await ctx.cleanup()
    }

    @Test("Insert multiple creates multiple entries")
    func testInsertMultipleCreatesMultipleEntries() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let users = [
            ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo"),
            ScalarIndexedUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka"),
            ScalarIndexedUser(id: "user3", email: "charlie@example.com", age: 35, city: "Kyoto")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 index entries after inserting 3 users")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update with same value does not change entry count")
    func testUpdateSameValueNoChange() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        // Update with same email (different age)
        let updatedUser = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 26, city: "Tokyo")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: updatedUser,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 index entry after update with same email")

        try await ctx.cleanup()
    }

    @Test("Update with different value replaces entry")
    func testUpdateDifferentValueReplacesEntry() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let keysBefore = try await ctx.getIndexKeys()

        // Update with different email
        let updatedUser = ScalarIndexedUser(id: "user1", email: "alice.new@example.com", age: 25, city: "Tokyo")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: updatedUser,
                transaction: transaction
            )
        }

        let keysAfter = try await ctx.getIndexKeys()

        #expect(keysAfter.count == 1, "Should still have 1 index entry")
        #expect(keysBefore != keysAfter, "Index key should be different after email change")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes index entry")
    func testDeleteRemovesIndexEntry() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 index entries after delete")

        try await ctx.cleanup()
    }

    @Test("Delete specific user among multiple")
    func testDeleteSpecificUser() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user1 = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")
        let user2 = ScalarIndexedUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka")

        // Insert both
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: user1, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: user2, transaction: transaction)
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 2)

        // Delete user1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user1,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 1, "Should have 1 index entry after deleting one user")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem creates index entry")
    func testScanItemCreatesEntry() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        let user = ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.scanItem(
                user,
                id: Tuple("user1"),
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Ordering Tests

    @Test("Index entries are ordered by field value")
    func testIndexEntriesOrdered() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await ScalarIndexContext()

        // Insert in random order
        let users = [
            ScalarIndexedUser(id: "user3", email: "charlie@example.com", age: 35, city: "Kyoto"),
            ScalarIndexedUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo"),
            ScalarIndexedUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        // Get keys and verify they are in alphabetical order by email
        let keys = try await ctx.getIndexKeys()
        #expect(keys.count == 3)

        // Keys should be ordered: alice, bob, charlie
        // (lexicographic ordering of email values)
        for i in 0..<(keys.count - 1) {
            #expect(keys[i].lexicographicallyPrecedes(keys[i + 1]), "Keys should be in lexicographic order")
        }

        try await ctx.cleanup()
    }

    // MARK: - Composite Index Tests

    @Test("Composite index with multiple fields")
    func testCompositeIndex() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "scalar", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("ScalarIndexedUser_city_age")

        let index = Index(
            name: "ScalarIndexedUser_city_age",
            kind: scalarIndexMetadata(
                fields: [
                    FieldIdentity(name: "city", number: 4),
                    FieldIdentity(name: "age", number: 3),
                ]
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "city"),
                FieldKeyExpression(fieldName: "age")
            ]),
            subspaceKey: "ScalarIndexedUser_city_age",
            itemTypes: Set(["ScalarIndexedUser"])
        )

        let maintainer = ScalarIndexMaintainer<ScalarIndexedUser>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let users = [
            ScalarIndexedUser(id: "user1", email: "a@example.com", age: 25, city: "Tokyo"),
            ScalarIndexedUser(id: "user2", email: "b@example.com", age: 30, city: "Tokyo"),
            ScalarIndexedUser(id: "user3", email: "c@example.com", age: 25, city: "Osaka")
        ]

        try await database.withTransaction { transaction in
            for user in users {
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        // Count entries
        let count = try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }

        #expect(count == 3, "Should have 3 composite index entries")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
#endif
