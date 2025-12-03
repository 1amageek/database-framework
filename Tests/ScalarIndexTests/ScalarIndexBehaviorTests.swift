// ScalarIndexBehaviorTests.swift
// Integration tests for ScalarIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// MARK: - Test Model

struct ScalarTestUser: Persistable {
    typealias ID = String

    var id: String
    var email: String
    var age: Int64
    var city: String

    init(id: String = UUID().uuidString, email: String, age: Int64, city: String) {
        self.id = id
        self.email = email
        self.age = age
        self.city = city
    }

    static var persistableType: String { "ScalarTestUser" }
    static var allFields: [String] { ["id", "email", "age", "city"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "email": return email
        case "age": return age
        case "city": return city
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<ScalarTestUser, Value>) -> String {
        switch keyPath {
        case \ScalarTestUser.id: return "id"
        case \ScalarTestUser.email: return "email"
        case \ScalarTestUser.age: return "age"
        case \ScalarTestUser.city: return "city"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<ScalarTestUser>) -> String {
        switch keyPath {
        case \ScalarTestUser.id: return "id"
        case \ScalarTestUser.email: return "email"
        case \ScalarTestUser.age: return "age"
        case \ScalarTestUser.city: return "city"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<ScalarTestUser> {
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
    let maintainer: ScalarIndexMaintainer<ScalarTestUser>

    init(indexName: String = "ScalarTestUser_email") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "scalar", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = Index(
            name: indexName,
            kind: ScalarIndexKind<ScalarTestUser>(fields: [\.email]),
            rootExpression: FieldKeyExpression(fieldName: "email"),
            subspaceKey: indexName,
            itemTypes: Set(["ScalarTestUser"])
        )

        self.maintainer = ScalarIndexMaintainer<ScalarTestUser>(
            index: index,
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

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func getIndexKeys() async throws -> [[UInt8]] {
        try await database.withTransaction { transaction -> [[UInt8]] in
            let (begin, end) = indexSubspace.range()
            var keys: [[UInt8]] = []
            for try await (key, _) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                keys.append(key)
            }
            return keys
        }
    }
}

// MARK: - Behavior Tests

@Suite("ScalarIndex Behavior Tests", .tags(.fdb), .serialized)
struct ScalarIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert creates index entry")
    func testInsertCreatesIndexEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let users = [
            ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo"),
            ScalarTestUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka"),
            ScalarTestUser(id: "user3", email: "charlie@example.com", age: 35, city: "Kyoto")
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        // Update with same email (different age)
        let updatedUser = ScalarTestUser(id: "user1", email: "alice@example.com", age: 26, city: "Tokyo")
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

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
        let updatedUser = ScalarTestUser(id: "user1", email: "alice.new@example.com", age: 25, city: "Tokyo")
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user1 = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")
        let user2 = ScalarTestUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka")

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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo")

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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert in random order
        let users = [
            ScalarTestUser(id: "user3", email: "charlie@example.com", age: 35, city: "Kyoto"),
            ScalarTestUser(id: "user1", email: "alice@example.com", age: 25, city: "Tokyo"),
            ScalarTestUser(id: "user2", email: "bob@example.com", age: 30, city: "Osaka")
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
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "scalar", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("ScalarTestUser_city_age")

        let index = Index(
            name: "ScalarTestUser_city_age",
            kind: ScalarIndexKind<ScalarTestUser>(fields: [\.city, \.age]),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "city"),
                FieldKeyExpression(fieldName: "age")
            ]),
            subspaceKey: "ScalarTestUser_city_age",
            itemTypes: Set(["ScalarTestUser"])
        )

        let maintainer = ScalarIndexMaintainer<ScalarTestUser>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let users = [
            ScalarTestUser(id: "user1", email: "a@example.com", age: 25, city: "Tokyo"),
            ScalarTestUser(id: "user2", email: "b@example.com", age: 30, city: "Tokyo"),
            ScalarTestUser(id: "user3", email: "c@example.com", age: 25, city: "Osaka")
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
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }

        #expect(count == 3, "Should have 3 composite index entries")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
