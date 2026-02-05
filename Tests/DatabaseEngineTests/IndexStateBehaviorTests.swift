// IndexStateBehaviorTests.swift
// Integration tests for index state behavior during writes and reads

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Model with Index

/// Test model with a scalar index for state behavior testing
struct IndexedUser: Persistable {
    typealias ID = String

    var id: String
    var email: String
    var name: String

    init(id: String = UUID().uuidString, email: String, name: String) {
        self.id = id
        self.email = email
        self.name = name
    }

    static var persistableType: String { "IndexedUser" }
    static var allFields: [String] { ["id", "email", "name"] }

    static var descriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "IndexedUser_email",
                keyPaths: [\IndexedUser.email],
                kind: ScalarIndexKind<IndexedUser>(fields: [\.email]),
                commonOptions: CommonIndexOptions(unique: true)
            )
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "email": return email
        case "name": return name
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<IndexedUser, Value>) -> String {
        switch keyPath {
        case \IndexedUser.id: return "id"
        case \IndexedUser.email: return "email"
        case \IndexedUser.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<IndexedUser>) -> String {
        switch keyPath {
        case \IndexedUser.id: return "id"
        case \IndexedUser.email: return "email"
        case \IndexedUser.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<IndexedUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

/// Test context for FDB integration tests
private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let container: FDBContainer

    init() throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "indexstate", String(testId)).pack())

        // Create a minimal container with IndexedUser schema
        let schema = Schema(
            entities: [Schema.Entity(from: IndexedUser.self)],
            version: Schema.Version(1, 0, 0)
        )
        self.container = FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    /// Clean up test data
    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Count index entries
    func countIndexEntries(indexName: String) async throws -> Int {
        let indexSubspace = subspace.subspace("I").subspace(indexName)
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }
}

// MARK: - Integration Tests

@Suite("Index State Behavior Tests", .tags(.fdb), .serialized)
struct IndexStateBehaviorTests {

    // MARK: - Disabled Index Tests

    @Test("Disabled index should not be maintained on insert")
    func testDisabledIndexNotMaintainedOnInsert() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Ensure index is disabled (default state)
            let initialState = try await indexStateManager.state(of: indexName)
            #expect(initialState == .disabled)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert user
            let user = IndexedUser(email: "alice@example.com", name: "Alice")
            try await dataStore.save([user])

            // Verify index entry was NOT created (because index is disabled)
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 0, "Disabled index should not have entries after insert")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Disabled index should not enforce unique constraint")
    func testDisabledIndexNoUniqueConstraint() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Ensure index is disabled
            let state = try await indexStateManager.state(of: indexName)
            #expect(state == .disabled)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert two users with same email - should NOT throw because index is disabled
            let user1 = IndexedUser(id: "user1", email: "duplicate@example.com", name: "User 1")
            let user2 = IndexedUser(id: "user2", email: "duplicate@example.com", name: "User 2")

            try await dataStore.save([user1])
            try await dataStore.save([user2])  // Should succeed because unique constraint is not enforced

            // Verify both users exist
            let fetchedUser1 = try await dataStore.fetch(IndexedUser.self, id: "user1")
            let fetchedUser2 = try await dataStore.fetch(IndexedUser.self, id: "user2")

            #expect(fetchedUser1 != nil)
            #expect(fetchedUser2 != nil)

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - WriteOnly Index Tests

    @Test("WriteOnly index should be maintained on insert")
    func testWriteOnlyIndexMaintainedOnInsert() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Enable index (disabled -> writeOnly)
            try await indexStateManager.enable(indexName)
            let state = try await indexStateManager.state(of: indexName)
            #expect(state == .writeOnly)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert user
            let user = IndexedUser(email: "bob@example.com", name: "Bob")
            try await dataStore.save([user])

            // Verify index entry WAS created
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 1, "WriteOnly index should have entry after insert")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("WriteOnly index should track unique constraint violations (not throw)")
    func testWriteOnlyIndexTracksUniqueConstraintViolations() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Enable index (puts it in writeOnly state)
            try await indexStateManager.enable(indexName)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert first user
            let user1 = IndexedUser(id: "user1", email: "unique@example.com", name: "User 1")
            try await dataStore.save([user1])

            // Insert second user with same email
            // In writeOnly mode, this should NOT throw but track the violation
            let user2 = IndexedUser(id: "user2", email: "unique@example.com", name: "User 2")
            try await dataStore.save([user2])

            // Both users should be saved (writeOnly mode tracks violations, doesn't throw)
            // This is the intended behavior for online indexing where we need to
            // continue building the index and resolve violations later

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Readable index should enforce unique constraint by throwing")
    func testReadableIndexEnforcesUniqueConstraint() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Enable and make readable
            try await indexStateManager.enable(indexName)
            try await indexStateManager.makeReadable(indexName)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert first user
            let user1 = IndexedUser(id: "user1", email: "unique@example.com", name: "User 1")
            try await dataStore.save([user1])

            // Insert second user with same email - should throw in readable mode
            let user2 = IndexedUser(id: "user2", email: "unique@example.com", name: "User 2")

            await #expect(throws: UniquenessViolationError.self) {
                try await dataStore.save([user2])
            }

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - Readable Index Tests

    @Test("Readable index should be maintained on insert")
    func testReadableIndexMaintainedOnInsert() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Enable and make readable (disabled -> writeOnly -> readable)
            try await indexStateManager.enable(indexName)
            try await indexStateManager.makeReadable(indexName)
            let state = try await indexStateManager.state(of: indexName)
            #expect(state == .readable)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Insert user
            let user = IndexedUser(email: "charlie@example.com", name: "Charlie")
            try await dataStore.save([user])

            // Verify index entry WAS created
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 1, "Readable index should have entry after insert")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - Delete Behavior Tests

    @Test("Disabled index should not be updated on delete")
    func testDisabledIndexNotUpdatedOnDelete() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            // Create FDBDataStore first, then use its internal indexStateManager
            // This ensures cache consistency between state changes and delete operations
            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Start with readable index (using dataStore's indexStateManager)
            try await dataStore.indexStateManager.enable(indexName)
            try await dataStore.indexStateManager.makeReadable(indexName)

            // Insert user (index entry created)
            let user = IndexedUser(id: "deletetest", email: "delete@example.com", name: "Delete Test")
            try await dataStore.save([user])

            // Verify index entry exists
            let countBefore = try await ctx.countIndexEntries(indexName: indexName)
            #expect(countBefore == 1)

            // Disable the index (using the same indexStateManager to ensure cache is invalidated)
            try await dataStore.indexStateManager.disable(indexName)

            // Delete user - index entry should remain because index is now disabled
            try await dataStore.delete([user])

            // Verify index entry still exists (stale entry)
            let countAfter = try await ctx.countIndexEntries(indexName: indexName)
            #expect(countAfter == 1, "Stale index entry should remain when index is disabled during delete")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - State Transition Tests

    @Test("Index state transitions follow correct sequence")
    func testStateTransitions() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "test_index"

            // Initial state is disabled
            let state1 = try await indexStateManager.state(of: indexName)
            #expect(state1 == .disabled)

            // disabled -> writeOnly
            try await indexStateManager.enable(indexName)
            let state2 = try await indexStateManager.state(of: indexName)
            #expect(state2 == .writeOnly)

            // writeOnly -> readable
            try await indexStateManager.makeReadable(indexName)
            let state3 = try await indexStateManager.state(of: indexName)
            #expect(state3 == .readable)

            // readable -> disabled
            try await indexStateManager.disable(indexName)
            let state4 = try await indexStateManager.state(of: indexName)
            #expect(state4 == .disabled)

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Invalid state transitions should fail")
    func testInvalidStateTransitions() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "test_invalid"

            // Cannot enable from writeOnly
            try await indexStateManager.enable(indexName)
            await #expect(throws: IndexStateError.self) {
                try await indexStateManager.enable(indexName)
            }

            // Cannot makeReadable from disabled
            try await indexStateManager.disable(indexName)
            await #expect(throws: IndexStateError.self) {
                try await indexStateManager.makeReadable(indexName)
            }

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - Batch Operations Tests

    @Test("Batch operations respect index state")
    func testBatchOperationsRespectIndexState() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let ctx = try TestContext()

            let indexStateManager = IndexStateManager(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            // Ensure index is disabled
            let state = try await indexStateManager.state(of: indexName)
            #expect(state == .disabled)

            let dataStore = FDBDataStore(container: ctx.container, subspace: ctx.subspace)

            // Batch insert via executeBatch
            let users = [
                IndexedUser(id: "batch1", email: "batch1@example.com", name: "Batch 1"),
                IndexedUser(id: "batch2", email: "batch2@example.com", name: "Batch 2"),
                IndexedUser(id: "batch3", email: "batch3@example.com", name: "Batch 3")
            ]
            try await dataStore.executeBatch(inserts: users, deletes: [])

            // Verify no index entries created
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 0, "Disabled index should have no entries after batch insert")

            // Verify records exist
            let allUsers = try await dataStore.fetchAll(IndexedUser.self)
            #expect(allUsers.count == 3)

            // Cleanup
            try await ctx.cleanup()
        }
    }
}
