#if !os(WASI)
#if FOUNDATION_DB
// IndexStateBehaviorTests.swift
// Integration tests for index state behavior during writes and reads

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime

// MARK: - Test Model with Index

/// Test model with a scalar index for state behavior testing
@Persistable
struct IndexedUser {
    #Index(
        .scalar,
        fields: [\IndexedUser.email],
        unique: true,
        name: "IndexedUser_email"
    )

    var id: String = UUID().uuidString
    var email: String
    var name: String
}

// MARK: - Test Helper

/// Test context for FDB integration tests
private struct IndexStateContext {
    let database: any StorageEngine
    let subspace: Subspace
    let container: DBContainer
    let dataStore: DatabaseDataStore

    init() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [try IndexedUser.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexedUser.self)]),
            security: .disabled
        )
        let dataStore = try await container.store(for: IndexedUser.self)

        self.database = database
        self.subspace = dataStore.subspace
        self.container = container
        self.dataStore = dataStore
    }

    /// Clean up test data
    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Count index entries
    func countIndexEntries(indexName: String) async throws -> Int {
        let indexSubspace = subspace.subspace("I").subspace(indexName)
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }
}

// MARK: - Integration Tests

@Suite("Index State Behavior Tests", .tags(.fdb), .foundationDBScenario, .serialized, .heartbeat)
struct IndexStateBehaviorTests {

    // MARK: - Disabled Index Tests

    @Test("Disabled index should not be maintained on insert")
    func testDisabledIndexNotMaintainedOnInsert() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await ctx.dataStore.indexLifecycleStore.disable(indexName)
            let initialState = try await indexLifecycleStore.state(of: indexName)
            #expect(initialState == .disabled)

            // Insert user
            let user = IndexedUser(email: "alice@example.com", name: "Alice")
            try await ctx.dataStore.save([user])

            // Verify index entry was NOT created (because index is disabled)
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 0, "Disabled index should not have entries after insert")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Disabled index should not enforce unique constraint")
    func testDisabledIndexNoUniqueConstraint() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await ctx.dataStore.indexLifecycleStore.disable(indexName)
            let state = try await indexLifecycleStore.state(of: indexName)
            #expect(state == .disabled)

            // Insert two users with same email - should NOT throw because index is disabled
            let user1 = IndexedUser(id: "user1", email: "duplicate@example.com", name: "User 1")
            let user2 = IndexedUser(id: "user2", email: "duplicate@example.com", name: "User 2")

            try await ctx.dataStore.save([user1])
            try await ctx.dataStore.save([user2])

            // Verify both users exist
            let fetchedUser1 = try await ctx.dataStore.fetch(IndexedUser.self, id: "user1")
            let fetchedUser2 = try await ctx.dataStore.fetch(IndexedUser.self, id: "user2")

            #expect(fetchedUser1 != nil)
            #expect(fetchedUser2 != nil)

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - WriteOnly Index Tests

    @Test("WriteOnly index should be maintained on insert")
    func testWriteOnlyIndexMaintainedOnInsert() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await indexLifecycleStore.disable(indexName)
            try await indexLifecycleStore.enable(indexName)
            let state = try await indexLifecycleStore.state(of: indexName)
            #expect(state == .writeOnly)

            // Insert user
            let user = IndexedUser(email: "bob@example.com", name: "Bob")
            try await ctx.dataStore.save([user])

            // Verify index entry WAS created
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 1, "WriteOnly index should have entry after insert")

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("WriteOnly index should track unique constraint violations (not throw)")
    func testWriteOnlyIndexTracksUniqueConstraintViolations() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await indexLifecycleStore.disable(indexName)
            try await indexLifecycleStore.enable(indexName)

            // Insert first user
            let user1 = IndexedUser(id: "user1", email: "unique@example.com", name: "User 1")
            try await ctx.dataStore.save([user1])

            // Insert second user with same email
            // In writeOnly mode, this should NOT throw but track the violation
            let user2 = IndexedUser(id: "user2", email: "unique@example.com", name: "User 2")
            try await ctx.dataStore.save([user2])

            // Both users should be saved (writeOnly mode tracks violations, doesn't throw)
            // This is the intended behavior for online indexing where we need to
            // continue building the index and resolve violations later

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Readable index should enforce unique constraint by throwing")
    func testReadableIndexEnforcesUniqueConstraint() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await indexLifecycleStore.disable(indexName)
            try await indexLifecycleStore.enable(indexName)
            try await indexLifecycleStore.makeReadable(indexName)

            // Insert first user
            let user1 = IndexedUser(id: "user1", email: "unique@example.com", name: "User 1")
            try await ctx.dataStore.save([user1])

            // Insert second user with same email - should throw in readable mode
            let user2 = IndexedUser(id: "user2", email: "unique@example.com", name: "User 2")

            await #expect(throws: UniquenessViolationError.self) {
                try await ctx.dataStore.save([user2])
            }

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - Readable Index Tests

    @Test("Readable index should be maintained on insert")
    func testReadableIndexMaintainedOnInsert() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await indexLifecycleStore.disable(indexName)
            try await indexLifecycleStore.enable(indexName)
            try await indexLifecycleStore.makeReadable(indexName)
            let state = try await indexLifecycleStore.state(of: indexName)
            #expect(state == .readable)

            // Insert user
            let user = IndexedUser(email: "charlie@example.com", name: "Charlie")
            try await ctx.dataStore.save([user])

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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let dataStore = ctx.dataStore
            let indexName = "IndexedUser_email"

            try await dataStore.indexLifecycleStore.disable(indexName)
            try await dataStore.indexLifecycleStore.enable(indexName)
            try await dataStore.indexLifecycleStore.makeReadable(indexName)

            // Insert user (index entry created)
            let user = IndexedUser(id: "deletetest", email: "delete@example.com", name: "Delete Test")
            try await dataStore.save([user])

            // Verify index entry exists
            let countBefore = try await ctx.countIndexEntries(indexName: indexName)
            #expect(countBefore == 1)

            // Disable the index (using the same indexLifecycleStore to ensure cache is invalidated)
            try await dataStore.indexLifecycleStore.disable(indexName)

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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "test_index"

            try await indexLifecycleStore.disable(indexName)
            let state1 = try await indexLifecycleStore.state(of: indexName)
            #expect(state1 == .disabled)

            // disabled -> writeOnly
            try await indexLifecycleStore.enable(indexName)
            let state2 = try await indexLifecycleStore.state(of: indexName)
            #expect(state2 == .writeOnly)

            // writeOnly -> readable
            try await indexLifecycleStore.makeReadable(indexName)
            let state3 = try await indexLifecycleStore.state(of: indexName)
            #expect(state3 == .readable)

            // readable -> disabled
            try await indexLifecycleStore.disable(indexName)
            let state4 = try await indexLifecycleStore.state(of: indexName)
            #expect(state4 == .disabled)

            // Cleanup
            try await ctx.cleanup()
        }
    }

    @Test("Invalid state transitions should fail")
    func testInvalidStateTransitions() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "test_invalid"

            try await indexLifecycleStore.disable(indexName)
            try await indexLifecycleStore.enable(indexName)
            await #expect(throws: IndexStateError.self) {
                try await indexLifecycleStore.enable(indexName)
            }

            // Cannot makeReadable from disabled
            try await indexLifecycleStore.disable(indexName)
            await #expect(throws: IndexStateError.self) {
                try await indexLifecycleStore.makeReadable(indexName)
            }

            // Cleanup
            try await ctx.cleanup()
        }
    }

    // MARK: - Batch Operations Tests

    @Test("Batch operations respect index state")
    func testBatchOperationsRespectIndexState() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let ctx = try await IndexStateContext()

            let indexLifecycleStore = IndexLifecycleStore(container: ctx.container, subspace: ctx.subspace)
            let indexName = "IndexedUser_email"

            try await indexLifecycleStore.disable(indexName)
            let state = try await indexLifecycleStore.state(of: indexName)
            #expect(state == .disabled)

            // Batch insert via executeBatch
            let users = [
                IndexedUser(id: "batch1", email: "batch1@example.com", name: "Batch 1"),
                IndexedUser(id: "batch2", email: "batch2@example.com", name: "Batch 2"),
                IndexedUser(id: "batch3", email: "batch3@example.com", name: "Batch 3")
            ]
            try await ctx.dataStore.executeBatch(inserts: users, deletes: [])

            // Verify no index entries created
            let indexEntryCount = try await ctx.countIndexEntries(indexName: indexName)
            #expect(indexEntryCount == 0, "Disabled index should have no entries after batch insert")

            // Verify entities exist
            let allUsers = try await ctx.dataStore.fetchAll(IndexedUser.self)
            #expect(allUsers.count == 3)

            // Cleanup
            try await ctx.cleanup()
        }
    }
}
#endif

#endif
