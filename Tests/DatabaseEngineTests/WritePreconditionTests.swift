#if !os(WASI)
#if FOUNDATION_DB
// WritePreconditionTests.swift
// Regression tests for explicit DatabaseContext mutation intent and the
// `WritePrecondition` values that control commit-time assertions.
//
// Contract under test:
//   - `insert` defaults to `.notExists` — a duplicate key throws
//     `DatabaseContextError.preconditionFailed` rather than silently upserting.
//   - `update` defaults to `.exists` — a missing row throws
//     `DatabaseContextError.preconditionFailed` rather than silently inserting.
//   - `delete` with `.exists` — a missing row throws
//     `DatabaseContextError.preconditionFailed` rather than being a no-op.
//   - `upsert` — blind write; succeeds whether the row exists or not.
//
// Operations that carry an explicit intent must surface mismatches through
// typed errors so callers can branch on them.

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import ScalarIndex

// MARK: - Test Model

@Persistable
struct WPUser {
    #Directory<WPUser>("write_precondition_tests", "users")

    var id: String = UUID().uuidString
    var email: String = ""

    #Index(.scalar, fields: [\WPUser.email])
}

// MARK: - Test Suite

@Suite("WritePrecondition explicit-intent APIs", .foundationDBScenario, .serialized)
struct WritePreconditionTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    private func makeContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try WPUser.schemaEntity])
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(WPUser.self)]),
            security: .testingDisabled,
        )
    }

    private func cleanup(_ container: DBContainer) async throws {
        let subspace = try await container.testBaseDirectory(for: WPUser.self)
        let (begin, end) = subspace.range()
        try await container.engine.withTransaction { transaction in
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
        try await container.ensureTestBaseIndexesReady()
    }

    private func uniq(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Insert

    @Test("Insert on empty key succeeds")
    func insertOnEmptyKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        var user = WPUser(email: uniq("e") + "@example.com")
        user.id = uniq("U")
        try context.insert(user)
        try await context.save()

        let hits = try await context.fetch(WPUser.self)
            .where(WPUser.fields.id == user.id)
            .execute()
        #expect(hits.count == 1)
        #expect(hits.first?.email == user.email)
    }

    @Test("Insert on existing key throws preconditionFailed")
    func insertOnExistingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        var user = WPUser(email: uniq("orig") + "@example.com")
        user.id = uniq("U")
        // Seed the row via upsert to avoid mixing create paths in setup.
        try context.upsert(user)
        try await context.save()

        // Attempt to create the same id again — must fail with preconditionFailed(.notExists).
        var duplicate = user
        duplicate.email = uniq("dup") + "@example.com"
        try context.insert(duplicate)

        await #expect(throws: DatabaseContextError.self) {
            try await context.save()
        }

        // Row must be unchanged: the original email still wins.
        let hits = try await context.fetch(WPUser.self)
            .where(WPUser.fields.id == user.id)
            .execute()
        #expect(hits.count == 1)
        #expect(hits.first?.email == user.email, "Failed create must not mutate the stored row")
    }

    // MARK: - Update

    @Test("Update on existing key succeeds and clears old index entry")
    func updateOnExistingKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"
        var user = WPUser(email: oldEmail)
        user.id = uniq("U")
        try context.upsert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        try context.update(updated)
        try await context.save()

        let byOld = try await context.fetch(WPUser.self)
            .where(WPUser.fields.email == oldEmail)
            .execute()
        let byNew = try await context.fetch(WPUser.self)
            .where(WPUser.fields.email == newEmail)
            .execute()
        #expect(byOld.isEmpty, "Old scalar index entry must be cleared after update")
        #expect(byNew.count == 1)
        #expect(byNew.first?.id == user.id)
    }

    @Test("Update with matching stored version succeeds and stale version fails")
    func updateWithMatchingStoredVersionSucceedsAndStaleVersionFails() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        let oldEmail = uniq("old") + "@example.com"
        var user = WPUser(email: oldEmail)
        user.id = uniq("U")
        try context.upsert(user)
        try await context.save()

        let version = try PersistableVersionTokenCodec.digest(
            from: #require(QueryRowCodec.encode(user).version)
        )

        var firstUpdate = user
        firstUpdate.email = uniq("first") + "@example.com"
        try context.update(
            firstUpdate,
            precondition: .matchesStored(version: version)
        )
        try await context.save()

        var staleUpdate = firstUpdate
        staleUpdate.email = uniq("stale") + "@example.com"
        try context.update(
            staleUpdate,
            precondition: .matchesStored(version: version)
        )

        await #expect(throws: DatabaseContextError.self) {
            try await context.save()
        }

        let stored = try await context.fetch(WPUser.self)
            .where(WPUser.fields.id == user.id)
            .execute()
        #expect(stored.first?.email == firstUpdate.email)
    }

    @Test("Update on missing key throws preconditionFailed")
    func updateOnMissingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        var ghostOld = WPUser(email: uniq("g-old") + "@example.com")
        ghostOld.id = uniq("U")
        var ghostNew = ghostOld
        ghostNew.email = uniq("g-new") + "@example.com"

        // The row was never written, so update must reject `.exists`.
        try context.update(ghostNew)

        await #expect(throws: DatabaseContextError.self) {
            try await context.save()
        }

        let hits = try await context.fetch(WPUser.self)
            .where(WPUser.fields.id == ghostOld.id)
            .execute()
        #expect(hits.isEmpty, "Failed update must not leak the new value into storage")
    }

    // MARK: - delete

    @Test("delete with .exists on missing key throws preconditionFailed")
    func deleteExistsOnMissingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        var ghost = WPUser(email: uniq("ghost") + "@example.com")
        ghost.id = uniq("U")

        try context.delete(ghost, precondition: .exists)

        await #expect(throws: DatabaseContextError.self) {
            try await context.save()
        }
    }

    @Test("delete defaults to .exists and rejects a missing key")
    func deleteDefaultExistsOnMissingKeyFails() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        var ghost = WPUser(email: uniq("ghost") + "@example.com")
        ghost.id = uniq("U")

        try context.delete(ghost)
        await #expect(throws: DatabaseContextError.self) {
            try await context.save()
        }
    }

    @Test("delete on existing key removes row and clears index entries")
    func deleteOnExistingKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        let email = uniq("e") + "@example.com"
        var user = WPUser(email: email)
        user.id = uniq("U")
        try context.upsert(user)
        try await context.save()

        try context.delete(user, precondition: .exists)
        try await context.save()

        let byEmail = try await context.fetch(WPUser.self)
            .where(WPUser.fields.email == email)
            .execute()
        #expect(byEmail.isEmpty)
    }

    // MARK: - upsert

    @Test("upsert writes whether key exists or not")
    func upsertWritesRegardlessOfExistence() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.testBaseContext()

        let email1 = uniq("v1") + "@example.com"
        let email2 = uniq("v2") + "@example.com"
        var user = WPUser(email: email1)
        user.id = uniq("U")

        // First upsert → row does not exist, must succeed (blind write).
        try context.upsert(user)
        try await context.save()
        let hits1 = try await context.fetch(WPUser.self)
            .where(WPUser.fields.id == user.id)
            .execute()
        #expect(hits1.first?.email == email1)

        // Second upsert → row exists, must still succeed and replace the value.
        var updated = user
        updated.email = email2
        try context.upsert(updated)
        try await context.save()

        let byOld = try await context.fetch(WPUser.self)
            .where(WPUser.fields.email == email1)
            .execute()
        let byNew = try await context.fetch(WPUser.self)
            .where(WPUser.fields.email == email2)
            .execute()
        #expect(byOld.isEmpty, "Upsert must update the scalar index entry")
        #expect(byNew.count == 1)
    }
}
#endif

#endif
