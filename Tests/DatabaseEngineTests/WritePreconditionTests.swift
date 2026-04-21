#if FOUNDATION_DB
// WritePreconditionTests.swift
// Regression tests for Phase 2: the explicit operation APIs on FDBContext
// (`create` / `upsert` / `replace(old:with:)` / `delete`) and the
// `WritePrecondition` enum that controls their commit-time assertions.
//
// Contract under test (per plan):
//   - `create` defaults to `.notExists` — a duplicate key throws
//     `FDBContextError.preconditionFailed` rather than silently upserting.
//   - `replace(old:with:)` defaults to `.exists` — a missing row throws
//     `FDBContextError.preconditionFailed` rather than silently inserting.
//   - `delete` with `.exists` — a missing row throws
//     `FDBContextError.preconditionFailed` rather than being a no-op.
//   - `upsert` — blind write; succeeds whether the row exists or not.
//
// These guard the CLAUDE.md rule: silent fallback is forbidden. Operations
// that carry an explicit intent must surface mismatches through typed errors
// so callers can branch on them.

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// MARK: - Test Model

@Persistable
struct WPUser {
    #Directory<WPUser>("write_precondition_tests", "users")

    var id: String = UUID().uuidString
    var email: String = ""

    #Index(ScalarIndexKind<WPUser>(fields: [\.email]))
}

// MARK: - Test Suite

@Suite("WritePrecondition explicit-intent APIs", .serialized)
struct WritePreconditionTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func makeContainer() async throws -> DBContainer {
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema([WPUser.self])
        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled,
        )
    }

    private func cleanup(_ container: DBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: WPUser.self)
        let (begin, end) = subspace.range()
        try await container.engine.withTransaction { transaction in
            transaction.clearRange(beginKey: begin, endKey: end)
        }
        try await container.ensureIndexesReady()
    }

    private func uniq(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - create

    @Test("create on empty key succeeds")
    func createOnEmptyKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        var user = WPUser(email: uniq("e") + "@example.com")
        user.id = uniq("U")
        context.create(user)
        try await context.save()

        let hits = try await context.fetch(WPUser.self).where(\.id == user.id).execute()
        #expect(hits.count == 1)
        #expect(hits.first?.email == user.email)
    }

    @Test("create on existing key throws preconditionFailed")
    func createOnExistingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        var user = WPUser(email: uniq("orig") + "@example.com")
        user.id = uniq("U")
        // Seed the row via upsert to avoid mixing create paths in setup.
        context.upsert(user)
        try await context.save()

        // Attempt to create the same id again — must fail with preconditionFailed(.notExists).
        var duplicate = user
        duplicate.email = uniq("dup") + "@example.com"
        context.create(duplicate)

        await #expect(throws: FDBContextError.self) {
            try await context.save()
        }

        // Row must be unchanged: the original email still wins.
        let hits = try await context.fetch(WPUser.self).where(\.id == user.id).execute()
        #expect(hits.count == 1)
        #expect(hits.first?.email == user.email, "Failed create must not mutate the stored row")
    }

    // MARK: - replace

    @Test("replace on existing key succeeds and clears old index entry")
    func replaceOnExistingKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"
        var user = WPUser(email: oldEmail)
        user.id = uniq("U")
        context.upsert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        context.replace(old: user, with: updated)
        try await context.save()

        let byOld = try await context.fetch(WPUser.self).where(\.email == oldEmail).execute()
        let byNew = try await context.fetch(WPUser.self).where(\.email == newEmail).execute()
        #expect(byOld.isEmpty, "Old scalar index entry must be cleared after replace")
        #expect(byNew.count == 1)
        #expect(byNew.first?.id == user.id)
    }

    @Test("replace on missing key throws preconditionFailed")
    func replaceOnMissingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        var ghostOld = WPUser(email: uniq("g-old") + "@example.com")
        ghostOld.id = uniq("U")
        var ghostNew = ghostOld
        ghostNew.email = uniq("g-new") + "@example.com"

        // The row was never written — replace must refuse with preconditionFailed(.exists).
        context.replace(old: ghostOld, with: ghostNew)

        await #expect(throws: FDBContextError.self) {
            try await context.save()
        }

        let hits = try await context.fetch(WPUser.self).where(\.id == ghostOld.id).execute()
        #expect(hits.isEmpty, "Failed replace must not leak the new value into storage")
    }

    // MARK: - delete

    @Test("delete with .exists on missing key throws preconditionFailed")
    func deleteExistsOnMissingKeyThrowsPreconditionFailed() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        var ghost = WPUser(email: uniq("ghost") + "@example.com")
        ghost.id = uniq("U")

        context.delete(ghost, precondition: .exists)

        await #expect(throws: FDBContextError.self) {
            try await context.save()
        }
    }

    @Test("delete default precondition (.none) on missing key succeeds as no-op")
    func deleteDefaultNoneOnMissingKeyIsNoop() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        var ghost = WPUser(email: uniq("ghost") + "@example.com")
        ghost.id = uniq("U")

        // Legacy/source-compat behavior: default precondition is .none, so a missing-row
        // delete must not throw. This is the compatibility contract for the existing
        // `delete(_:)` call sites throughout the codebase.
        context.delete(ghost)
        try await context.save()
    }

    @Test("delete on existing key removes row and clears index entries")
    func deleteOnExistingKeySucceeds() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        let email = uniq("e") + "@example.com"
        var user = WPUser(email: email)
        user.id = uniq("U")
        context.upsert(user)
        try await context.save()

        context.delete(user, precondition: .exists)
        try await context.save()

        let byEmail = try await context.fetch(WPUser.self).where(\.email == email).execute()
        #expect(byEmail.isEmpty)
    }

    // MARK: - upsert

    @Test("upsert writes whether key exists or not")
    func upsertWritesRegardlessOfExistence() async throws {
        let container = try await makeContainer()
        try await cleanup(container)
        let context = container.newContext()

        let email1 = uniq("v1") + "@example.com"
        let email2 = uniq("v2") + "@example.com"
        var user = WPUser(email: email1)
        user.id = uniq("U")

        // First upsert → row does not exist, must succeed (blind write).
        context.upsert(user)
        try await context.save()
        let hits1 = try await context.fetch(WPUser.self).where(\.id == user.id).execute()
        #expect(hits1.first?.email == email1)

        // Second upsert → row exists, must still succeed and replace the value.
        var updated = user
        updated.email = email2
        context.upsert(updated)
        try await context.save()

        let byOld = try await context.fetch(WPUser.self).where(\.email == email1).execute()
        let byNew = try await context.fetch(WPUser.self).where(\.email == email2).execute()
        #expect(byOld.isEmpty, "Upsert must update the scalar index entry")
        #expect(byNew.count == 1)
    }
}
#endif
