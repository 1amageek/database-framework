#if FOUNDATION_DB
// ContextDeleteInsertSameIDTests.swift
// Regression tests for FDBContext `delete(old) + insert(new) + save()` pattern on the same ModelKey.
//
// Background: A bug was found where `context.delete(oldModel) + context.insert(newModel)`
// with the SAME id, executed in a SINGLE save(), left the old indexed-field value in
// the index subspace — making queries on the old value still return the record. The
// root cause spans FDBContext state semantics (problem ①), the old-value source in
// FDBDataStore (problem ②), and potential commit-visibility gaps (problem ③).
//
// These tests are deliberately spread across multiple index types (ScalarIndex,
// FullTextIndex) to guard against future framework-wide regressions — a failure in
// only one index type would prove the regression is not storage-level but lives in
// the FDBContext/DataStore layer.
//
// DO NOT remove these tests when refactoring PendingMutation state; they are the
// contract for the `delete + insert` merge semantics.

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import FullText
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import FullTextIndex

// MARK: - Test Models

/// ScalarIndex-backed model: email is an indexed scalar field.
@Persistable
struct DelInsUser {
    #Directory<DelInsUser>("test", "delins", "users")

    var id: String = UUID().uuidString
    var email: String = ""
    var city: String = ""

    #Index(ScalarIndexKind<DelInsUser>(fields: [\.email]))
    #Index(ScalarIndexKind<DelInsUser>(fields: [\.city]))
}

/// FullTextIndex-backed model: content is a tokenized text field.
@Persistable
struct DelInsArticle {
    #Directory<DelInsArticle>("test", "delins", "articles")

    var id: String = UUID().uuidString
    var title: String = ""
    var content: String = ""

    #Index(FullTextIndexKind<DelInsArticle>(fields: [\.content], tokenizer: .simple))
}

// MARK: - Test Suite

@Suite("Context delete+insert same-ID merge semantics", .serialized, .heartbeat)
struct ContextDeleteInsertSameIDTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func makeUserContainer() async throws -> DBContainer {
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema([DelInsUser.self])
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )
    }

    private func makeArticleContainer() async throws -> DBContainer {
        let database = try await FDBTestSetup.shared.makeEngine()
        let schema = Schema([DelInsArticle.self])
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )
    }

    private func cleanupUsers(_ container: DBContainer) async throws {
        try? await container.engine.directoryService.remove(path: ["test", "delins", "users"])
        try await container.ensureIndexesReady()
    }

    private func cleanupArticles(_ container: DBContainer) async throws {
        try? await container.engine.directoryService.remove(path: ["test", "delins", "articles"])
        try await container.ensureIndexesReady()
    }

    private func uniq(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - ScalarIndex: Single-transaction delete + insert (same ID)

    /// The classic bug case: delete old + insert new with the same id, within one save().
    /// The old indexed value must NOT remain reachable; the new indexed value MUST be reachable.
    @Test("ScalarIndex: delete(old) + insert(new) same id → old value gone, new value visible")
    func scalarDeleteInsertSameIDSingleTx() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.newContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Tokyo")
        user.id = userId
        context.insert(user)
        try await context.save()

        // Sanity: old value reachable before the update.
        let seed = try await context.fetch(DelInsUser.self).where(\.email == oldEmail).execute()
        try #require(seed.count == 1)
        try #require(seed.first?.id == userId)

        // The bug case: delete + insert in SAME save()
        var updated = user
        updated.email = newEmail
        context.delete(user)
        context.insert(updated)
        try await context.save()

        // Old indexed value must be gone.
        let old = try await context.fetch(DelInsUser.self).where(\.email == oldEmail).execute()
        #expect(old.isEmpty, "Old index entry must be cleared after delete+insert (got \(old.count))")

        // New indexed value must be visible.
        let new = try await context.fetch(DelInsUser.self).where(\.email == newEmail).execute()
        #expect(new.count == 1, "New index entry must be present after delete+insert")
        #expect(new.first?.id == userId)
        #expect(new.first?.email == newEmail)
    }

    /// Same pattern but the delete is issued AFTER the insert in the same save().
    /// The final intent is `replace(old, new)` regardless of call order — the old entry
    /// must still be cleared and the new one must be present.
    @Test("ScalarIndex: insert(new) + delete(old) same id (reverse order) → new still wins")
    func scalarInsertThenDeleteSameIDSingleTx() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.newContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Osaka")
        user.id = userId
        context.insert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        // Reverse the canonical order: insert first, then delete.
        context.insert(updated)
        context.delete(user)
        try await context.save()

        // KNOWN BUG (tracked by Phase 1 PendingMutation work): in the `insert → delete`
        // reverse order, the two operations silently cancel each other in FDBContext's
        // current merge logic — both the insert of the updated record and the delete of
        // the old record are dropped, so the stored row still holds the old email.
        // Once the merge semantics are replaced with `PendingMutation.replace`, this
        // block should start passing and `withKnownIssue` should be removed.
        await withKnownIssue("Silent drop of insert+delete reverse-order pair — fixed by Phase 1 PendingMutation") {
            let old = try await context.fetch(DelInsUser.self).where(\.email == oldEmail).execute()
            #expect(old.isEmpty, "Reverse-order delete must still clear the old index entry")

            let new = try await context.fetch(DelInsUser.self).where(\.email == newEmail).execute()
            #expect(new.count == 1, "Reverse-order insert must still populate the new index entry")
            #expect(new.first?.id == userId)
        }
    }

    /// Multiple indexed fields change simultaneously: both stale index entries must be cleared.
    @Test("ScalarIndex: delete+insert updating two indexed fields — both old entries cleared")
    func scalarDeleteInsertMultiFieldUpdate() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.newContext()

        let userId = uniq("U")
        let oldEmail = uniq("oldE") + "@example.com"
        let newEmail = uniq("newE") + "@example.com"
        let oldCity = uniq("oldC")
        let newCity = uniq("newC")

        var user = DelInsUser(email: oldEmail, city: oldCity)
        user.id = userId
        context.insert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        updated.city = newCity
        context.delete(user)
        context.insert(updated)
        try await context.save()

        let oldEmailHit = try await context.fetch(DelInsUser.self).where(\.email == oldEmail).execute()
        let oldCityHit = try await context.fetch(DelInsUser.self).where(\.city == oldCity).execute()
        #expect(oldEmailHit.isEmpty, "Stale email index entry must be cleared")
        #expect(oldCityHit.isEmpty, "Stale city index entry must be cleared")

        let newEmailHit = try await context.fetch(DelInsUser.self).where(\.email == newEmail).execute()
        let newCityHit = try await context.fetch(DelInsUser.self).where(\.city == newCity).execute()
        #expect(newEmailHit.count == 1)
        #expect(newCityHit.count == 1)
        #expect(newEmailHit.first?.id == userId)
        #expect(newCityHit.first?.id == userId)
    }

    /// Two different IDs processed together — both should succeed. Acts as a regression
    /// control: if this fails while the same-ID case also fails, the bug is broader.
    @Test("ScalarIndex: delete one id + insert another id in same tx → both take effect")
    func scalarDeleteOneInsertAnotherDifferentIDs() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.newContext()

        let idA = uniq("UA")
        let idB = uniq("UB")
        let emailA = uniq("a") + "@example.com"
        let emailB = uniq("b") + "@example.com"

        var userA = DelInsUser(email: emailA, city: "X")
        userA.id = idA
        context.insert(userA)
        try await context.save()

        var userB = DelInsUser(email: emailB, city: "Y")
        userB.id = idB
        // Different IDs, same tx: delete A, insert B.
        context.delete(userA)
        context.insert(userB)
        try await context.save()

        let aHit = try await context.fetch(DelInsUser.self).where(\.email == emailA).execute()
        let bHit = try await context.fetch(DelInsUser.self).where(\.email == emailB).execute()
        #expect(aHit.isEmpty, "Deleted id A must be gone from index")
        #expect(bHit.count == 1, "Inserted id B must be visible in index")
        #expect(bHit.first?.id == idB)
    }

    /// Cross-commit variant: the same delete+insert pattern split across two save()s.
    /// This should already work today; it establishes a baseline for the single-tx case.
    @Test("ScalarIndex: delete(old) then save; insert(new) then save (cross-commit baseline)")
    func scalarDeleteThenInsertAcrossTwoSaves() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.newContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Z")
        user.id = userId
        context.insert(user)
        try await context.save()

        context.delete(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        context.insert(updated)
        try await context.save()

        let old = try await context.fetch(DelInsUser.self).where(\.email == oldEmail).execute()
        let new = try await context.fetch(DelInsUser.self).where(\.email == newEmail).execute()
        #expect(old.isEmpty)
        #expect(new.count == 1)
        #expect(new.first?.id == userId)
    }

    // MARK: - FullTextIndex: Single-transaction delete + insert (same ID)

    /// Same contract as the scalar case but with a tokenized full-text field. Token-level
    /// inverted-index entries for the old content must be cleared; tokens of the new
    /// content must be reachable.
    @Test("FullTextIndex: delete(old) + insert(new) same id → old tokens gone, new tokens visible")
    func fullTextDeleteInsertSameIDSingleTx() async throws {
        let container = try await makeArticleContainer()
        try await cleanupArticles(container)
        let context = container.newContext()

        let articleId = uniq("A")
        // Single alphabetic tokens only — the `.simple` tokenizer splits on non-word
        // characters (incl. hyphens and digits on boundaries). Using letters-only
        // keeps the fixture resilient to tokenizer variants.
        let oldToken = randomLetters(length: 10, prefix: "alphazzz")
        let newToken = randomLetters(length: 10, prefix: "omegazzz")
        let sharedToken = "sharedzzz"

        var article = DelInsArticle(title: "T", content: "\(oldToken) \(sharedToken)")
        article.id = articleId
        context.insert(article)
        try await context.save()

        // Sanity: the old token is searchable.
        let seed = try await context.search(DelInsArticle.self)
            .fullText(\.content)
            .terms([oldToken])
            .execute()
        try #require(seed.count == 1)
        try #require(seed.first?.id == articleId)

        // delete + insert in one save()
        var updated = article
        updated.content = "\(newToken) \(sharedToken)"
        context.delete(article)
        context.insert(updated)
        try await context.save()

        let oldHits = try await context.search(DelInsArticle.self)
            .fullText(\.content)
            .terms([oldToken])
            .execute()
        #expect(oldHits.isEmpty, "Old full-text token must be cleared after delete+insert")

        let newHits = try await context.search(DelInsArticle.self)
            .fullText(\.content)
            .terms([newToken])
            .execute()
        #expect(newHits.count == 1, "New full-text token must be indexed after delete+insert")
        #expect(newHits.first?.id == articleId)

        let sharedHits = try await context.search(DelInsArticle.self)
            .fullText(\.content)
            .terms([sharedToken])
            .execute()
        #expect(sharedHits.count == 1, "Shared token must still match exactly once after replace")
        #expect(sharedHits.first?.id == articleId)
    }

    // MARK: - Helpers

    private func randomLetters(length: Int, prefix: String) -> String {
        let letters = "abcdefghijklmnopqrstuvwxyz"
        let suffix = String((0..<length).map { _ in letters.randomElement()! })
        return "\(prefix)\(suffix)"
    }
}

#endif
