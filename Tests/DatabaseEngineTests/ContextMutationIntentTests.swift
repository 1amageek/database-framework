#if !os(WASI)
#if FOUNDATION_DB
// ContextMutationIntentTests.swift
// Regression tests for explicit DatabaseContext mutation intent and index maintenance.

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
@testable import FullTextIndex

// MARK: - Test Models

/// ScalarIndex-backed model: email is an indexed scalar field.
@Persistable
struct DelInsUser {
    #Directory<DelInsUser>("context_delete_insert_same_id", "users")

    var id: String = UUID().uuidString
    var email: String = ""
    var city: String = ""

    #Index(
        .ordered(
            name: "DelInsUser_email", keys: [.ascending(\DelInsUser.email)], unique: false))
    #Index(
        .ordered(
            name: "DelInsUser_city", keys: [.ascending(\DelInsUser.city)], unique: false))
}

/// FullTextIndex-backed model: content is a tokenized text field.
@Persistable
struct DelInsArticle {
    #Directory<DelInsArticle>("context_delete_insert_same_id", "articles")

    var id: String = UUID().uuidString
    var title: String = ""
    var content: String = ""

    #Index(
        .text(
            name: "DelInsArticle_fulltext_content", fields: [\DelInsArticle.content],
            mode: .fullText(
                tokenizer: .simple, storePositions: true, ngramSize: 3, minimumTermLength: 2
            )))
}

// MARK: - Test Suite

@Suite("Context mutation intent semantics", .foundationDBScenario, .serialized, .heartbeat)
struct ContextMutationIntentTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Helpers

    private func makeUserContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try DelInsUser.schemaEntity])
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(DelInsUser.self), try DatabaseFrameworkRuntime.entity(DelInsArticle.self),
                ]),
            security: .testingDisabled,
        )
    }

    private func makeArticleContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try DelInsArticle.schemaEntity])
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(DelInsUser.self), try DatabaseFrameworkRuntime.entity(DelInsArticle.self),
                ]),
            security: .testingDisabled,
        )
    }

    private func cleanupUsers(_ container: DBContainer) async throws {
        try await container.resetTestBaseData()
    }

    private func cleanupArticles(_ container: DBContainer) async throws {
        try await container.resetTestBaseData()
    }

    private func uniq(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Scalar index updates

    @Test("ScalarIndex: update removes the old value and adds the new value")
    func scalarUpdateMaintainsIndex() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.testBaseContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Tokyo")
        user.id = userId
        try context.insert(user)
        try await context.save()

        // Sanity: old value reachable before the update.
        let seed = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == oldEmail).execute()
        try #require(seed.count == 1)
        try #require(seed.first?.id == userId)

        var updated = user
        updated.email = newEmail
        try context.update(updated)
        try await context.save()

        // Old indexed value must be gone.
        let old = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == oldEmail).execute()
        #expect(old.isEmpty, "Old index entry must be cleared after update (got \(old.count))")

        // New indexed value must be visible.
        let new = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == newEmail).execute()
        #expect(new.count == 1, "New index entry must be present after update")
        #expect(new.first?.id == userId)
        #expect(new.first?.email == newEmail)
    }

    @Test("Strict insert followed by delete retracts the pending write")
    func strictInsertThenDeleteRetractsPendingWrite() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.testBaseContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Osaka")
        user.id = userId
        try context.insert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        try context.insert(updated)
        try context.delete(user)
        #expect(!context.hasChanges)
        try await context.save()

        let old = try await context.fetch(DelInsUser.self)
            .where(DelInsUser.fields.email == oldEmail)
            .execute()
        let new = try await context.fetch(DelInsUser.self)
            .where(DelInsUser.fields.email == newEmail)
            .execute()
        #expect(old.count == 1)
        #expect(old.first?.id == userId)
        #expect(new.isEmpty)
    }

    /// Multiple indexed fields change simultaneously: both stale index entries must be cleared.
    @Test("ScalarIndex: update clears both old indexed fields")
    func scalarMultiFieldUpdate() async throws {
        let container = try await makeUserContainer()
        try await cleanupUsers(container)
        let context = container.testBaseContext()

        let userId = uniq("U")
        let oldEmail = uniq("oldE") + "@example.com"
        let newEmail = uniq("newE") + "@example.com"
        let oldCity = uniq("oldC")
        let newCity = uniq("newC")

        var user = DelInsUser(email: oldEmail, city: oldCity)
        user.id = userId
        try context.insert(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        updated.city = newCity
        try context.update(updated)
        try await context.save()

        let oldEmailHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == oldEmail).execute()
        let oldCityHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.city == oldCity).execute()
        #expect(oldEmailHit.isEmpty, "Stale email index entry must be cleared")
        #expect(oldCityHit.isEmpty, "Stale city index entry must be cleared")

        let newEmailHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == newEmail).execute()
        let newCityHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.city == newCity).execute()
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
        let context = container.testBaseContext()

        let idA = uniq("UA")
        let idB = uniq("UB")
        let emailA = uniq("a") + "@example.com"
        let emailB = uniq("b") + "@example.com"

        var userA = DelInsUser(email: emailA, city: "X")
        userA.id = idA
        try context.insert(userA)
        try await context.save()

        var userB = DelInsUser(email: emailB, city: "Y")
        userB.id = idB
        // Different IDs, same tx: delete A, insert B.
        try context.delete(userA)
        try context.insert(userB)
        try await context.save()

        let aHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == emailA).execute()
        let bHit = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == emailB).execute()
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
        let context = container.testBaseContext()

        let userId = uniq("U")
        let oldEmail = uniq("old") + "@example.com"
        let newEmail = uniq("new") + "@example.com"

        var user = DelInsUser(email: oldEmail, city: "Z")
        user.id = userId
        try context.insert(user)
        try await context.save()

        try context.delete(user)
        try await context.save()

        var updated = user
        updated.email = newEmail
        try context.insert(updated)
        try await context.save()

        let old = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == oldEmail).execute()
        let new = try await context.fetch(DelInsUser.self).where(DelInsUser.fields.email == newEmail).execute()
        #expect(old.isEmpty)
        #expect(new.count == 1)
        #expect(new.first?.id == userId)
    }

    // MARK: - Full-text index updates

    /// Same contract as the scalar case but with a tokenized full-text field. Token-level
    /// inverted-index entries for the old content must be cleared; tokens of the new
    /// content must be reachable.
    @Test("FullTextIndex: update removes old tokens and adds new tokens")
    func fullTextUpdateMaintainsIndex() async throws {
        let container = try await makeArticleContainer()
        try await cleanupArticles(container)
        let context = container.testBaseContext()

        let articleId = uniq("A")
        // Single alphabetic tokens only — the `.simple` tokenizer splits on non-word
        // characters (incl. hyphens and digits on boundaries). Using letters-only
        // keeps the indexed input resilient to tokenizer variants.
        let oldToken = randomLetters(length: 10, prefix: "alphazzz")
        let newToken = randomLetters(length: 10, prefix: "omegazzz")
        let sharedToken = "sharedzzz"

        var article = DelInsArticle(title: "T", content: "\(oldToken) \(sharedToken)")
        article.id = articleId
        try context.insert(article)
        try await context.save()

        // Sanity: the old token is searchable.
        let seed = try await context.search(DelInsArticle.self)
            .fullText(DelInsArticle.fields.content)
            .terms([oldToken])
            .execute()
        try #require(seed.count == 1)
        try #require(seed.first?.id == articleId)

        var updated = article
        updated.content = "\(newToken) \(sharedToken)"
        try context.update(updated)
        try await context.save()

        let oldHits = try await context.search(DelInsArticle.self)
            .fullText(DelInsArticle.fields.content)
            .terms([oldToken])
            .execute()
        #expect(oldHits.isEmpty, "Old full-text token must be cleared after update")

        let newHits = try await context.search(DelInsArticle.self)
            .fullText(DelInsArticle.fields.content)
            .terms([newToken])
            .execute()
        #expect(newHits.count == 1, "New full-text token must be indexed after update")
        #expect(newHits.first?.id == articleId)

        let sharedHits = try await context.search(DelInsArticle.self)
            .fullText(DelInsArticle.fields.content)
            .terms([sharedToken])
            .execute()
        #expect(sharedHits.count == 1, "Shared token must still match exactly once after update")
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

#endif
