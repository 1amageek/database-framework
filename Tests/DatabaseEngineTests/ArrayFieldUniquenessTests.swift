import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for Array Field Uniqueness Enforcement
///
/// **Critical Bug Fix Verification**:
/// This test suite verifies that the fix for array field uniqueness checking works correctly.
///
/// **Previous Bug**:
/// - Index created entries per array element: [subspace]["elem"][id]
/// - Uniqueness check searched for composite key: [subspace]["elem1"]["elem2"]...
/// - Result: No uniqueness enforcement for array fields!
///
/// **Fixed Behavior**:
/// - Each array element is checked individually for uniqueness
/// - Matches how ScalarIndexMaintainer creates index entries
@Suite("Array Field Uniqueness Tests", .serialized)
struct ArrayFieldUniquenessTests {

    // MARK: - Test Models

    /// Model with unique constraint on array field (tags)
    @Persistable
    struct TaggedDocument {
        #Directory<TaggedDocument>("test", "array_uniqueness", "documents")
        #Index(ScalarIndexKind<TaggedDocument>(fields: [\.tags]), unique: true, name: "TaggedDocument_tags")

        var id: String = ULID().ulidString
        var title: String
        var tags: [String]
    }

    /// Model with unique constraint on scalar field (for comparison)
    @Persistable
    struct UniqueEmail {
        #Directory<UniqueEmail>("test", "array_uniqueness", "emails")
        #Index(ScalarIndexKind<UniqueEmail>(fields: [\.email]), unique: true, name: "UniqueEmail_email")

        var id: String = ULID().ulidString
        var email: String
        var name: String
    }

    /// Model with UUID ID type (for ID comparison fix verification)
    /// Tests that Tuple equality works for non-String/Int64 ID types
    @Persistable
    struct UUIDTaggedDocument {
        #Directory<UUIDTaggedDocument>("test", "array_uniqueness", "uuid_docs")
        #Index(ScalarIndexKind<UUIDTaggedDocument>(fields: [\.tags]), unique: true, name: "UUIDTaggedDocument_tags")

        var id: UUID = UUID()
        var title: String
        var tags: [String]
    }

    /// Model with Int64 ID type (baseline for ID comparison)
    @Persistable
    struct Int64TaggedDocument {
        #Directory<Int64TaggedDocument>("test", "array_uniqueness", "int64_docs")
        #Index(ScalarIndexKind<Int64TaggedDocument>(fields: [\.tags]), unique: true, name: "Int64TaggedDocument_tags")

        var id: Int64 = Int64(Date().timeIntervalSince1970 * 1000000) + Int64.random(in: 0..<1000000)
        var title: String
        var tags: [String]
    }

    // MARK: - Helper Methods

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [TaggedDocument.self, UniqueEmail.self, UUIDTaggedDocument.self, Int64TaggedDocument.self],
            version: Schema.Version(1, 0, 0)
        )

        let container = FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )

        // Make indexes readable via store
        let tagStore = try await container.store(for: TaggedDocument.self)
        if let fdbStore = tagStore as? FDBDataStore {
            let state = try await fdbStore.indexStateManager.state(of: "TaggedDocument_tags")
            if state != .readable {
                if state == .disabled {
                    try await fdbStore.indexStateManager.enable("TaggedDocument_tags")
                }
                try await fdbStore.indexStateManager.makeReadable("TaggedDocument_tags")
            }
        }

        let emailStore = try await container.store(for: UniqueEmail.self)
        if let fdbStore = emailStore as? FDBDataStore {
            let state = try await fdbStore.indexStateManager.state(of: "UniqueEmail_email")
            if state != .readable {
                if state == .disabled {
                    try await fdbStore.indexStateManager.enable("UniqueEmail_email")
                }
                try await fdbStore.indexStateManager.makeReadable("UniqueEmail_email")
            }
        }

        // UUID ID model index
        let uuidStore = try await container.store(for: UUIDTaggedDocument.self)
        if let fdbStore = uuidStore as? FDBDataStore {
            let state = try await fdbStore.indexStateManager.state(of: "UUIDTaggedDocument_tags")
            if state != .readable {
                if state == .disabled {
                    try await fdbStore.indexStateManager.enable("UUIDTaggedDocument_tags")
                }
                try await fdbStore.indexStateManager.makeReadable("UUIDTaggedDocument_tags")
            }
        }

        // Int64 ID model index
        let int64Store = try await container.store(for: Int64TaggedDocument.self)
        if let fdbStore = int64Store as? FDBDataStore {
            let state = try await fdbStore.indexStateManager.state(of: "Int64TaggedDocument_tags")
            if state != .readable {
                if state == .disabled {
                    try await fdbStore.indexStateManager.enable("Int64TaggedDocument_tags")
                }
                try await fdbStore.indexStateManager.makeReadable("Int64TaggedDocument_tags")
            }
        }

        return container
    }

    // MARK: - Scalar Field Uniqueness (Baseline)

    @Test("Scalar unique constraint: duplicate value throws error")
    func scalarDuplicateThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let email = "unique-\(UUID().uuidString.prefix(8))@test.com"

        // Insert first record
        var user1 = UniqueEmail(email: email, name: "User 1")
        user1.id = uniqueID("U1")
        context.insert(user1)
        try await context.save()

        // Try to insert second record with same email
        var user2 = UniqueEmail(email: email, name: "User 2")
        user2.id = uniqueID("U2")
        context.insert(user2)

        // Should throw UniquenessViolationError
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError but save succeeded")
        } catch let error as UniquenessViolationError {
            #expect(error.indexName == "UniqueEmail_email")
            #expect(error.conflictingValues.contains(email))
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    @Test("Scalar unique constraint: different values allowed")
    func scalarDifferentValuesAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let email1 = "alice-\(UUID().uuidString.prefix(8))@test.com"
        let email2 = "bob-\(UUID().uuidString.prefix(8))@test.com"

        var user1 = UniqueEmail(email: email1, name: "Alice")
        user1.id = uniqueID("U1")
        var user2 = UniqueEmail(email: email2, name: "Bob")
        user2.id = uniqueID("U2")

        context.insert(user1)
        context.insert(user2)

        // Should succeed - different emails
        try await context.save()

        // Verify both exist
        let count = try await context.fetch(UniqueEmail.self)
            .where(\.email == email1 || \.email == email2)
            .execute()
            .count
        #expect(count == 2)
    }

    // MARK: - Array Field Uniqueness (Bug Fix Verification)

    @Test("Array field unique constraint: duplicate element throws error")
    func arrayDuplicateElementThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let sharedTag = "shared-tag-\(UUID().uuidString.prefix(8))"

        // Insert first document with the tag
        var doc1 = TaggedDocument(title: "Document 1", tags: [sharedTag])
        doc1.id = uniqueID("D1")
        context.insert(doc1)
        try await context.save()

        // Try to insert second document with same tag (among others)
        var doc2 = TaggedDocument(title: "Document 2", tags: [sharedTag, "other-tag"])
        doc2.id = uniqueID("D2")
        context.insert(doc2)

        // Should throw UniquenessViolationError
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError but save succeeded - array uniqueness not enforced!")
        } catch let error as UniquenessViolationError {
            #expect(error.indexName == "TaggedDocument_tags")
            #expect(error.conflictingValues.contains(sharedTag))
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    @Test("Array field unique constraint: different elements allowed")
    func arrayDifferentElementsAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tag1 = "tag1-\(UUID().uuidString.prefix(8))"
        let tag2 = "tag2-\(UUID().uuidString.prefix(8))"
        let tag3 = "tag3-\(UUID().uuidString.prefix(8))"

        var doc1 = TaggedDocument(title: "Document 1", tags: [tag1])
        doc1.id = uniqueID("D1")
        var doc2 = TaggedDocument(title: "Document 2", tags: [tag2, tag3])
        doc2.id = uniqueID("D2")

        context.insert(doc1)
        context.insert(doc2)

        // Should succeed - no overlapping tags
        try await context.save()
    }

    @Test("Array field unique constraint: multiple elements all checked")
    func arrayMultipleElementsChecked() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tag1 = "tagA-\(UUID().uuidString.prefix(8))"
        let tag2 = "tagB-\(UUID().uuidString.prefix(8))"
        let sharedTag = "shared-\(UUID().uuidString.prefix(8))"

        // Insert first document with sharedTag as second element
        var doc1 = TaggedDocument(title: "Document 1", tags: [tag1, sharedTag])
        doc1.id = uniqueID("D1")
        context.insert(doc1)
        try await context.save()

        // Try to insert second document where sharedTag is also not first
        var doc2 = TaggedDocument(title: "Document 2", tags: [tag2, sharedTag])
        doc2.id = uniqueID("D2")
        context.insert(doc2)

        // Should throw - sharedTag duplicated (even though not first element)
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError - middle array elements not checked!")
        } catch let error as UniquenessViolationError {
            #expect(error.conflictingValues.contains(sharedTag))
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    // MARK: - Update Cases

    // Note: Tests for self-update scenarios are complex because context.insert()
    // for an existing record requires the system to detect it as an update.
    // This is handled by FDBDataStore when it detects the record already exists.

    @Test("Array field update: cannot add element that exists elsewhere")
    func arrayUpdateAddDuplicateThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let takenTag = "taken-\(UUID().uuidString.prefix(8))"
        let myTag = "mine-\(UUID().uuidString.prefix(8))"

        // Insert document 1 with takenTag
        var doc1 = TaggedDocument(title: "Doc 1", tags: [takenTag])
        doc1.id = uniqueID("D1")
        context.insert(doc1)
        try await context.save()

        // Insert document 2 with different tag
        var doc2 = TaggedDocument(title: "Doc 2", tags: [myTag])
        doc2.id = uniqueID("D2")
        context.insert(doc2)
        try await context.save()

        // Try to update doc2 to include takenTag
        let fetched = try await context.model(for: doc2.id, as: TaggedDocument.self)
        var updated = fetched!
        updated.tags = [myTag, takenTag]  // Adding takenTag which belongs to doc1
        context.insert(updated)

        // Should throw
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError when adding duplicate tag")
        } catch is UniquenessViolationError {
            // Expected
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    // MARK: - Edge Cases

    @Test("Array field: completely different tags allowed")
    func arrayDifferentTagsAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        var doc1 = TaggedDocument(title: "Doc 1", tags: ["unique1-\(UUID().uuidString.prefix(8))"])
        doc1.id = uniqueID("D1")
        var doc2 = TaggedDocument(title: "Doc 2", tags: ["unique2-\(UUID().uuidString.prefix(8))"])
        doc2.id = uniqueID("D2")

        context.insert(doc1)
        context.insert(doc2)

        // Should succeed - completely different tags
        try await context.save()
    }

    @Test("Array field: single element array works like scalar")
    func arraySingleElementLikeScalar() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let singleTag = "single-\(UUID().uuidString.prefix(8))"

        var doc1 = TaggedDocument(title: "Doc 1", tags: [singleTag])
        doc1.id = uniqueID("D1")
        context.insert(doc1)
        try await context.save()

        var doc2 = TaggedDocument(title: "Doc 2", tags: [singleTag])
        doc2.id = uniqueID("D2")
        context.insert(doc2)

        // Should throw - even with single element
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError for single-element array duplicate")
        } catch is UniquenessViolationError {
            // Expected
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    // MARK: - UUID ID Type Tests (ID Comparison Fix Verification)

    /// Core test for the ID comparison fix.
    /// Before fix: UUID comparison falls through to `matches = false`, causing false uniqueness violation.
    /// After fix: Tuple equality handles UUID correctly.
    @Test("UUID ID: self-update with same tags allowed")
    func uuidSelfUpdateAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tag = "unique-tag-\(UUID().uuidString.prefix(8))"

        // Insert document with UUID ID
        let doc = UUIDTaggedDocument(title: "Doc 1", tags: [tag])
        context.insert(doc)
        try await context.save()

        // Fetch and update (keeping same tag)
        let fetched = try await context.model(for: doc.id, as: UUIDTaggedDocument.self)
        var updated = fetched!
        updated.title = "Updated Title"  // Change title only, keep tags
        context.insert(updated)

        // Should succeed - same record, same tags
        // Before fix: Fails because UUID comparison returns false (falls through to else branch)
        // After fix: Succeeds because Tuple equality works for UUID
        try await context.save()

        // Verify update succeeded
        let verified = try await context.model(for: doc.id, as: UUIDTaggedDocument.self)
        #expect(verified?.title == "Updated Title")
    }

    @Test("UUID ID: cannot add element that exists in another document")
    func uuidUpdateAddDuplicateThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let takenTag = "taken-\(UUID().uuidString.prefix(8))"
        let myTag = "mine-\(UUID().uuidString.prefix(8))"

        // Document 1 owns takenTag
        let doc1 = UUIDTaggedDocument(title: "Doc 1", tags: [takenTag])
        context.insert(doc1)
        try await context.save()

        // Document 2 with different tag
        let doc2 = UUIDTaggedDocument(title: "Doc 2", tags: [myTag])
        context.insert(doc2)
        try await context.save()

        // Try to add takenTag to doc2
        let fetched = try await context.model(for: doc2.id, as: UUIDTaggedDocument.self)
        var updated = fetched!
        updated.tags = [myTag, takenTag]
        context.insert(updated)

        // Should throw - takenTag belongs to doc1
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError when adding duplicate tag")
        } catch is UniquenessViolationError {
            // Expected - uniqueness constraint still works
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }

    @Test("UUID ID: different documents with different tags allowed")
    func uuidDifferentDocumentsAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tag1 = "tag1-\(UUID().uuidString.prefix(8))"
        let tag2 = "tag2-\(UUID().uuidString.prefix(8))"

        let doc1 = UUIDTaggedDocument(title: "Doc 1", tags: [tag1])
        let doc2 = UUIDTaggedDocument(title: "Doc 2", tags: [tag2])

        context.insert(doc1)
        context.insert(doc2)

        // Should succeed - different tags
        try await context.save()
    }

    // MARK: - Int64 ID Type Tests (Baseline)

    @Test("Int64 ID: self-update with same tags allowed")
    func int64SelfUpdateAllowed() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tag = "unique-tag-\(UUID().uuidString.prefix(8))"

        // Insert document with Int64 ID
        let doc = Int64TaggedDocument(title: "Doc 1", tags: [tag])
        context.insert(doc)
        try await context.save()

        // Fetch and update (keeping same tag)
        let fetched = try await context.model(for: doc.id, as: Int64TaggedDocument.self)
        var updated = fetched!
        updated.title = "Updated Title"
        context.insert(updated)

        // Should succeed - Int64 was already supported, but verify it still works
        try await context.save()

        // Verify update succeeded
        let verified = try await context.model(for: doc.id, as: Int64TaggedDocument.self)
        #expect(verified?.title == "Updated Title")
    }

    @Test("Int64 ID: cannot add element that exists in another document")
    func int64UpdateAddDuplicateThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let takenTag = "taken-\(UUID().uuidString.prefix(8))"
        let myTag = "mine-\(UUID().uuidString.prefix(8))"

        // Document 1 owns takenTag
        let doc1 = Int64TaggedDocument(title: "Doc 1", tags: [takenTag])
        context.insert(doc1)
        try await context.save()

        // Document 2 with different tag
        let doc2 = Int64TaggedDocument(title: "Doc 2", tags: [myTag])
        context.insert(doc2)
        try await context.save()

        // Try to add takenTag to doc2
        let fetched = try await context.model(for: doc2.id, as: Int64TaggedDocument.self)
        var updated = fetched!
        updated.tags = [myTag, takenTag]
        context.insert(updated)

        // Should throw - takenTag belongs to doc1
        do {
            try await context.save()
            Issue.record("Expected UniquenessViolationError when adding duplicate tag")
        } catch is UniquenessViolationError {
            // Expected
        } catch {
            Issue.record("Expected UniquenessViolationError but got: \(error)")
        }
    }
}
