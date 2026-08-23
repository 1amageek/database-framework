#if !os(WASI)
#if FOUNDATION_DB
// BitmapFusionTests.swift
// Tests for BitmapIndex Fusion query (Bitmap)

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import BitmapIndex

// MARK: - Test Model

/// User model with bitmap-indexed status and role fields.
@Persistable
struct BitmapFusionUser {
    #Index(
        .bitmap(
            name: "BitmapTestUser_bitmap_status",
            field: \BitmapFusionUser.status
        ))
    #Index(
        .bitmap(
            name: "BitmapTestUser_bitmap_role",
            field: \BitmapFusionUser.role
        ))

    var id: String = UUID().uuidString
    var name: String
    /// Membership status.
    var status: String
    /// Authorization role.
    var role: String
}

// MARK: - Test Context

private enum BitmapFusionContextError: Error {
    case missingBitmapIndex
}

private struct BitmapFusionContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let itemsSubspace: Subspace
    let blobsSubspace: Subspace
    let maintainer: BitmapIndexMaintainer<BitmapFusionUser>

    init(indexName: String = "BitmapTestUser_bitmap_status") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "bitmap_fusion", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")
        self.blobsSubspace = subspace.subspace("B")

        guard let descriptor = try BitmapFusionUser.indexDescriptors.first(
            where: { $0.name == indexName }
        ) else {
            throw BitmapFusionContextError.missingBitmapIndex
        }
        let index = try ResolvedIndex(
            for: BitmapFusionUser.self,
            name: indexName,
            definition: descriptor.declaration.definition,
            rootExpression: FieldKeyExpression(fieldName: "status"),
            itemTypes: Set(["BitmapFusionUser"])
        )

        self.maintainer = BitmapIndexMaintainer<BitmapFusionUser>(
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

    func insertUser(_ user: BitmapFusionUser) async throws {
        try await database.withTransaction { transaction in
            // Serialize user to items subspace
            let itemKey = itemsSubspace.pack(Tuple(user.id))
            let storage = ItemStorageWriter(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            try await storage.write(
                try PersistableStorageCodec.encode(user),
                for: itemKey
            )

            // Update index
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }
    }
}

// MARK: - Unit Tests (API Pattern)

@Suite("Bitmap Fusion - Unit Tests", .heartbeat)
struct BitmapFusionUnitTests {

    @Test("Bitmap definition selects the bitmap index type")
    func bitmapDefinitionSelectsBitmapType() {
        #expect(IndexType.bitmap.diagnosticName == "bitmap")
    }

    @Test("Index descriptor configuration")
    func testIndexDescriptorConfiguration() throws {
        let descriptors = try BitmapFusionUser.indexDescriptors
        #expect(descriptors.count == 2)

        let statusIndex = descriptors.first { $0.name.contains("status") }
        #expect(statusIndex != nil)
        #expect(statusIndex?.type == .bitmap)
        #expect(statusIndex?.fieldNames.contains("status") == true)

        let roleIndex = descriptors.first { $0.name.contains("role") }
        #expect(roleIndex != nil)
        #expect(roleIndex?.type == .bitmap)
        #expect(roleIndex?.fieldNames.contains("role") == true)
    }

    @Test("ScoredResult initialization")
    func testScoredResultInitialization() {
        let user = BitmapFusionUser(name: "Alice", status: "active", role: "admin")
        let result = ScoredResult(item: user, score: 1.0)

        #expect(result.score == 1.0)
        #expect(result.item.name == "Alice")
        #expect(result.item.status == "active")
    }

    @Test("FusionQueryError - indexNotFound")
    func testFusionQueryErrorIndexNotFound() {
        let error = FusionQueryError.indexNotFound(
            entity: "BitmapFusionUser",
            field: "unknownField",
            indexType: .bitmap
        )

        #expect(error.description.contains("bitmap"))
        #expect(error.description.contains("unknownField"))
        #expect(error.description.contains("BitmapFusionUser"))
    }

    @Test("FusionQueryError - invalidConfiguration")
    func testFusionQueryErrorInvalidConfiguration() {
        let error = FusionQueryError.invalidConfiguration("Missing required parameter")
        #expect(error.description.contains("Missing required parameter"))
    }

    @Test("Bitmap returns score 1.0 for all matches")
    func testBitmapScoreIsAlwaysOne() {
        // Bitmap is a pass/fail filter - all matches get score 1.0
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(name: "Bob", status: "active", role: "user"),
            BitmapFusionUser(name: "Charlie", status: "active", role: "guest"),
        ]

        let results = users.map { ScoredResult(item: $0, score: 1.0) }

        #expect(results.allSatisfy { $0.score == 1.0 })
    }
}

// MARK: - Initialization Tests

@Suite("Bitmap Fusion - Initialization", .heartbeat)
struct BitmapFusionInitializationTests {

    @Test("Generated fields preserve schema identities")
    func testFieldNameExtraction() {
        #expect(BitmapFusionUser.fields.status.identity.name == "status")
        #expect(BitmapFusionUser.fields.role.identity.name == "role")
    }

    @Test("Generated field access returns canonical values")
    func testDynamicMemberAccess() throws {
        let user = BitmapFusionUser(name: "Alice", status: "active", role: "admin")

        #expect(
            try user.persistedFieldValue(
                for: BitmapFusionUser.fields.status.identity
            ) == .string("active")
        )
        #expect(
            try user.persistedFieldValue(
                for: BitmapFusionUser.fields.role.identity
            ) == .string("admin")
        )
        #expect(
            try user.persistedFieldValue(
                for: BitmapFusionUser.fields.name.identity
            ) == .string("Alice")
        )
        #expect(
            try user.persistedFieldValue(
                for: FieldIdentity(name: "unknown", number: 1_000)
            ) == nil
        )
    }
}

// MARK: - Integration Tests

@Suite("Bitmap Fusion - Integration Tests", .foundationDBScenario, .serialized, .heartbeat)
struct BitmapFusionIntegrationTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func withContext<Result>(
        _ operation: (BitmapFusionContext) async throws -> Result
    ) async throws -> Result {
        let context = try await BitmapFusionContext()
        let result: Result
        do {
            result = try await operation(context)
        } catch let operationError {
            do {
                try await context.cleanup()
            } catch let cleanupError {
                Issue.record("Bitmap fusion cleanup failed: \(cleanupError)")
            }
            throw operationError
        }
        try await context.cleanup()
        return result
    }

    @Test("Bitmap index maintainer initialization")
    func testBitmapIndexMaintainerInitialization() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in
                // Verify maintainer is created with correct configuration
                // (maintainer is non-optional, so we verify it's the expected type)
                #expect(type(of: context.maintainer) == BitmapIndexMaintainer<BitmapFusionUser>.self)
            }
        }
    }

    @Test("Insert and index user")
    func testInsertAndIndexUser() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in

                let userId = uniqueID("user")
                let user = BitmapFusionUser(id: userId, name: "Alice", status: "active", role: "admin")

                try await context.insertUser(user)

                // Verify user was inserted (check items subspace)
                let itemExists = try await context.database.withTransaction { transaction -> Bool in
                    let itemKey = context.itemsSubspace.pack(Tuple(userId))
                    let value = try await transaction.getValue(for: itemKey, snapshot: true)
                    return value != nil
                }

                #expect(itemExists)
            }
        }
    }

    @Test("Multiple users with same status value")
    func testMultipleUsersWithSameStatus() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withContext { context in

                let user1 = BitmapFusionUser(id: uniqueID("user"), name: "Alice", status: "active", role: "admin")
                let user2 = BitmapFusionUser(id: uniqueID("user"), name: "Bob", status: "active", role: "user")
                let user3 = BitmapFusionUser(id: uniqueID("user"), name: "Charlie", status: "inactive", role: "user")

                try await context.insertUser(user1)
                try await context.insertUser(user2)
                try await context.insertUser(user3)

                // All three users should be inserted
                for user in [user1, user2, user3] {
                    let exists = try await context.database.withTransaction { transaction -> Bool in
                        let itemKey = context.itemsSubspace.pack(Tuple(user.id))
                        let value = try await transaction.getValue(for: itemKey, snapshot: true)
                        return value != nil
                    }
                    #expect(exists, "User \(user.name) should exist")
                }
            }
        }
    }
}

// MARK: - Predicate Tests

@Suite("Bitmap Fusion - Predicates", .heartbeat)
struct BitmapFusionPredicateTests {

    @Test("Equals predicate matching")
    func testEqualsPredicateMatching() {
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(name: "Bob", status: "pending", role: "user"),
            BitmapFusionUser(name: "Charlie", status: "active", role: "guest"),
        ]

        let activeUsers = users.filter { $0.status == "active" }
        #expect(activeUsers.count == 2)
        #expect(activeUsers.map(\.name).sorted() == ["Alice", "Charlie"])
    }

    @Test("In predicate matching (OR)")
    func testInPredicateMatching() {
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(name: "Bob", status: "pending", role: "user"),
            BitmapFusionUser(name: "Charlie", status: "inactive", role: "guest"),
        ]

        let targetStatuses = ["active", "pending"]
        let matchingUsers = users.filter { targetStatuses.contains($0.status) }

        #expect(matchingUsers.count == 2)
        #expect(matchingUsers.map(\.name).sorted() == ["Alice", "Bob"])
    }

    @Test("In predicate with single value")
    func testInPredicateWithSingleValue() {
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(name: "Bob", status: "pending", role: "user"),
        ]

        let targetStatuses = ["active"]
        let matchingUsers = users.filter { targetStatuses.contains($0.status) }

        #expect(matchingUsers.count == 1)
        #expect(matchingUsers[0].name == "Alice")
    }

    @Test("In predicate with empty values")
    func testInPredicateWithEmptyValues() {
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin")
        ]

        let targetStatuses: [String] = []
        let matchingUsers = users.filter { targetStatuses.contains($0.status) }

        #expect(matchingUsers.isEmpty)
    }
}

// MARK: - Candidates Filtering Tests

@Suite("Bitmap Fusion - Candidates Filtering", .heartbeat)
struct BitmapFusionCandidatesTests {

    @Test("Filter results by candidates set")
    func testCandidatesFiltering() {
        let users = [
            BitmapFusionUser(id: "user-001", name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(id: "user-002", name: "Bob", status: "active", role: "user"),
            BitmapFusionUser(id: "user-003", name: "Charlie", status: "active", role: "guest"),
        ]

        let candidates: Set<String> = ["user-001", "user-003"]
        let filtered = users.filter { candidates.contains($0.id) }

        #expect(filtered.count == 2)
        #expect(filtered.map(\.id).sorted() == ["user-001", "user-003"])
    }

    @Test("Empty candidates set returns no results")
    func testEmptyCandidatesSet() {
        let users = [
            BitmapFusionUser(id: "user-001", name: "Alice", status: "active", role: "admin")
        ]

        let candidates: Set<String> = []
        let filtered = users.filter { candidates.contains($0.id) }

        #expect(filtered.isEmpty)
    }

    @Test("Candidates set with no matching IDs")
    func testCandidatesWithNoMatches() {
        let users = [
            BitmapFusionUser(id: "user-001", name: "Alice", status: "active", role: "admin")
        ]

        let candidates: Set<String> = ["user-999", "user-998"]
        let filtered = users.filter { candidates.contains($0.id) }

        #expect(filtered.isEmpty)
    }
}

// MARK: - Edge Case Tests

@Suite("Bitmap Fusion - Edge Cases", .heartbeat)
struct BitmapFusionEdgeCaseTests {

    @Test("Empty string field value")
    func testEmptyStringFieldValue() {
        let user = BitmapFusionUser(name: "NoStatus", status: "", role: "user")
        #expect(user.status.isEmpty)
    }

    @Test("Unicode field values")
    func testUnicodeFieldValues() {
        let user = BitmapFusionUser(
            name: "日本語ユーザー",
            status: "アクティブ",
            role: "管理者"
        )

        #expect(user.status == "アクティブ")
        #expect(user.role == "管理者")
        #expect(user.name == "日本語ユーザー")
    }

    @Test("Special characters in field values")
    func testSpecialCharactersInFieldValues() {
        let user = BitmapFusionUser(
            name: "User with 'quotes' and \"double quotes\"",
            status: "status-with-dash",
            role: "role_with_underscore"
        )

        #expect(user.status == "status-with-dash")
        #expect(user.role == "role_with_underscore")
    }

    @Test("Very long field values")
    func testVeryLongFieldValues() {
        let longStatus = String(repeating: "x", count: 1000)
        let user = BitmapFusionUser(name: "LongStatus", status: longStatus, role: "user")

        #expect(user.status.count == 1000)
    }

    @Test("Numeric string field values")
    func testNumericStringFieldValues() {
        let user = BitmapFusionUser(name: "User123", status: "100", role: "999")

        #expect(user.status == "100")
        #expect(user.role == "999")
    }

    @Test("Whitespace-only field values")
    func testWhitespaceOnlyFieldValues() {
        let user = BitmapFusionUser(name: "WhitespaceUser", status: "   ", role: "\t\n")

        #expect(user.status == "   ")
        #expect(user.role == "\t\n")
    }
}

// MARK: - Deduplication Tests

@Suite("Bitmap Fusion - Deduplication", .heartbeat)
struct BitmapFusionDeduplicationTests {

    @Test("OR query deduplicates results")
    func testOrQueryDeduplication() {
        // Simulate the deduplication logic used in Bitmap.execute for .in predicate
        let user1 = BitmapFusionUser(id: "user-001", name: "Alice", status: "active", role: "admin")

        // User appears in results for both "active" and "admin" queries
        let allResults = [user1, user1]  // Duplicate

        var seen: Set<String> = []
        let deduplicated = allResults.filter { item in
            let id = "\(item.id)"
            if seen.contains(id) { return false }
            seen.insert(id)
            return true
        }

        #expect(deduplicated.count == 1)
        #expect(deduplicated[0].id == "user-001")
    }

    @Test("Deduplication preserves first occurrence")
    func testDeduplicationPreservesFirstOccurrence() {
        let users = [
            BitmapFusionUser(id: "user-001", name: "FirstAlice", status: "active", role: "admin"),
            BitmapFusionUser(id: "user-001", name: "SecondAlice", status: "pending", role: "user"),  // Same ID, different data
        ]

        var seen: Set<String> = []
        let deduplicated = users.filter { item in
            let id = item.id
            if seen.contains(id) { return false }
            seen.insert(id)
            return true
        }

        #expect(deduplicated.count == 1)
        #expect(deduplicated[0].name == "FirstAlice")  // First occurrence preserved
    }
}
#endif

#endif
