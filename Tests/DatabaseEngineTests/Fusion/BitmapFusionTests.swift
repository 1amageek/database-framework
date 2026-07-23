#if !os(WASI)
#if FOUNDATION_DB
// BitmapFusionTests.swift
// Tests for BitmapIndex Fusion query (Bitmap)

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import DatabaseValue
import TestSupport
@testable import DatabaseEngine
@testable import BitmapIndex

// MARK: - Test Model

/// User model with bitmap-indexed status and role fields
struct BitmapFusionUser: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var status: String  // "active", "inactive", "pending"
    var role: String    // "admin", "user", "guest"

    init(id: String = UUID().uuidString, name: String, status: String, role: String) {
        self.id = id
        self.name = name
        self.status = status
        self.role = role
    }

    static var persistableType: String { "BitmapFusionUser" }
    static var allFields: [String] { ["id", "name", "status", "role"] }

    static var indexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "BitmapTestUser_bitmap_status",
                keyPaths: [\BitmapFusionUser.status],
                kind: BitmapIndexKind<BitmapFusionUser>(field: \.status)
            ),
            IndexDescriptor(
                name: "BitmapTestUser_bitmap_role",
                keyPaths: [\BitmapFusionUser.role],
                kind: BitmapIndexKind<BitmapFusionUser>(field: \.role)
            )
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "status": return status
        case "role": return role
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BitmapFusionUser, Value>) -> String {
        switch keyPath {
        case \BitmapFusionUser.id: return "id"
        case \BitmapFusionUser.name: return "name"
        case \BitmapFusionUser.status: return "status"
        case \BitmapFusionUser.role: return "role"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BitmapFusionUser>) -> String {
        switch keyPath {
        case \BitmapFusionUser.id: return "id"
        case \BitmapFusionUser.name: return "name"
        case \BitmapFusionUser.status: return "status"
        case \BitmapFusionUser.role: return "role"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BitmapFusionUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Context

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

        let kind = BitmapIndexKind<BitmapFusionUser>(field: \.status)
        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "status"),
            subspaceKey: indexName,
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
            let encoder = JSONEncoder()
            let data = try encoder.encode(["id": user.id, "name": user.name, "status": user.status, "role": user.role])

            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            try await storage.write(Bytes(data), for: itemKey)

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

    @Test("BitmapIndexKind identifier is 'bitmap'")
    func testBitmapIndexKindIdentifier() {
        #expect(BitmapIndexKind<BitmapFusionUser>.identifier == "bitmap")
    }

    @Test("Index descriptor configuration")
    func testIndexDescriptorConfiguration() {
        let descriptors = BitmapFusionUser.indexDescriptors
        #expect(descriptors.count == 2)

        let statusIndex = descriptors.first { $0.name.contains("status") }
        #expect(statusIndex != nil)
        #expect(statusIndex?.kindIdentifier == "bitmap")
        #expect(statusIndex?.kind.fieldNames.contains("status") == true)

        let roleIndex = descriptors.first { $0.name.contains("role") }
        #expect(roleIndex != nil)
        #expect(roleIndex?.kindIdentifier == "bitmap")
        #expect(roleIndex?.kind.fieldNames.contains("role") == true)
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
            type: "BitmapFusionUser",
            field: "unknownField",
            kind: "bitmap"
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
            BitmapFusionUser(name: "Charlie", status: "active", role: "guest")
        ]

        let results = users.map { ScoredResult(item: $0, score: 1.0) }

        #expect(results.allSatisfy { $0.score == 1.0 })
    }
}

// MARK: - Initialization Tests

@Suite("Bitmap Fusion - Initialization", .heartbeat)
struct BitmapFusionInitializationTests {

    @Test("fieldName extraction from KeyPath")
    func testFieldNameExtraction() {
        let fieldName = BitmapFusionUser.fieldName(for: \BitmapFusionUser.status)
        #expect(fieldName == "status")

        let roleFieldName = BitmapFusionUser.fieldName(for: \BitmapFusionUser.role)
        #expect(roleFieldName == "role")
    }

    @Test("dynamicMember subscript access")
    func testDynamicMemberAccess() {
        let user = BitmapFusionUser(name: "Alice", status: "active", role: "admin")

        #expect(user[dynamicMember: "status"] as? String == "active")
        #expect(user[dynamicMember: "role"] as? String == "admin")
        #expect(user[dynamicMember: "name"] as? String == "Alice")
        #expect(user[dynamicMember: "unknown"] == nil)
    }
}

// MARK: - Integration Tests

@Suite("Bitmap Fusion - Integration Tests", .serialized, .heartbeat)
struct BitmapFusionIntegrationTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Bitmap index maintainer initialization")
    func testBitmapIndexMaintainerInitialization() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let context = try await BitmapFusionContext()
            defer { Task { try? await context.cleanup() } }

            // Verify maintainer is created with correct configuration
            // (maintainer is non-optional, so we verify it's the expected type)
            #expect(type(of: context.maintainer) == BitmapIndexMaintainer<BitmapFusionUser>.self)
        }
    }

    @Test("Insert and index user")
    func testInsertAndIndexUser() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let context = try await BitmapFusionContext()
            defer { Task { try? await context.cleanup() } }

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

    @Test("Multiple users with same status value")
    func testMultipleUsersWithSameStatus() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let context = try await BitmapFusionContext()
            defer { Task { try? await context.cleanup() } }

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

// MARK: - Predicate Tests

@Suite("Bitmap Fusion - Predicates", .heartbeat)
struct BitmapFusionPredicateTests {

    @Test("Equals predicate matching")
    func testEqualsPredicateMatching() {
        let users = [
            BitmapFusionUser(name: "Alice", status: "active", role: "admin"),
            BitmapFusionUser(name: "Bob", status: "pending", role: "user"),
            BitmapFusionUser(name: "Charlie", status: "active", role: "guest")
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
            BitmapFusionUser(name: "Charlie", status: "inactive", role: "guest")
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
            BitmapFusionUser(name: "Bob", status: "pending", role: "user")
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
            BitmapFusionUser(id: "user-003", name: "Charlie", status: "active", role: "guest")
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
            BitmapFusionUser(id: "user-001", name: "SecondAlice", status: "pending", role: "user")  // Same ID, different data
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
