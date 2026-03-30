#if FOUNDATION_DB
// SecurityFilteringTests.swift
// Tests for LIST+GET filtering, index query security, and diagnostic improvements

import Testing
import TestHeartbeat
import Foundation
import Core
@testable import DatabaseEngine

// MARK: - Test Models

/// Model WITH SecurityPolicy: owner-based GET access
private struct OwnedItem: Persistable, SecurityPolicy {
    typealias ID = String

    var id: String
    var ownerID: String
    var name: String

    init(id: String = UUID().uuidString, ownerID: String, name: String) {
        self.id = id
        self.ownerID = ownerID
        self.name = name
    }

    static var persistableType: String { "OwnedItem" }
    static var allFields: [String] { ["id", "ownerID", "name"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "ownerID": return ownerID
        case "name": return name
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<OwnedItem, Value>) -> String {
        switch keyPath {
        case \OwnedItem.id: return "id"
        case \OwnedItem.ownerID: return "ownerID"
        case \OwnedItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<OwnedItem>) -> String {
        switch keyPath {
        case \OwnedItem.id: return "id"
        case \OwnedItem.ownerID: return "ownerID"
        case \OwnedItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<OwnedItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }

    // MARK: - SecurityPolicy

    static func allowGet(resource: OwnedItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<OwnedItem>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: OwnedItem, auth: (any AuthContext)?) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(resource: OwnedItem, newResource: OwnedItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowDelete(resource: OwnedItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }
}

/// Model WITHOUT SecurityPolicy (for strict mode tests)
private struct UnprotectedItem: Persistable {
    typealias ID = String

    var id: String
    var name: String

    init(id: String = UUID().uuidString, name: String) {
        self.id = id
        self.name = name
    }

    static var persistableType: String { "UnprotectedItem" }
    static var allFields: [String] { ["id", "name"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<UnprotectedItem, Value>) -> String {
        switch keyPath {
        case \UnprotectedItem.id: return "id"
        case \UnprotectedItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<UnprotectedItem>) -> String {
        switch keyPath {
        case \UnprotectedItem.id: return "id"
        case \UnprotectedItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<UnprotectedItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Simple auth context for testing
private struct TestAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

// MARK: - Category A: filterByGetAccess Tests

@Suite("Security Filtering - filterByGetAccess", .serialized, .heartbeat)
struct FilterByGetAccessTests {

    // A1: Only owner's items pass through
    @Test("filterByGetAccess returns only items owned by current user")
    func ownerOnlyPassThrough() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice's item"),
            OwnedItem(ownerID: "bob", name: "Bob's item"),
            OwnedItem(ownerID: "alice", name: "Alice's second item"),
        ]

        let filtered = AuthContextKey.$current.withValue(TestAuth(userID: "alice")) {
            delegate.filterByGetAccess(items)
        }

        #expect(filtered.count == 2)
        #expect(filtered.allSatisfy { $0.ownerID == "alice" })
    }

    // A2: All items denied when unauthenticated
    @Test("filterByGetAccess returns empty array when unauthenticated")
    func allDeniedWhenUnauthenticated() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice's item"),
            OwnedItem(ownerID: "bob", name: "Bob's item"),
        ]

        // No auth context set (nil)
        let filtered = delegate.filterByGetAccess(items)

        #expect(filtered.isEmpty)
    }

    // A3: Admin passes all items through
    @Test("filterByGetAccess returns all items for admin")
    func adminBypassesFilter() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true, adminRoles: ["admin"])
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice's item"),
            OwnedItem(ownerID: "bob", name: "Bob's item"),
            OwnedItem(ownerID: "carol", name: "Carol's item"),
        ]

        let filtered = AuthContextKey.$current.withValue(TestAuth(userID: "admin-user", roles: ["admin"])) {
            delegate.filterByGetAccess(items)
        }

        #expect(filtered.count == 3)
    }

    // A4: No SecurityPolicy + strict:false → all items pass
    @Test("filterByGetAccess returns all items when SecurityPolicy not implemented and strict is false")
    func noSecurityPolicyNonStrict() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: false)
        )

        let items = [
            UnprotectedItem(name: "Item 1"),
            UnprotectedItem(name: "Item 2"),
        ]

        let filtered = AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            delegate.filterByGetAccess(items)
        }

        #expect(filtered.count == 2)
    }

    // A5: No SecurityPolicy + strict:true → all items denied
    @Test("filterByGetAccess returns empty array when SecurityPolicy not implemented and strict is true")
    func noSecurityPolicyStrict() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let items = [
            UnprotectedItem(name: "Item 1"),
            UnprotectedItem(name: "Item 2"),
        ]

        let filtered = AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            delegate.filterByGetAccess(items)
        }

        #expect(filtered.isEmpty)
    }
}

// MARK: - Category B: DefaultSecurityDelegate LIST+GET Integration Tests

@Suite("Security Filtering - LIST+GET Integration", .serialized, .heartbeat)
struct ListGetIntegrationTests {

    // B1: evaluateList succeeds, then filterByGetAccess filters by owner
    @Test("LIST passes then GET filters by owner")
    func listPassGetFilters() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice"),
            OwnedItem(ownerID: "bob", name: "Bob"),
            OwnedItem(ownerID: "carol", name: "Carol"),
        ]

        let result = AuthContextKey.$current.withValue(TestAuth(userID: "alice")) {
            // Step 1: LIST evaluation (should succeed for authenticated user)
            do {
                try delegate.evaluateList(
                    type: OwnedItem.self,
                    limit: nil,
                    offset: nil,
                    orderBy: nil
                )
            } catch {
                Issue.record("LIST should not throw for authenticated user: \(error)")
                return [OwnedItem]()
            }

            // Step 2: GET filtering
            return delegate.filterByGetAccess(items)
        }

        #expect(result.count == 1)
        #expect(result.first?.ownerID == "alice")
    }

    // B3: LIST denied → SecurityError
    @Test("LIST denied throws SecurityError")
    func listDeniedThrows() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        // OwnedItem.allowList requires auth != nil, so no auth → denied
        #expect(throws: SecurityError.self) {
            try delegate.evaluateList(
                type: OwnedItem.self,
                limit: nil,
                offset: nil,
                orderBy: nil
            )
        }
    }

    // B5: fetchCount should not require GET filter (counts, not items)
    @Test("Count operations do not apply GET filtering")
    func countDoesNotFilter() throws {
        // Verify the delegate protocol has no count-specific method
        // Count operations only evaluate LIST, which is correct behavior
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        // LIST should succeed for authenticated user
        try AuthContextKey.$current.withValue(TestAuth(userID: "alice")) {
            try delegate.evaluateList(
                type: OwnedItem.self,
                limit: nil,
                offset: nil,
                orderBy: nil
            )
            // No GET evaluation needed for count - this is the expected behavior
        }
    }

    // B6: Single GET (fetch by ID) throws on denial (not filter)
    @Test("Single GET throws SecurityError on denial")
    func singleGetThrowsOnDenial() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = OwnedItem(ownerID: "alice", name: "Alice's item")

        // Bob trying to GET Alice's item → throws
        AuthContextKey.$current.withValue(TestAuth(userID: "bob")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateGet(item)
            }
        }
    }

    // B6 complement: Single GET succeeds for owner
    @Test("Single GET succeeds for resource owner")
    func singleGetSucceedsForOwner() throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = OwnedItem(ownerID: "alice", name: "Alice's item")

        try AuthContextKey.$current.withValue(TestAuth(userID: "alice")) {
            try delegate.evaluateGet(item)
        }
    }
}

// MARK: - Category C: DisabledSecurityDelegate Tests

@Suite("Security Filtering - DisabledSecurityDelegate", .heartbeat)
struct DisabledDelegateTests {

    // C1: DisabledSecurityDelegate filterByGetAccess passes all items
    @Test("DisabledSecurityDelegate passes all items through filter")
    func disabledDelegatePassesAll() {
        let delegate = DisabledSecurityDelegate()

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice"),
            OwnedItem(ownerID: "bob", name: "Bob"),
        ]

        let filtered = delegate.filterByGetAccess(items)
        #expect(filtered.count == 2)
    }
}

// MARK: - Category D: SecurityError Diagnostic Tests

@Suite("Security Filtering - SecurityError Diagnostics", .heartbeat)
struct SecurityErrorDiagnosticTests {

    // D1: GET denial includes userID
    @Test("GET denial SecurityError includes userID")
    func getDenialIncludesUserID() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = OwnedItem(ownerID: "alice", name: "Alice's item")

        AuthContextKey.$current.withValue(TestAuth(userID: "bob")) {
            do {
                try delegate.evaluateGet(item)
                Issue.record("Should have thrown SecurityError")
            } catch let error as SecurityError {
                #expect(error.operation == .get)
                #expect(error.userID == "bob")
                #expect(error.targetType == "OwnedItem")
                #expect(error.resourceID != nil)
            } catch {
                Issue.record("Unexpected error type: \(error)")
            }
        }
    }

    // D2: LIST denial includes targetType
    @Test("LIST denial SecurityError includes targetType")
    func listDenialIncludesTargetType() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        // No auth → LIST denied for OwnedItem
        do {
            try delegate.evaluateList(
                type: OwnedItem.self,
                limit: nil,
                offset: nil,
                orderBy: nil
            )
            Issue.record("Should have thrown SecurityError")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
            #expect(error.targetType == "OwnedItem")
        } catch {
            Issue.record("Unexpected error type: \(error)")
        }
    }

    // D3: Strict mode error mentions SecurityPolicy
    @Test("Strict mode error reason contains SecurityPolicy")
    func strictModeErrorMentionsSecurityPolicy() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnprotectedItem(name: "Test")

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            do {
                try delegate.evaluateGet(item)
                Issue.record("Should have thrown SecurityError")
            } catch let error as SecurityError {
                #expect(error.reason.contains("SecurityPolicy"))
            } catch {
                Issue.record("Unexpected error type: \(error)")
            }
        }
    }

    // D4: description format includes all fields
    @Test("SecurityError description includes all diagnostic fields")
    func descriptionIncludesAllFields() {
        let error = SecurityError(
            operation: .get,
            targetType: "TestModel",
            reason: "Access denied",
            resourceID: "res-123",
            userID: "user-456"
        )

        let desc = error.description
        #expect(desc.contains("get"))
        #expect(desc.contains("TestModel"))
        #expect(desc.contains("res-123"))
        #expect(desc.contains("user-456"))
        #expect(desc.contains("Access denied"))
    }

    // D4b: description without optional fields
    @Test("SecurityError description works without optional fields")
    func descriptionWithoutOptionalFields() {
        let error = SecurityError(
            operation: .list,
            targetType: "TestModel",
            reason: "Not allowed"
        )

        let desc = error.description
        #expect(desc.contains("list"))
        #expect(desc.contains("TestModel"))
        #expect(desc.contains("Not allowed"))
        #expect(!desc.contains("resource:"))
        #expect(!desc.contains("by user"))
    }
}

// MARK: - Category E: Edge Case Tests

@Suite("Security Filtering - Edge Cases", .heartbeat)
struct SecurityEdgeCaseTests {

    // E1: Security disabled → no filtering
    @Test("Security disabled returns all items without filtering")
    func securityDisabledNoFiltering() {
        let delegate = DefaultSecurityDelegate(
            configuration: .disabled
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice"),
            OwnedItem(ownerID: "bob", name: "Bob"),
        ]

        // Even without auth, all items pass (security disabled)
        let filtered = delegate.filterByGetAccess(items)
        #expect(filtered.count == 2)
    }

    // E2: Admin bypasses all filtering
    @Test("Admin role bypasses GET filtering completely")
    func adminBypassesFiltering() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true, adminRoles: ["admin"])
        )

        let items = [
            OwnedItem(ownerID: "alice", name: "Alice"),
            OwnedItem(ownerID: "bob", name: "Bob"),
            OwnedItem(ownerID: "carol", name: "Carol"),
        ]

        let filtered = AuthContextKey.$current.withValue(TestAuth(userID: "superadmin", roles: ["admin"])) {
            delegate.filterByGetAccess(items)
        }

        #expect(filtered.count == 3)
    }

    // E3: No auth + strict:false + no SecurityPolicy → all items pass
    @Test("No auth with non-strict mode and no SecurityPolicy returns all items")
    func noAuthNonStrictNoPolicy() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: false)
        )

        let items = [
            UnprotectedItem(name: "Item 1"),
            UnprotectedItem(name: "Item 2"),
        ]

        // No auth context, strict: false, no SecurityPolicy → all pass
        // shouldEvaluate is true (no auth), but no SecurityPolicy + strict:false → allows
        let filtered = delegate.filterByGetAccess(items)
        #expect(filtered.count == 2)
    }

    // E4: Empty array input
    @Test("filterByGetAccess handles empty array")
    func emptyArrayHandled() {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let filtered: [OwnedItem] = AuthContextKey.$current.withValue(TestAuth(userID: "alice")) {
            delegate.filterByGetAccess([])
        }

        #expect(filtered.isEmpty)
    }

    // E5: SecurityError backward compatibility (init without new fields)
    @Test("SecurityError init without resourceID and userID still works")
    func securityErrorBackwardCompatibility() {
        let error = SecurityError(
            operation: .get,
            targetType: "Test",
            reason: "Denied"
        )

        #expect(error.resourceID == nil)
        #expect(error.userID == nil)
        #expect(error.operation == .get)
        #expect(error.targetType == "Test")
        #expect(error.reason == "Denied")
    }
}
#endif
