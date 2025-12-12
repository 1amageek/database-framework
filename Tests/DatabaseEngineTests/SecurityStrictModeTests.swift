// SecurityStrictModeTests.swift
// Tests for strict mode security behavior

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Models

/// Model WITHOUT SecurityPolicy (for strict mode rejection testing)
private struct UnsecuredItem: Persistable {
    typealias ID = String

    var id: String
    var name: String

    init(id: String = UUID().uuidString, name: String) {
        self.id = id
        self.name = name
    }

    static var persistableType: String { "UnsecuredItem" }
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

    static func fieldName<Value>(for keyPath: KeyPath<UnsecuredItem, Value>) -> String {
        switch keyPath {
        case \UnsecuredItem.id: return "id"
        case \UnsecuredItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<UnsecuredItem>) -> String {
        switch keyPath {
        case \UnsecuredItem.id: return "id"
        case \UnsecuredItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<UnsecuredItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Model WITH SecurityPolicy (for comparison)
private struct SecuredItem: Persistable, SecurityPolicy {
    typealias ID = String

    var id: String
    var ownerID: String
    var name: String

    init(id: String = UUID().uuidString, ownerID: String, name: String) {
        self.id = id
        self.ownerID = ownerID
        self.name = name
    }

    static var persistableType: String { "SecuredItem" }
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

    static func fieldName<Value>(for keyPath: KeyPath<SecuredItem, Value>) -> String {
        switch keyPath {
        case \SecuredItem.id: return "id"
        case \SecuredItem.ownerID: return "ownerID"
        case \SecuredItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SecuredItem>) -> String {
        switch keyPath {
        case \SecuredItem.id: return "id"
        case \SecuredItem.ownerID: return "ownerID"
        case \SecuredItem.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SecuredItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }

    // MARK: - SecurityPolicy

    static func allowGet(resource: SecuredItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<SecuredItem>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: SecuredItem, auth: (any AuthContext)?) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(resource: SecuredItem, newResource: SecuredItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowDelete(resource: SecuredItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }
}

/// Simple auth context for testing
private struct TestAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

// MARK: - Tests

@Suite("Security Strict Mode", .serialized)
struct SecurityStrictModeTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Strict Mode Tests

    @Test("Strict mode rejects models without SecurityPolicy on create")
    func strictModeRejectsUnsecuredCreate() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnsecuredItem(name: "Test")

        // Non-admin user
        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateCreate(item)
            }
        }
    }

    @Test("Strict mode rejects models without SecurityPolicy on get")
    func strictModeRejectsUnsecuredGet() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnsecuredItem(name: "Test")

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateGet(item)
            }
        }
    }

    @Test("Strict mode rejects models without SecurityPolicy on list")
    func strictModeRejectsUnsecuredList() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateList(
                    type: UnsecuredItem.self,
                    limit: nil,
                    offset: nil,
                    orderBy: nil
                )
            }
        }
    }

    @Test("Strict mode rejects models without SecurityPolicy on update")
    func strictModeRejectsUnsecuredUpdate() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnsecuredItem(name: "Test")
        let newItem = UnsecuredItem(id: item.id, name: "Updated")

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateUpdate(item, newResource: newItem)
            }
        }
    }

    @Test("Strict mode rejects models without SecurityPolicy on delete")
    func strictModeRejectsUnsecuredDelete() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnsecuredItem(name: "Test")

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateDelete(item)
            }
        }
    }

    // MARK: - Non-Strict Mode Tests

    @Test("Non-strict mode allows models without SecurityPolicy")
    func nonStrictModeAllowsUnsecured() throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: false)
        )

        let item = UnsecuredItem(name: "Test")

        try AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            // Should NOT throw
            try delegate.evaluateCreate(item)
            try delegate.evaluateGet(item)
            try delegate.evaluateDelete(item)
            try delegate.evaluateList(
                type: UnsecuredItem.self,
                limit: nil,
                offset: nil,
                orderBy: nil
            )
        }
    }

    // MARK: - Secured Model Tests

    @Test("Strict mode evaluates SecurityPolicy for secured models")
    func strictModeEvaluatesSecurityPolicy() throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = SecuredItem(ownerID: "user1", name: "Test")

        // Owner can access
        try AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            try delegate.evaluateCreate(item)
            try delegate.evaluateGet(item)
            try delegate.evaluateDelete(item)
        }

        // Non-owner cannot access
        AuthContextKey.$current.withValue(TestAuth(userID: "user2")) {
            #expect(throws: SecurityError.self) {
                try delegate.evaluateGet(item)
            }
        }
    }

    // MARK: - Admin Bypass Tests

    @Test("Admin bypasses strict mode check")
    func adminBypassesStrictMode() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true, adminRoles: ["admin"])
        )

        let item = UnsecuredItem(name: "Test")

        // Admin can access even without SecurityPolicy
        try AuthContextKey.$current.withValue(TestAuth(userID: "admin1", roles: ["admin"])) {
            try delegate.evaluateCreate(item)
            try delegate.evaluateGet(item)
        }
    }

    // MARK: - Error Message Tests

    @Test("Error message indicates SecurityPolicy not implemented")
    func errorMessageIndicatesSecurityPolicyNotImplemented() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        let item = UnsecuredItem(name: "Test")

        let error: SecurityError? = AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            do {
                try delegate.evaluateCreate(item)
                return nil
            } catch let error as SecurityError {
                return error
            } catch {
                return nil
            }
        }

        let securityError = try #require(error)
        #expect(securityError.reason.contains("SecurityPolicy"))
        #expect(securityError.operation == .create)
        #expect(securityError.targetType == "UnsecuredItem")
    }

    // MARK: - requireAdmin Tests

    @Test("requireAdmin uses admin operation type")
    func requireAdminUsesAdminOperationType() async throws {
        let delegate = DefaultSecurityDelegate(
            configuration: .enabled(strict: true)
        )

        AuthContextKey.$current.withValue(TestAuth(userID: "user1")) {
            #expect(throws: SecurityError.self) {
                try delegate.requireAdmin(operation: "clearAll", targetType: "TestType")
            }
        }
    }
}
