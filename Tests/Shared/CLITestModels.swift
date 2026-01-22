// CLITestModels.swift
// Shared test models for DatabaseCLI tests

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Mock IndexKind for CLI Tests

/// A simple mock IndexKind for CLI testing
/// This doesn't need the complex setup of ScalarIndexKind
public struct MockIndexKind: IndexKind {
    public static var identifier: String { "mock" }
    public static var subspaceStructure: SubspaceStructure { .flat }

    public let indexName: String
    public let fieldNames: [String]

    public init(name: String, fields: [String]) {
        self.indexName = name
        self.fieldNames = fields
    }

    public static func validateTypes(_ types: [Any.Type]) throws {
        // Accept all types for mock
    }
}

// MARK: - CLITestUser Model

/// User model for CLI testing with indexes
public struct CLITestUser: Persistable {
    public typealias ID = String

    public var id: String
    public var name: String
    public var email: String
    public var age: Int

    public init(id: String = UUID().uuidString, name: String = "", email: String = "", age: Int = 0) {
        self.id = id
        self.name = name
        self.email = email
        self.age = age
    }

    public static var persistableType: String { "CLITestUser" }

    public static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("cli"), Path("users")]
    }

    public static var allFields: [String] { ["id", "name", "email", "age"] }

    // NOTE: Override `descriptors` instead of `indexDescriptors`
    // because indexDescriptors is a computed property that derives from descriptors.
    // When accessed via type erasure (any Persistable.Type), Swift dispatches to the
    // protocol extension's default implementation which reads from `descriptors`.
    public static var descriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "CLITestUser_email",
                keyPaths: [\CLITestUser.email],
                kind: MockIndexKind(name: "CLITestUser_email", fields: ["email"]),
                commonOptions: .init(unique: true)
            )
        ]
    }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "email": return email
        case "age": return age
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<CLITestUser, Value>) -> String {
        switch keyPath {
        case \CLITestUser.id: return "id"
        case \CLITestUser.name: return "name"
        case \CLITestUser.email: return "email"
        case \CLITestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<CLITestUser>) -> String {
        switch keyPath {
        case \CLITestUser.id: return "id"
        case \CLITestUser.name: return "name"
        case \CLITestUser.email: return "email"
        case \CLITestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CLITestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - CLITestOrder Model

/// Order model for CLI testing with indexes
public struct CLITestOrder: Persistable {
    public typealias ID = String

    public var id: String
    public var userId: String
    public var total: Double
    public var status: String

    public init(id: String = UUID().uuidString, userId: String = "", total: Double = 0.0, status: String = "pending") {
        self.id = id
        self.userId = userId
        self.total = total
        self.status = status
    }

    public static var persistableType: String { "CLITestOrder" }

    public static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("cli"), Path("orders")]
    }

    public static var allFields: [String] { ["id", "userId", "total", "status"] }

    // Override `descriptors` - see CLITestUser for explanation
    public static var descriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "CLITestOrder_userId",
                keyPaths: [\CLITestOrder.userId],
                kind: MockIndexKind(name: "CLITestOrder_userId", fields: ["userId"]),
                commonOptions: .init(unique: false)
            )
        ]
    }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "userId": return userId
        case "total": return total
        case "status": return status
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<CLITestOrder, Value>) -> String {
        switch keyPath {
        case \CLITestOrder.id: return "id"
        case \CLITestOrder.userId: return "userId"
        case \CLITestOrder.total: return "total"
        case \CLITestOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<CLITestOrder>) -> String {
        switch keyPath {
        case \CLITestOrder.id: return "id"
        case \CLITestOrder.userId: return "userId"
        case \CLITestOrder.total: return "total"
        case \CLITestOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CLITestOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
