// CLIScenarioRecords.swift
// Persistable records and index metadata for DatabaseCLI scenarios.

import Foundation
import Core
import DatabaseValue
import DatabaseEngine
import StorageKit

// MARK: - CLI Index Kind

/// A flat index kind used by CLI schema scenarios.
public struct CLIFlatIndexKind: IndexKind {
    public static var identifier: String { "cli-flat" }
    public static var subspaceStructure: SubspaceStructure { .flat }

    public let indexName: String
    public let fieldNames: [String]

    public init(name: String, fields: [String]) {
        self.indexName = name
        self.fieldNames = fields
    }

    public static func validateTypes(_ types: [Any.Type]) throws {
        // CLI schema scenarios intentionally accept every field type.
    }
}

// MARK: - CLIUser Model

/// User model for CLI testing with indexes
public struct CLIUser: Persistable {
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

    public static var persistableType: String { "CLIUser" }

    public static var directoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("test"), .staticPath("cli"), .staticPath("users")]
    }

    public static var allFields: [String] { ["id", "name", "email", "age"] }

    // NOTE: Override `descriptors` instead of `indexDescriptors`
    // because indexDescriptors is a computed property that derives from descriptors.
    // When accessed via type erasure (any Persistable.Type), Swift dispatches to the
    // protocol extension's default implementation which reads from `descriptors`.
    public static var descriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "CLIUser_email",
                keyPaths: [\CLIUser.email],
                kind: CLIFlatIndexKind(name: "CLIUser_email", fields: ["email"]),
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

    public static func fieldName<Value>(for keyPath: KeyPath<CLIUser, Value>) -> String {
        switch keyPath {
        case \CLIUser.id: return "id"
        case \CLIUser.name: return "name"
        case \CLIUser.email: return "email"
        case \CLIUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<CLIUser>) -> String {
        switch keyPath {
        case \CLIUser.id: return "id"
        case \CLIUser.name: return "name"
        case \CLIUser.email: return "email"
        case \CLIUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CLIUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - CLIOrder Model

/// Order model for CLI testing with indexes
public struct CLIOrder: Persistable {
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

    public static var persistableType: String { "CLIOrder" }

    public static var directoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("test"), .staticPath("cli"), .staticPath("orders")]
    }

    public static var allFields: [String] { ["id", "userId", "total", "status"] }

    // Override `descriptors` - see CLIUser for explanation
    public static var descriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "CLIOrder_userId",
                keyPaths: [\CLIOrder.userId],
                kind: CLIFlatIndexKind(name: "CLIOrder_userId", fields: ["userId"]),
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

    public static func fieldName<Value>(for keyPath: KeyPath<CLIOrder, Value>) -> String {
        switch keyPath {
        case \CLIOrder.id: return "id"
        case \CLIOrder.userId: return "userId"
        case \CLIOrder.total: return "total"
        case \CLIOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<CLIOrder>) -> String {
        switch keyPath {
        case \CLIOrder.id: return "id"
        case \CLIOrder.userId: return "userId"
        case \CLIOrder.total: return "total"
        case \CLIOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CLIOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
