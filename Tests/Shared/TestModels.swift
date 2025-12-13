// TestModels.swift
// Shared test models conforming to Persistable for all test targets

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Player Model (for Rank Index tests)

/// Player model for rank index testing
public struct Player: Persistable {
    public typealias ID = String

    public var id: String
    public var name: String
    public var score: Int64
    public var level: Int

    public init(id: String = UUID().uuidString, name: String, score: Int64, level: Int) {
        self.id = id
        self.name = name
        self.score = score
        self.level = level
    }

    public static var persistableType: String { "Player" }

    public static var allFields: [String] { ["id", "name", "score", "level"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "score": return score
        case "level": return level
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Player, Value>) -> String {
        switch keyPath {
        case \Player.id: return "id"
        case \Player.name: return "name"
        case \Player.score: return "score"
        case \Player.level: return "level"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Player>) -> String {
        switch keyPath {
        case \Player.id: return "id"
        case \Player.name: return "name"
        case \Player.score: return "score"
        case \Player.level: return "level"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Player> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Location Model (for Spatial Index tests)

/// Location model for spatial index testing
public struct Location: Persistable {
    public typealias ID = String

    public var id: String
    public var name: String
    public var latitude: Double
    public var longitude: Double

    public init(id: String = UUID().uuidString, name: String, latitude: Double, longitude: Double) {
        self.id = id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
    }

    public static var persistableType: String { "Location" }

    public static var allFields: [String] { ["id", "name", "latitude", "longitude"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "latitude": return latitude
        case "longitude": return longitude
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Location, Value>) -> String {
        switch keyPath {
        case \Location.id: return "id"
        case \Location.name: return "name"
        case \Location.latitude: return "latitude"
        case \Location.longitude: return "longitude"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Location>) -> String {
        switch keyPath {
        case \Location.id: return "id"
        case \Location.name: return "name"
        case \Location.latitude: return "latitude"
        case \Location.longitude: return "longitude"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Location> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - VectorItem Model (for Vector Index tests)

/// Vector item model for vector index testing
public struct VectorItem: Persistable {
    public typealias ID = String

    public var id: String
    public var name: String
    public var embedding: [Float]

    public init(id: String = UUID().uuidString, name: String, embedding: [Float]) {
        self.id = id
        self.name = name
        self.embedding = embedding
    }

    public static var persistableType: String { "VectorItem" }

    public static var allFields: [String] { ["id", "name", "embedding"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "embedding": return embedding
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<VectorItem, Value>) -> String {
        switch keyPath {
        case \VectorItem.id: return "id"
        case \VectorItem.name: return "name"
        case \VectorItem.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<VectorItem>) -> String {
        switch keyPath {
        case \VectorItem.id: return "id"
        case \VectorItem.name: return "name"
        case \VectorItem.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<VectorItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Article Model (for Full-Text Index tests)

/// Article model for full-text index testing
public struct Article: Persistable {
    public typealias ID = String

    public var id: String
    public var title: String
    public var body: String

    public init(id: String = UUID().uuidString, title: String, body: String) {
        self.id = id
        self.title = title
        self.body = body
    }

    public static var persistableType: String { "Article" }

    public static var allFields: [String] { ["id", "title", "body"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        case "body": return body
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Article, Value>) -> String {
        switch keyPath {
        case \Article.id: return "id"
        case \Article.title: return "title"
        case \Article.body: return "body"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Article>) -> String {
        switch keyPath {
        case \Article.id: return "id"
        case \Article.title: return "title"
        case \Article.body: return "body"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Article> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Address Model (for Permuted Index tests)

/// Address model for permuted index testing (compound fields: country, city, district)
public struct Address: Persistable {
    public typealias ID = String

    public var id: String
    public var country: String
    public var city: String
    public var district: String
    public var postalCode: String

    public init(id: String = UUID().uuidString, country: String, city: String, district: String, postalCode: String) {
        self.id = id
        self.country = country
        self.city = city
        self.district = district
        self.postalCode = postalCode
    }

    public static var persistableType: String { "Address" }

    public static var allFields: [String] { ["id", "country", "city", "district", "postalCode"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "country": return country
        case "city": return city
        case "district": return district
        case "postalCode": return postalCode
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Address, Value>) -> String {
        switch keyPath {
        case \Address.id: return "id"
        case \Address.country: return "country"
        case \Address.city: return "city"
        case \Address.district: return "district"
        case \Address.postalCode: return "postalCode"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Address>) -> String {
        switch keyPath {
        case \Address.id: return "id"
        case \Address.country: return "country"
        case \Address.city: return "city"
        case \Address.district: return "district"
        case \Address.postalCode: return "postalCode"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Address> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - TenantOrder Model (for Partitioned Directory tests)

/// Order model with dynamic directory for multi-tenant partitioning
///
/// Directory: ["tenants", Field(\.tenantID), "orders"]
/// This model is partitioned by tenantID for testing dynamic directory support.
public struct TenantOrder: Persistable {
    public typealias ID = String

    public var id: String
    public var tenantID: String
    public var status: String
    public var total: Double

    public init(id: String = UUID().uuidString, tenantID: String, status: String = "pending", total: Double = 0) {
        self.id = id
        self.tenantID = tenantID
        self.status = status
        self.total = total
    }

    public static var persistableType: String { "TenantOrder" }

    public static var allFields: [String] { ["id", "tenantID", "status", "total"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    // Dynamic directory: ["tenants", Field(\.tenantID), "orders"]
    public static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("tenants"), Field<TenantOrder>(\.tenantID), Path("orders")]
    }

    public static var directoryLayer: Core.DirectoryLayer { .partition }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "tenantID": return tenantID
        case "status": return status
        case "total": return total
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<TenantOrder, Value>) -> String {
        switch keyPath {
        case \TenantOrder.id: return "id"
        case \TenantOrder.tenantID: return "tenantID"
        case \TenantOrder.status: return "status"
        case \TenantOrder.total: return "total"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<TenantOrder>) -> String {
        switch keyPath {
        case \TenantOrder.id: return "id"
        case \TenantOrder.tenantID: return "tenantID"
        case \TenantOrder.status: return "status"
        case \TenantOrder.total: return "total"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TenantOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
