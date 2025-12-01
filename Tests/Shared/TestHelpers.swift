// TestHelpers.swift
// Test helper models conforming to Persistable for index layer tests

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Product Model (for Scalar Index tests)

/// Product model for scalar index testing
public struct Product: Persistable {
    public typealias ID = Int64

    public var id: Int64
    public var productID: Int64 { id }  // Alias
    public var category: String
    public var price: Int64
    public var name: String
    public var inStock: Bool?  // Optional field for sparse index testing

    public init(productID: Int64, category: String, price: Int64, name: String, inStock: Bool?) {
        self.id = productID
        self.category = category
        self.price = price
        self.name = name
        self.inStock = inStock
    }

    public static var persistableType: String { "Product" }

    public static var allFields: [String] { ["id", "productID", "category", "price", "name", "inStock"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id", "productID": return id
        case "category": return category
        case "price": return price
        case "name": return name
        case "inStock": return inStock
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Product, Value>) -> String {
        switch keyPath {
        case \Product.id: return "id"
        case \Product.productID: return "productID"
        case \Product.category: return "category"
        case \Product.price: return "price"
        case \Product.name: return "name"
        case \Product.inStock: return "inStock"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Product>) -> String {
        switch keyPath {
        case \Product.id: return "id"
        case \Product.productID: return "productID"
        case \Product.category: return "category"
        case \Product.price: return "price"
        case \Product.name: return "name"
        case \Product.inStock: return "inStock"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Product> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Order Model (for Composite Index tests)

/// Order model for composite index testing
public struct Order: Persistable {
    public typealias ID = Int64

    public var id: Int64
    public var orderID: Int64 { id }  // Alias
    public var customerID: Int64
    public var status: String
    public var amount: Int64
    public var createdAt: Int64  // Unix timestamp

    public init(orderID: Int64, customerID: Int64, status: String, amount: Int64, createdAt: Int64) {
        self.id = orderID
        self.customerID = customerID
        self.status = status
        self.amount = amount
        self.createdAt = createdAt
    }

    public static var persistableType: String { "Order" }

    public static var allFields: [String] { ["id", "orderID", "customerID", "status", "amount", "createdAt"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id", "orderID": return id
        case "customerID": return customerID
        case "status": return status
        case "amount": return amount
        case "createdAt": return createdAt
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Order, Value>) -> String {
        switch keyPath {
        case \Order.id: return "id"
        case \Order.orderID: return "orderID"
        case \Order.customerID: return "customerID"
        case \Order.status: return "status"
        case \Order.amount: return "amount"
        case \Order.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Order>) -> String {
        switch keyPath {
        case \Order.id: return "id"
        case \Order.orderID: return "orderID"
        case \Order.customerID: return "customerID"
        case \Order.status: return "status"
        case \Order.amount: return "amount"
        case \Order.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Order> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Sale Model (for Aggregation Index tests)

/// Sale model for aggregation index testing
public struct Sale: Persistable {
    public typealias ID = Int64

    public var id: Int64
    public var category: String
    public var amount: Int64
    public var quantity: Int64

    public init(id: Int64, category: String, amount: Int64, quantity: Int64) {
        self.id = id
        self.category = category
        self.amount = amount
        self.quantity = quantity
    }

    public static var persistableType: String { "Sale" }

    public static var allFields: [String] { ["id", "category", "amount", "quantity"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "category": return category
        case "amount": return amount
        case "quantity": return quantity
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Sale, Value>) -> String {
        switch keyPath {
        case \Sale.id: return "id"
        case \Sale.category: return "category"
        case \Sale.amount: return "amount"
        case \Sale.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Sale>) -> String {
        switch keyPath {
        case \Sale.id: return "id"
        case \Sale.category: return "category"
        case \Sale.amount: return "amount"
        case \Sale.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Sale> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Document Model (for Version Index tests)

/// Document model for version index testing
public struct Document: Persistable {
    public typealias ID = Int64

    public var id: Int64
    public var documentID: Int64 { id }  // Alias
    public var title: String
    public var content: String
    public var version: Int

    public init(documentID: Int64, title: String, content: String, version: Int) {
        self.id = documentID
        self.title = title
        self.content = content
        self.version = version
    }

    public static var persistableType: String { "Document" }

    public static var allFields: [String] { ["id", "documentID", "title", "content", "version"] }

    public static var indexDescriptors: [IndexDescriptor] { [] }

    public static func fieldNumber(for fieldName: String) -> Int? { nil }

    public static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    public subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id", "documentID": return id
        case "title": return title
        case "content": return content
        case "version": return version
        default: return nil
        }
    }

    public static func fieldName<Value>(for keyPath: KeyPath<Document, Value>) -> String {
        switch keyPath {
        case \Document.id: return "id"
        case \Document.documentID: return "documentID"
        case \Document.title: return "title"
        case \Document.content: return "content"
        case \Document.version: return "version"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: PartialKeyPath<Document>) -> String {
        switch keyPath {
        case \Document.id: return "id"
        case \Document.documentID: return "documentID"
        case \Document.title: return "title"
        case \Document.content: return "content"
        case \Document.version: return "version"
        default: return "\(keyPath)"
        }
    }

    public static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Document> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
