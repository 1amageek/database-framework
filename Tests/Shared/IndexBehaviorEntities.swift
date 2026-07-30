// IndexBehaviorEntities.swift
// Persistable entities shared by index behavior scenarios.

import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

// MARK: - Product Model (for Scalar Index tests)

/// Product model for scalar index testing
@Persistable
public struct Product {
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

}

// MARK: - Order Model (for Composite Index tests)

/// Order model for composite index testing
@Persistable
public struct Order {
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

}

// MARK: - Sale Model (for Aggregation Index tests)

/// Sale model for aggregation index testing
@Persistable
public struct Sale {
    public var id: Int64
    public var category: String
    public var amount: Int64
    public var quantity: Int64

}

// MARK: - Document Model (for Version Index tests)

/// Document model for version index testing
@Persistable
public struct Document {
    public var id: Int64
    public var documentID: Int64 { id }  // Alias
    public var title: String
    public var content: String
    public var version: Int32

    public init(documentID: Int64, title: String, content: String, version: Int32) {
        self.id = documentID
        self.title = title
        self.content = content
        self.version = version
    }

}
