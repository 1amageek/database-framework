// MinIndexBehaviorTests.swift
// Integration tests for MinIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct MinTestProduct: Persistable {
    typealias ID = String

    var id: String
    var category: String
    var brand: String
    var price: Int64

    init(id: String = UUID().uuidString, category: String, brand: String, price: Int64) {
        self.id = id
        self.category = category
        self.brand = brand
        self.price = price
    }

    static var persistableType: String { "MinTestProduct" }
    static var allFields: [String] { ["id", "category", "brand", "price"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "category": return category
        case "brand": return brand
        case "price": return price
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<MinTestProduct, Value>) -> String {
        switch keyPath {
        case \MinTestProduct.id: return "id"
        case \MinTestProduct.category: return "category"
        case \MinTestProduct.brand: return "brand"
        case \MinTestProduct.price: return "price"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<MinTestProduct>) -> String {
        switch keyPath {
        case \MinTestProduct.id: return "id"
        case \MinTestProduct.category: return "category"
        case \MinTestProduct.brand: return "brand"
        case \MinTestProduct.price: return "price"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<MinTestProduct> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: MinIndexMaintainer<MinTestProduct>

    init(indexName: String = "MinTestProduct_category_price") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "min", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: category + price (grouping + min value)
        let index = Index(
            name: indexName,
            kind: MinIndexKind<MinTestProduct>(groupBy: [\.category], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["MinTestProduct"])
        )

        self.maintainer = MinIndexMaintainer<MinTestProduct>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func getMin(for category: String) async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getMin(
                groupingValues: [category],
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("MinIndex Behavior Tests", .tags(.fdb), .serialized)
struct MinIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to sorted set")
    func testInsertAddsToSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: product,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create multiple entries")
    func testMultipleInserts() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999),
            MinTestProduct(id: "p2", category: "Electronics", brand: "Samsung", price: 799),
            MinTestProduct(id: "p3", category: "Electronics", brand: "Sony", price: 599)
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 index entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from sorted set")
    func testDeleteRemovesFromSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: product,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: product,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update changes position in sorted set")
    func testUpdateChangesPosition() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: product,
                transaction: transaction
            )
        }

        // Update price
        let updatedProduct = MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 499)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: product,
                newItem: updatedProduct,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 entry after update")

        let min = try await ctx.getMin(for: "Electronics")
        #expect(min == 499, "Min should be updated to 499")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("getMin returns minimum value")
    func testGetMinReturnsMinimum() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999),
            MinTestProduct(id: "p2", category: "Electronics", brand: "Samsung", price: 799),
            MinTestProduct(id: "p3", category: "Electronics", brand: "Budget", price: 199)
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let min = try await ctx.getMin(for: "Electronics")
        #expect(min == 199, "Min should be 199 (lowest price)")

        try await ctx.cleanup()
    }

    @Test("Multiple groups are independent")
    func testMultipleGroupsIndependent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999),
            MinTestProduct(id: "p2", category: "Electronics", brand: "Budget", price: 199),
            MinTestProduct(id: "p3", category: "Clothing", brand: "Nike", price: 150),
            MinTestProduct(id: "p4", category: "Clothing", brand: "Budget", price: 29)
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let electronicsMin = try await ctx.getMin(for: "Electronics")
        let clothingMin = try await ctx.getMin(for: "Clothing")

        #expect(electronicsMin == 199, "Electronics min should be 199")
        #expect(clothingMin == 29, "Clothing min should be 29")

        try await ctx.cleanup()
    }

    @Test("getMin for non-existent group throws error")
    func testGetMinNonExistentGroupThrowsError() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        await #expect(throws: IndexError.self) {
            _ = try await ctx.getMin(for: "NonExistent")
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to sorted set")
    func testScanItemAddsToSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            MinTestProduct(id: "p1", category: "Electronics", brand: "Apple", price: 999),
            MinTestProduct(id: "p2", category: "Electronics", brand: "Budget", price: 199)
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.scanItem(
                    product,
                    id: Tuple(product.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 entries after scanItem")

        let min = try await ctx.getMin(for: "Electronics")
        #expect(min == 199, "Min should be 199")

        try await ctx.cleanup()
    }

    // MARK: - Edge Cases

    @Test("Min updates correctly when minimum item is deleted")
    func testMinUpdatesOnMinimumDelete() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            MinTestProduct(id: "p1", category: "Electronics", brand: "Expensive", price: 999),
            MinTestProduct(id: "p2", category: "Electronics", brand: "Cheap", price: 99)
        ]

        // Insert both
        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let minBefore = try await ctx.getMin(for: "Electronics")
        #expect(minBefore == 99, "Min should be 99")

        // Delete the minimum item
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: products[1],
                newItem: nil,
                transaction: transaction
            )
        }

        let minAfter = try await ctx.getMin(for: "Electronics")
        #expect(minAfter == 999, "Min should now be 999 after deleting 99")

        try await ctx.cleanup()
    }
}
