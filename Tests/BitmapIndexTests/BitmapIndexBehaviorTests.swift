// BitmapIndexBehaviorTests.swift
// Comprehensive tests for BitmapIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import BitmapIndex

// MARK: - Test Model

struct TestProduct: Persistable {
    typealias ID = String

    var id: String
    var category: String
    var brand: String
    var inStock: Bool

    init(id: String = UUID().uuidString, category: String, brand: String, inStock: Bool = true) {
        self.id = id
        self.category = category
        self.brand = brand
        self.inStock = inStock
    }

    static var persistableType: String { "TestProduct" }
    static var allFields: [String] { ["id", "category", "brand", "inStock"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "category": return category
        case "brand": return brand
        case "inStock": return inStock
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestProduct, Value>) -> String {
        switch keyPath {
        case \TestProduct.id: return "id"
        case \TestProduct.category: return "category"
        case \TestProduct.brand: return "brand"
        case \TestProduct.inStock: return "inStock"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestProduct>) -> String {
        switch keyPath {
        case \TestProduct.id: return "id"
        case \TestProduct.category: return "category"
        case \TestProduct.brand: return "brand"
        case \TestProduct.inStock: return "inStock"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestProduct> {
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
    let maintainer: BitmapIndexMaintainer<TestProduct>

    init(indexName: String = "TestProduct_category") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "bitmap", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = Index(
            name: indexName,
            kind: BitmapIndexKind<TestProduct>(field: \.category),
            rootExpression: FieldKeyExpression(fieldName: "category"),
            subspaceKey: indexName,
            itemTypes: Set(["TestProduct"])
        )

        self.maintainer = BitmapIndexMaintainer<TestProduct>(
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

    func getBitmap(for value: String) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.getBitmap(for: [value], transaction: transaction)
        }
    }

    func getCount(for value: String) async throws -> Int {
        try await database.withTransaction { transaction in
            try await maintainer.getCount(for: [value], transaction: transaction)
        }
    }

    func andQuery(values: [[any TupleElement]]) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.andQuery(values: values, transaction: transaction)
        }
    }

    func orQuery(values: [[any TupleElement]]) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.orQuery(values: values, transaction: transaction)
        }
    }

    func getPrimaryKeys(from bitmap: RoaringBitmap) async throws -> [Tuple] {
        try await database.withTransaction { transaction in
            try await maintainer.getPrimaryKeys(from: bitmap, transaction: transaction)
        }
    }

    func getAllDistinctValues() async throws -> [String] {
        try await database.withTransaction { transaction in
            let values = try await maintainer.getAllDistinctValues(transaction: transaction)
            return values.compactMap { $0.first as? String }
        }
    }
}

// MARK: - RoaringBitmap Unit Tests

@Suite("RoaringBitmap Unit Tests")
struct RoaringBitmapUnitTests {

    @Test("Add and contains single value")
    func testAddAndContains() {
        var bitmap = RoaringBitmap()
        bitmap.add(42)

        #expect(bitmap.contains(42), "Should contain 42")
        #expect(!bitmap.contains(43), "Should not contain 43")
        #expect(bitmap.cardinality == 1)
    }

    @Test("Add multiple values")
    func testAddMultipleValues() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)
        bitmap.add(100)
        bitmap.add(1000)
        bitmap.add(10000)

        #expect(bitmap.cardinality == 4)
        #expect(bitmap.contains(1))
        #expect(bitmap.contains(100))
        #expect(bitmap.contains(1000))
        #expect(bitmap.contains(10000))
    }

    @Test("Remove value")
    func testRemove() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)
        bitmap.add(2)
        bitmap.add(3)

        bitmap.remove(2)

        #expect(bitmap.cardinality == 2)
        #expect(bitmap.contains(1))
        #expect(!bitmap.contains(2))
        #expect(bitmap.contains(3))
    }

    @Test("Remove non-existent value is no-op")
    func testRemoveNonExistent() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)

        bitmap.remove(999)

        #expect(bitmap.cardinality == 1)
        #expect(bitmap.contains(1))
    }

    @Test("AND operation")
    func testAndOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)
        a.add(3)

        var b = RoaringBitmap()
        b.add(2)
        b.add(3)
        b.add(4)

        let result = a && b

        #expect(result.cardinality == 2)
        #expect(!result.contains(1))
        #expect(result.contains(2))
        #expect(result.contains(3))
        #expect(!result.contains(4))
    }

    @Test("OR operation")
    func testOrOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)

        var b = RoaringBitmap()
        b.add(3)
        b.add(4)

        let result = a || b

        #expect(result.cardinality == 4)
        #expect(result.contains(1))
        #expect(result.contains(2))
        #expect(result.contains(3))
        #expect(result.contains(4))
    }

    @Test("Difference operation (ANDNOT)")
    func testDifferenceOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)
        a.add(3)

        var b = RoaringBitmap()
        b.add(2)
        b.add(3)
        b.add(4)

        let result = a - b

        #expect(result.cardinality == 1)
        #expect(result.contains(1))
        #expect(!result.contains(2))
        #expect(!result.contains(3))
    }

    @Test("Serialization round-trip")
    func testSerializationRoundTrip() throws {
        var original = RoaringBitmap()
        for i in stride(from: 0, to: 1000, by: 7) {
            original.add(UInt32(i))
        }

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(RoaringBitmap.self, from: data)

        #expect(decoded == original)
        #expect(decoded.cardinality == original.cardinality)
    }

    @Test("Empty bitmap operations")
    func testEmptyBitmapOperations() {
        let empty = RoaringBitmap()

        #expect(empty.cardinality == 0)
        #expect(!empty.contains(0))
        #expect(!empty.contains(UInt32.max))

        var nonEmpty = RoaringBitmap()
        nonEmpty.add(1)

        // AND with empty
        let andResult = nonEmpty && empty
        #expect(andResult.cardinality == 0)

        // OR with empty
        let orResult = nonEmpty || empty
        #expect(orResult.cardinality == 1)
    }

    @Test("Large cardinality bitmap")
    func testLargeCardinality() {
        var bitmap = RoaringBitmap()

        // Add 10,000 values
        for i: UInt32 in 0..<10000 {
            bitmap.add(i)
        }

        #expect(bitmap.cardinality == 10000)
        #expect(bitmap.contains(0))
        #expect(bitmap.contains(5000))
        #expect(bitmap.contains(9999))
        #expect(!bitmap.contains(10000))
    }

    @Test("Values across multiple containers")
    func testMultipleContainers() {
        var bitmap = RoaringBitmap()

        // Container boundaries are at 65536 (2^16)
        bitmap.add(0)           // Container 0
        bitmap.add(65535)       // Container 0 (last)
        bitmap.add(65536)       // Container 1 (first)
        bitmap.add(131072)      // Container 2

        #expect(bitmap.cardinality == 4)
        #expect(bitmap.contains(0))
        #expect(bitmap.contains(65535))
        #expect(bitmap.contains(65536))
        #expect(bitmap.contains(131072))
    }

    @Test("Duplicate add is idempotent")
    func testDuplicateAdd() {
        var bitmap = RoaringBitmap()
        bitmap.add(42)
        bitmap.add(42)
        bitmap.add(42)

        #expect(bitmap.cardinality == 1)
    }
}

// MARK: - BitmapIndexMaintainer Behavior Tests

@Suite("BitmapIndex Maintainer Behavior Tests", .tags(.fdb), .serialized)
struct BitmapIndexMaintainerBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to bitmap")
    func testInsertAddsToBitmap() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = TestProduct(id: "p1", category: "electronics", brand: "Sony")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestProduct?,
                newItem: product,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 1, "Should have 1 entry in electronics bitmap")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts with same category")
    func testMultipleInsertsWithSameCategory() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "electronics", brand: "Samsung"),
            TestProduct(id: "p3", category: "electronics", brand: "LG")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 3, "Should have 3 entries in electronics bitmap")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts with different categories")
    func testMultipleInsertsWithDifferentCategories() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "clothing", brand: "Nike"),
            TestProduct(id: "p3", category: "books", brand: "Penguin")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3, "Should have 3 distinct categories")

        let electronicsCount = try await ctx.getCount(for: "electronics")
        let clothingCount = try await ctx.getCount(for: "clothing")
        let booksCount = try await ctx.getCount(for: "books")

        #expect(electronicsCount == 1)
        #expect(clothingCount == 1)
        #expect(booksCount == 1)

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from bitmap")
    func testDeleteRemovesFromBitmap() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = TestProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestProduct?,
                newItem: product,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.getCount(for: "electronics")
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: product,
                newItem: nil as TestProduct?,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.getCount(for: "electronics")
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    @Test("Delete one of many maintains others")
    func testDeleteOneOfMany() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "electronics", brand: "Samsung"),
            TestProduct(id: "p3", category: "electronics", brand: "LG")
        ]

        // Insert all
        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Delete one
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: products[1],  // Samsung
                newItem: nil as TestProduct?,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 2, "Should have 2 entries after deleting one")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update category changes bitmap membership")
    func testUpdateCategory() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let oldProduct = TestProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestProduct?,
                newItem: oldProduct,
                transaction: transaction
            )
        }

        let electronicsCountBefore = try await ctx.getCount(for: "electronics")
        #expect(electronicsCountBefore == 1)

        // Update category
        let newProduct = TestProduct(id: "p1", category: "appliances", brand: "Sony")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        let electronicsCountAfter = try await ctx.getCount(for: "electronics")
        let appliancesCount = try await ctx.getCount(for: "appliances")

        #expect(electronicsCountAfter == 0, "Electronics should be empty")
        #expect(appliancesCount == 1, "Appliances should have 1 entry")

        try await ctx.cleanup()
    }

    @Test("Update non-indexed field keeps bitmap membership")
    func testUpdateNonIndexedField() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let oldProduct = TestProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as TestProduct?,
                newItem: oldProduct,
                transaction: transaction
            )
        }

        // Update brand (non-indexed field)
        let newProduct = TestProduct(id: "p1", category: "electronics", brand: "Panasonic")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 1, "Should still have 1 entry")

        try await ctx.cleanup()
    }

    // MARK: - AND Query Tests

    @Test("AND query returns intersection")
    func testAndQueryReturnsIntersection() async throws {
        try await FDBTestSetup.shared.initialize()
        // Create separate maintainers for category and brand
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "bitmap", String(testId)).pack())

        let categoryIndexSubspace = subspace.subspace("I").subspace("category_idx")
        let brandIndexSubspace = subspace.subspace("I").subspace("brand_idx")

        let categoryMaintainer = BitmapIndexMaintainer<TestProduct>(
            index: Index(
                name: "category_idx",
                kind: BitmapIndexKind<TestProduct>(field: \.category),
                rootExpression: FieldKeyExpression(fieldName: "category"),
                subspaceKey: "category_idx",
                itemTypes: Set(["TestProduct"])
            ),
            subspace: categoryIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let brandMaintainer = BitmapIndexMaintainer<TestProduct>(
            index: Index(
                name: "brand_idx",
                kind: BitmapIndexKind<TestProduct>(field: \.brand),
                rootExpression: FieldKeyExpression(fieldName: "brand"),
                subspaceKey: "brand_idx",
                itemTypes: Set(["TestProduct"])
            ),
            subspace: brandIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "electronics", brand: "Samsung"),
            TestProduct(id: "p3", category: "clothing", brand: "Sony"),
            TestProduct(id: "p4", category: "clothing", brand: "Nike")
        ]

        // Insert into both indexes
        try await database.withTransaction { transaction in
            for product in products {
                try await categoryMaintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
                try await brandMaintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Get bitmaps and perform AND
        let electronicsBitmap = try await database.withTransaction { transaction in
            try await categoryMaintainer.getBitmap(for: ["electronics"], transaction: transaction)
        }
        let sonyBitmap = try await database.withTransaction { transaction in
            try await brandMaintainer.getBitmap(for: ["Sony"], transaction: transaction)
        }

        let intersection = electronicsBitmap && sonyBitmap
        #expect(intersection.cardinality == 1, "Only 1 product is both electronics AND Sony")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - OR Query Tests

    @Test("OR query returns union")
    func testOrQueryReturnsUnion() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "clothing", brand: "Nike"),
            TestProduct(id: "p3", category: "books", brand: "Penguin")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let result = try await ctx.orQuery(values: [["electronics"], ["clothing"]])
        #expect(result.cardinality == 2, "Should have 2 products (electronics OR clothing)")

        try await ctx.cleanup()
    }

    // MARK: - GetAllDistinctValues Tests

    @Test("getAllDistinctValues returns all categories")
    func testGetAllDistinctValues() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "clothing", brand: "Nike"),
            TestProduct(id: "p3", category: "books", brand: "Penguin"),
            TestProduct(id: "p4", category: "electronics", brand: "Samsung")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3, "Should have 3 distinct categories")
        #expect(distinctValues.contains("electronics"))
        #expect(distinctValues.contains("clothing"))
        #expect(distinctValues.contains("books"))

        try await ctx.cleanup()
    }

    // MARK: - Primary Key Retrieval Tests

    @Test("getPrimaryKeys returns correct IDs")
    func testGetPrimaryKeysReturnsCorrectIds() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "product-001", category: "electronics", brand: "Sony"),
            TestProduct(id: "product-002", category: "electronics", brand: "Samsung"),
            TestProduct(id: "product-003", category: "clothing", brand: "Nike")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let bitmap = try await ctx.getBitmap(for: "electronics")
        let primaryKeys = try await ctx.getPrimaryKeys(from: bitmap)

        #expect(primaryKeys.count == 2, "Should have 2 primary keys for electronics")

        let idStrings = primaryKeys.compactMap { $0[0] as? String }
        #expect(idStrings.contains("product-001"))
        #expect(idStrings.contains("product-002"))
        #expect(!idStrings.contains("product-003"))

        try await ctx.cleanup()
    }

    // MARK: - ScanItem Tests

    @Test("scanItem adds to bitmap")
    func testScanItemAddsToBitmap() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics", brand: "Sony"),
            TestProduct(id: "p2", category: "electronics", brand: "Samsung")
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

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 2, "Should have 2 entries after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - computeIndexKeys Tests

    @Test("computeIndexKeys returns expected keys")
    func testComputeIndexKeys() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let product = TestProduct(id: "p1", category: "electronics", brand: "Sony")
        let keys = try await ctx.maintainer.computeIndexKeys(for: product, id: Tuple("p1"))

        // Should have keys for data subspace entry
        #expect(!keys.isEmpty, "Should have at least one index key")

        try await ctx.cleanup()
    }
}

// MARK: - Edge Cases Tests

@Suite("BitmapIndex Edge Cases", .tags(.fdb), .serialized)
struct BitmapIndexEdgeCasesTests {

    @Test("Empty bitmap query returns empty")
    func testEmptyBitmapQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let bitmap = try await ctx.getBitmap(for: "nonexistent")
        #expect(bitmap.cardinality == 0, "Non-existent category should return empty bitmap")

        try await ctx.cleanup()
    }

    @Test("Sequential ID management across transactions")
    func testSequentialIdManagement() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert products in separate transactions
        for i in 1...5 {
            let product = TestProduct(id: "p\(i)", category: "electronics", brand: "Brand\(i)")
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 5, "Should have 5 entries with sequential IDs")

        let bitmap = try await ctx.getBitmap(for: "electronics")
        let primaryKeys = try await ctx.getPrimaryKeys(from: bitmap)
        #expect(primaryKeys.count == 5, "Should retrieve all 5 primary keys")

        try await ctx.cleanup()
    }

    @Test("Large number of entries")
    func testLargeNumberOfEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 100 products
        try await ctx.database.withTransaction { transaction in
            for i in 1...100 {
                let product = TestProduct(id: "p\(i)", category: "electronics", brand: "Brand")
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 100, "Should have 100 entries")

        let bitmap = try await ctx.getBitmap(for: "electronics")
        #expect(bitmap.cardinality == 100)

        try await ctx.cleanup()
    }

    @Test("Special characters in field values")
    func testSpecialCharactersInValues() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let products = [
            TestProduct(id: "p1", category: "electronics & gadgets", brand: "Sony"),
            TestProduct(id: "p2", category: "home/kitchen", brand: "KitchenAid"),
            TestProduct(id: "p3", category: "toys (kids)", brand: "LEGO")
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as TestProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3)

        let count1 = try await ctx.getCount(for: "electronics & gadgets")
        let count2 = try await ctx.getCount(for: "home/kitchen")
        let count3 = try await ctx.getCount(for: "toys (kids)")

        #expect(count1 == 1)
        #expect(count2 == 1)
        #expect(count3 == 1)

        try await ctx.cleanup()
    }
}
