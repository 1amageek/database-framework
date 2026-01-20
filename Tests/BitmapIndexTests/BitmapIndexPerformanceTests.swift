// BitmapIndexPerformanceTests.swift
// Performance tests for BitmapIndex

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import BitmapIndex

// MARK: - Test Model

private struct PerfProduct: Persistable {
    typealias ID = String

    var id: String
    var category: String
    var brand: String
    var status: String

    init(id: String = UUID().uuidString, category: String, brand: String, status: String = "active") {
        self.id = id
        self.category = category
        self.brand = brand
        self.status = status
    }

    static var persistableType: String { "PerfProduct" }
    static var allFields: [String] { ["id", "category", "brand", "status"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "category": return category
        case "brand": return brand
        case "status": return status
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PerfProduct, Value>) -> String {
        switch keyPath {
        case \PerfProduct.id: return "id"
        case \PerfProduct.category: return "category"
        case \PerfProduct.brand: return "brand"
        case \PerfProduct.status: return "status"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PerfProduct>) -> String {
        switch keyPath {
        case \PerfProduct.id: return "id"
        case \PerfProduct.category: return "category"
        case \PerfProduct.brand: return "brand"
        case \PerfProduct.status: return "status"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PerfProduct> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Performance Test Helper

private struct PerfTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let maintainer: BitmapIndexMaintainer<PerfProduct>
    let indexName: String

    init(testName: String) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.indexName = "PerfProduct_bitmap_category"
        self.subspace = Subspace(prefix: Tuple("test", "bitmap_perf", String(testId), testName).pack())

        let indexSubspace = subspace.subspace("I").subspace(indexName)
        let index = Index(
            name: indexName,
            kind: BitmapIndexKind<PerfProduct>(field: \.category),
            rootExpression: FieldKeyExpression(fieldName: "category"),
            subspaceKey: indexName,
            itemTypes: Set(["PerfProduct"])
        )

        self.maintainer = BitmapIndexMaintainer<PerfProduct>(
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
}

// MARK: - Benchmark Helper

private func benchmark(_ name: String, iterations: Int = 1, operation: () async throws -> Void) async throws -> (totalMs: Double, perIterationMs: Double) {
    let start = DispatchTime.now()
    for _ in 0..<iterations {
        try await operation()
    }
    let end = DispatchTime.now()
    let totalNs = Double(end.uptimeNanoseconds - start.uptimeNanoseconds)
    let totalMs = totalNs / 1_000_000
    let perIterationMs = totalMs / Double(iterations)
    return (totalMs, perIterationMs)
}

// MARK: - RoaringBitmap Performance Tests

@Suite("RoaringBitmap Performance Tests")
struct RoaringBitmapPerformanceTests {

    @Test("Add performance - 10,000 values")
    func testAddPerformance() async throws {
        var bitmap = RoaringBitmap()

        let (totalMs, _) = try await benchmark("Add 10,000 values") {
            for i: UInt32 in 0..<10000 {
                bitmap.add(i)
            }
        }

        #expect(bitmap.cardinality == 10000)
        print("Add 10,000 values: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 10000.0 / (totalMs / 1000))) ops/s")
    }

    @Test("Contains performance - 10,000 lookups")
    func testContainsPerformance() async throws {
        var bitmap = RoaringBitmap()
        for i: UInt32 in 0..<10000 {
            bitmap.add(i)
        }

        let (totalMs, _) = try await benchmark("Contains 10,000 lookups") {
            for i: UInt32 in 0..<10000 {
                _ = bitmap.contains(i)
            }
        }

        print("Contains 10,000 lookups: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 10000.0 / (totalMs / 1000))) ops/s")
    }

    @Test("AND operation performance")
    func testAndPerformance() async throws {
        var a = RoaringBitmap()
        var b = RoaringBitmap()

        // Create overlapping bitmaps
        for i: UInt32 in 0..<10000 {
            a.add(i)
        }
        for i: UInt32 in 5000..<15000 {
            b.add(i)
        }

        var result: RoaringBitmap!
        let (totalMs, _) = try await benchmark("AND 10,000 × 10,000", iterations: 100) {
            result = a && b
        }

        #expect(result.cardinality == 5000) // 5000-9999 overlap
        print("AND operation (100 iterations): \(String(format: "%.2f", totalMs))ms")
        print("Per operation: \(String(format: "%.3f", totalMs / 100))ms")
    }

    @Test("OR operation performance")
    func testOrPerformance() async throws {
        var a = RoaringBitmap()
        var b = RoaringBitmap()

        for i: UInt32 in 0..<10000 {
            a.add(i)
        }
        for i: UInt32 in 5000..<15000 {
            b.add(i)
        }

        var result: RoaringBitmap!
        let (totalMs, _) = try await benchmark("OR 10,000 × 10,000", iterations: 100) {
            result = a || b
        }

        #expect(result.cardinality == 15000) // 0-14999 union
        print("OR operation (100 iterations): \(String(format: "%.2f", totalMs))ms")
        print("Per operation: \(String(format: "%.3f", totalMs / 100))ms")
    }

    @Test("Cardinality counting performance")
    func testCardinalityPerformance() async throws {
        var bitmap = RoaringBitmap()
        for i: UInt32 in 0..<100000 {
            bitmap.add(i)
        }

        var count = 0
        let (totalMs, _) = try await benchmark("Cardinality 100,000", iterations: 1000) {
            count = bitmap.cardinality
        }

        #expect(count == 100000)
        print("Cardinality (1000 iterations): \(String(format: "%.2f", totalMs))ms")
        print("Per operation: \(String(format: "%.4f", totalMs / 1000))ms")
    }

    @Test("Serialization performance")
    func testSerializationPerformance() async throws {
        var bitmap = RoaringBitmap()
        for i: UInt32 in 0..<10000 {
            bitmap.add(i)
        }

        var data: Data!
        let (serializeMs, _) = try await benchmark("Serialize 10,000", iterations: 100) {
            data = try bitmap.serialize()
        }

        print("Serialize (100 iterations): \(String(format: "%.2f", serializeMs))ms")
        print("Per serialize: \(String(format: "%.3f", serializeMs / 100))ms")
        print("Serialized size: \(data.count) bytes")

        var restored: RoaringBitmap!
        let (deserializeMs, _) = try await benchmark("Deserialize 10,000", iterations: 100) {
            restored = try RoaringBitmap.deserialize(data)
        }

        #expect(restored.cardinality == 10000)
        print("Deserialize (100 iterations): \(String(format: "%.2f", deserializeMs))ms")
        print("Per deserialize: \(String(format: "%.3f", deserializeMs / 100))ms")
    }

    @Test("Large bitmap performance - 1M values")
    func testLargeBitmapPerformance() async throws {
        var bitmap = RoaringBitmap()

        let (addMs, _) = try await benchmark("Add 1M values") {
            for i: UInt32 in 0..<1_000_000 {
                bitmap.add(i)
            }
        }

        #expect(bitmap.cardinality == 1_000_000)
        print("Add 1M values: \(String(format: "%.2f", addMs))ms")
        print("Throughput: \(String(format: "%.0f", 1_000_000.0 / (addMs / 1000))) ops/s")

        // Test cardinality on large bitmap
        var count = 0
        let (cardinalityMs, _) = try await benchmark("Cardinality 1M", iterations: 100) {
            count = bitmap.cardinality
        }

        #expect(count == 1_000_000)
        print("Cardinality 1M (100 iterations): \(String(format: "%.2f", cardinalityMs))ms")
        print("Per cardinality: \(String(format: "%.3f", cardinalityMs / 100))ms")
    }

    @Test("Sparse bitmap performance")
    func testSparseBitmapPerformance() async throws {
        var bitmap = RoaringBitmap()

        // Add sparse values (every 1000th value)
        for i in stride(from: UInt32(0), to: 10_000_000, by: 1000) {
            bitmap.add(i)
        }

        #expect(bitmap.cardinality == 10000)

        // Test operations on sparse bitmap
        var result: RoaringBitmap!
        var other = RoaringBitmap()
        for i in stride(from: UInt32(500), to: 10_000_000, by: 1000) {
            other.add(i)
        }

        let (andMs, _) = try await benchmark("AND sparse", iterations: 100) {
            result = bitmap && other
        }

        #expect(result.cardinality == 0) // No overlap
        print("AND sparse bitmaps (100 iterations): \(String(format: "%.2f", andMs))ms")
        print("Per AND: \(String(format: "%.3f", andMs / 100))ms")
    }
}

// MARK: - BitmapIndex FDB Performance Tests

@Suite("BitmapIndex FDB Performance Tests", .tags(.fdb), .serialized)
struct BitmapIndexFDBPerformanceTests {

    @Test("Bulk insert performance - 100 records, 10 categories")
    func testBulkInsert100Records() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "bulk_insert_100")

        let categories = (0..<10).map { "category-\($0)" }
        let products = (0..<100).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: categories[i % 10],
                brand: "brand-\(i % 5)",
                status: "active"
            )
        }

        let (totalMs, _) = try await benchmark("Insert 100 records") {
            try await ctx.database.withTransaction { transaction in
                for product in products {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfProduct?,
                        newItem: product,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 100 records: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 100.0 / (totalMs / 1000))) records/s")

        // Verify
        let count = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
        }
        #expect(count == 10, "Each category should have 10 products")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - 1000 records, 10 categories")
    func testBulkInsert1000Records() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "bulk_insert_1000")

        let categories = (0..<10).map { "category-\($0)" }
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: categories[i % 10],
                brand: "brand-\(i % 50)",
                status: i % 3 == 0 ? "inactive" : "active"
            )
        }

        let (totalMs, _) = try await benchmark("Insert 1000 records") {
            try await ctx.database.withTransaction { transaction in
                for product in products {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfProduct?,
                        newItem: product,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 1000 records: \(String(format: "%.2f", totalMs))ms")
        print("Throughput: \(String(format: "%.0f", 1000.0 / (totalMs / 1000))) records/s")

        // Verify
        let count = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
        }
        #expect(count == 100, "Each category should have 100 products")

        try await ctx.cleanup()
    }

    @Test("Query performance - single value lookup")
    func testQueryPerformanceSingleValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "query_single")

        // Setup: Insert 1000 records
        let categories = (0..<10).map { "category-\($0)" }
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: categories[i % 10],
                brand: "brand-\(i % 50)",
                status: "active"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Get bitmap for single value
        var bitmap: RoaringBitmap!
        let (getMs, _) = try await benchmark("Get bitmap", iterations: 100) {
            bitmap = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getBitmap(for: ["category-0"], transaction: transaction)
            }
        }

        #expect(bitmap.cardinality == 100)
        print("Get bitmap (100 iterations): \(String(format: "%.2f", getMs))ms")
        print("Per query: \(String(format: "%.3f", getMs / 100))ms")

        // Benchmark: Get count (no primary key lookup)
        var count = 0
        let (countMs, _) = try await benchmark("Get count", iterations: 100) {
            count = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
            }
        }

        #expect(count == 100)
        print("Get count (100 iterations): \(String(format: "%.2f", countMs))ms")
        print("Per count: \(String(format: "%.3f", countMs / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Query performance - OR query")
    func testQueryPerformanceOrQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "query_or")

        // Setup: Insert 1000 records
        let categories = (0..<10).map { "category-\($0)" }
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: categories[i % 10],
                brand: "brand-\(i % 50)",
                status: "active"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Benchmark: OR query across 3 categories
        var bitmap: RoaringBitmap!
        let (orMs, _) = try await benchmark("OR query (3 values)", iterations: 100) {
            bitmap = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.orQuery(
                    values: [["category-0"], ["category-1"], ["category-2"]],
                    transaction: transaction
                )
            }
        }

        #expect(bitmap.cardinality == 300) // 100 each
        print("OR query (100 iterations): \(String(format: "%.2f", orMs))ms")
        print("Per OR query: \(String(format: "%.3f", orMs / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Query performance - AND query across indexes")
    func testQueryPerformanceAndQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "bitmap_perf", String(testId), "query_and").pack())

        // Create two indexes: category and brand
        let categoryMaintainer = BitmapIndexMaintainer<PerfProduct>(
            index: Index(
                name: "category_idx",
                kind: BitmapIndexKind<PerfProduct>(field: \.category),
                rootExpression: FieldKeyExpression(fieldName: "category"),
                subspaceKey: "category_idx",
                itemTypes: Set(["PerfProduct"])
            ),
            subspace: subspace.subspace("I").subspace("category_idx"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let brandMaintainer = BitmapIndexMaintainer<PerfProduct>(
            index: Index(
                name: "brand_idx",
                kind: BitmapIndexKind<PerfProduct>(field: \.brand),
                rootExpression: FieldKeyExpression(fieldName: "brand"),
                subspaceKey: "brand_idx",
                itemTypes: Set(["PerfProduct"])
            ),
            subspace: subspace.subspace("I").subspace("brand_idx"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Setup: Insert 1000 records
        let categories = (0..<10).map { "category-\($0)" }
        let brands = (0..<20).map { "brand-\($0)" }
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: categories[i % 10],
                brand: brands[i % 20],
                status: "active"
            )
        }

        try await database.withTransaction { transaction in
            for product in products {
                try await categoryMaintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
                try await brandMaintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Benchmark: AND query (category-0 AND brand-0)
        var result: RoaringBitmap!
        let (andMs, _) = try await benchmark("AND query (2 indexes)", iterations: 100) {
            let categoryBitmap = try await database.withTransaction { transaction in
                try await categoryMaintainer.getBitmap(for: ["category-0"], transaction: transaction)
            }
            let brandBitmap = try await database.withTransaction { transaction in
                try await brandMaintainer.getBitmap(for: ["brand-0"], transaction: transaction)
            }
            result = categoryBitmap && brandBitmap
        }

        // category-0: indices 0, 10, 20, ... (100 products)
        // brand-0: indices 0, 20, 40, ... (50 products)
        // Intersection: indices where i % 10 == 0 AND i % 20 == 0 => i % 20 == 0
        // That's 0, 20, 40, 60, 80, ... up to 980 => 50 products
        #expect(result.cardinality == 50)
        print("AND query across 2 indexes (100 iterations): \(String(format: "%.2f", andMs))ms")
        print("Per AND query: \(String(format: "%.3f", andMs / 100))ms")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Primary key retrieval performance")
    func testPrimaryKeyRetrievalPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "pk_retrieval")

        // Setup: Insert 1000 records
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: "category-\(i % 10)",
                brand: "brand-\(i % 50)",
                status: "active"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Get bitmap
        let bitmap = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getBitmap(for: ["category-0"], transaction: transaction)
        }

        // Benchmark: Convert bitmap to primary keys
        var primaryKeys: [Tuple]!
        let (retrieveMs, _) = try await benchmark("Get primary keys (100 items)", iterations: 100) {
            primaryKeys = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getPrimaryKeys(from: bitmap, transaction: transaction)
            }
        }

        #expect(primaryKeys.count == 100)
        print("Get primary keys from bitmap (100 iterations): \(String(format: "%.2f", retrieveMs))ms")
        print("Per retrieval: \(String(format: "%.3f", retrieveMs / 100))ms")

        try await ctx.cleanup()
    }

    @Test("Update performance - category change")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "update")

        // Setup: Insert 100 records
        var products = (0..<100).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: "category-0",
                brand: "brand-\(i % 10)",
                status: "active"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Verify initial state
        let initialCount = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
        }
        #expect(initialCount == 100)

        // Benchmark: Update category for all products
        let (updateMs, _) = try await benchmark("Update 100 records") {
            try await ctx.database.withTransaction { transaction in
                for i in 0..<100 {
                    let oldProduct = products[i]
                    var newProduct = oldProduct
                    newProduct.category = "category-1"

                    try await ctx.maintainer.updateIndex(
                        oldItem: oldProduct,
                        newItem: newProduct,
                        transaction: transaction
                    )

                    products[i] = newProduct
                }
            }
        }

        print("Update 100 records: \(String(format: "%.2f", updateMs))ms")
        print("Per update: \(String(format: "%.3f", updateMs / 100))ms")

        // Verify final state
        let finalCount0 = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
        }
        let finalCount1 = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-1"], transaction: transaction)
        }
        #expect(finalCount0 == 0)
        #expect(finalCount1 == 100)

        try await ctx.cleanup()
    }

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "delete")

        // Setup: Insert 100 records
        let products = (0..<100).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: "category-\(i % 10)",
                brand: "brand-\(i % 10)",
                status: "active"
            )
        }

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PerfProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Delete all products
        let (deleteMs, _) = try await benchmark("Delete 100 records") {
            try await ctx.database.withTransaction { transaction in
                for product in products {
                    try await ctx.maintainer.updateIndex(
                        oldItem: product,
                        newItem: nil as PerfProduct?,
                        transaction: transaction
                    )
                }
            }
        }

        print("Delete 100 records: \(String(format: "%.2f", deleteMs))ms")
        print("Per delete: \(String(format: "%.3f", deleteMs / 100))ms")

        // Verify
        let finalCount = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCount(for: ["category-0"], transaction: transaction)
        }
        #expect(finalCount == 0)

        try await ctx.cleanup()
    }

    @Test("ScanItem performance")
    func testScanItemPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "scan_item")

        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: "category-\(i % 10)",
                brand: "brand-\(i % 50)",
                status: "active"
            )
        }

        let (scanMs, _) = try await benchmark("ScanItem 1000 records") {
            try await ctx.database.withTransaction { transaction in
                for product in products {
                    try await ctx.maintainer.scanItem(
                        product,
                        id: Tuple(product.id),
                        transaction: transaction
                    )
                }
            }
        }

        print("ScanItem 1000 records: \(String(format: "%.2f", scanMs))ms")
        print("Per scanItem: \(String(format: "%.3f", scanMs / 1000))ms")
        print("Throughput: \(String(format: "%.0f", 1000.0 / (scanMs / 1000))) records/s")

        try await ctx.cleanup()
    }

    @Test("High cardinality performance - 100 distinct values")
    func testHighCardinalityPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try PerfTestContext(testName: "high_cardinality")

        // 100 distinct categories, 10 products each
        let products = (0..<1000).map { i in
            PerfProduct(
                id: "product-\(i)",
                category: "category-\(i % 100)",
                brand: "brand-\(i % 50)",
                status: "active"
            )
        }

        let (insertMs, _) = try await benchmark("Insert 1000 records (100 categories)") {
            try await ctx.database.withTransaction { transaction in
                for product in products {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil as PerfProduct?,
                        newItem: product,
                        transaction: transaction
                    )
                }
            }
        }

        print("Insert 1000 records (100 categories): \(String(format: "%.2f", insertMs))ms")

        // Query distinct values
        var distinctValues: [[any TupleElement]]!
        let (distinctMs, _) = try await benchmark("Get all distinct values", iterations: 10) {
            distinctValues = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getAllDistinctValues(transaction: transaction)
            }
        }

        #expect(distinctValues.count == 100)
        print("Get all distinct values (10 iterations): \(String(format: "%.2f", distinctMs))ms")
        print("Per query: \(String(format: "%.3f", distinctMs / 10))ms")

        try await ctx.cleanup()
    }
}
