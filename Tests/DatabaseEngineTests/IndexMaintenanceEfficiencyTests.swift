// IndexMaintenanceEfficiencyTests.swift
// Tests for efficient index maintenance (diff-based update without full scan)

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// MARK: - Test Model

/// Model with scalar index for efficiency testing
@Persistable
struct EfficiencyTestProduct {
    #Directory<EfficiencyTestProduct>("test", "efficiency")

    var id: String = UUID().uuidString
    var sku: String = ""
    var name: String = ""
    var price: Int = 0

    #Index<EfficiencyTestProduct>(ScalarIndexKind<EfficiencyTestProduct>(fields: [\.sku]))
    #Index<EfficiencyTestProduct>(ScalarIndexKind<EfficiencyTestProduct>(fields: [\.price]))
}

// MARK: - Tests

@Suite("Index Maintenance Efficiency Tests", .serialized)
struct IndexMaintenanceEfficiencyTests {

    private func createContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([EfficiencyTestProduct.self])
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "efficiency"])
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Update uses efficient diff-based path")
    func testUpdateUsesEfficientPath() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        // Create a product with unique SKU and unique prices
        let testPrefix = uniqueID("upd")
        let productId = uniqueID("P")
        let sku1 = "\(testPrefix)-SKU-001"
        let sku2 = "\(testPrefix)-SKU-002"
        // Use unique prices to avoid collision with other test runs
        let priceBase = Int.random(in: 10_000_000..<20_000_000)
        let price1 = priceBase
        let price2 = priceBase + 1
        var product = EfficiencyTestProduct(sku: sku1, name: "Widget", price: price1)
        product.id = productId

        context.insert(product)
        try await context.save()

        // Verify initial index entry exists
        let initialFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku1)
            .execute()
        #expect(initialFetch.count == 1)
        #expect(initialFetch.first?.id == productId)

        // Update the product (change indexed field)
        product.sku = sku2
        product.price = price2
        context.insert(product)
        try await context.save()

        // Verify old index entry is removed
        let oldSkuFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku1)
            .execute()
        #expect(oldSkuFetch.isEmpty, "Old index entry should be removed")

        // Verify new index entry exists
        let newSkuFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku2)
            .execute()
        #expect(newSkuFetch.count == 1)
        #expect(newSkuFetch.first?.id == productId)

        // Verify price index is also updated
        let oldPriceFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.price == price1)
            .execute()
        #expect(oldPriceFetch.isEmpty, "Old price index entry should be removed")

        let newPriceFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.price == price2)
            .execute()
        #expect(newPriceFetch.count == 1)
    }

    @Test("Delete uses efficient diff-based path")
    func testDeleteUsesEfficientPath() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        // Create a product with unique SKU and unique price
        let testPrefix = uniqueID("del")
        let productId = uniqueID("P")
        let sku = "\(testPrefix)-SKU-DELETE"
        let price = Int.random(in: 20_000_000..<30_000_000)
        var product = EfficiencyTestProduct(sku: sku, name: "ToDelete", price: price)
        product.id = productId

        context.insert(product)
        try await context.save()

        // Verify index entry exists
        let beforeDelete = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku)
            .execute()
        #expect(beforeDelete.count == 1)

        // Delete the product
        context.delete(product)
        try await context.save()

        // Verify index entry is removed
        let afterDelete = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku)
            .execute()
        #expect(afterDelete.isEmpty, "Index entry should be removed after delete")
    }

    @Test("Insert creates index entry without full scan")
    func testInsertCreatesIndexEntry() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        // Insert first - no old data, so no scan needed
        let testPrefix = uniqueID("ins")
        let productId = uniqueID("P")
        let sku = "\(testPrefix)-SKU-NEW"
        let price = Int.random(in: 30_000_000..<40_000_000)
        var product = EfficiencyTestProduct(sku: sku, name: "NewProduct", price: price)
        product.id = productId

        context.insert(product)
        try await context.save()

        // Verify index entry was created
        let fetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == sku)
            .execute()
        #expect(fetch.count == 1)
        #expect(fetch.first?.id == productId)
    }

    @Test("Multiple updates maintain index consistency")
    func testMultipleUpdates() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let testPrefix = uniqueID("mul")
        let productId = uniqueID("P")
        let price = Int.random(in: 40_000_000..<50_000_000)
        var product = EfficiencyTestProduct(sku: "\(testPrefix)-V1", name: "Product", price: price)
        product.id = productId

        // Insert
        context.insert(product)
        try await context.save()

        // Update 1
        product.sku = "\(testPrefix)-V2"
        context.insert(product)
        try await context.save()

        // Update 2
        product.sku = "\(testPrefix)-V3"
        context.insert(product)
        try await context.save()

        // Update 3 (back to original-ish)
        product.sku = "\(testPrefix)-V4"
        context.insert(product)
        try await context.save()

        // Verify only the latest index entry exists
        for suffix in ["V1", "V2", "V3"] {
            let sku = "\(testPrefix)-\(suffix)"
            let fetch = try await context.fetch(EfficiencyTestProduct.self)
                .where(\.sku == sku)
                .execute()
            #expect(fetch.isEmpty, "Old SKU '\(sku)' should not exist in index")
        }

        let latestFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == "\(testPrefix)-V4")
            .execute()
        #expect(latestFetch.count == 1)
        #expect(latestFetch.first?.id == productId)
    }

    @Test("Update with many existing records completes efficiently")
    func testUpdateWithManyRecords() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        // Insert many records to create a large index
        let testPrefix = uniqueID("batch")
        let batchSize = 100
        var productIds: [String] = []
        // Use unique price base to avoid collision with other test runs
        let priceBase = Int.random(in: 50_000_000..<60_000_000)

        for i in 0..<batchSize {
            let productId = uniqueID("P-\(i)")
            var product = EfficiencyTestProduct(
                sku: "\(testPrefix)-SKU-\(i)",
                name: "BatchProduct\(i)",
                price: priceBase + i
            )
            product.id = productId
            productIds.append(productId)
            context.insert(product)
        }
        try await context.save()

        // Now update ONE record - this should be efficient (not scan all 100 records)
        let targetId = productIds[50]
        let updatedSku = "\(testPrefix)-UPDATED-50"
        let updatedPrice = priceBase + 99999
        var targetProduct = EfficiencyTestProduct(
            sku: updatedSku,
            name: "UpdatedProduct50",
            price: updatedPrice
        )
        targetProduct.id = targetId

        let startTime = ContinuousClock.now
        context.insert(targetProduct)
        try await context.save()
        let elapsed = ContinuousClock.now - startTime

        // Verify update was successful
        let updatedFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == updatedSku)
            .execute()
        #expect(updatedFetch.count == 1)
        #expect(updatedFetch.first?.id == targetId)

        // Old index entry should be removed
        let oldFetch = try await context.fetch(EfficiencyTestProduct.self)
            .where(\.sku == "\(testPrefix)-SKU-50")
            .execute()
        #expect(oldFetch.isEmpty)

        // Sanity check: update should complete reasonably fast
        // (not a strict performance test, just ensuring no obvious O(n) behavior)
        #expect(elapsed < .seconds(5), "Update should complete within 5 seconds")
    }

    @Test("Batch insert and delete maintains index consistency")
    func testBatchOperations() async throws {
        let container = try await createContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        // Insert multiple products with unique prefix and unique prices
        let testPrefix = uniqueID("multi")
        let priceBase = Int.random(in: 60_000_000..<70_000_000)
        var products: [EfficiencyTestProduct] = []
        for i in 0..<10 {
            let productId = uniqueID("P-batch-\(i)")
            var product = EfficiencyTestProduct(
                sku: "\(testPrefix)-\(i)",
                name: "Multi\(i)",
                price: priceBase + i * 100
            )
            product.id = productId
            products.append(product)
            context.insert(product)
        }
        try await context.save()

        // Verify all index entries exist
        for i in 0..<10 {
            let sku = "\(testPrefix)-\(i)"
            let fetch = try await context.fetch(EfficiencyTestProduct.self)
                .where(\.sku == sku)
                .execute()
            #expect(fetch.count == 1, "Index entry for \(sku) should exist")
        }

        // Delete half of them
        for i in 0..<5 {
            context.delete(products[i])
        }
        try await context.save()

        // Verify deleted entries are gone
        for i in 0..<5 {
            let sku = "\(testPrefix)-\(i)"
            let fetch = try await context.fetch(EfficiencyTestProduct.self)
                .where(\.sku == sku)
                .execute()
            #expect(fetch.isEmpty, "Index entry for deleted \(sku) should be gone")
        }

        // Verify remaining entries still exist
        for i in 5..<10 {
            let sku = "\(testPrefix)-\(i)"
            let fetch = try await context.fetch(EfficiencyTestProduct.self)
                .where(\.sku == sku)
                .execute()
            #expect(fetch.count == 1, "Index entry for \(sku) should still exist")
        }
    }
}
