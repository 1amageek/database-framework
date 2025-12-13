// PartitionedDirectoryTests.swift
// Tests for Dynamic Directory (Partitioned Directory) support

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core
import FoundationDB
import TestSupport

@Suite("Partitioned Directory Tests", .serialized)
struct PartitionedDirectoryTests {

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([Player.self, TenantOrder.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    // MARK: - hasDynamicDirectory Tests

    @Test("TenantOrder has dynamic directory")
    func testTenantOrderHasDynamicDirectory() {
        #expect(TenantOrder.hasDynamicDirectory == true)
        #expect(TenantOrder.directoryFieldNames == ["tenantID"])
    }

    @Test("Player does not have dynamic directory")
    func testPlayerHasStaticDirectory() {
        #expect(Player.hasDynamicDirectory == false)
        #expect(Player.directoryFieldNames.isEmpty)
    }

    // MARK: - Save Tests

    @Test("Save TenantOrder extracts tenantID from model")
    func testSaveTenantOrderExtractsTenantID() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
        order.id = orderID

        context.insert(order)
        try await context.save()

        // Fetch using partition
        let fetched = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenantID)
            .where(\.id == orderID)
            .first()

        #expect(fetched != nil)
        #expect(fetched?.tenantID == tenantID)
        #expect(fetched?.status == "pending")
    }

    @Test("Save multiple orders to different tenants")
    func testSaveOrdersToDifferentTenants() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenant1 = uniqueID("tenant1")
        let tenant2 = uniqueID("tenant2")
        let order1ID = uniqueID("order1")
        let order2ID = uniqueID("order2")

        var order1 = TenantOrder(tenantID: tenant1, status: "completed", total: 50.0)
        order1.id = order1ID

        var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 75.0)
        order2.id = order2ID

        context.insert(order1)
        context.insert(order2)
        try await context.save()

        // Fetch tenant1 orders
        let tenant1Orders = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenant1)
            .execute()

        #expect(tenant1Orders.count >= 1)
        #expect(tenant1Orders.contains { $0.id == order1ID })
        #expect(!tenant1Orders.contains { $0.id == order2ID })

        // Fetch tenant2 orders
        let tenant2Orders = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenant2)
            .execute()

        #expect(tenant2Orders.count >= 1)
        #expect(tenant2Orders.contains { $0.id == order2ID })
        #expect(!tenant2Orders.contains { $0.id == order1ID })
    }

    // MARK: - Fetch Tests

    @Test("Fetch without partition throws for dynamic directory type")
    func testFetchWithoutPartitionThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        await #expect(throws: DirectoryPathError.self) {
            _ = try await context.fetch(TenantOrder.self).execute()
        }
    }

    @Test("Fetch with partition returns correct data")
    func testFetchWithPartitionReturnsCorrectData() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        var order = TenantOrder(tenantID: tenantID, status: "shipped", total: 200.0)
        order.id = orderID
        context.insert(order)
        try await context.save()

        let results = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenantID)
            .where(\.status == "shipped")
            .execute()

        #expect(results.contains { $0.id == orderID })
    }

    @Test("Fetch with where clause filters within partition")
    func testFetchWithWhereFiltersWithinPartition() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let order1ID = uniqueID("order1")
        let order2ID = uniqueID("order2")

        var order1 = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
        order1.id = order1ID

        var order2 = TenantOrder(tenantID: tenantID, status: "completed", total: 150.0)
        order2.id = order2ID

        context.insert(order1)
        context.insert(order2)
        try await context.save()

        // Filter by status within partition
        let pendingOrders = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenantID)
            .where(\.status == "pending")
            .execute()

        #expect(pendingOrders.contains { $0.id == order1ID })
        #expect(!pendingOrders.contains { $0.id == order2ID })
    }

    // MARK: - Delete Tests

    @Test("Delete TenantOrder from correct partition")
    func testDeleteFromCorrectPartition() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        var order = TenantOrder(tenantID: tenantID, status: "pending", total: 50.0)
        order.id = orderID
        context.insert(order)
        try await context.save()

        // Verify exists
        let beforeDelete = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenantID)
            .where(\.id == orderID)
            .first()
        #expect(beforeDelete != nil)

        // Delete
        if let toDelete = beforeDelete {
            context.delete(toDelete)
            try await context.save()
        }

        // Verify deleted
        let afterDelete = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenantID)
            .where(\.id == orderID)
            .first()
        #expect(afterDelete == nil)
    }

    // MARK: - deleteAll Tests

    @Test("deleteAll without partition throws for dynamic directory type")
    func testDeleteAllWithoutPartitionThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        await #expect(throws: DirectoryPathError.self) {
            try await context.deleteAll(TenantOrder.self)
        }
    }

    @Test("deleteAll with partition deletes only from that partition")
    func testDeleteAllWithPartition() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenant1 = uniqueID("tenant1")
        let tenant2 = uniqueID("tenant2")
        let order1ID = uniqueID("order1")
        let order2ID = uniqueID("order2")

        var order1 = TenantOrder(tenantID: tenant1, status: "pending", total: 100.0)
        order1.id = order1ID

        var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 200.0)
        order2.id = order2ID

        context.insert(order1)
        context.insert(order2)
        try await context.save()

        // Delete all from tenant1
        try await context.deleteAll(TenantOrder.self, partition: \.tenantID, equals: tenant1)
        try await context.save()

        // tenant1 should be empty
        let tenant1Orders = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenant1)
            .execute()
        #expect(!tenant1Orders.contains { $0.id == order1ID })

        // tenant2 should still have data
        let tenant2Orders = try await context.fetch(TenantOrder.self)
            .partition(\.tenantID, equals: tenant2)
            .execute()
        #expect(tenant2Orders.contains { $0.id == order2ID })
    }

    // MARK: - enumerate Tests

    @Test("enumerate without partition throws for dynamic directory type")
    func testEnumerateWithoutPartitionThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        await #expect(throws: DirectoryPathError.self) {
            try await context.enumerate(TenantOrder.self) { _ in }
        }
    }

    @Test("enumerate with partition enumerates only that partition")
    func testEnumerateWithPartition() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
        order.id = orderID
        context.insert(order)
        try await context.save()

        var found = false
        try await context.enumerate(TenantOrder.self, partition: \.tenantID, equals: tenantID) { enumOrder in
            if enumOrder.id == orderID {
                found = true
            }
        }
        #expect(found)
    }

    // MARK: - DirectoryPath Tests

    @Test("DirectoryPath validates missing fields")
    func testDirectoryPathValidatesMissingFields() async throws {
        let binding = DirectoryPath<TenantOrder>()

        // Should throw because tenantID is required but not bound
        #expect(throws: DirectoryPathError.self) {
            try binding.validate()
        }
    }

    @Test("DirectoryPath validates complete binding")
    func testDirectoryPathValidatesCompleteBinding() async throws {
        var binding = DirectoryPath<TenantOrder>()
        binding.set(\.tenantID, to: "tenant_123")

        // Should not throw
        try binding.validate()
    }

    @Test("DirectoryPath.from extracts values from model")
    func testDirectoryPathFromModel() {
        let order = TenantOrder(tenantID: "tenant_xyz", status: "pending", total: 50.0)
        let binding = DirectoryPath<TenantOrder>.from(order)

        #expect(binding.value(for: \.tenantID) == "tenant_xyz")
    }

    // MARK: - Static Directory Tests (Regression)

    @Test("Static directory types work without partition")
    func testStaticDirectoryTypesWorkWithoutPartition() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let playerID = uniqueID("player")
        var player = Player(name: "Test Player", score: 100, level: 5)
        player.id = playerID

        context.insert(player)
        try await context.save()

        // Should work without partition
        let fetched = try await context.fetch(Player.self)
            .where(\.id == playerID)
            .first()

        #expect(fetched != nil)
        #expect(fetched?.name == "Test Player")
    }

    @Test("deleteAll works for static directory types")
    func testDeleteAllWorksForStaticDirectoryTypes() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let playerID = uniqueID("player")
        var player = Player(name: "Delete Test", score: 50, level: 1)
        player.id = playerID

        context.insert(player)
        try await context.save()

        // Should work without partition
        try await context.deleteAll(Player.self)
        try await context.save()

        // Note: This may affect other tests if run in parallel
        // The player we inserted should be deleted
    }

    // MARK: - model(for:as:partition:) Tests

    @Test("model(for:as:) throws for dynamic directory types without partition")
    func testModelWithoutPartitionThrows() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        await #expect(throws: DirectoryPathError.self) {
            _ = try await context.model(for: "any-id", as: TenantOrder.self)
        }
    }

    @Test("model(for:as:partition:) returns correct data")
    func testModelWithPartitionReturnsCorrectData() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        var order = TenantOrder(tenantID: tenantID, status: "processing", total: 150.0)
        order.id = orderID

        context.insert(order)
        try await context.save()

        // Fetch using model(for:as:partition:)
        var binding = DirectoryPath<TenantOrder>()
        binding.set(\.tenantID, to: tenantID)

        let fetched = try await context.model(for: orderID, as: TenantOrder.self, partition: binding)

        #expect(fetched != nil)
        #expect(fetched?.status == "processing")
    }

    // MARK: - TransactionContext Partition Tests

    @Test("TransactionContext set/get works for dynamic directory types")
    func testTransactionContextSetGetDynamicDirectory() async throws {
        let container = try await setupContainer()
        let tenantID = uniqueID("tenant")
        let orderID = uniqueID("order")

        // Use withTransaction to perform operations
        try await container.database.withTransaction { transaction in
            let txContext = TransactionContext(transaction: transaction, container: container)

            var order = TenantOrder(tenantID: tenantID, status: "tx-test", total: 500.0)
            order.id = orderID

            // Set should work (extracts partition from model)
            try await txContext.set(order)

            // Get with partition binding should work
            var binding = DirectoryPath<TenantOrder>()
            binding.set(\.tenantID, to: tenantID)
            let fetched = try await txContext.get(TenantOrder.self, id: orderID, partition: binding)

            #expect(fetched != nil)
            #expect(fetched?.status == "tx-test")
        }
    }

    @Test("TransactionContext get throws without partition for dynamic types")
    func testTransactionContextGetThrowsWithoutPartition() async throws {
        let container = try await setupContainer()

        await #expect(throws: DirectoryPathError.self) {
            try await container.database.withTransaction { transaction in
                let txContext = TransactionContext(transaction: transaction, container: container)
                _ = try await txContext.get(TenantOrder.self, id: "any-id")
            }
        }
    }
}
