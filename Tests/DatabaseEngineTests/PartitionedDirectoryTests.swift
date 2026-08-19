#if !os(WASI)
#if FOUNDATION_DB
// PartitionedDirectoryTests.swift
// Tests for Dynamic Directory (Partitioned Directory) support

import Testing
import Foundation
@testable import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseKit
import StorageKit
import FDBStorage
import TestSupport

@Suite("Partitioned Directory Tests", .foundationDBScenario, .serialized, .heartbeat)
struct PartitionedDirectoryTests {

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try Player.schemaEntity, try TenantOrder.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: database), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(Player.self), try DatabaseFrameworkRuntime.entity(TenantOrder.self),
                ]), security: .testingDisabled)
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
            order.id = orderID

            try context.insert(order)
            try await context.save()

            // Fetch using partition
            let fetched = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.id == orderID)
                .first()

            #expect(fetched != nil)
            #expect(fetched?.tenantID == tenantID)
            #expect(fetched?.status == "pending")
        }
    }

    @Test("Save multiple orders to different tenants")
    func testSaveOrdersToDifferentTenants() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenant1 = uniqueID("tenant1")
            let tenant2 = uniqueID("tenant2")
            let order1ID = uniqueID("order1")
            let order2ID = uniqueID("order2")

            var order1 = TenantOrder(tenantID: tenant1, status: "completed", total: 50.0)
            order1.id = order1ID

            var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 75.0)
            order2.id = order2ID

            try context.insert(order1)
            try context.insert(order2)
            try await context.save()

            // Fetch tenant1 orders
            let tenant1Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant1)
                .execute()

            #expect(tenant1Orders.count >= 1)
            #expect(tenant1Orders.contains { $0.id == order1ID })
            #expect(!tenant1Orders.contains { $0.id == order2ID })

            // Fetch tenant2 orders
            let tenant2Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant2)
                .execute()

            #expect(tenant2Orders.count >= 1)
            #expect(tenant2Orders.contains { $0.id == order2ID })
            #expect(!tenant2Orders.contains { $0.id == order1ID })
        }
    }

    // MARK: - Fetch Tests

    @Test("Fetch without partition throws for dynamic directory type")
    func testFetchWithoutPartitionThrows() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            await #expect(throws: DirectoryPathError.self) {
                _ = try await context.fetch(TenantOrder.self).execute()
            }
        }
    }

    @Test("Fetch with partition returns correct data")
    func testFetchWithPartitionReturnsCorrectData() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "shipped", total: 200.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            let results = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.status == "shipped")
                .execute()

            #expect(results.contains { $0.id == orderID })
        }
    }

    @Test("Fetch with where clause filters within partition")
    func testFetchWithWhereFiltersWithinPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let order1ID = uniqueID("order1")
            let order2ID = uniqueID("order2")

            var order1 = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
            order1.id = order1ID

            var order2 = TenantOrder(tenantID: tenantID, status: "completed", total: 150.0)
            order2.id = order2ID

            try context.insert(order1)
            try context.insert(order2)
            try await context.save()

            // Filter by status within partition
            let pendingOrders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.status == "pending")
                .execute()

            #expect(pendingOrders.contains { $0.id == order1ID })
            #expect(!pendingOrders.contains { $0.id == order2ID })
        }
    }

    // MARK: - Delete Tests

    @Test("Delete TenantOrder from correct partition")
    func testDeleteFromCorrectPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 50.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            // Verify exists
            let beforeDelete = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.id == orderID)
                .first()
            #expect(beforeDelete != nil)

            // Delete
            if let toDelete = beforeDelete {
                try context.delete(toDelete)
                try await context.save()
            }

            // Verify deleted
            let afterDelete = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.id == orderID)
                .first()
            #expect(afterDelete == nil)
        }
    }

    // MARK: - deleteAll Tests

    @Test("deleteAll without partition throws for dynamic directory type")
    func testDeleteAllWithoutPartitionThrows() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.deleteAll(TenantOrder.self)
            }
        }
    }

    @Test("deleteAll with partition deletes only from that partition")
    func testDeleteAllWithPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenant1 = uniqueID("tenant1")
            let tenant2 = uniqueID("tenant2")
            let order1ID = uniqueID("order1")
            let order2ID = uniqueID("order2")

            var order1 = TenantOrder(tenantID: tenant1, status: "pending", total: 100.0)
            order1.id = order1ID

            var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 200.0)
            order2.id = order2ID

            try context.insert(order1)
            try context.insert(order2)
            try await context.save()

            // Delete all from tenant1
            try await context.deleteAll(TenantOrder.self, partition: TenantOrder.fields.tenantID, equals: tenant1)
            try await context.save()

            // tenant1 should be empty
            let tenant1Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant1)
                .execute()
            #expect(!tenant1Orders.contains { $0.id == order1ID })

            // tenant2 should still have data
            let tenant2Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant2)
                .execute()
            #expect(tenant2Orders.contains { $0.id == order2ID })
        }
    }

    // MARK: - enumerate Tests

    @Test("enumerate without partition throws for dynamic directory type")
    func testEnumerateWithoutPartitionThrows() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.enumerate(TenantOrder.self) { _ in }
            }
        }
    }

    @Test("enumerate with partition enumerates only that partition")
    func testEnumerateWithPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            var found = false
            try await context.enumerate(TenantOrder.self, partition: TenantOrder.fields.tenantID, equals: tenantID) { enumOrder in
                if enumOrder.id == orderID {
                    found = true
                }
            }
            #expect(found)
        }
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
        binding.set(TenantOrder.fields.tenantID, to: "tenant_123")

        // Should not throw
        try binding.validate()
    }

    @Test("DirectoryPath.from extracts values from model")
    func testDirectoryPathFromModel() throws {
        let order = TenantOrder(tenantID: "tenant_xyz", status: "pending", total: 50.0)
        let binding = try DirectoryPath<TenantOrder>.from(order)

        #expect(
            try binding.value(
                for: TenantOrder.fields.tenantID
            ) == "tenant_xyz"
        )
    }

    // MARK: - Static Directory Tests (Regression)

    @Test("Static directory types work without partition")
    func testStaticDirectoryTypesWorkWithoutPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let playerID = uniqueID("player")
            var player = Player(name: "Test Player", score: 100, level: 5)
            player.id = playerID

            try context.insert(player)
            try await context.save()

            // Should work without partition
            let fetched = try await context.fetch(Player.self)
                .where(Player.fields.id == playerID)
                .first()

            #expect(fetched != nil)
            #expect(fetched?.name == "Test Player")
        }
    }

    @Test("deleteAll works for static directory types")
    func testDeleteAllWorksForStaticDirectoryTypes() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let playerID = uniqueID("player")
            var player = Player(name: "Delete Test", score: 50, level: 1)
            player.id = playerID

            try context.insert(player)
            try await context.save()

            try await context.deleteAll(Player.self)
            try await context.save()
        }
    }

    // MARK: - model(for:as:partition:) Tests

    @Test("model(for:as:) throws for dynamic directory types without partition")
    func testModelWithoutPartitionThrows() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            await #expect(throws: DirectoryPathError.self) {
                _ = try await context.model(for: "any-id", as: TenantOrder.self)
            }
        }
    }

    @Test("model(for:as:partition:) returns correct data")
    func testModelWithPartitionReturnsCorrectData() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.testBaseContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "processing", total: 150.0)
            order.id = orderID

            try context.insert(order)
            try await context.save()

            // Fetch using model(for:as:partition:)
            var binding = DirectoryPath<TenantOrder>()
            binding.set(TenantOrder.fields.tenantID, to: tenantID)

            let fetched = try await context.model(for: orderID, as: TenantOrder.self, partition: binding)

            #expect(fetched != nil)
            #expect(fetched?.status == "processing")
        }
    }

    // MARK: - DatabaseTransaction Partition Tests

    @Test("DatabaseTransaction saves and fetches dynamic directory types")
    func transactionSavesAndFetchesDynamicDirectoryModel() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            try await container.testBaseContext().withTransaction { transaction in
                var order = TenantOrder(tenantID: tenantID, status: "tx-test", total: 500.0)
                order.id = orderID

                try await transaction.save(order, precondition: .notExists)

                var binding = DirectoryPath<TenantOrder>()
                binding.set(TenantOrder.fields.tenantID, to: tenantID)
                let fetched = try await transaction.fetch(
                    TenantOrder.self,
                    identifiedBy: orderID,
                    in: binding,
                    consistency: .serializable
                )

                #expect(fetched != nil)
                #expect(fetched?.status == "tx-test")
            }
        }
    }

    @Test("DatabaseTransaction fetch rejects a missing dynamic partition")
    func transactionFetchRejectsMissingDynamicPartition() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()

            await #expect(throws: DirectoryPathError.self) {
                try await container.testBaseContext().withTransaction { transaction in
                    _ = try await transaction.fetch(
                        TenantOrder.self,
                        identifiedBy: "any-id",
                        consistency: .serializable
                    )
                }
            }
        }
    }
}
#endif

#endif
