#if POSTGRESQL
// PostgreSQLPartitionTests.swift
// Dynamic Directory (Partitioned Directory) tests against PostgreSQL backend
//
// Validates that StaticDirectoryService-based partition resolution works correctly
// with PostgreSQL backend (FDB uses FDBDirectoryService with HCA prefix allocation).

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import DatabaseKit
import TestSupport

@Suite("PostgreSQL Partition Metadata Tests")
struct PostgreSQLPartitionMetadataTests {
    @Test("TenantOrder has dynamic directory on PostgreSQL")
    func tenantOrderHasDynamicDirectory() {
        #expect(TenantOrder.hasDynamicDirectory == true)
        #expect(TenantOrder.directoryFieldNames == ["tenantID"])
    }

    @Test("Player does not have dynamic directory")
    func playerHasStaticDirectory() {
        #expect(Player.hasDynamicDirectory == false)
        #expect(Player.directoryFieldNames.isEmpty)
    }

    @Test("DirectoryPath validates missing fields")
    func directoryPathValidatesMissingFields() {
        let binding = DirectoryPath<TenantOrder>()

        #expect(throws: DirectoryPathError.self) {
            try binding.validate()
        }
    }

    @Test("DirectoryPath validates complete binding")
    func directoryPathValidatesCompleteBinding() {
        var binding = DirectoryPath<TenantOrder>()
        binding.set(TenantOrder.fields.tenantID, to: "tenant_123")

        #expect(throws: Never.self) {
            try binding.validate()
        }
    }

    @Test("DirectoryPath.from extracts values from model")
    func directoryPathFromModel() throws {
        let order = TenantOrder(tenantID: "tenant_xyz", status: "pending", total: 50.0)
        let binding = try DirectoryPath<TenantOrder>.from(order)
        let tenantID = try binding.value(for: TenantOrder.fields.tenantID)

        #expect(tenantID == "tenant_xyz")
    }
}

@Suite("PostgreSQL Partition Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct PostgreSQLPartitionTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [
                try Player.schemaEntity,
                try TenantOrder.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, persistableTypes: [Player.self, TenantOrder.self])
    }

    // MARK: - Save Tests

    @Test("Save TenantOrder extracts tenantID from model")
    func saveTenantOrderExtractsTenantID() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

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
    func saveOrdersToDifferentTenants() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenant1 = uniqueID("t1")
            let tenant2 = uniqueID("t2")
            let order1ID = uniqueID("o1")
            let order2ID = uniqueID("o2")

            var order1 = TenantOrder(tenantID: tenant1, status: "completed", total: 50.0)
            order1.id = order1ID

            var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 75.0)
            order2.id = order2ID

            try context.insert(order1)
            try context.insert(order2)
            try await context.save()

            // Fetch tenant1 orders — should not contain tenant2's order
            let tenant1Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant1)
                .execute()

            #expect(tenant1Orders.contains { $0.id == order1ID })
            #expect(!tenant1Orders.contains { $0.id == order2ID })

            // Fetch tenant2 orders — should not contain tenant1's order
            let tenant2Orders = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenant2)
                .execute()

            #expect(tenant2Orders.contains { $0.id == order2ID })
            #expect(!tenant2Orders.contains { $0.id == order1ID })
        }
    }

    // MARK: - Fetch Tests

    @Test("Fetch without partition throws for dynamic directory type")
    func fetchWithoutPartitionThrows() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                _ = try await context.fetch(TenantOrder.self).execute()
            }
        }
    }

    @Test("Fetch with partition and where clause filters within partition")
    func fetchWithWhereFiltersWithinPartition() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let order1ID = uniqueID("o1")
            let order2ID = uniqueID("o2")

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
    func deleteFromCorrectPartition() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 50.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            // Verify exists
            let before = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.id == orderID)
                .first()
            #expect(before != nil)

            // Delete
            if let toDelete = before {
                try context.delete(toDelete)
                try await context.save()
            }

            // Verify deleted
            let after = try await context.fetch(TenantOrder.self)
                .partition(TenantOrder.fields.tenantID, equals: tenantID)
                .where(TenantOrder.fields.id == orderID)
                .first()
            #expect(after == nil)
        }
    }

    // MARK: - deleteAll Tests

    @Test("deleteAll without partition throws for dynamic directory type")
    func deleteAllWithoutPartitionThrows() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.deleteAll(TenantOrder.self)
            }
        }
    }

    @Test("deleteAll with partition deletes only from that partition")
    func deleteAllWithPartition() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenant1 = uniqueID("t1")
            let tenant2 = uniqueID("t2")
            let order1ID = uniqueID("o1")
            let order2ID = uniqueID("o2")

            var order1 = TenantOrder(tenantID: tenant1, status: "pending", total: 100.0)
            order1.id = order1ID

            var order2 = TenantOrder(tenantID: tenant2, status: "pending", total: 200.0)
            order2.id = order2ID

            try context.insert(order1)
            try context.insert(order2)
            try await context.save()

            // Delete all from tenant1
            try await context.deleteAll(
                TenantOrder.self,
                partition: TenantOrder.fields.tenantID,
                equals: tenant1
            )
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
    func enumerateWithoutPartitionThrows() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.enumerate(TenantOrder.self) { _ in }
            }
        }
    }

    @Test("enumerate with partition enumerates only that partition")
    func enumerateWithPartition() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            var found = false
            try await context.enumerate(
                TenantOrder.self,
                partition: TenantOrder.fields.tenantID,
                equals: tenantID
            ) { item in
                if item.id == orderID {
                    found = true
                }
            }
            #expect(found)
        }
    }

    // MARK: - Static Directory Types (Regression)

    @Test("Static directory types work without partition on PostgreSQL")
    func staticDirectoryTypesWork() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let playerID = uniqueID("player")
            var player = Player(name: "Test Player", score: 100, level: 5)
            player.id = playerID

            try context.insert(player)
            try await context.save()

            let fetched = try await context.fetch(Player.self)
                .where(Player.fields.id == playerID)
                .first()

            #expect(fetched != nil)
            #expect(fetched?.name == "Test Player")
        }
    }

    // MARK: - DatabaseTransaction Partition Tests

    @Test("DatabaseTransaction saves and fetches dynamic directory types on PostgreSQL")
    func transactionSavesAndFetchesDynamicDirectoryModel() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            try await container.newContext().withTransaction { transaction in
                var order = TenantOrder(tenantID: tenantID, status: "tx-test", total: 500.0)
                order.id = orderID

                try await transaction.save(
                    order,
                    precondition: .notExists
                )

                var binding = DirectoryPath<TenantOrder>()
                binding.set(TenantOrder.fields.tenantID, to: tenantID)
                let fetched = try await transaction.fetch(
                    TenantOrder.self,
                    identifiedBy: orderID,
                    in: binding
                )

                #expect(fetched != nil)
                #expect(fetched?.status == "tx-test")
            }
        }
    }

    @Test("DatabaseTransaction fetch rejects a missing PostgreSQL partition")
    func transactionFetchRejectsMissingDynamicPartition() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()

            await #expect(throws: DirectoryPathError.self) {
                try await container.newContext().withTransaction { transaction in
                    _ = try await transaction.fetch(
                        TenantOrder.self,
                        identifiedBy: "any-id"
                    )
                }
            }
        }
    }
}
#endif
