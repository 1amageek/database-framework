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
@testable import Core
import TestSupport

@Suite("PostgreSQL Partition Tests", .serialized, .heartbeat)
struct PostgreSQLPartitionTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema([Player.self, TenantOrder.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    // MARK: - hasDynamicDirectory Tests

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

    // MARK: - Save Tests

    @Test("Save TenantOrder extracts tenantID from model")
    func saveTenantOrderExtractsTenantID() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
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
    }

    @Test("Save multiple orders to different tenants")
    func saveOrdersToDifferentTenants() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
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

            context.insert(order1)
            context.insert(order2)
            try await context.save()

            // Fetch tenant1 orders — should not contain tenant2's order
            let tenant1Orders = try await context.fetch(TenantOrder.self)
                .partition(\.tenantID, equals: tenant1)
                .execute()

            #expect(tenant1Orders.contains { $0.id == order1ID })
            #expect(!tenant1Orders.contains { $0.id == order2ID })

            // Fetch tenant2 orders — should not contain tenant1's order
            let tenant2Orders = try await context.fetch(TenantOrder.self)
                .partition(\.tenantID, equals: tenant2)
                .execute()

            #expect(tenant2Orders.contains { $0.id == order2ID })
            #expect(!tenant2Orders.contains { $0.id == order1ID })
        }
    }

    // MARK: - Fetch Tests

    @Test("Fetch without partition throws for dynamic directory type")
    func fetchWithoutPartitionThrows() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                _ = try await context.fetch(TenantOrder.self).execute()
            }
        }
    }

    @Test("Fetch with partition and where clause filters within partition")
    func fetchWithWhereFiltersWithinPartition() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let order1ID = uniqueID("o1")
            let order2ID = uniqueID("o2")

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
    }

    // MARK: - Delete Tests

    @Test("Delete TenantOrder from correct partition")
    func deleteFromCorrectPartition() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 50.0)
            order.id = orderID
            context.insert(order)
            try await context.save()

            // Verify exists
            let before = try await context.fetch(TenantOrder.self)
                .partition(\.tenantID, equals: tenantID)
                .where(\.id == orderID)
                .first()
            #expect(before != nil)

            // Delete
            if let toDelete = before {
                context.delete(toDelete)
                try await context.save()
            }

            // Verify deleted
            let after = try await context.fetch(TenantOrder.self)
                .partition(\.tenantID, equals: tenantID)
                .where(\.id == orderID)
                .first()
            #expect(after == nil)
        }
    }

    // MARK: - deleteAll Tests

    @Test("deleteAll without partition throws for dynamic directory type")
    func deleteAllWithoutPartitionThrows() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.deleteAll(TenantOrder.self)
            }
        }
    }

    @Test("deleteAll with partition deletes only from that partition")
    func deleteAllWithPartition() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
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
    }

    // MARK: - enumerate Tests

    @Test("enumerate without partition throws for dynamic directory type")
    func enumerateWithoutPartitionThrows() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            await #expect(throws: DirectoryPathError.self) {
                try await context.enumerate(TenantOrder.self) { _ in }
            }
        }
    }

    @Test("enumerate with partition enumerates only that partition")
    func enumerateWithPartition() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 100.0)
            order.id = orderID
            context.insert(order)
            try await context.save()

            var found = false
            try await context.enumerate(TenantOrder.self, partition: \.tenantID, equals: tenantID) { item in
                if item.id == orderID {
                    found = true
                }
            }
            #expect(found)
        }
    }

    // MARK: - DirectoryPath Tests

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
        binding.set(\.tenantID, to: "tenant_123")

        // Should not throw
        #expect(throws: Never.self) {
            try binding.validate()
        }
    }

    @Test("DirectoryPath.from extracts values from model")
    func directoryPathFromModel() {
        let order = TenantOrder(tenantID: "tenant_xyz", status: "pending", total: 50.0)
        let binding = DirectoryPath<TenantOrder>.from(order)

        #expect(binding.value(for: \.tenantID) == "tenant_xyz")
    }

    // MARK: - Static Directory Types (Regression)

    @Test("Static directory types work without partition on PostgreSQL")
    func staticDirectoryTypesWork() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let playerID = uniqueID("player")
            var player = Player(name: "Test Player", score: 100, level: 5)
            player.id = playerID

            context.insert(player)
            try await context.save()

            let fetched = try await context.fetch(Player.self)
                .where(\.id == playerID)
                .first()

            #expect(fetched != nil)
            #expect(fetched?.name == "Test Player")
        }
    }

    // MARK: - TransactionContext Partition Tests

    @Test("TransactionContext set/get works for dynamic directory types on PostgreSQL")
    func transactionContextSetGetDynamicDirectory() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")

            try await container.engine.withTransaction { transaction in
                let txContext = TransactionContext(transaction: transaction, container: container)

                var order = TenantOrder(tenantID: tenantID, status: "tx-test", total: 500.0)
                order.id = orderID

                try await txContext.set(order)

                var binding = DirectoryPath<TenantOrder>()
                binding.set(\.tenantID, to: tenantID)
                let fetched = try await txContext.get(TenantOrder.self, id: orderID, partition: binding)

                #expect(fetched != nil)
                #expect(fetched?.status == "tx-test")
            }
        }
    }

    @Test("TransactionContext get throws without partition for dynamic types")
    func transactionContextGetThrowsWithoutPartition() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()

            await #expect(throws: DirectoryPathError.self) {
                try await container.engine.withTransaction { transaction in
                    let txContext = TransactionContext(transaction: transaction, container: container)
                    _ = try await txContext.get(TenantOrder.self, id: "any-id")
                }
            }
        }
    }
}
#endif
