// PostgreSQLTransactionTests.swift
// Transaction semantics tests against PostgreSQL backend

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import Core
import TestSupport

@Persistable
struct PGTxItem: Equatable {
    #Directory<PGTxItem>("test", "pg", "transaction")

    var id: String = UUID().uuidString
    var counter: Int = 0
}

@Suite("PostgreSQL Transaction Tests", .serialized)
struct PostgreSQLTransactionTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema([PGTxItem.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    // MARK: - Basic Transaction

    @Test("Transaction commit persists data")
    func transactionCommit() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("tx-commit")

            // Write in transaction
            try await context.withTransaction { tx in
                var item = PGTxItem()
                item.id = itemId
                item.counter = 42
                try await tx.set(item)
            }

            // Read in separate transaction
            try await context.withTransaction { tx in
                let fetched = try await tx.get(PGTxItem.self, id: itemId)
                #expect(fetched != nil)
                #expect(fetched?.counter == 42)
            }
        }
    }

    @Test("Multiple writes in single transaction are atomic")
    func atomicMultipleWrites() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let ids = (0..<5).map { _ in uniqueID("tx-atomic") }

            // Write multiple items in single transaction
            try await context.withTransaction { tx in
                for (i, id) in ids.enumerated() {
                    var item = PGTxItem()
                    item.id = id
                    item.counter = i * 10
                    try await tx.set(item)
                }
            }

            // All items should be readable
            try await context.withTransaction { tx in
                for (i, id) in ids.enumerated() {
                    let fetched = try await tx.get(PGTxItem.self, id: id)
                    #expect(fetched != nil, "Item \(i) should exist")
                    #expect(fetched?.counter == i * 10)
                }
            }
        }
    }

    // MARK: - Read-Your-Writes

    @Test("Read-your-writes within transaction")
    func readYourWrites() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("tx-ryw")

            try await context.withTransaction { tx in
                // Write
                var item = PGTxItem()
                item.id = itemId
                item.counter = 99
                try await tx.set(item)

                // Read in same transaction should see the write
                let fetched = try await tx.get(PGTxItem.self, id: itemId)
                #expect(fetched != nil)
                #expect(fetched?.counter == 99)
            }
        }
    }

    @Test("Update within transaction is visible")
    func updateWithinTransaction() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("tx-upd")

            // Insert initial
            try await context.withTransaction { tx in
                var item = PGTxItem()
                item.id = itemId
                item.counter = 1
                try await tx.set(item)
            }

            // Update and read in same transaction
            try await context.withTransaction { tx in
                guard var existing = try await tx.get(PGTxItem.self, id: itemId) else {
                    Issue.record("Item not found")
                    return
                }
                existing.counter = 100
                try await tx.set(existing)

                let fetched = try await tx.get(PGTxItem.self, id: itemId)
                #expect(fetched?.counter == 100)
            }
        }
    }

    // MARK: - Delete within transaction

    @Test("Delete within transaction is visible")
    func deleteWithinTransaction() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("tx-del")

            // Insert
            try await context.withTransaction { tx in
                var item = PGTxItem()
                item.id = itemId
                item.counter = 5
                try await tx.set(item)
            }

            // Delete and verify in same transaction
            try await context.withTransaction { tx in
                try await tx.delete(PGTxItem.self, id: itemId)

                let fetched = try await tx.get(PGTxItem.self, id: itemId)
                #expect(fetched == nil)
            }
        }
    }

    // MARK: - Transaction isolation

    @Test("Separate contexts see committed data")
    func separateContextsCommitted() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()

            let itemId = uniqueID("tx-iso")

            // Context 1: write and commit
            let ctx1 = container.newContext()
            try await ctx1.withTransaction { tx in
                var item = PGTxItem()
                item.id = itemId
                item.counter = 77
                try await tx.set(item)
            }

            // Context 2: should see committed data
            let ctx2 = container.newContext()
            try await ctx2.withTransaction { tx in
                let fetched = try await tx.get(PGTxItem.self, id: itemId)
                #expect(fetched != nil)
                #expect(fetched?.counter == 77)
            }
        }
    }

    // MARK: - Change tracking + save pattern

    @Test("Change tracking save commits as single transaction")
    func changeTrackingSave() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let ids = (0..<3).map { _ in uniqueID("ct") }

            for (i, id) in ids.enumerated() {
                var item = PGTxItem()
                item.id = id
                item.counter = i
                context.insert(item)
            }
            #expect(context.hasChanges == true)

            try await context.save()
            #expect(context.hasChanges == false)

            // Verify all persisted
            for (i, id) in ids.enumerated() {
                let fetched = try await context.fetch(PGTxItem.self)
                    .where(\.id == id)
                    .first()
                #expect(fetched?.counter == i)
            }
        }
    }

    @Test("Rollback discards uncommitted changes")
    func rollbackDiscards() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            var item = PGTxItem()
            item.id = uniqueID("rollback")
            item.counter = 999

            context.insert(item)
            #expect(context.hasChanges == true)

            context.rollback()
            #expect(context.hasChanges == false)
        }
    }
}
