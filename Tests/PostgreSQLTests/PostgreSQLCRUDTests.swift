// PostgreSQLCRUDTests.swift
// Basic CRUD round-trip tests against PostgreSQL backend

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import Core
import TestSupport

@Persistable
struct PGDemoItem: Equatable {
    #Directory<PGDemoItem>("test", "pg", "demo")

    var id: String = UUID().uuidString
    var name: String = ""
    var value: Int = 0
    var tags: [String] = []
}

@Suite("PostgreSQL CRUD Tests", .serialized)
struct PostgreSQLCRUDTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let schema = Schema([PGDemoItem.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    // MARK: - Basic CRUD

    @Test("Create -> Read -> Update -> Delete round-trip")
    func roundTrip() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("demo")

            // 1. CREATE
            try await context.withTransaction { tx in
                var item = PGDemoItem()
                item.id = itemId
                item.name = "PostgreSQL test item"
                item.value = 42
                item.tags = ["pg", "test"]
                try await tx.set(item)
            }

            // 2. READ
            try await context.withTransaction { tx in
                let fetched = try await tx.get(PGDemoItem.self, id: itemId)
                #expect(fetched != nil)
                #expect(fetched?.name == "PostgreSQL test item")
                #expect(fetched?.value == 42)
                #expect(fetched?.tags == ["pg", "test"])
            }

            // 3. UPDATE
            try await context.withTransaction { tx in
                guard var updated = try await tx.get(PGDemoItem.self, id: itemId) else {
                    Issue.record("Item not found for update")
                    return
                }
                updated.name = "Updated item"
                updated.value = 100
                updated.tags.append("updated")
                try await tx.set(updated)
            }

            // 3.1 READ after UPDATE
            try await context.withTransaction { tx in
                let fetched = try await tx.get(PGDemoItem.self, id: itemId)
                #expect(fetched?.name == "Updated item")
                #expect(fetched?.value == 100)
                #expect(fetched?.tags.contains("updated") == true)
            }

            // 4. DELETE
            try await context.withTransaction { tx in
                try await tx.delete(PGDemoItem.self, id: itemId)
            }

            // 4.1 READ after DELETE (should be nil)
            try await context.withTransaction { tx in
                let fetched = try await tx.get(PGDemoItem.self, id: itemId)
                #expect(fetched == nil)
            }
        }
    }

    // MARK: - Change Tracking (insert/save pattern)

    @Test("Insert multiple items and save in batch")
    func batchInsertSave() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let ids = (0..<5).map { _ in uniqueID("batch") }

            // Insert multiple items
            for (i, id) in ids.enumerated() {
                var item = PGDemoItem()
                item.id = id
                item.name = "Item \(i)"
                item.value = i * 10
                context.insert(item)
            }

            // Save all at once
            try await context.save()

            // Verify all items are persisted
            for (i, id) in ids.enumerated() {
                let fetched = try await context.fetch(PGDemoItem.self)
                    .where(\.id == id)
                    .first()
                #expect(fetched != nil, "Item \(i) should exist")
                #expect(fetched?.name == "Item \(i)")
            }
        }
    }

    @Test("Delete via change tracking")
    func deleteViaChangeTracking() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("del")
            var item = PGDemoItem()
            item.id = itemId
            item.name = "To be deleted"

            context.insert(item)
            try await context.save()

            // Verify exists
            let before = try await context.fetch(PGDemoItem.self)
                .where(\.id == itemId)
                .first()
            #expect(before != nil)

            // Delete via change tracking
            if let toDelete = before {
                context.delete(toDelete)
                try await context.save()
            }

            // Verify deleted
            let after = try await context.fetch(PGDemoItem.self)
                .where(\.id == itemId)
                .first()
            #expect(after == nil)
        }
    }

    // MARK: - Fetch with predicates

    @Test("Fetch with where clause")
    func fetchWithWhere() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let id1 = uniqueID("fw1")
            let id2 = uniqueID("fw2")
            let id3 = uniqueID("fw3")

            var item1 = PGDemoItem(); item1.id = id1; item1.name = "Alpha"; item1.value = 10
            var item2 = PGDemoItem(); item2.id = id2; item2.name = "Beta"; item2.value = 20
            var item3 = PGDemoItem(); item3.id = id3; item3.name = "Alpha"; item3.value = 30

            context.insert(item1)
            context.insert(item2)
            context.insert(item3)
            try await context.save()

            // Filter by name
            let alphas = try await context.fetch(PGDemoItem.self)
                .where(\.name == "Alpha")
                .execute()

            let matchingAlphas = alphas.filter { $0.id == id1 || $0.id == id3 }
            #expect(matchingAlphas.count == 2)
            #expect(!alphas.contains { $0.id == id2 })
        }
    }

    @Test("Fetch with limit")
    func fetchWithLimit() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            // Insert several items
            for i in 0..<10 {
                var item = PGDemoItem()
                item.id = uniqueID("lim\(i)")
                item.name = "Limit test"
                item.value = i
                context.insert(item)
            }
            try await context.save()

            let results = try await context.fetch(PGDemoItem.self)
                .where(\.name == "Limit test")
                .limit(3)
                .execute()

            #expect(results.count <= 3)
        }
    }

    // MARK: - Empty and edge cases

    @Test("Read non-existent item returns nil")
    func readNonExistent() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            try await context.withTransaction { tx in
                let fetched = try await tx.get(PGDemoItem.self, id: "nonexistent-id")
                #expect(fetched == nil)
            }
        }
    }

    @Test("Insert same ID overwrites (upsert behavior)")
    func upsertBehavior() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("upsert")

            // First insert
            var item1 = PGDemoItem()
            item1.id = itemId
            item1.name = "Original"
            item1.value = 1
            context.insert(item1)
            try await context.save()

            // Second insert with same ID
            var item2 = PGDemoItem()
            item2.id = itemId
            item2.name = "Overwritten"
            item2.value = 2
            context.insert(item2)
            try await context.save()

            // Should have the second value
            let fetched = try await context.fetch(PGDemoItem.self)
                .where(\.id == itemId)
                .first()
            #expect(fetched?.name == "Overwritten")
            #expect(fetched?.value == 2)
        }
    }

    @Test("Empty array field round-trip")
    func emptyArrayRoundTrip() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("empty-arr")

            var item = PGDemoItem()
            item.id = itemId
            item.name = "Empty tags"
            item.tags = []

            context.insert(item)
            try await context.save()

            let fetched = try await context.fetch(PGDemoItem.self)
                .where(\.id == itemId)
                .first()
            #expect(fetched?.tags == [])
        }
    }
}
