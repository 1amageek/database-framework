#if SQLITE
import Testing
import Foundation
import Database
import DatabaseRuntime
import TestHeartbeat

@Persistable
struct FDBiteItem {
    #Directory<FDBiteItem>("test", "fdbite", "items")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int64 = 0

    #Index(.scalar, fields: [\FDBiteItem.age])
}

@Persistable
struct FDBiteNote {
    #Directory<FDBiteNote>("test", "fdbite", "notes")

    var id: String = UUID().uuidString
    var title: String = ""
    var body: String = ""
}

@Suite("FDBite Tests", .serialized, .heartbeat)
struct FDBiteTests {

    private func makeItemContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )
    }

    // MARK: - Container Creation

    @Test("Container creation with in-memory SQLite")
    func containerCreation() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )
        #expect(container.schema.entities.count == 1)
    }

    @Test("Container creation with file-based SQLite")
    func fileBasedContainer() async throws {
        let tmpDir = FileManager.default.temporaryDirectory
        let dbPath = tmpDir.appendingPathComponent("fdbite-test-\(UUID().uuidString).sqlite").path

        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: dbPath,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        // Insert and verify persistence
        let context = container.newContext()
        var item = FDBiteItem()
        item.id = "file-test"
        item.name = "Persisted"
        item.age = 40
        try context.insert(item)
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "Persisted")

        if FileManager.default.fileExists(atPath: dbPath) {
            do {
                try FileManager.default.removeItem(atPath: dbPath)
            } catch {
                Issue.record("Failed to remove SQLite test database: \(error)")
            }
        }
    }

    @Test("Container creation with multiple entity types")
    func multipleEntities() async throws {
        let schema = try Schema(
            entities: [
                try FDBiteItem.schemaEntity,
                try FDBiteNote.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self, FDBiteNote.self]
            ),
            security: .disabled
        )
        #expect(container.schema.entities.count == 2)

        let context = container.newContext()

        var item = FDBiteItem()
        item.id = "item-1"
        item.name = "Alice"
        item.age = 30
        try context.insert(item)

        var note = FDBiteNote()
        note.id = "note-1"
        note.title = "Hello"
        note.body = "World"
        try context.insert(note)

        try await context.save()

        let items = try await context.fetch(FDBiteItem.self).execute()
        let notes = try await context.fetch(FDBiteNote.self).execute()
        #expect(items.count == 1)
        #expect(notes.count == 1)
        #expect(items.first?.name == "Alice")
        #expect(notes.first?.title == "Hello")
    }

    // MARK: - CRUD Operations

    @Test("Insert and fetch round-trip")
    func insertAndFetch() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()
        let itemId = "fdbite-\(UUID().uuidString.prefix(8))"

        var item = FDBiteItem()
        item.id = itemId
        item.name = "Alice"
        item.age = 30

        try context.insert(item)
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)

        let fetched = results.first { $0.id == itemId }
        #expect(fetched != nil)
        #expect(fetched?.name == "Alice")
        #expect(fetched?.age == 30)
    }

    @Test("Explicit upsert replaces an existing model")
    func explicitUpsertReplacesExistingModel() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()
        let itemId = "upsert-\(UUID().uuidString.prefix(8))"

        // Initial insert
        var item = FDBiteItem()
        item.id = itemId
        item.name = "Before"
        item.age = 20
        try context.insert(item)
        try await context.save()

        // Replace the existing model through the explicit upsert contract.
        var updated = FDBiteItem()
        updated.id = itemId
        updated.name = "After"
        updated.age = 30
        try context.upsert(updated)
        try await context.save()

        // Verify: should have 1 item with updated values
        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "After")
        #expect(results.first?.age == 30)
    }

    @Test("Multiple inserts in single transaction")
    func batchInsert() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()

        for i in 0..<5 {
            var item = FDBiteItem()
            item.id = "batch-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = Int64(20 + i)
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 5)
    }

    @Test("Delete item")
    func deleteItem() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()

        var item = FDBiteItem()
        item.id = "del-\(UUID().uuidString.prefix(8))"
        item.name = "ToDelete"
        item.age = 25
        try context.insert(item)
        try await context.save()

        let beforeDelete = try await context.fetch(FDBiteItem.self).execute()
        #expect(beforeDelete.count == 1)

        try context.delete(item)
        try await context.save()

        let afterDelete = try await context.fetch(FDBiteItem.self).execute()
        #expect(afterDelete.isEmpty)
    }

    // MARK: - Context Consistency

    @Test("Pending insert is visible only inside the staging context until save")
    func pendingInsertVisibilityIsContextScopedUntilSave() async throws {
        let container = try await makeItemContainer()
        let writer = container.newContext()
        let itemID = "pending-insert-\(UUID().uuidString.prefix(8))"

        var item = FDBiteItem()
        item.id = itemID
        item.name = "Pending"
        item.age = 42

        try writer.insert(item)

        let staged = try await writer.model(for: itemID, as: FDBiteItem.self)
        let isolatedBeforeSave = try await container.newContext().model(
            for: itemID,
            as: FDBiteItem.self
        )

        #expect(staged?.name == "Pending")
        #expect(isolatedBeforeSave == nil)

        try await writer.save()

        let persisted = try await container.newContext().model(
            for: itemID,
            as: FDBiteItem.self
        )
        #expect(persisted?.name == "Pending")
        #expect(persisted?.age == 42)
    }

    @Test("Pending delete hides the persisted row only inside the staging context")
    func pendingDeleteVisibilityIsContextScopedUntilSave() async throws {
        let container = try await makeItemContainer()
        let itemID = "pending-delete-\(UUID().uuidString.prefix(8))"

        var item = FDBiteItem()
        item.id = itemID
        item.name = "Stored"
        item.age = 31

        let seedContext = container.newContext()
        try seedContext.insert(item)
        try await seedContext.save()

        let deletingContext = container.newContext()
        let stored = try #require(
            try await deletingContext.model(for: itemID, as: FDBiteItem.self)
        )
        try deletingContext.delete(stored)

        let hiddenInDeletingContext = try await deletingContext.model(
            for: itemID,
            as: FDBiteItem.self
        )
        let visibleBeforeSave = try await container.newContext().model(
            for: itemID,
            as: FDBiteItem.self
        )

        #expect(hiddenInDeletingContext == nil)
        #expect(visibleBeforeSave?.name == "Stored")

        try await deletingContext.save()

        let visibleAfterSave = try await container.newContext().model(
            for: itemID,
            as: FDBiteItem.self
        )
        #expect(visibleAfterSave == nil)
    }

    @Test("Delete cancels a pending insert before save")
    func deleteCancelsPendingInsertBeforeSave() async throws {
        let container = try await makeItemContainer()
        let context = container.newContext()
        let itemID = "insert-delete-\(UUID().uuidString.prefix(8))"

        var item = FDBiteItem()
        item.id = itemID
        item.name = "Transient"
        item.age = 19

        try context.insert(item)
        try context.delete(item)

        let stagedView = try await context.model(for: itemID, as: FDBiteItem.self)
        #expect(stagedView == nil)

        try await context.save()

        let persisted = try await container.newContext().model(
            for: itemID,
            as: FDBiteItem.self
        )
        let allItems = try await container.newContext().fetch(FDBiteItem.self).execute()

        #expect(persisted == nil)
        #expect(allItems.isEmpty)
    }

    @Test("Rollback discards pending insert and delete mutations")
    func rollbackDiscardsPendingMutations() async throws {
        let container = try await makeItemContainer()

        var storedItem = FDBiteItem()
        storedItem.id = "rollback-stored-\(UUID().uuidString.prefix(8))"
        storedItem.name = "Stored"
        storedItem.age = 28

        let seedContext = container.newContext()
        try seedContext.insert(storedItem)
        try await seedContext.save()

        let context = container.newContext()
        let stored = try #require(
            try await context.model(for: storedItem.id, as: FDBiteItem.self)
        )
        try context.delete(stored)

        var newItem = FDBiteItem()
        newItem.id = "rollback-new-\(UUID().uuidString.prefix(8))"
        newItem.name = "New"
        newItem.age = 35
        try context.insert(newItem)

        #expect(try await context.model(for: storedItem.id, as: FDBiteItem.self) == nil)
        #expect(try await context.model(for: newItem.id, as: FDBiteItem.self)?.name == "New")

        try context.rollback()

        #expect(try await context.model(for: storedItem.id, as: FDBiteItem.self)?.name == "Stored")
        #expect(try await context.model(for: newItem.id, as: FDBiteItem.self) == nil)

        try await context.save()

        let verificationContext = container.newContext()
        #expect(
            try await verificationContext.model(for: storedItem.id, as: FDBiteItem.self)?.name
                == "Stored"
        )
        #expect(try await verificationContext.model(for: newItem.id, as: FDBiteItem.self) == nil)
    }

    // MARK: - Query Operations

    @Test("Fetch with where clause")
    func fetchWithWhere() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()

        for (i, name) in ["Alice", "Bob", "Carol"].enumerated() {
            var item = FDBiteItem()
            item.id = "where-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = Int64(20 + i * 10)  // 20, 30, 40
            try context.insert(item)
        }
        try await context.save()

        // Verify data exists first
        let all = try await context.fetch(FDBiteItem.self).execute()
        #expect(all.count == 3)

        // Test where clause with predicate evaluation
        let results = try await context.fetch(FDBiteItem.self)
            .where(FDBiteItem.fields.age > 25)
            .execute()
        #expect(results.count == 2)
    }

    @Test("Fetch with orderBy")
    func fetchWithOrderBy() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()

        for (i, name) in ["Charlie", "Alice", "Bob"].enumerated() {
            var item = FDBiteItem()
            item.id = "order-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = [Int64(30), 10, 20][i]
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self)
            .orderBy(FDBiteItem.fields.age)
            .execute()
        #expect(results.count == 3)
        #expect(results[0].name == "Alice")
        #expect(results[1].name == "Bob")
        #expect(results[2].name == "Charlie")
    }

    @Test("Fetch with limit")
    func fetchWithLimit() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()

        for i in 0..<5 {
            var item = FDBiteItem()
            item.id = "limit-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = Int64(20 + i)
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self)
            .limit(2)
            .execute()
        #expect(results.count == 2)
    }

    // MARK: - Edge Cases

    @Test("Fetch from empty store returns empty array")
    func emptyFetch() async throws {
        let schema = try Schema(entities: [try FDBiteItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [FDBiteItem.self]
            ),
            security: .disabled
        )

        let context = container.newContext()
        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.isEmpty)
    }
}
#endif
