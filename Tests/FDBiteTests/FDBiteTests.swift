import Testing
import Foundation
import FDBite

@Persistable
struct FDBiteItem {
    #Directory<FDBiteItem>("test", "fdbite", "items")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int = 0

    #Index(ScalarIndexKind<FDBiteItem>(fields: [\.age]))
}

@Persistable
struct FDBiteNote {
    #Directory<FDBiteNote>("test", "fdbite", "notes")

    var id: String = UUID().uuidString
    var title: String = ""
    var body: String = ""
}

@Suite("FDBite Tests", .serialized)
struct FDBiteTests {

    // MARK: - Container Creation

    @Test("Container creation with in-memory SQLite")
    func containerCreation() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )
        #expect(container.schema.entities.count == 1)
    }

    @Test("Container creation with file-based SQLite")
    func fileBasedContainer() async throws {
        let tmpDir = FileManager.default.temporaryDirectory
        let dbPath = tmpDir.appendingPathComponent("fdbite-test-\(UUID().uuidString).sqlite").path

        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.sqlite(
            for: schema,
            path: dbPath,
            security: .disabled
        )

        // Insert and verify persistence
        let context = container.newContext()
        var item = FDBiteItem()
        item.id = "file-test"
        item.name = "Persisted"
        item.age = 40
        context.insert(item)
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "Persisted")

        // Cleanup
        try? FileManager.default.removeItem(atPath: dbPath)
    }

    @Test("Container creation with multiple entity types")
    func multipleEntities() async throws {
        let schema = Schema(
            [FDBiteItem.self, FDBiteNote.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )
        #expect(container.schema.entities.count == 2)

        let context = container.newContext()

        var item = FDBiteItem()
        item.id = "item-1"
        item.name = "Alice"
        item.age = 30
        context.insert(item)

        var note = FDBiteNote()
        note.id = "note-1"
        note.title = "Hello"
        note.body = "World"
        context.insert(note)

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
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()
        let itemId = "fdbite-\(UUID().uuidString.prefix(8))"

        var item = FDBiteItem()
        item.id = itemId
        item.name = "Alice"
        item.age = 30

        context.insert(item)
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)

        let fetched = results.first { $0.id == itemId }
        #expect(fetched != nil)
        #expect(fetched?.name == "Alice")
        #expect(fetched?.age == 30)
    }

    @Test("Update via re-insert (upsert)")
    func updateViaReInsert() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()
        let itemId = "upsert-\(UUID().uuidString.prefix(8))"

        // Initial insert
        var item = FDBiteItem()
        item.id = itemId
        item.name = "Before"
        item.age = 20
        context.insert(item)
        try await context.save()

        // Update via re-insert with same ID
        var updated = FDBiteItem()
        updated.id = itemId
        updated.name = "After"
        updated.age = 30
        context.insert(updated)
        try await context.save()

        // Verify: should have 1 item with updated values
        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "After")
        #expect(results.first?.age == 30)
    }

    @Test("Multiple inserts in single transaction")
    func batchInsert() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()

        for i in 0..<5 {
            var item = FDBiteItem()
            item.id = "batch-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = 20 + i
            context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.count == 5)
    }

    @Test("Delete item")
    func deleteItem() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()

        var item = FDBiteItem()
        item.id = "del-\(UUID().uuidString.prefix(8))"
        item.name = "ToDelete"
        item.age = 25
        context.insert(item)
        try await context.save()

        let beforeDelete = try await context.fetch(FDBiteItem.self).execute()
        #expect(beforeDelete.count == 1)

        context.delete(item)
        try await context.save()

        let afterDelete = try await context.fetch(FDBiteItem.self).execute()
        #expect(afterDelete.isEmpty)
    }

    // MARK: - Query Operations

    @Test("Fetch with where clause")
    func fetchWithWhere() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()

        for (i, name) in ["Alice", "Bob", "Carol"].enumerated() {
            var item = FDBiteItem()
            item.id = "where-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = 20 + i * 10  // 20, 30, 40
            context.insert(item)
        }
        try await context.save()

        // Verify data exists first
        let all = try await context.fetch(FDBiteItem.self).execute()
        #expect(all.count == 3)

        // Test where clause with predicate evaluation
        let results = try await context.fetch(FDBiteItem.self)
            .where(\.age > 25)
            .execute()
        #expect(results.count == 2)
    }

    @Test("Fetch with orderBy")
    func fetchWithOrderBy() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()

        for (i, name) in ["Charlie", "Alice", "Bob"].enumerated() {
            var item = FDBiteItem()
            item.id = "order-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = [30, 10, 20][i]
            context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(FDBiteItem.self)
            .orderBy(\.age)
            .execute()
        #expect(results.count == 3)
        #expect(results[0].name == "Alice")
        #expect(results[1].name == "Bob")
        #expect(results[2].name == "Charlie")
    }

    @Test("Fetch with limit")
    func fetchWithLimit() async throws {
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()

        for i in 0..<5 {
            var item = FDBiteItem()
            item.id = "limit-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = 20 + i
            context.insert(item)
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
        let schema = Schema([FDBiteItem.self], version: Schema.Version(1, 0, 0))
        let container = try await FDBContainer.inMemory(
            for: schema,
            security: .disabled
        )

        let context = container.newContext()
        let results = try await context.fetch(FDBiteItem.self).execute()
        #expect(results.isEmpty)
    }
}
