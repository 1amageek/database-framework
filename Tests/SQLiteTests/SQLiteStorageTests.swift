#if SQLITE
import Testing
import Foundation
import Database
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import TestSupport
import TestHeartbeat
@testable import DatabaseEngine

@Persistable
struct SQLiteStoredItem {
    #Directory<SQLiteStoredItem>("test", "sqlite_storage", "items")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int64 = 0

    #Index(
        .ordered(
            name: "SQLiteStoredItem_age", keys: [.ascending(\SQLiteStoredItem.age)],
            unique: false))
}

@Persistable
struct SQLiteStoredNote {
    #Directory<SQLiteStoredNote>("test", "sqlite_storage", "notes")

    var id: String = UUID().uuidString
    var title: String = ""
    var body: String = ""
}

@Persistable
struct SQLiteStoredRichItem {
    #Directory<SQLiteStoredRichItem>("test", "sqlite_storage", "rich_items")

    var id: String
    var exactAmount: QueryNumericValue
    var lexicalDocument: QueryJSONValue
    var binaryDocument: QueryJSONBValue
    var durationValue: QueryIntervalValue
    var integerValues: QueryArray<Int32>
}

@Suite("SQLite Storage Tests", .serialized, .heartbeat)
struct SQLiteStorageTests {

    private func makeItemContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )
    }

    @Test("Declared rich values survive SQLite save, update and restart", .timeLimit(.minutes(2)))
    func richValuesSurviveSQLiteRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "sqlite-rich-values")
        defer { database.remove() }
        let schema = try Schema(entities: [try SQLiteStoredRichItem.schemaEntity],
                                version: Schema.Version(1, 0, 0))
        let declaredEntity = try #require(schema.entities.first)
        let catalogEntity = try SchemaEntityEntryCodec.decode(SchemaEntityEntryCodec.encode(declaredEntity))
        #expect(catalogEntity.fieldMapByName == declaredEntity.fieldMapByName)
        #expect(catalogEntity.fields.filter { $0.queryType != nil }.count == 5)
        #expect(try SchemaManifest(schema: Schema(entities: [catalogEntity], version: schema.version)).fingerprint()
            == SchemaManifest(schema: schema).fingerprint())
        let runtime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "database-tests", revision: 1),
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredRichItem.self)]
        )
        let first = try await DBContainer.sqlite(for: schema, path: database.path,
            runtimeConfiguration: runtime, security: .testingDisabled)
        defer { await first.shutdown() }

        let original = try makeRichItem(updated: false)
        let writer = first.testBaseContext()
        try writer.insert(original)
        try await writer.save()
        let inserted = try await first.testBaseContext().fetch(SQLiteStoredRichItem.self)
            .cachePolicy(.server).execute()
        #expect(inserted.count == 1)
        let reloaded = try #require(inserted.first)
        #expect(richValues(reloaded) == richValues(original))
        #expect(reloaded.exactAmount.displayScale == 6)
        #expect(reloaded.lexicalDocument.bytes == original.lexicalDocument.bytes)
        #expect(reloaded.integerValues.arrayValue.dimensions[0].lowerBound == -2)

        let updated = try makeRichItem(updated: true)
        let updater = first.testBaseContext()
        try updater.update(updated)
        try await updater.save()
        await first.shutdown()

        let reopened = try await DBContainer.sqlite(for: schema, path: database.path,
            runtimeConfiguration: runtime, security: .testingDisabled)
        defer { await reopened.shutdown() }
        let context = reopened.testBaseContext()
        let persisted = try await context.fetch(SQLiteStoredRichItem.self).cachePolicy(.server).execute()
        #expect(persisted.count == 1)
        let restored = try #require(persisted.first)
        let expected = richValues(updated)
        #expect(richValues(restored) == expected)
        #expect(restored.exactAmount.displayScale == 8)
        #expect(restored.lexicalDocument.bytes == updated.lexicalDocument.bytes)
        #expect(restored.integerValues.arrayValue.dimensions[0].lowerBound == 3)

        let projection = "SELECT exactAmount, lexicalDocument, binaryDocument, durationValue, integerValues FROM SQLiteStoredRichItem"
        let selected = try await context.executeSQL(projection)
        #expect(selected.rows.count == 1)
        #expect(selected.rows[0].values == expected)
        let types: [QueryColumnType] = [.sql(.decimal(precision: nil, scale: nil)), .sql(.json),
            .sql(.jsonb), .sql(.interval), .sql(.array(.integer))]
        #expect(selected.columns.map(\.type) == types)
        #expect(selected.columns.allSatisfy { !$0.isNullable })
        let empty = try await context.executeSQL(projection + " WHERE FALSE")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns == selected.columns)

        let names = ["exactAmount", "lexicalDocument", "binaryDocument", "durationValue", "integerValues"]
        let literalQuery = SelectQuery(projection: .items(expected.indices.map { index in
            ProjectionItem(.literal(.queryValue(expected[index], type: types[index])), alias: names[index])
        }), source: .values([[]], columnNames: []))
        let literals = try await context.query(literalQuery)
        #expect(literals.rows.count == 1)
        #expect(literals.rows[0].values == expected)
        #expect(literals.columns == selected.columns)

        var parameters = expected.indices.map { index in
            QueryParameter(position: UInt32(index + 1), name: names[index],
                           value: expected[index], type: types[index])
        }
        parameters.append(QueryParameter(position: 6, name: "absent", value: .primitive(.null), type: .sql(.json)))
        let parameterSQL = "SELECT :exactAmount AS exactAmount, :lexicalDocument AS lexicalDocument, :binaryDocument AS binaryDocument, :durationValue AS durationValue, :integerValues AS integerValues, :absent AS absent"
        let bound = try await context.executeSQL(parameterSQL, parameters: parameters)
        #expect(bound.rows.count == 1)
        #expect(bound.rows[0].values == expected + [.primitive(.null)])
        #expect(bound.columns.map(\.type) == types + [.sql(.json)])
        #expect(bound.columns[5].isNullable)
        let emptyBound = try await context.executeSQL(parameterSQL + " WHERE FALSE", parameters: parameters)
        #expect(emptyBound.rows.isEmpty)
        #expect(emptyBound.columns == bound.columns)
    }

    private func richValues(_ item: SQLiteStoredRichItem) -> [QueryValue] {
        [.numeric(item.exactAmount), .json(item.lexicalDocument), .jsonb(item.binaryDocument),
         .interval(item.durationValue), .array(item.integerValues.arrayValue)]
    }

    private func makeRichItem(updated: Bool) throws -> SQLiteStoredRichItem {
        let limits = QueryValueLimits(maximumRetainedBytes: 16_384, maximumTotalNodes: 1_024,
                                     maximumNestingDepth: 32, maximumCollectionElements: 256)
        let text = updated ? " {\"b\":3.50, \"a\":[true,null], \"b\":4} " : " {\"b\":1.00, \"a\":null, \"b\":2} "
        let json = try QueryJSONValue(bytes: ByteString(utf8: text), limits: limits)
        return try SQLiteStoredRichItem(id: "rich",
            exactAmount: QueryNumericValue(value: ExactDecimal(coefficient: updated ? 456 : 123, scale: 2),
                                       displayScale: updated ? 8 : 6),
            lexicalDocument: json, binaryDocument: QueryJSONBValue(json: json, limits: limits),
            durationValue: QueryIntervalValue(period: CalendarPeriod(months: updated ? 4 : -2, days: updated ? -5 : 3),
                                         time: TimeSpan(seconds: updated ? 2 : -1, nanoseconds: 500_000_000)),
            integerValues: QueryArray(arrayValue: QueryArrayValue(elementType: .integer,
                dimensions: [QueryArrayDimension(lowerBound: updated ? 3 : -2, length: 2)],
                elements: [.primitive(.int32(updated ? 7 : 1)), .primitive(.int32(updated ? 9 : 2))],
                limits: limits), limits: limits))
    }

    // MARK: - Container Creation

    @Test("Container creation with in-memory SQLite")
    func containerCreation() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )
        #expect(container.schema.entities.count == 1)
    }

    @Test("Container creation with file-based SQLite")
    func fileBasedContainer() async throws {
        let tmpDir = FileManager.default.temporaryDirectory
        let dbPath = tmpDir.appendingPathComponent(
            "sqlite-storage-test-\(UUID().uuidString).sqlite"
        ).path

        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.sqlite(
            for: schema,
            path: dbPath,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )
        defer {
            await container.shutdown()
            if FileManager.default.fileExists(atPath: dbPath) {
                do {
                    try FileManager.default.removeItem(atPath: dbPath)
                } catch {
                    Issue.record("Failed to remove SQLite test database: \(error)")
                }
            }
        }

        // Insert and verify persistence
        let context = container.testBaseContext()
        var item = SQLiteStoredItem()
        item.id = "file-test"
        item.name = "Persisted"
        item.age = 40
        try context.insert(item)
        try await context.save()

        let results = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "Persisted")

    }

    @Test("Container creation with multiple entity types")
    func multipleEntities() async throws {
        let schema = try Schema(
            entities: [
                try SQLiteStoredItem.schemaEntity,
                try SQLiteStoredNote.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self), try DatabaseFrameworkRuntime.entity(SQLiteStoredNote.self),
                ]
            ),
            security: .testingDisabled
        )
        #expect(container.schema.entities.count == 2)

        let context = container.testBaseContext()

        var item = SQLiteStoredItem()
        item.id = "item-1"
        item.name = "Alice"
        item.age = 30
        try context.insert(item)

        var note = SQLiteStoredNote()
        note.id = "note-1"
        note.title = "Hello"
        note.body = "World"
        try context.insert(note)

        try await context.save()

        let items = try await context.fetch(SQLiteStoredItem.self).execute()
        let notes = try await context.fetch(SQLiteStoredNote.self).execute()
        #expect(items.count == 1)
        #expect(notes.count == 1)
        #expect(items.first?.name == "Alice")
        #expect(notes.first?.title == "Hello")
    }

    // MARK: - CRUD Operations

    @Test("Insert and fetch round-trip")
    func insertAndFetch() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()
        let itemId = "sqlite-storage-\(UUID().uuidString.prefix(8))"

        var item = SQLiteStoredItem()
        item.id = itemId
        item.name = "Alice"
        item.age = 30

        try context.insert(item)
        try await context.save()

        let results = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(results.count == 1)

        let fetched = results.first { $0.id == itemId }
        #expect(fetched != nil)
        #expect(fetched?.name == "Alice")
        #expect(fetched?.age == 30)
    }

    @Test("Explicit upsert replaces an existing model")
    func explicitUpsertReplacesExistingModel() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()
        let itemId = "upsert-\(UUID().uuidString.prefix(8))"

        // Initial insert
        var item = SQLiteStoredItem()
        item.id = itemId
        item.name = "Before"
        item.age = 20
        try context.insert(item)
        try await context.save()

        // Replace the existing model through the explicit upsert contract.
        var updated = SQLiteStoredItem()
        updated.id = itemId
        updated.name = "After"
        updated.age = 30
        try context.upsert(updated)
        try await context.save()

        // Verify: should have 1 item with updated values
        let results = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(results.count == 1)
        #expect(results.first?.name == "After")
        #expect(results.first?.age == 30)
    }

    @Test("Multiple inserts in single transaction")
    func batchInsert() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()

        for i in 0..<5 {
            var item = SQLiteStoredItem()
            item.id = "batch-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = Int64(20 + i)
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(results.count == 5)
    }

    @Test("Delete item")
    func deleteItem() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()

        var item = SQLiteStoredItem()
        item.id = "del-\(UUID().uuidString.prefix(8))"
        item.name = "ToDelete"
        item.age = 25
        try context.insert(item)
        try await context.save()

        let beforeDelete = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(beforeDelete.count == 1)

        try context.delete(item)
        try await context.save()

        let afterDelete = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(afterDelete.isEmpty)
    }

    // MARK: - Context Consistency

    @Test("Pending insert is visible only inside the staging context until save")
    func pendingInsertVisibilityIsContextScopedUntilSave() async throws {
        let container = try await makeItemContainer()
        let writer = container.testBaseContext()
        let itemID = "pending-insert-\(UUID().uuidString.prefix(8))"

        var item = SQLiteStoredItem()
        item.id = itemID
        item.name = "Pending"
        item.age = 42

        try writer.insert(item)

        let staged = try await writer.model(for: itemID, as: SQLiteStoredItem.self)
        let isolatedBeforeSave = try await container.testBaseContext().model(
            for: itemID,
            as: SQLiteStoredItem.self
        )

        #expect(staged?.name == "Pending")
        #expect(isolatedBeforeSave == nil)

        try await writer.save()

        let persisted = try await container.testBaseContext().model(
            for: itemID,
            as: SQLiteStoredItem.self
        )
        #expect(persisted?.name == "Pending")
        #expect(persisted?.age == 42)
    }

    @Test("Pending delete hides the persisted row only inside the staging context")
    func pendingDeleteVisibilityIsContextScopedUntilSave() async throws {
        let container = try await makeItemContainer()
        let itemID = "pending-delete-\(UUID().uuidString.prefix(8))"

        var item = SQLiteStoredItem()
        item.id = itemID
        item.name = "Stored"
        item.age = 31

        let seedContext = container.testBaseContext()
        try seedContext.insert(item)
        try await seedContext.save()

        let deletingContext = container.testBaseContext()
        let stored = try #require(
            try await deletingContext.model(for: itemID, as: SQLiteStoredItem.self)
        )
        try deletingContext.delete(stored)

        let hiddenInDeletingContext = try await deletingContext.model(
            for: itemID,
            as: SQLiteStoredItem.self
        )
        let visibleBeforeSave = try await container.testBaseContext().model(
            for: itemID,
            as: SQLiteStoredItem.self
        )

        #expect(hiddenInDeletingContext == nil)
        #expect(visibleBeforeSave?.name == "Stored")

        try await deletingContext.save()

        let visibleAfterSave = try await container.testBaseContext().model(
            for: itemID,
            as: SQLiteStoredItem.self
        )
        #expect(visibleAfterSave == nil)
    }

    @Test("Delete cancels a pending insert before save")
    func deleteCancelsPendingInsertBeforeSave() async throws {
        let container = try await makeItemContainer()
        let context = container.testBaseContext()
        let itemID = "insert-delete-\(UUID().uuidString.prefix(8))"

        var item = SQLiteStoredItem()
        item.id = itemID
        item.name = "Transient"
        item.age = 19

        try context.insert(item)
        try context.delete(item)

        let stagedView = try await context.model(for: itemID, as: SQLiteStoredItem.self)
        #expect(stagedView == nil)

        try await context.save()

        let persisted = try await container.testBaseContext().model(
            for: itemID,
            as: SQLiteStoredItem.self
        )
        let allItems = try await container.testBaseContext().fetch(SQLiteStoredItem.self).execute()

        #expect(persisted == nil)
        #expect(allItems.isEmpty)
    }

    @Test("Rollback discards pending insert and delete mutations")
    func rollbackDiscardsPendingMutations() async throws {
        let container = try await makeItemContainer()

        var storedItem = SQLiteStoredItem()
        storedItem.id = "rollback-stored-\(UUID().uuidString.prefix(8))"
        storedItem.name = "Stored"
        storedItem.age = 28

        let seedContext = container.testBaseContext()
        try seedContext.insert(storedItem)
        try await seedContext.save()

        let context = container.testBaseContext()
        let stored = try #require(
            try await context.model(for: storedItem.id, as: SQLiteStoredItem.self)
        )
        try context.delete(stored)

        var newItem = SQLiteStoredItem()
        newItem.id = "rollback-new-\(UUID().uuidString.prefix(8))"
        newItem.name = "New"
        newItem.age = 35
        try context.insert(newItem)

        #expect(try await context.model(for: storedItem.id, as: SQLiteStoredItem.self) == nil)
        #expect(try await context.model(for: newItem.id, as: SQLiteStoredItem.self)?.name == "New")

        try context.rollback()

        #expect(try await context.model(for: storedItem.id, as: SQLiteStoredItem.self)?.name == "Stored")
        #expect(try await context.model(for: newItem.id, as: SQLiteStoredItem.self) == nil)

        try await context.save()

        let verificationContext = container.testBaseContext()
        #expect(
            try await verificationContext.model(for: storedItem.id, as: SQLiteStoredItem.self)?
                .name
                == "Stored"
        )
        #expect(try await verificationContext.model(for: newItem.id, as: SQLiteStoredItem.self) == nil)
    }

    // MARK: - Query Operations

    @Test("Fetch with where clause")
    func fetchWithWhere() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()

        for (i, name) in ["Alice", "Bob", "Carol"].enumerated() {
            var item = SQLiteStoredItem()
            item.id = "where-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = Int64(20 + i * 10)  // 20, 30, 40
            try context.insert(item)
        }
        try await context.save()

        // Verify data exists first
        let all = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(all.count == 3)

        // Test where clause with predicate evaluation
        let results = try await context.fetch(SQLiteStoredItem.self)
            .where(SQLiteStoredItem.fields.age > 25)
            .execute()
        #expect(results.count == 2)
    }

    @Test("Fetch with orderBy")
    func fetchWithOrderBy() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()

        for (i, name) in ["Charlie", "Alice", "Bob"].enumerated() {
            var item = SQLiteStoredItem()
            item.id = "order-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = name
            item.age = [Int64(30), 10, 20][i]
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(SQLiteStoredItem.self)
            .orderBy(SQLiteStoredItem.fields.age)
            .execute()
        #expect(results.count == 3)
        #expect(results[0].name == "Alice")
        #expect(results[1].name == "Bob")
        #expect(results[2].name == "Charlie")
    }

    @Test("Fetch with limit")
    func fetchWithLimit() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()

        for i in 0..<5 {
            var item = SQLiteStoredItem()
            item.id = "limit-\(i)-\(UUID().uuidString.prefix(8))"
            item.name = "User\(i)"
            item.age = Int64(20 + i)
            try context.insert(item)
        }
        try await context.save()

        let results = try await context.fetch(SQLiteStoredItem.self)
            .limit(2)
            .execute()
        #expect(results.count == 2)
    }

    // MARK: - Edge Cases

    @Test("Fetch from empty store returns empty array")
    func emptyFetch() async throws {
        let schema = try Schema(entities: [try SQLiteStoredItem.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SQLiteStoredItem.self)]
            ),
            security: .testingDisabled
        )

        let context = container.testBaseContext()
        let results = try await context.fetch(SQLiteStoredItem.self).execute()
        #expect(results.isEmpty)
    }
}
#endif
