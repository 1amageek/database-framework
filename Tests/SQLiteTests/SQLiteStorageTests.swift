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

        let projection = "SELECT \"exactAmount\", \"lexicalDocument\", \"binaryDocument\", \"durationValue\", \"integerValues\" FROM \"SQLiteStoredRichItem\""
        let selected = try await context.executeSQL(projection)
        #expect(selected.rows.count == 1)
        #expect(selected.rows[0].values == expected)
        let types: [QueryColumnType] = [.sql(.decimal(precision: nil, scale: nil)), .sql(.json),
            .sql(.jsonb), .sql(.interval()), .sql(.array(.integer))]
        #expect(selected.columns.map(\.type) == types)
        #expect(selected.columns.allSatisfy { !$0.isNullable })
        let empty = try await context.executeSQL(projection + " WHERE FALSE")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns == selected.columns)

        let names = ["exactAmount", "lexicalDocument", "binaryDocument", "durationValue", "integerValues"]
        let literalQuery = SelectQuery(projection: expected.indices.map { index in
            ProjectionItem(.literal(.queryValue(expected[index], type: types[index])), alias: names[index])
        },
            source: .values([[]]))
        let literals = try await context.query(literalQuery)
        #expect(literals.rows.count == 1)
        #expect(literals.rows[0].values == expected)
        #expect(literals.columns == selected.columns)

        let literalBuilder = SQLQueryBuilder(source: literalQuery.source).select(
            ProjectionItem(.literal(.queryValue(expected[0], type: types[0])), alias: names[0]),
            ProjectionItem(.literal(.queryValue(expected[1], type: types[1])), alias: names[1]),
            ProjectionItem(.literal(.queryValue(expected[2], type: types[2])), alias: names[2]),
            ProjectionItem(.literal(.queryValue(expected[3], type: types[3])), alias: names[3]),
            ProjectionItem(.literal(.queryValue(expected[4], type: types[4])), alias: names[4])
        )
        let emitted = try await context.executeSQL(literalBuilder.toSQL())
        #expect(emitted.columns == literals.columns)
        #expect(emitted.rows.map(\.values) == literals.rows.map(\.values))

        var parameters = expected.indices.map { index in
            QueryParameter(position: UInt32(index + 1), name: names[index],
                           value: expected[index], type: types[index])
        }
        parameters.append(QueryParameter(position: 6, name: "absent", value: .primitive(.null), type: .sql(.json)))
        let parameterSQL = "SELECT :exactAmount AS \"exactAmount\", :lexicalDocument AS \"lexicalDocument\", :binaryDocument AS \"binaryDocument\", :durationValue AS \"durationValue\", :integerValues AS \"integerValues\", :absent AS absent"
        let bound = try await context.executeSQL(parameterSQL, parameters: parameters)
        #expect(bound.rows.count == 1)
        #expect(bound.rows[0].values == expected + [.primitive(.null)])
        #expect(bound.columns.map(\.type) == types + [.sql(.json)])
        #expect(bound.columns[5].isNullable)
        let emptyBound = try await context.executeSQL(parameterSQL + " WHERE FALSE", parameters: parameters)
        #expect(emptyBound.rows.isEmpty)
        #expect(emptyBound.columns == bound.columns)

        let numericSQL = "SELECT \"exactAmount\" + \"exactAmount\", ROUND(\"exactAmount\", 1), CASE \"exactAmount\" WHEN 4.56 THEN \"exactAmount\" ELSE NULL END, 7 = ANY(\"integerValues\") FROM \"SQLiteStoredRichItem\""
        let computed = try await context.executeSQL(numericSQL)
        #expect(computed.rows[0].values == [
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 912, scale: 2), displayScale: 8)),
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 46, scale: 1), displayScale: 1)),
            .numeric(updated.exactAmount), .primitive(.bool(true))])
        let emptyComputed = try await context.executeSQL(numericSQL + " WHERE FALSE")
        #expect(emptyComputed.rows.isEmpty)
        #expect(emptyComputed.columns == computed.columns)
        let aggregates = try await context.executeSQL(
            "SELECT SUM(\"exactAmount\"), AVG(\"exactAmount\"), MIN(\"exactAmount\"), MAX(\"exactAmount\") FROM \"SQLiteStoredRichItem\"")
        #expect(aggregates.rows[0].values == [
            .numeric(updated.exactAmount),
            .numeric(try QueryNumericValue(value: updated.exactAmount.value, displayScale: 16)),
            .numeric(updated.exactAmount), .numeric(updated.exactAmount)])
        let collections = try await context.executeSQL("""
            SELECT "integerValues"[3], array_lower("integerValues",1), cardinality("integerValues"),
                "lexicalDocument" ->> 'b', "binaryDocument" ->> 'b',
                "binaryDocument" @> '{"b":4}'::jsonb,
                array_to_string("integerValues",',')
            FROM "SQLiteStoredRichItem"
            """)
        #expect(collections.columns.map(\.type) == [.sql(.integer), .sql(.integer), .sql(.integer),
            .sql(.text), .sql(.text), .sql(.boolean), .sql(.text)])
        #expect(collections.rows.map(\.values) == [[.primitive(.int32(7)), .primitive(.int32(3)),
            .primitive(.int32(2)), .primitive(.string("4")), .primitive(.string("4")),
            .primitive(.bool(true)), .primitive(.string("7,9"))]])
    }

    @Test("SQL syntax preserves exact names and rejects pending execution capabilities", .timeLimit(.minutes(2)))
    func syntaxCarriagePreservesPublicExecutionContract() async throws {
        let container = try await makeItemContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(SQLiteStoredItem(id: "syntax", name: "quoted", age: 7))
        try await context.save()
        let builder = SQLQueryBuilder(from: "SQLiteStoredItem")
            .select("id", "name").where("age", .greaterThanOrEqual, .int(7))
        let direct = try await context.query(builder.buildAST())
        let emitted = try await context.executeSQL(builder.toSQL())
        #expect(emitted.columns == direct.columns)
        #expect(emitted.rows.map(\.values) == direct.rows.map(\.values))
        #expect(emitted.rows.count == 1)
        do {
            _ = try await context.executeSQL("SELECT id FROM SQLiteStoredItem")
            Issue.record("Expected the folded SQL source name to reject binding")
        } catch let failure as SQLBindingSourceFailure {
            guard case .read(.unsupportedSource("Entity 'sqlitestoreditem' not found in schema")) = failure.cause else {
                Issue.record("Expected the exact missing SQL source rejection")
                return
            }
            #expect(failure.position == 15)
            #expect(failure.occurrence != nil)
        }
        let composite = try await context.executeSQL("SELECT ROW(1, NULL) AS value")
        try #require(composite.rows.count == 1)
        guard case .composite(let record) = composite.rows[0].values[0] else {
            Issue.record("Expected the selected SQL row constructor")
            return
        }
        #expect(composite.columns.map(\.type) == [.sql(.composite(record.type))])
        #expect(composite.columns.map(\.isNullable) == [false])
        #expect(record.type.columns.map(\.name) == ["f1", "f2"])
        #expect(record.type.columns.map(\.type) == [.sql(.integer), .sql(.text)])
        #expect(record.type.columns.map(\.isNullable) == [false, true])
        #expect(Array(record.fields) == [.primitive(.int32(1)), .primitive(.null)])
        let emptyComposite = try await context.executeSQL("SELECT ROW(1, NULL) AS value WHERE FALSE")
        #expect(emptyComposite.rows.isEmpty)
        #expect(emptyComposite.columns == composite.columns)
        let array = try await context.executeSQL("SELECT ARRAY[1,NULL] AS values")
        #expect(array.columns.map(\.type) == [.sql(.array(.integer))])
        guard case .array(let arrayValue) = array.rows[0].values[0] else {
            Issue.record("Expected the selected SQL array constructor")
            return
        }
        #expect(arrayValue.elements.elementsEqual([QueryValue.primitive(.int32(1)), .primitive(.null)]))
        #expect(arrayValue.dimensions.elementsEqual([try QueryArrayDimension(lowerBound: 1, length: 2)]))
        let emptyArray = try await context.executeSQL("SELECT ARRAY[1,NULL] AS values WHERE FALSE")
        #expect(emptyArray.rows.isEmpty)
        #expect(emptyArray.columns == array.columns)
        let temporal = try await context.executeSQL("SELECT TIMESTAMP(3) '2026-10-15 10:20:30', CAST(TIMESTAMP(3) '2026-10-15 10:20:30' AS text)")
        let expectedTemporal = CivilDateTime(date: try CivilDate(year: 2026, month: 10, day: 15),
            time: try CivilTime(hour: 10, minute: 20, second: 30))
        #expect(temporal.columns.map(\.type) == [.sql(.timestamp(withTimeZone: false, precision: 3)), .sql(.text)])
        #expect(temporal.rows[0].values == [.primitive(.dateTime(expectedTemporal)), .primitive(.string("2026-10-15 10:20:30"))])
        for type: DataType in [
            .time(withTimeZone: false, precision: 0),
            .time(withTimeZone: true, precision: 3),
            .timestamp(withTimeZone: false, precision: 3),
            .timestamp(withTimeZone: true, precision: 6),
        ] {
            let nullBuilder = SQLQueryBuilder(source: .values([[]]))
                .select(ProjectionItem(.cast(.null, targetType: type), alias: "clock"))
            let directNull = try await context.query(nullBuilder.buildAST())
            let emittedNull = try await context.executeSQL(nullBuilder.toSQL())
            #expect(directNull.columns == emittedNull.columns)
            #expect(directNull.rows.map(\.values) == emittedNull.rows.map(\.values))
            #expect(emittedNull.columns.map(\.type) == [.sql(type)])
            #expect(emittedNull.rows.map(\.values) == [[.primitive(.null)]])
            #expect(emittedNull.columns[0].isNullable)
        }
        let typedNull = try await context.executeSQL("SELECT NULL::TIMESTAMP(3)")
        #expect(typedNull.columns.map(\.type) == [.sql(.timestamp(withTimeZone: false, precision: 3))])
        #expect(typedNull.rows.map(\.values) == [[.primitive(.null)]])
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
