#if SQLITE
@_spi(DatabaseExecution) import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S11RestoredCompiledItem {
    #Directory<S11RestoredCompiledItem>("s11-restored-compiled")
    var id: String = ""
    var value: String = ""
}

@Suite("S11 SQLite SQL catalog restoration", .serialized, .heartbeat)
struct S11SQLCatalogRestorationTests {
    @Test("Mixed backing, stable logical IDs, composite key and hidden row identity survive close and reopen")
    func mixedCatalogRestoresAfterRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "s11-mixed-catalog")
        defer { database.remove() }
        let first = try await openInitial(database)
        let catalog: SQLCatalog
        let publication: DatabaseSchemaPublicationResult
        let originalFingerprint = first.schemaFingerprint
        do {
            try await saveRows(in: first)
            catalog = try renamedCatalog(first.sqlCatalog)
            publication = try await first.withSchemaLease { lease in
                let oldExecution = lease.executionRuntimeFingerprint
                let oldIndexes = lease.indexPhysicalFingerprint
                let result = try await first.publishSQLCatalog(catalog, expectedGeneration: lease.generation,
                    idempotencyKey: "rename-before-restart", authorization: TestBaseEnvironment.authorization)
                #expect(result.executionRuntimeFingerprint != oldExecution)
                #expect(result.indexPhysicalFingerprint == oldIndexes)
                let old = try await first.testBaseContext().executeSQL("SELECT value FROM \"S11RestoredCompiledItem\"")
                #expect(old.rows.map(\.values) == [[.primitive(.string("compiled-value"))]])
                return result
            }
            let before = try await first.testBaseContext().executeSQL("SELECT * FROM reports.entries ORDER BY area")
            #expect(before.columns.map(\.name) == ["area", "label"])
            #expect(before.rows.map(\.values) == compositeVisibleRows)
            await first.shutdown()
        } catch {
            await first.shutdown()
            throw error
        }

        let reopened = try await restore(database)
        defer { await reopened.shutdown() }
        #expect(reopened.sqlCatalog == catalog)
        #expect(reopened.schemaGeneration == publication.generation)
        #expect(reopened.schemaFingerprint == originalFingerprint)
        let dynamic = try #require(reopened.sqlCatalog.relations.first { $0.entityName == "S11RestoredDynamic" })
        #expect(dynamic.id == "entity:S11RestoredDynamic")
        #expect(dynamic.columns.map(\.id) == ["field:3", "field:2"])
        #expect(dynamic.columns.map(\.fieldNumber) == [3, 2])
        #expect(dynamic.primaryKeyColumnIDs == ["field:3", "field:2"])
        let context = reopened.testBaseContext()
        let rows = try await context.executeSQL("SELECT * FROM reports.entries ORDER BY area")
        #expect(rows.columns.map(\.name) == ["area", "label"])
        #expect(rows.rows.map(\.values) == compositeVisibleRows)
        let labels = try await context.executeSQL("SELECT label FROM reports.entries ORDER BY area")
        #expect(labels.rows.map(\.values) == [[.primitive(.string("same"))], [.primitive(.string("same"))]])
        let distinct = try await context.executeSQL("SELECT DISTINCT label FROM reports.entries")
        #expect(distinct.rows.map(\.values) == [[.primitive(.string("same"))]])
        let compiled = try await context.executeSQL("SELECT content FROM reports.documents")
        #expect(compiled.rows.map(\.values) == [[.primitive(.string("compiled-value"))]])
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await context.executeSQL("SELECT id FROM reports.entries")
        }
        let physical = try await context.query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S11RestoredDynamic")), orderBy: [SortKey(.column(ColumnRef("id")))]))
        #expect(physical.rows.map(\.values) == [[.primitive(.string("first"))], [.primitive(.string("second"))]])
        let third = try await context.executeSQL("SELECT content FROM reports.documents")
        #expect(third.rows == compiled.rows)
    }

    @Test("Persisted compiled protection rejects removal, remapping and key changes after schema-driven restore")
    func compiledProtectionSurvivesDynamicRestore() async throws {
        let database = try SQLiteTestDatabase(prefix: "s11-static-protection")
        defer { database.remove() }
        let first = try await openInitial(database)
        do {
            try await saveRows(in: first)
            _ = try await first.publishSQLCatalog(renamedCatalog(first.sqlCatalog), expectedGeneration: first.schemaGeneration,
                idempotencyKey: "protected-rename", authorization: TestBaseEnvironment.authorization)
            await first.shutdown()
        } catch {
            await first.shutdown()
            throw error
        }
        let reopened = try await restore(database)
        defer { await reopened.shutdown() }
        let catalog = reopened.sqlCatalog
        let generation = reopened.schemaGeneration
        let protected = try #require(catalog.relations.first { $0.entityName == S11RestoredCompiledItem.persistableType })
        let dynamic = try #require(catalog.relations.first { $0.entityName == "S11RestoredDynamic" })
        let removed = try SQLCatalog(namespaces: catalog.namespaces, relations: [dynamic])
        await #expect(throws: SQLCatalogBindingError.protectedRelationRemoved(protected.id)) {
            try await reopened.publishSQLCatalog(removed, expectedGeneration: generation,
                idempotencyKey: "remove-protected", authorization: TestBaseEnvironment.authorization)
        }
        let firstColumn = try #require(protected.columns.first)
        let secondColumn = try #require(protected.columns.last)
        let retargeted = try SQLRelation(id: protected.id, namespaceID: protected.namespaceID, name: protected.name,
            entityName: protected.entityName, columns: [
                SQLColumn(id: firstColumn.id, name: firstColumn.name, backing: secondColumn.backing, policy: secondColumn.policy),
                SQLColumn(id: secondColumn.id, name: secondColumn.name, backing: firstColumn.backing, policy: firstColumn.policy)],
            primaryKeyColumnIDs: protected.primaryKeyColumnIDs,
            primaryKeyConstraint: protected.primaryKeyConstraint, constraints: protected.constraints)
        await #expect(throws: SQLCatalogBindingError.columnRetargeted(relation: protected.id, column: firstColumn.id)) {
            try await reopened.publishSQLCatalog(SQLCatalog(namespaces: catalog.namespaces, relations: [retargeted, dynamic]),
                expectedGeneration: generation, idempotencyKey: "retarget-protected", authorization: TestBaseEnvironment.authorization)
        }
        let noKey = try SQLRelation(id: protected.id, namespaceID: protected.namespaceID, name: protected.name,
            entityName: protected.entityName, columns: protected.columns, primaryKeyColumnIDs: nil)
        await #expect(throws: SQLCatalogBindingError.protectedPrimaryKeyChanged(protected.id)) {
            try await reopened.publishSQLCatalog(SQLCatalog(namespaces: catalog.namespaces, relations: [noKey, dynamic]),
                expectedGeneration: generation, idempotencyKey: "change-protected-key", authorization: TestBaseEnvironment.authorization)
        }
        #expect(reopened.sqlCatalog == catalog)
        #expect(reopened.schemaGeneration == generation)
        let response = try await reopened.testBaseContext().executeSQL("SELECT content FROM reports.documents")
        #expect(response.rows.map(\.values) == [[.primitive(.string("compiled-value"))]])
    }

    private var compositeVisibleRows: [[QueryValue]] {
        [[.primitive(.string("east")), .primitive(.string("same"))],
            [.primitive(.string("west")), .primitive(.string("same"))]]
    }

    private func openInitial(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let dynamic = try Schema.Entity(name: "S11RestoredDynamic", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "code", fieldNumber: 2, type: .string),
            FieldSchema(name: "region", fieldNumber: 3, type: .string)], directoryComponents: [.staticPath("s11-restored-dynamic")])
        let schema = try Schema(entities: [S11RestoredCompiledItem.schemaEntity, dynamic])
        return try await DBContainer.open(for: schema, configuration: .file(database.path),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s11-sqlite-catalog", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S11RestoredCompiledItem.self),
                    EntityRuntimeDefinition(schemaDriven: dynamic).registration()]), security: .testingDisabled)
    }

    private func restore(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
                try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s11-sqlite-catalog", revision: 1),
                    schema: schema)
            }
    }

    private func renamedCatalog(_ original: SQLCatalog) throws -> SQLCatalog {
        let relations = try original.relations.map { relation in
            if relation.entityName == S11RestoredCompiledItem.persistableType {
                return try SQLRelation(id: relation.id, namespaceID: "reports", name: "documents", entityName: relation.entityName,
                    columns: relation.columns.map { column in
                        try SQLColumn(id: column.id, name: column.name == "value" ? "content" : column.name,
                            backing: column.backing, policy: column.policy)
                    }, primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
                    primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)
            }
            return try SQLRelation(id: relation.id, namespaceID: "reports", name: "entries", entityName: relation.entityName,
                columns: [SQLColumn(id: "field:3", name: "area", fieldNumber: 3),
                    SQLColumn(id: "field:2", name: "label", fieldNumber: 2)], primaryKeyColumnIDs: ["field:3", "field:2"])
        }
        return try SQLCatalog(namespaces: [SQLNamespace(id: "reports", name: "reports")], relations: relations)
    }

    private func saveRows(in container: DBContainer) async throws {
        let context = container.testBaseContext()
        try context.insert(S11RestoredCompiledItem(id: "compiled", value: "compiled-value"))
        try await context.save()
        let rows = try ["first", "second"].map { id in
            try PersistedModel(entity: "S11RestoredDynamic", fields: [
                PersistableField(number: 1, name: "id", value: .string(id)),
                PersistableField(number: 2, name: "code", value: .string("same")),
                PersistableField(number: 3, name: "region", value: .string(id == "first" ? "east" : "west"))])
        }
        try await context.withTransaction(configuration: .batch) { transaction in
            for row in rows { try await transaction.savePersistedModel(row, precondition: .notExists) }
        }
    }
}
#endif
