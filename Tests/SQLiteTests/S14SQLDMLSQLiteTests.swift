#if SQLITE
import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S14DMLSource {
    #Directory<S14DMLSource>("s14-dml-source")
    var id: String = ""
    var value: String = ""
}

@Persistable
private struct S14DMLDestination {
    #Directory<S14DMLDestination>("s14-dml-destination")
    #Index(.ordered(name: "s14_destination_value", keys: [.ascending(\S14DMLDestination.value)], unique: false))
    var id: String = ""
    var value: String = ""
}

@Suite("S14 file-backed SQLite DML", .timeLimit(.minutes(1)), .heartbeat)
struct S14SQLDMLSQLiteTests {
    @Test("Writable DELETE RETURNING moves rows and ordinary identity rekey preserves indexes after reopen")
    func writableMoveAndIndexedRelocationPersist() async throws {
        let database = try SQLiteTestDatabase(prefix: "s14-dml-move")
        defer { database.remove() }
        let first = try await openCompiled(database)
        defer { await first.shutdown() }
        let context = first.testBaseContext()
        try context.insert(S14DMLSource(id: "old", value: "before"))
        try await context.save()
        let move = QueryStatement.with(SQLWithStatement(statements: [SQLNamedStatement(name: "moved",
            statement: .delete(DeleteQuery(target: TableRef(S14DMLSource.persistableType),
                returning: [.column("id"), .column("value")])))] ,
            body: .insert(InsertQuery(target: TableRef(S14DMLDestination.persistableType),
                columns: ["id", "value"], source: .select(SelectQuery(projection: [.column("id"), .column("value")],
                    source: .table(TableRef("moved"))))))))
        _ = try await execute(first, [.statement(move)])
        let initialIndex = try await indexed(first, value: "before")
        #expect(initialIndex.rows.map(\.values) == [[.primitive(.string("old"))]])
        _ = try await execute(first, [.statement(.update(UpdateQuery(target: TableRef(S14DMLDestination.persistableType),
            assignments: [Assignment(column: "id", value: .string("new")), Assignment(column: "value", value: .string("after"))])))])
        #expect(try await indexed(first, value: "before").rows.isEmpty)
        #expect(try await indexed(first, value: "after").rows.map(\.values) == [[.primitive(.string("new"))]])
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        let restoredContext = restored.testBaseContext()
        let source = try await restoredContext.query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef(S14DMLSource.persistableType))))
        #expect(source.rows.isEmpty)
        #expect(try await indexed(restored, value: "before").rows.isEmpty)
        #expect(try await indexed(restored, value: "after").rows.map(\.values) == [[.primitive(.string("new"))]])
        try await restoredContext.withTransaction { transaction in
            let old = try await transaction.loadPersistedModel(entity: S14DMLDestination.persistableType,
                id: Tuple("old"), partition: nil)
            let new = try await transaction.loadPersistedModel(entity: S14DMLDestination.persistableType,
                id: Tuple("new"), partition: nil)
            #expect(old == nil)
            #expect(new?.value(forFieldNamed: "value") == .string("after"))
        }
    }

    @Test("Hidden row IDs and owned identity TRUNCATE rollback and CONTINUE survive file-backed reopen")
    func hiddenIdentityAndTruncatePersist() async throws {
        let database = try SQLiteTestDatabase(prefix: "s14-dml-identity")
        defer { database.remove() }
        let first = try await openHidden(database)
        defer { await first.shutdown() }
        let inserted = try await execute(first, [.statement(.insert(InsertQuery(target: TableRef("serial_rows"),
            columns: ["label"], source: .values([[.string("same")], [.string("same")]]), returning: [.column("serial")])) )])
        #expect(try values(inserted, at: 0) == [[.primitive(.int64(10))], [.primitive(.int64(11))]])
        let hidden = try await first.testBaseContext().query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S14HiddenRow"))))
        #expect(hidden.rows.count == 2)
        #expect(hidden.rows[0].values != hidden.rows[1].values)
        let rollback = try await execute(first, [.savepoint("before"),
            .statement(.truncate(TruncateQuery(tables: [TableRef("serial_rows")], identity: .restart))),
            .statement(.insert(InsertQuery(target: TableRef("serial_rows"), columns: ["label"],
                source: .values([[.string("transient")]]), returning: [.column("serial")]))),
            .rollbackTo("before"), next()])
        #expect(try values(rollback, at: 2) == [[.primitive(.int64(10))]])
        #expect(try values(rollback, at: 4) == [[.primitive(.int64(12))]])
        let restoredRows = try await first.testBaseContext().executeSQL("SELECT serial, label FROM serial_rows ORDER BY serial")
        #expect(restoredRows.rows.map(\.values) == [[.primitive(.int64(10)), .primitive(.string("same"))],
            [.primitive(.int64(11)), .primitive(.string("same"))]])
        let continued = try await execute(first, [
            .statement(.truncate(TruncateQuery(tables: [TableRef("serial_rows")]))),
            .statement(.insert(InsertQuery(target: TableRef("serial_rows"), columns: ["label"],
                source: .values([[.string("survivor")]]), returning: [.column("serial")])) )])
        #expect(try values(continued, at: 1) == [[.primitive(.int64(13))]])
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        let persisted = try await restored.testBaseContext().executeSQL("SELECT serial, label FROM serial_rows")
        #expect(persisted.rows.map(\.values) == [[.primitive(.int64(13)), .primitive(.string("survivor"))]])
        #expect(try values(try await execute(restored, [next()]), at: 0) == [[.primitive(.int64(14))]])
        let physical = try await restored.testBaseContext().query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S14HiddenRow"))))
        #expect(physical.rows.count == 1)
    }

    private func openCompiled(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let schema = try Schema(entities: [S14DMLSource.schemaEntity, S14DMLDestination.schemaEntity])
        return try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s14-sqlite-dml", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S14DMLSource.self),
                    DatabaseFrameworkRuntime.entity(S14DMLDestination.self)]), security: .testingDisabled)
    }

    private func openHidden(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let entity = try Schema.Entity(name: "S14HiddenRow", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "serial", fieldNumber: 2, type: .int64),
                FieldSchema(name: "label", fieldNumber: 3, type: .string)], directoryComponents: [.staticPath("s14-hidden")])
        let schema = try Schema(entities: [entity])
        let container = try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s14-sqlite-dml", revision: 1), schema: schema), security: .testingDisabled)
        let sequence = try SQLSequence(id: "serial_counter", namespaceID: "public", name: "serial_counter",
            start: 10, increment: 1, minimum: 1, maximum: 1_000,
            ownedBy: SQLSequenceOwner(relationID: "serial_rows", columnID: "serial"))
        let relation = try SQLRelation(id: "serial_rows", namespaceID: "public", name: "serial_rows", entityName: entity.name,
            columns: [SQLColumn(id: "serial", name: "serial", backing: .field(number: 2),
                policy: .identity(SQLIdentityColumn(sequenceID: sequence.id, generation: .byDefault))),
                SQLColumn(id: "label", name: "label", fieldNumber: 3)])
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")],
            relations: [relation], sequences: [sequence]), expectedGeneration: container.schemaGeneration,
            idempotencyKey: "s14-hidden", authorization: TestBaseEnvironment.authorization)
        return container
    }

    private func restore(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s14-sqlite-dml", revision: 1), schema: schema)
        }
    }

    private func indexed(_ container: DBContainer, value: String) async throws -> QueryResponse {
        try await container.testBaseContext().query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef(S14DMLDestination.persistableType)),
            accessPath: .index(IndexScanSource(indexName: "s14_destination_value", indexType: .ordered)),
            filter: .equal(.col("value"), .string(value))))
    }

    private func next() -> SQLBlockStep {
        .statement(.select(SelectQuery(projection: [.expression(.function(FunctionCall(name: "nextval",
            arguments: [.string("serial_counter")])))], source: .values([[]]))))
    }

    private func execute(_ container: DBContainer, _ steps: [SQLBlockStep]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps),
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000,
                maximumDepth: 16, maximumRetainedBytes: 4 * 1_024 * 1_024,
                maximumWorkUnits: 1_000_000, maximumReadPageRows: 128)))
    }

    private func values(_ result: SQLBlockResult, at index: Int) throws -> [[QueryValue]] {
        guard case .rows(let response) = result.results[index] else { throw SQLSequenceExecutionError.invalidPersistedState }
        return response.rows.map(\.values)
    }
}
#endif
