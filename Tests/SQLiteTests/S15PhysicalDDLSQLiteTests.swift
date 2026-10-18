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

@Suite("S15 file-backed physical DDL", .timeLimit(.minutes(1)), .heartbeat)
struct S15PhysicalDDLSQLiteTests {
    @Test("TYPE rewrite failure rolls back and the rebuilt SQL index survives a fresh file-backed engine")
    func physicalRewriteAndIndexPersist() async throws {
        let database = try SQLiteTestDatabase(prefix: "s15-physical-ddl")
        defer { database.remove() }
        let first = try await open(database)
        defer { await close(first) }
        _ = try await run(first, [
            .ddl(.createTable(SQLCreateTable(name: TableRef("items"), source: .columns([
                SQLColumnDefinition(name: "value", type: .text)], constraints: [])))),
            insert([.string("2"), .string("bad")]),
            .ddl(.createIndex(SQLCreateIndex(name: TableRef("items_value"), table: TableRef("items"),
                keys: [SortKey(.col("value"))], unique: true)))])
        let before = first.acquirePublishedSchemaLease()
        let relation = try #require(before.sqlCatalog.catalog.relations.first)
        let column = try #require(relation.columns.first)
        let index = try #require(before.sqlCatalog.catalog.indexes.first)
        let originalEntity = try #require(before.schema.entities.first)
        let textRows: [[QueryValue]] = [[.primitive(.string("2"))], [.primitive(.string("bad"))]]
        #expect(try await rows(first).rows.map(\.values) == textRows)

        // The first statement rewrites rows and stages a generation before the later cast fails.
        do {
            _ = try await run(first, [
                .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                    .addColumn(SQLColumnDefinition(name: "uncommitted", type: .integer,
                        policy: .input(defaultExpression: .int(7))), ifNotExists: false)]))),
                alterType()])
            Issue.record("Expected invalid text to reject the complete DDL block")
        } catch CanonicalReadError.expressionEvaluation(.invalidCast(_), _) {}
        let rolledBack = first.acquirePublishedSchemaLease()
        #expect(rolledBack.schema == before.schema)
        #expect(rolledBack.sqlCatalog.catalog == before.sqlCatalog.catalog)
        #expect(rolledBack.generation == before.generation)
        #expect(rolledBack.indexPhysicalFingerprint == before.indexPhysicalFingerprint)
        #expect(rolledBack.executionRuntimeFingerprint == before.executionRuntimeFingerprint)
        #expect(try await rows(first).rows.map(\.values) == textRows)
        await #expect {
            _ = try await run(first, [insert([.string("2")])])
        } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .unique && actualRelation == relation.id && actualConstraint == index.name
        }
        #expect(try await rows(first).rows.map(\.values) == textRows)

        _ = try await run(first, [
            .update(UpdateQuery(target: TableRef("items"),
                assignments: [Assignment(column: "value", value: .string("5"))],
                filter: .equal(.col("value"), .string("bad")))),
            alterType(),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .addColumn(SQLColumnDefinition(name: "added", type: .integer,
                    policy: .input(defaultExpression: .int(9))), ifNotExists: false)])))])
        let changed = first.acquirePublishedSchemaLease()
        let changedRelation = try #require(changed.sqlCatalog.catalog.relations.first)
        let changedIndex = try #require(changed.sqlCatalog.catalog.indexes.first)
        #expect(changedRelation.id == relation.id)
        #expect(changedRelation.entityName == relation.entityName)
        #expect(changedRelation.columns.first?.id == column.id)
        #expect(changedRelation.columns.first?.fieldNumber == column.fieldNumber)
        #expect(changed.schema.entities.first?.directoryComponents == originalEntity.directoryComponents)
        #expect(changedIndex.id == index.id)
        #expect(changedIndex.bindings.first?.id == column.id)
        #expect(changedIndex.bindings.first?.type == .sql(.integer))
        #expect(changed.indexPhysicalFingerprint != before.indexPhysicalFingerprint)
        let integerRows: [[QueryValue]] = [
            [.primitive(.int32(2)), .primitive(.int32(9))],
            [.primitive(.int32(5)), .primitive(.int32(9))]]
        let rewritten = try await rows(first)
        #expect(rewritten.columns.map(\.type) == [.sql(.integer), .sql(.integer)])
        #expect(rewritten.rows.map(\.values) == integerRows)
        await close(first)

        let applicationOpen = try await open(database)
        defer { await close(applicationOpen) }
        let applicationLease = applicationOpen.acquirePublishedSchemaLease()
        #expect(applicationLease.schema == changed.schema)
        #expect(applicationLease.sqlCatalog.catalog == changed.sqlCatalog.catalog)
        #expect(applicationLease.generation == changed.generation)
        #expect(applicationLease.indexPhysicalFingerprint == changed.indexPhysicalFingerprint)
        #expect(applicationLease.executionRuntimeFingerprint == changed.executionRuntimeFingerprint)
        #expect(try await rows(applicationOpen).rows.map(\.values) == integerRows)
        await close(applicationOpen)

        let restored = try await reopen(database)
        defer { await close(restored) }
        let reopened = restored.acquirePublishedSchemaLease()
        #expect(reopened.schema == changed.schema)
        #expect(reopened.sqlCatalog.catalog == changed.sqlCatalog.catalog)
        #expect(reopened.generation == changed.generation)
        #expect(reopened.fingerprint == changed.fingerprint)
        #expect(reopened.indexPhysicalFingerprint == changed.indexPhysicalFingerprint)
        #expect(reopened.executionRuntimeFingerprint == changed.executionRuntimeFingerprint)
        #expect(reopened.indexPhysicalLayouts == changed.indexPhysicalLayouts)
        let persisted = try await rows(restored)
        #expect(persisted.columns.map(\.type) == [.sql(.integer), .sql(.integer)])
        #expect(persisted.rows.map(\.values) == integerRows)
        await #expect {
            _ = try await run(restored, [insert([.int(2)])])
        } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .unique && actualRelation == relation.id && actualConstraint == index.name
        }
        #expect(try await rows(restored).rows.map(\.values) == integerRows)
        _ = try await run(restored, [insert([.int(8)])])
        #expect(try await rows(restored).rows.map(\.values) == integerRows + [
            [.primitive(.int32(8)), .primitive(.int32(9))]])
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s15-sqlite-physical-ddl", revision: 1), schema: schema),
            security: .testingDisabled)
    }

    private func reopen(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s15-sqlite-physical-ddl", revision: 1), schema: schema)
        }
    }

    private func close(_ container: DBContainer) async {
        // Container shutdown awaits the SQLite coordinator and connection close before file removal.
        await container.shutdown()
        #expect(throws: DatabaseContainerLifecycleError.shutdown) {
            _ = try container.engine.createTransaction()
        }
    }

    private func alterType() -> QueryStatement {
        .ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
            .alterColumnType(name: "value", type: .integer,
                using: .cast(.col("value"), targetType: .integer))])))
    }

    private func insert(_ values: [Expression]) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), columns: ["value"], source: .values(values.map { [$0] })))
    }

    private func run(_ container: DBContainer, _ statements: [QueryStatement]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: statements.map { .statement($0) }), options: SQLBlockExecutionOptions(
                checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000,
                    maximumDepth: 16, maximumRetainedBytes: 16_777_216,
                    maximumWorkUnits: 1_000_000, maximumReadPageRows: 128)))
    }

    private func rows(_ container: DBContainer) async throws -> QueryResponse {
        let result = try await run(container, [.select(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef("items")), orderBy: [SortKey(.col("value"))]))])
        guard case .rows(let response) = result.results[0] else { throw SQLBlockExecutionError.unsupportedStatement }
        return response
    }
}
#endif
