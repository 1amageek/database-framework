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
private struct S12RestartItem {
    #Directory<S12RestartItem>("s12-restart-items")
    var id: String = ""
    var value: String = ""
}

private enum S12RestartFailure: Error, Equatable {
    case rejected
}

@Suite("S12 SQLite catalog staging restart", .timeLimit(.minutes(1)), .heartbeat)
struct S12SchemaStagingSQLiteTests {
    @Test("SC01/SC02/SC03 surviving candidate reads rows in the request and survives file-backed restart")
    func survivingCandidateRestarts() async throws {
        let database = try SQLiteTestDatabase(prefix: "s12-surviving-catalog")
        defer { database.remove() }
        let first = try await open(database)
        defer { await first.shutdown() }
        let context = first.testBaseContext()
        try await context.withTransaction { transaction in
            try await transaction.save(S12RestartItem(id: "one", value: "retained"), precondition: .notExists)
        }
        let old = first.acquirePublishedSchemaLease()
        let survivor = try renamed(old.sqlCatalog.catalog, to: "surviving_items")
        let discarded = try renamed(survivor, to: "discarded_items")
        let publication = try await context.withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
            let publication = try await transaction.stageSQLCatalog(survivor,
                expectedGeneration: old.generation, idempotencyKey: "survivor")
            let marker = try await transaction.createCheckpoint()
            _ = try await transaction.stageSQLCatalog(discarded,
                expectedGeneration: publication.generation, idempotencyKey: "discarded")
            try await transaction.rollback(to: marker)
            try await transaction.withStatementScope {
                let rows = try await context.executeSQL("SELECT value FROM surviving_items")
                #expect(rows.rows.map(\.values) == [[.primitive(.string("retained"))]])
            }
            #expect(first.acquirePublishedSchemaLease().sqlCatalog.catalog == old.sqlCatalog.catalog)
            return publication
        }
        #expect(first.sqlCatalog == survivor)
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        #expect(restored.sqlCatalog == survivor)
        #expect(restored.schemaGeneration == publication.generation)
        let rows = try await restored.testBaseContext().executeSQL("SELECT value FROM surviving_items")
        #expect(rows.rows.map(\.values) == [[.primitive(.string("retained"))]])
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await restored.testBaseContext().executeSQL("SELECT value FROM discarded_items")
        }
    }

    @Test("SC03 whole-request failure leaves the original catalog after file-backed restart")
    func rolledBackCandidateDoesNotRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "s12-rolledback-catalog")
        defer { database.remove() }
        let first = try await open(database)
        defer { await first.shutdown() }
        let old = first.acquirePublishedSchemaLease()
        let candidate = try renamed(old.sqlCatalog.catalog, to: "uncommitted_items")
        await #expect(throws: S12RestartFailure.rejected) {
            try await first.testBaseContext().withCheckpointTransaction(
                checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
                _ = try await transaction.stageSQLCatalog(candidate,
                    expectedGeneration: old.generation, idempotencyKey: "rollback")
                throw S12RestartFailure.rejected
            }
        }
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        #expect(restored.sqlCatalog == old.sqlCatalog.catalog)
        #expect(restored.schemaGeneration == old.generation)
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await restored.testBaseContext().executeSQL("SELECT value FROM uncommitted_items")
        }
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let schema = try Schema(entities: [S12RestartItem.schemaEntity])
        return try await DBContainer.open(for: schema, configuration: .file(database.path),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s12-restart", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S12RestartItem.self)]),
            security: .testingDisabled)
    }

    private func restore(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s12-restart", revision: 1),
                schema: schema)
        }
    }

    private func renamed(_ catalog: SQLCatalog, to name: String) throws -> SQLCatalog {
        let relation = try #require(catalog.relations.first)
        return try SQLCatalog(namespaces: catalog.namespaces, relations: [SQLRelation(
            id: relation.id, namespaceID: relation.namespaceID, name: name, entityName: relation.entityName,
            columns: relation.columns, primaryKeyColumnIDs: relation.primaryKeyColumnIDs)])
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 16,
            maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 100_000, maximumReadPageRows: 128)
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())
    }
}
#endif
