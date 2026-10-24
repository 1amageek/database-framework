#if SQLITE
import DatabaseEmbedded
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import Synchronization
import TestSupport
import Testing
import Database
import DatabaseEngine

@Suite("Embedded logical access ownership", .timeLimit(.minutes(1)))
struct EmbeddedAccessTests {
    private enum Failure: Error, Equatable { case selection }
    private var identity: DatabaseExecutionRuntimeIdentity { .init(identifier: "embedded-access", revision: 1) }

    @Test("SQL, IR and blocks retain identical values after logical close")
    func representations() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-representations")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let access = try await configuration(container).open()
        defer { await access.close() }
        for text in ["SELECT 7::integer AS v, 1.2300::numeric AS v",
                     "SELECT '[0:1]={1,2}'::integer[] AS a, NULL::text AS empty",
                     "WITH c AS (SELECT 2::integer AS n) SELECT sum(n)::bigint AS total FROM c LIMIT 1"] {
            let statement = try SQLParser(preservesCastOutputNames: true).parse(text)
            let sql = try await execute(access, .sql(text))
            let ir = try await execute(access, .statement(statement))
            let block = try await execute(access, .block(SQLBlock(steps: [.statement(statement)])))
            #expect(sql.columns == ir.columns && ir.columns == block.columns)
            #expect(try await values(sql.rows) == values(ir.rows))
            #expect(try await values(ir.rows) == values(block.rows))
        }
        let retained = try await execute(access, .sql("SELECT 7 AS n"))
        await access.close()
        #expect(try await values(retained.rows) == [[.primitive(.int32(7))]])
    }

    @Test("Borrowed close preserves external ownership and owned close releases the container")
    func resourceOwnership() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-ownership")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let borrowed = try await configuration(container).open()
        await borrowed.close()
        await #expect(throws: QueryConnectionError.closed) { try await execute(borrowed, .sql("SELECT 1")) }
        let context = container.testBaseContext()
        let independent = try await context.openConnection()
        await independent.close()
        let owned = try DatabaseConfiguration.owningEmbedded(maximumConcurrentExecutions: 2,
            makeContainer: { container }, context: { $0.testBaseContext() })
        let access = try await owned.open()
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await access.close() }
            group.addTask { await access.close() }
        }
        try await assertContainerClosed(context)
    }

    @Test("Metadata rejection precedes effects and original SQL policies apply to SQL and IR")
    func authorityAndMetadata() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-authority")
        defer { database.remove() }
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema,
            schemaDrivenAuthorizationPolicyFactory: { entity in AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true } },
            sqlAuthorizationPolicyFactory: { entity, namespace, name in
                guard namespace.name == "public", name == "allowed" else { return nil }
                return AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            })
        let container = try await DBContainer.open(for: schema, configuration: .file(database.path),
            runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        let access = try await configuration(container).open()
        defer { await access.close() }
        _ = try await execute(access, .sql("CREATE TABLE allowed(id integer); CREATE TABLE denied(id integer)"))
        for text in ["SELECT id FROM denied", "INSERT INTO denied VALUES (1)"] {
            await #expect(throws: (any Error).self) { try await execute(access, .sql(text)) }
            await #expect(throws: (any Error).self) { try await execute(access, .statement(SQLParser().parse(text))) }
        }
        await #expect(throws: EmbeddedAccessError.unsupportedRequestMetadata) {
            try await access.execute(.sql("INSERT INTO allowed VALUES (1)"), parameters: [], options: .init(), metadata: .init(idempotencyKey: "key"))
        }
        #expect(try await values(execute(access, .sql("SELECT id FROM allowed")).rows).isEmpty)
    }

    @Test("Budgets and late statement errors rollback while completed effects survive restart")
    func rollbackAndRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-rollback")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let access = try await configuration(container).open()
        defer { await access.close() }
        _ = try await execute(access, .sql("CREATE TABLE items(id integer); INSERT INTO items VALUES (1)"))
        for budget in [ExecutionBudget(maximumRows: 0), ExecutionBudget(maximumWorkUnits: 0), ExecutionBudget(maximumIntermediateBytes: 0)] {
            do {
                _ = try await access.execute(.sql("INSERT INTO items VALUES (2)"), parameters: [], options: .init(budget: budget), metadata: .init())
                Issue.record("Expected resource rejection")
            } catch let error as SQLExecutionFailure { #expect(error.diagnostic.sqlState == "54000") }
        }
        await #expect(throws: SQLExecutionFailure.self) {
            try await execute(access, .sql("INSERT INTO items VALUES (3); SELECT missing; INSERT INTO items VALUES (4)"))
        }
        await access.close()
        await container.shutdown()
        let restored = try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
                try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema)
            }
        defer { await restored.shutdown() }
        let reopened = try await configuration(restored).open()
        defer { await reopened.close() }
        #expect(try await values(execute(reopened, .sql("SELECT id FROM items ORDER BY id")).rows) == [[.primitive(.int32(1))]])
    }

    @Test("Failed context selection awaits owned container shutdown")
    func failedAcquisition() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-failed")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let config = try DatabaseConfiguration.owningEmbedded(maximumConcurrentExecutions: 1,
            makeContainer: { container }, context: { _ in throw Failure.selection })
        await #expect(throws: Failure.selection) { try await config.open() }
        try await assertContainerClosed(context)
    }

    @Test("Cancelled acquisition awaits logical and owned container cleanup")
    func cancelledAcquisition() async throws {
        let database = try SQLiteTestDatabase(prefix: "access-cancelled")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let context = container.testBaseContext(), gate = EmbeddedAcquisitionGate()
        let config = try DatabaseConfiguration.owningEmbedded(maximumConcurrentExecutions: 1,
            makeContainer: { await gate.hold(); return container }, context: { $0.testBaseContext() })
        let task = Task { try await config.open() }
        await gate.waitUntilEntered()
        task.cancel()
        await gate.release()
        await #expect(throws: CancellationError.self) { try await task.value }
        try await assertContainerClosed(context)
    }

    private func assertContainerClosed(_ context: DatabaseContext) async throws {
        let connection = try await context.openConnection()
        defer { await connection.close() }
        await #expect(throws: (any Error).self) { try await connection.execute("SELECT 1") }
    }
    private func configuration(_ container: DBContainer) throws -> DatabaseConfiguration {
        try .borrowingEmbedded(context: container.testBaseContext(), maximumConcurrentExecutions: 2)
    }
    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: []), configuration: .file(database.path),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: identity, entityRuntimes: []), security: .testingDisabled)
    }
    private func execute(_ access: any DatabaseExecuting, _ input: QueryInput) async throws -> AccessResult {
        try await access.execute(input, parameters: [], options: .init(), metadata: .init())
    }
    private func values<Rows: QueryRows>(_ rows: Rows) async throws -> [[QueryValue]] {
        var output: [[QueryValue]] = []
        for try await row in rows { output.append(row.values) }
        return output
    }
}

private actor EmbeddedAcquisitionGate {
    private var entered = false
    private var entryWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?
    func hold() async {
        entered = true
        entryWaiters.forEach { $0.resume() }; entryWaiters.removeAll()
        await withCheckedContinuation { releaseContinuation = $0 }
    }
    func waitUntilEntered() async {
        if entered { return }
        await withCheckedContinuation { entryWaiters.append($0) }
    }
    func release() { releaseContinuation?.resume(); releaseContinuation = nil }
}
#endif
