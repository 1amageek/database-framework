#if SQLITE
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import QueryAST
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Canonical query connections", .timeLimit(.minutes(1)))
struct QueryConnectionTests {
    @Test("SQL, semantic statements and blocks preserve exact values and independent schemas")
    func representationAgreement() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-representations")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let connection = try await container.testBaseContext().openConnection()
        defer { await connection.close() }
        let texts = [
            "SELECT 7::integer AS v, 1.2300::numeric AS v",
            "SELECT '[0:1]={1,2}'::integer[] AS a, NULL::text AS empty",
            "WITH c AS (SELECT 2::integer AS n) SELECT sum(n)::bigint AS total FROM c LIMIT 1",
            "SELECT n, row_number() OVER (ORDER BY n) AS ordinal FROM (VALUES (2),(1)) AS v(n) ORDER BY n",
        ]
        for text in texts {
            let statement = try SQLParser(preservesCastOutputNames: true).parse(text)
            let sql = try await connection.execute(SQL(text: text))
            let ir = try await connection.execute(statement)
            let block = try await connection.execute(SQLBlock(steps: [.statement(statement)]))
            #expect(sql.columns == ir.columns && ir.columns == block.columns)
            let expected = try await values(sql.rows)
            #expect(try await values(ir.rows) == expected)
            #expect(try await values(block.rows) == expected)
            #expect(sql.rows.metadata == ir.rows.metadata)
        }
        let manual = QueryStatement.select(SelectQuery(
            projection: [.expression(.literal(.queryValue(.primitive(.int32(7)),
                type: .sql(.integer))), alias: "answer")], source: .values([[]])))
        let text = try await connection.execute("SELECT 7 AS answer")
        let ir = try await connection.execute(manual)
        #expect(text.columns == ir.columns)
        #expect(try await values(text.rows) == values(ir.rows))
        let wide = QueryStatement.select(SelectQuery(
            projection: [.expression(.int(7), alias: "answer")], source: .values([[]])))
        let wideSQL = try await connection.execute("SELECT 7::bigint AS answer")
        let wideIR = try await connection.execute(wide)
        #expect(wideSQL.columns == wideIR.columns)
        #expect(try await values(wideSQL.rows) == values(wideIR.rows))
        let hostile = "Robert'); DROP TABLE items; --"
        let bound = try await connection.execute("SELECT \(hostile)::text AS value")
        #expect(try await values(bound.rows) == [[.primitive(.string(hostile))]])
    }

    @Test("Explicit paged blocks are rejected before storage while SQL and IR reads remain equivalent")
    func pagedBlockAdmission() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-paged-block")
        defer { database.remove() }
        let engine = ControlledStorageEngine(base: try SQLiteStorageEngine(configuration: .file(database.path)))
        let container = try await open(database, engine: engine)
        defer { await container.shutdown() }
        let connection = try await container.testBaseContext().openConnection()
        defer { await connection.close() }
        let statement = try SQLParser(preservesCastOutputNames: true).parse("SELECT 7 AS n")
        for block in [try SQLBlock(steps: [.statement(statement)]),
                      try SQLBlock(steps: [.statement(statement), .statement(statement)])] {
            let reads = engine.control.dataReadOperationCount
            do {
                _ = try await connection.execute(block, options: .init(pageSize: 1))
                Issue.record("Expected explicit paged block admission to fail")
            } catch let failure as SQLExecutionFailure {
                #expect(failure.underlyingError as? SQLBlockExecutionError == .unsupportedPagedQuery)
                #expect(failure.diagnostic.sqlState == "0A000")
            }
            #expect(engine.control.dataReadOperationCount == reads)
        }
        let sql = try await connection.execute("SELECT 7 AS n", options: .init(pageSize: 1))
        let ir = try await connection.execute(statement, options: .init(pageSize: 1))
        let complete = try await connection.execute(SQLBlock(steps: [.statement(statement)]))
        #expect(sql.columns == ir.columns && ir.columns == complete.columns)
        #expect(try await values(sql.rows) == [[.primitive(.int32(7))]])
        #expect(try await values(ir.rows) == values(complete.rows))
    }

    @Test("Effects, RETURNING, first-error rollback and restart share one completed execution")
    func effectsRollbackAndRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-effects")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let connection = try await container.testBaseContext().openConnection()
        defer { await connection.close() }
        let created = try await connection.execute("CREATE TABLE items(id integer, value text)")
        #expect(created.command == .createTable && created.affectedRows == nil)
        #expect(try await values(created.rows).isEmpty)
        let inserted = try await connection.execute("INSERT INTO items VALUES (1, 'one') RETURNING id, value")
        #expect(inserted.affectedRows == 1)
        #expect(try await values(inserted.rows) == [[.primitive(.int32(1)), .primitive(.string("one"))]])
        let text = "UPDATE items SET value = 'changed' RETURNING id, value"
        let sql = try await connection.execute(SQL(text: text))
        let ir = try await connection.execute(SQLParser().parse(text))
        #expect(sql.affectedRows == 1 && sql.affectedRows == ir.affectedRows)
        #expect(sql.columns == ir.columns)
        #expect(try await values(sql.rows) == values(ir.rows))
        do {
            _ = try await connection.execute("INSERT INTO items VALUES (2, 'rollback'); SELECT missing_column; INSERT INTO items VALUES (3, 'unreached')")
            Issue.record("Expected first-error rollback")
        } catch let failure as SQLExecutionFailure {
            #expect(failure.diagnostic.sqlState == "42703")
        }
        let retained = try await connection.execute("SELECT id, value FROM items ORDER BY id")
        await connection.close()
        #expect(try await values(retained.rows) == [[.primitive(.int32(1)), .primitive(.string("changed"))]])
        let independent = try await container.testBaseContext().openConnection()
        defer { await independent.close() }
        #expect(try await values(independent.execute("SELECT id FROM items ORDER BY id").rows) == [[.primitive(.int32(1))]])
        await container.shutdown()
        let restored = try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema)
        }
        defer { await restored.shutdown() }
        let reopened = try await restored.testBaseContext().openConnection()
        defer { await reopened.close() }
        #expect(try await values(reopened.execute("SELECT id FROM items ORDER BY id").rows) == [[.primitive(.int32(1))]])
    }

    @Test("Original logical authorization, budgets and pre-commit Wire failure are enforced")
    func authorizationAndBudgets() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-authority")
        defer { database.remove() }
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema,
            schemaDrivenAuthorizationPolicyFactory: { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            }, sqlAuthorizationPolicyFactory: { entity, namespace, name in
                guard namespace.name == "public", name == "allowed" else { return nil }
                return AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            })
        let container = try await DBContainer.open(for: schema, configuration: .file(database.path),
            runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let connection = try await context.openConnection()
        defer { await connection.close() }
        _ = try await connection.execute("CREATE TABLE allowed(id integer); CREATE TABLE denied(id integer)")
        for statement in ["SELECT id FROM denied", "INSERT INTO denied VALUES (1)"] {
            await #expect(throws: (any Error).self) { try await connection.execute(SQL(text: statement)) }
            await #expect(throws: (any Error).self) { try await connection.execute(SQLParser().parse(statement)) }
        }
        for budget in [ExecutionBudget(maximumRows: 0), ExecutionBudget(maximumWorkUnits: 0),
                       ExecutionBudget(maximumIntermediateBytes: 0)] {
            do {
                _ = try await connection.execute("INSERT INTO allowed VALUES (2)", options: .init(budget: budget))
                Issue.record("Expected a typed resource failure")
            } catch let failure as SQLExecutionFailure { #expect(failure.diagnostic.sqlState == "54000") }
        }
        let limits = try DatabaseWireLimits(maximumFrameBytes: 25, maximumStringBytes: 1000,
            maximumByteStringBytes: 1000, maximumCollectionCount: 1000,
            maximumNestingDepth: 64, maximumObjectCount: 1000)
        let bounded = try await context.openConnection(limits: limits)
        defer { await bounded.close() }
        do {
            _ = try await bounded.execute("INSERT INTO allowed VALUES (3) RETURNING id")
            Issue.record("Expected pre-commit response rejection")
        } catch let failure as SQLExecutionFailure { #expect(failure.diagnostic.sqlState == "54000") }
        let empty = try await connection.execute("SELECT id FROM allowed")
        #expect(try await values(empty.rows).isEmpty)
        await #expect(throws: SQLExecutionFailure.self) {
            try await connection.execute("INSERT INTO allowed VALUES (4)", options: .init(pageSize: 1))
        }
        _ = try await context.withTransaction { _ in
            await #expect(throws: SQLExecutionFailure.self) { try await connection.execute("SELECT 1") }
        }
    }

    @Test("Close cancels and drains a real held commit, rejects calls and leaves the container usable")
    func closeDrainsExecution() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-close")
        defer { database.remove() }
        let base = try SQLiteStorageEngine(configuration: .file(database.path))
        let engine = ControlledStorageEngine(base: base)
        let container = try await open(database, engine: engine)
        defer { await container.shutdown() }
        let connection = try await container.testBaseContext().openConnection()
        defer { await connection.close() }
        _ = try await connection.execute("CREATE TABLE items(id integer)")
        let barrier = engine.control.suspendNextMutatingCommit()
        defer { barrier.release() }
        let execution = Task { try await connection.execute("INSERT INTO items VALUES (1)") }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: execution)
        let finished = Mutex(false)
        let closing = Task { await connection.close(); finished.withLock { $0 = true } }
        var rejected = false
        for _ in 0..<1000 {
            do { _ = try await connection.execute("SELECT 1") }
            catch QueryConnectionError.closed { rejected = true; break }
            catch QueryConnectionError.concurrentExecutionLimitReached { await Task.yield() }
        }
        #expect(rejected)
        #expect(!finished.withLock { $0 })
        barrier.release()
        await closing.value
        await monitor.value
        await #expect(throws: CancellationError.self) { try await execution.value }
        await connection.close()
        await #expect(throws: QueryConnectionError.closed) { try await connection.execute("SELECT 1") }
        #expect(engine.control.openedRangeCursorCount == engine.control.finishedRangeCursorCount)
        let other = try await container.testBaseContext().openConnection()
        defer { await other.close() }
        #expect(try await values(other.execute("SELECT id FROM items").rows) == [[.primitive(.int32(1))]])
    }

    @Test("SPARQL text and IR preserve boolean and RDF graph outcomes")
    func graphOutcomes() async throws {
        let database = try SQLiteTestDatabase(prefix: "query-graph")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let connection = try await container.testBaseContext().openConnection()
        defer { await connection.close() }
        for text in ["ASK {}", "CONSTRUCT { <urn:s> <urn:p> \"value\" } WHERE {}"] {
            let source = try await connection.execute(QueryInput.text(language: .sparql, statement: text))
            let ir = try await connection.execute(SPARQLParser().parse(text))
            switch (source, ir) {
            case (.boolean(let first), .boolean(let second)): #expect(first.value && second.value)
            case (.rdfGraph(let first), .rdfGraph(let second)):
                #expect(first.quadCount == 1 && second.quadCount == 1)
                #expect(try first.materializedQuads(maximumCount: 1) == second.materializedQuads(maximumCount: 1))
            default: Issue.record("Graph outcome changed kinds")
            }
        }
        let limits = try DatabaseWireLimits(maximumFrameBytes: 1_024, maximumStringBytes: 10_000,
            maximumByteStringBytes: 10_000, maximumCollectionCount: 1_000,
            maximumNestingDepth: 64, maximumObjectCount: 10_000)
        let bounded = try await container.testBaseContext().openConnection(limits: limits)
        defer { await bounded.close() }
        let tinyLimits = try DatabaseWireLimits(maximumFrameBytes: 1, maximumStringBytes: 10_000,
            maximumByteStringBytes: 10_000, maximumCollectionCount: 1_000,
            maximumNestingDepth: 64, maximumObjectCount: 10_000)
        let tiny = try await container.testBaseContext().openConnection(limits: tinyLimits)
        defer { await tiny.close() }
        do {
            _ = try await tiny.execute(QueryInput.text(language: .sparql, statement: "ASK {}"))
            Issue.record("Expected a bounded Boolean response failure")
        } catch let error as QueryResponseEncodingError {
            guard case .resourceLimit(.frameTooLarge(_, let maximum)) = error else {
                Issue.record("Expected the Boolean response frame limit cause"); return
            }
            #expect(maximum == 1)
        }
        let largeValue = String(repeating: "g", count: 2_000)
        do {
            _ = try await bounded.execute(QueryInput.text(language: .sparql,
                statement: "CONSTRUCT { <urn:s> <urn:p> \"\(largeValue)\" } WHERE {}"))
            Issue.record("Expected a bounded Graph response failure")
        } catch let error as QueryResponseEncodingError {
            guard case .resourceLimit(.frameTooLarge(let actual, let maximum)) = error else {
                Issue.record("Expected the original frame limit cause"); return
            }
            #expect(actual > maximum && maximum == 1_024)
        }
        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await connection.execute(QueryInput.text(language: .sparql,
                statement: "CONSTRUCT { <urn:s> <urn:p> \"\(largeValue)\" } WHERE {}"),
                options: .init(budget: .init(maximumWorkUnits: 1)))
        }
        let next = try await bounded.execute(QueryInput.text(language: .sparql, statement: "ASK {}"))
        guard case .boolean(let answer) = next else { Issue.record("Expected a closed failed read"); return }
        #expect(answer.value)
    }

    private var identity: DatabaseExecutionRuntimeIdentity { .init(identifier: "query-connections", revision: 1) }
    private func open(_ database: SQLiteTestDatabase, engine: (any StorageEngine)? = nil) async throws -> DBContainer {
        let selected: any StorageEngine
        if let engine { selected = engine }
        else { selected = try SQLiteStorageEngine(configuration: .file(database.path)) }
        return try await DBContainer.open(for: Schema(entities: []), configuration: .testing(storageEngine: selected),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: identity, entityRuntimes: []),
            security: .testingDisabled)
    }

    private func values<Rows: QueryRows>(_ rows: Rows) async throws -> [[QueryValue]] {
        var output: [[QueryValue]] = []
        for try await row in rows { output.append(row.values) }
        return output
    }
}
#endif
