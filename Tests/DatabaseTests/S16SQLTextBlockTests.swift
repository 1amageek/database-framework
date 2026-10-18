import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S16 SQL text execution and source diagnostics", .timeLimit(.minutes(1)))
struct S16SQLTextBlockTests {
    @Test("Text blocks compose DDL, prepared parameters, own writes and terminal commit")
    func textPreparedBlock() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQLBlock("""
            BEGIN;
            CREATE TABLE items (id integer, value text);
            PREPARE add_item(integer, text) AS INSERT INTO items VALUES ($1, $2);
            EXECUTE add_item(7, 'kept');
            SELECT id, value FROM items;
            COMMIT;
            """, options: options())
        guard case .rows(let rows) = result.results[4] else { Issue.record("Expected prepared insert readback"); return }
        #expect(rows.rows.map(\.values) == [[.primitive(.int32(7)), .primitive(.string("kept"))]])
        #expect(try await context.executeSQL("SELECT id FROM items").rows.count == 1)
    }

    @Test("Explicit text ROLLBACK returns observations after undoing the writes")
    func textRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("CREATE TABLE items (id integer)", options: options())
        let result = try await context.executeSQLBlock("BEGIN; INSERT INTO items VALUES (1); SELECT id FROM items; ROLLBACK;", options: options())
        guard case .rows(let rows) = result.results[2] else { Issue.record("Expected pre-rollback observation"); return }
        #expect(rows.rows.map(\.values) == [[.primitive(.int32(1))]])
        #expect(try await context.executeSQL("SELECT id FROM items").rows.isEmpty)
    }

    @Test("Missing request parameter retains its typed cause and undefined-parameter SQLSTATE")
    func missingParameter() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        do {
            _ = try await container.testBaseContext().executeSQLBlock("SELECT $1", parameters: [], options: options())
            Issue.record("Expected missing request parameter")
        } catch let error as SQLExecutionFailure {
            #expect(error.diagnostic.sqlState == "42P02")
            #expect(error.diagnostic.category == .name)
            #expect(error.diagnostic.position == nil)
            #expect(error.underlyingError as? QueryParameterBindingError == .missingPosition(1))
        }
    }

    @Test("Multibyte batch binding failure uses one-based source characters and retains its typed cause")
    func bindingSourcePosition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let sql = "SELECT '猫';\r\nSELECT missing_column"
        do {
            _ = try await container.testBaseContext().executeSQLBlock(sql, options: options())
            Issue.record("Expected missing column")
        } catch let error as SQLExecutionFailure {
            #expect(error.diagnostic.sqlState == "42703")
            #expect(error.diagnostic.column == "missing_column")
            #expect(error.diagnostic.position == 21)
            #expect(error.underlyingError is CanonicalSQLBindingFailure)
        }
    }

    @Test("Syntax failure preserves parser cause and absolute position after CRLF and multibyte text")
    func syntaxSourcePosition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        do {
            _ = try await container.testBaseContext().executeSQLBlock("SELECT '猫';\r\nSELECT FROM", options: options())
            Issue.record("Expected syntax failure")
        } catch let error as SQLExecutionFailure {
            #expect(error.diagnostic.sqlState == "42601")
            #expect(error.diagnostic.position == 21)
            #expect(error.underlyingError is SQLParser.ParseError)
        }
    }

    @Test("Control failure receives its own step position without wrapping semantic error APIs")
    func controlSourcePosition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        do {
            _ = try await container.testBaseContext().executeSQLBlock("SELECT '猫'; EXECUTE missing", options: options())
            Issue.record("Expected unknown prepared name")
        } catch let error as SQLExecutionFailure {
            #expect(error.diagnostic.sqlState == "26000")
            #expect(error.diagnostic.position == 13)
            #expect(error.underlyingError is SQLPreparedExecutionError)
        }
    }

    @Test("Constraint diagnostics retain the failed object's identity after complete rollback")
    func constraintAfterRollback() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("CREATE TABLE items (id integer, CONSTRAINT item_key UNIQUE (id))", options: options())
        do {
            _ = try await context.executeSQLBlock("INSERT INTO items VALUES (1); INSERT INTO items VALUES (1)", options: options())
            Issue.record("Expected unique violation")
        } catch let error as SQLExecutionFailure {
            #expect(error.diagnostic.sqlState == "23505")
            #expect(error.diagnostic.schema == "public")
            #expect(error.diagnostic.table == "items")
            #expect(error.diagnostic.constraint == "item_key")
            #expect(error.underlyingError is SQLConstraintError)
        }
        #expect(try await context.executeSQL("SELECT id FROM items").rows.isEmpty)
    }

    @Test("Inline COPY, typed JSON EXPLAIN and metadata query share the admitted text transaction")
    func operationalTextComposition() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQLBlock("""
            BEGIN;
            CREATE TABLE items (id integer, value text);
            COPY items FROM STDIN WITH (FORMAT csv);
            1,one
            2,two
            \\.
            EXPLAIN (FORMAT JSON) SELECT * FROM items;
            SELECT table_name FROM information_schema.tables WHERE table_name = 'items';
            COMMIT;
            """, options: options())
        guard case .rows(let explain) = result.results[3], case .rows(let catalog) = result.results[4] else {
            Issue.record("Expected explain and metadata rows"); return
        }
        #expect(explain.columns[0].type == .sql(.json))
        #expect(catalog.rows.map(\.values) == [[.primitive(.string("items"))]])
    }

    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(
            maximumEntries: 100, maximumDepth: 32, maximumRetainedBytes: 8_000_000,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 1000))
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "s16-text", revision: 1), entityRuntimes: [],
            schemaDrivenAuthorizationPolicyFactory: { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, authorization in
                    authorization.principal?.roles.contains("test-runner") == true
                }
            })
        #if SQLITE
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        #else
        let engine = InMemoryEngine()
        #endif
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: runtime, security: .enabled())
    }
}
