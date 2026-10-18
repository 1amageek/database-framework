import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S15SecuredViewSource: SecurityPolicy {
    #Directory<S15SecuredViewSource>("s15-secured-view-source")
    var id: String = ""
    var title: String = ""
    @Restricted(read: .roles(["secret-reader"]))
    var secret: String = ""
    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("source-reader") == true
    }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("source-reader") == true
    }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsUpdate(from resource: borrowing Self, to updated: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
}

@Suite("S15 durable SQL views", .timeLimit(.minutes(1)))
struct S15SQLViewExecutionTests {
    @Test("Ordinary definitions freeze identities, wildcard shape and only consumed column dependencies")
    func stableDefinitionAndWildcard() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("old")]]),
            view("narrow", projection: [.column("a")]), view("wide", projection: [.wildcard()])])
        let narrow = try #require(container.sqlCatalog.views.first { $0.name == "narrow" })
        #expect(narrow.definition.bindings.first?.columns.map(\.name) == ["a"])
        #expect(!container.schema.entities.contains { $0.name == "narrow" })
        _ = try await run(container, [.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
            .renameColumn(name: "a", to: "renamed"), .renameTable(to: "renamed_items"),
            .addColumn(SQLColumnDefinition(name: "extra", type: .text), ifNotExists: false)])))])
        let narrowRows = try await select(container, "narrow")
        #expect(narrowRows.columns.map(\.name) == ["a"])
        #expect(narrowRows.rows.map(\.values) == [[.primitive(.int32(7))]])
        let wide = try await select(container, "wide")
        #expect(wide.columns.map(\.name) == ["a", "b"])
        #expect(wide.rows.first?.values == [.primitive(.int32(7)), .primitive(.string("old"))])
        _ = try await run(container, [.ddl(.dropView(names: [TableRef("wide")], ifExists: false, behavior: .restrict)),
            .ddl(.alterTable(SQLAlterTable(name: TableRef("renamed_items"), actions: [
                .dropColumn(name: "b", ifExists: false, behavior: .restrict)])))])
        #expect(try await select(container, "narrow").rows.count == 1)
        let snapshot = container.acquirePublishedSchemaLease().sqlCatalog
        let restored = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(snapshot))
        #expect(restored.catalog == snapshot.catalog)
    }

    @Test("CREATE OR REPLACE keeps existing output prefix and rejects incompatible replacements atomically")
    func replacement() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let source = DataSource.values([[]])
        _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("v"), columns: ["a"],
            query: SelectQuery(projection: [.expression(.int(1))], source: source))))])
        _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("v"), columns: ["a", "b"],
            query: SelectQuery(projection: [.expression(.int(2)), .expression(.string("added"))], source: source), orReplace: true)))])
        #expect(try await select(container, "v").columns.map(\.name) == ["a", "b"])
        await #expect(throws: Error.self) {
            _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("v"), columns: ["changed"],
                query: SelectQuery(projection: [.expression(.int(3))], source: source), orReplace: true)))])
        }
        let rows = try await select(container, "v")
        #expect(rows.rows.first?.values == [.primitive(.int64(2)), .primitive(.string("added"))])
        let allColumnsAreNullable = rows.columns.allSatisfy { $0.isNullable }
        #expect(allColumnsAreNullable)
    }

    @Test("Stored definitions cannot capture caller CTEs, search paths or invocation parameters")
    func lexicalIsolationAndParameters() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("stored")]]), view("v", projection: [.column("a")])])
        let shadow = SelectQuery(projection: [.expression(.int(999), alias: "a")], source: .values([[]]))
        let query = SelectQuery(projection: [.wildcard()], source: .table(TableRef(schema: "public", table: "v")),
            subqueries: [NamedSubquery(name: "items", query: shadow)])
        let result = try await run(container, [.select(query)])
        #expect(try rows(result, index: 0).rows.first?.values == [.primitive(.int32(7))])
        let foreignPath = try SQLSearchPath(entries: [.schema("missing")])
        let qualified = SelectQuery(projection: [.wildcard()], source: .table(TableRef(schema: "public", table: "v")))
        let explicit = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(.select(qualified))]), options: options(searchPath: foreignPath))
        #expect(try rows(explicit, index: 0).rows.first?.values == [.primitive(.int32(7))])
        let parameter = Expression.parameter(.position(1))
        await #expect(throws: Error.self) {
            _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("bad"),
                query: SelectQuery(projection: [.expression(parameter)], source: .values([[]])))))])
        }
        #expect(!container.sqlCatalog.views.contains { $0.name == "bad" })
    }

    @Test("Self replacement cycles and changes to referenced input types fail before publication")
    func cyclesAndInputTypes() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("base")]]), view("v", projection: [.column("a")])])
        await #expect(throws: Error.self) {
            _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("v"),
                query: SelectQuery(projection: [.column("a")], source: .table(TableRef("v"))), orReplace: true)))])
        }
        await #expect(throws: Error.self) {
            _ = try await run(container, [.ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [
                .alterColumnType(name: "a", type: .bigint, using: nil)])))])
        }
        #expect(try await select(container, "v").rows.first?.values == [.primitive(.int32(7))])
    }

    @Test("Every user mutation entry rejects views without modifying underlying rows")
    func readOnlyGates() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("base")]]), view("v", projection: [.column("a")])])
        let commands: [QueryStatement] = [
            .insert(InsertQuery(target: TableRef("v"), columns: ["a"], source: .values([[.int(8)]]))),
            .update(UpdateQuery(target: TableRef("v"), assignments: [Assignment(column: "a", value: .int(8))])),
            .delete(DeleteQuery(target: TableRef("v"))),
            .truncate(TruncateQuery(tables: [TableRef("v")]))]
        for command in commands { await #expect(throws: Error.self) { _ = try await run(container, [command]) } }
        #expect(try await select(container, "v").rows.first?.values == [.primitive(.int32(7))])
    }

    @Test("A writable CTE reads view dependencies from the statement's old image")
    func statementSnapshot() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("base")]]), view("v", projection: [.column("a")])])
        let mutation = QueryStatement.update(UpdateQuery(target: TableRef("items"),
            assignments: [Assignment(column: "a", value: .int(8))], returning: [.column("a")]))
        let statement = QueryStatement.with(SQLWithStatement(statements: [SQLNamedStatement(name: "changed", statement: mutation)],
            body: .select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("v"))))))
        #expect(try rows(await run(container, [statement]), index: 0).rows.first?.values == [.primitive(.int32(7))])
        #expect(try await select(container, "v").rows.first?.values == [.primitive(.int32(8))])
    }

    @Test("Materialization retains duplicates and NULLs, hides backing names and enforces NO DATA")
    func materializedBagAndNoData() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .null], [.int(7), .null]]),
            .ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("mv"),
                query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("items"))), withData: false)))])
        await #expect(throws: Error.self) { _ = try await select(container, "mv") }
        await #expect(throws: Error.self) {
            _ = try await run(container, [.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("mv")),
                pagination: QueryPagination(limit: .count(.fixed(0), withTies: false))))])
        }
        _ = try await run(container, [.ddl(.refreshMaterializedView(name: TableRef("mv"), withData: true))])
        let output = try await select(container, "mv")
        #expect(output.rows.map(\.values) == [[.primitive(.int32(7)), .primitive(.null)], [.primitive(.int32(7)), .primitive(.null)]])
        _ = try await run(container, [insert([[.int(8), .null]])])
        let generation = container.schemaGeneration
        let bounded = try SQLBlockExecutionOptions(checkpointLimits: options().checkpointLimits,
            budget: ExecutionBudget(maximumRows: 0))
        await #expect(throws: DatabaseWorkLimitError.maximumRows(stage: .mutationPlanning, consumed: 0, requested: 3, maximum: 0)) {
            _ = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
                .statement(.ddl(.refreshMaterializedView(name: TableRef("mv"), withData: true)))]), options: bounded)
        }
        #expect(container.schemaGeneration == generation)
        #expect(try await select(container, "mv").rows.map(\.values) == output.rows.map(\.values))
        let view = try #require(container.sqlCatalog.materializedViews.first)
        let backing = try #require(container.sqlCatalog.relations.first { $0.id == view.backingRelationID })
        await #expect(throws: Error.self) { _ = try await select(container, backing.name) }
        await #expect(throws: Error.self) {
            _ = try await run(container, [.select(SelectQuery(projection: [.wildcard()],
                source: .table(TableRef(schema: "public", table: backing.name))))])
        }
        await #expect(throws: Error.self) { _ = try await run(container, [.delete(DeleteQuery(target: TableRef("mv")))]) }
        _ = try await run(container, [.ddl(.refreshMaterializedView(name: TableRef("mv"), withData: false))])
        await #expect(throws: Error.self) { _ = try await select(container, "mv") }
    }

    @Test("Dependent view removal obeys RESTRICT and CASCADE and rollback restores the whole chain")
    func dependencyClosureAndRollback() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("base")]]), view("v", projection: [.column("a")]),
            .ddl(.createView(SQLCreateView(name: TableRef("nested"),
                query: SelectQuery(projection: [.wildcard()], source: .table(TableRef("v"))))))])
        await #expect(throws: Error.self) { _ = try await run(container, [.ddl(.dropTable(names: [TableRef("items")], ifExists: false, behavior: .restrict))]) }
        let failure = QueryStatement.select(SelectQuery(projection: [.expression(.divide(.int(1), .int(0)))], source: .values([[]])))
        await #expect(throws: Error.self) {
            _ = try await run(container, [.ddl(.dropTable(names: [TableRef("items")], ifExists: false, behavior: .cascade)), failure])
        }
        #expect(try await select(container, "nested").rows.count == 1)
        _ = try await run(container, [.ddl(.dropTable(names: [TableRef("items")], ifExists: false, behavior: .cascade))])
        #expect(container.sqlCatalog.views.isEmpty)
    }

    @Test("Invoker entity and field policies protect ordinary views while materialized reads use their own backing policy")
    func invokerSecurity() async throws {
        let schema = try Schema(entities: [S15SecuredViewSource.schemaEntity])
        let configuration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "s15-view-security", revision: 1),
            entityRuntimes: [DatabaseFrameworkRuntime.entity(S15SecuredViewSource.self)],
            authorizationPolicies: [AuthorizationPolicyHandler(S15SecuredViewSource.self)],
            schemaDrivenAuthorizationPolicyFactory: { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, authorization in
                    authorization.principal?.roles.contains("materialized-reader") == true
                        || authorization.principal?.roles.contains("source-reader") == true
                }
            })
        let container = try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()), runtimeConfiguration: configuration, security: .enabled())
        defer { await container.shutdown() }
        func authorization(_ roles: Set<String>) -> AuthorizationContext {
            .authenticated(Principal(identifier: "test-runner", roles: roles.union(["admin", "test-runner"])))
        }
        let reader = authorization(["source-reader", "secret-reader"])
        let fieldDenied = authorization(["source-reader"])
        let materializedReader = authorization(["materialized-reader"])
        let context = container.testBaseContext(authorization: reader)
        try context.insert(S15SecuredViewSource(id: "one", title: "public", secret: "private")); try await context.save()
        let definition = SelectQuery(projection: [.column("title"), .column("secret")],
            source: .table(TableRef(S15SecuredViewSource.persistableType)))
        _ = try await SQLBlockExecutor(context: context).execute(SQLBlock(steps: [
            .statement(.ddl(.createView(SQLCreateView(name: TableRef("secured_view"), query: definition)))),
            .statement(.ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("secured_materialized"), query: definition))))]),
            options: options())
        let viewRead = try SQLBlock(steps: [.statement(.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("secured_view")))))])
        let allowed = try await SQLBlockExecutor(context: context).execute(viewRead, options: options())
        #expect(try rows(allowed, index: 0).rows.first?.values == [.primitive(.string("public")), .primitive(.string("private"))])
        for denied in [fieldDenied, materializedReader] {
            await #expect(throws: Error.self) {
                _ = try await SQLBlockExecutor(context: container.testBaseContext(authorization: denied)).execute(viewRead, options: options())
            }
        }
        let materializedContext = container.testBaseContext(authorization: materializedReader)
        let materialized = try await SQLBlockExecutor(context: materializedContext).execute(SQLBlock(steps: [
            .statement(.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("secured_materialized")))))]), options: options())
        #expect(try rows(materialized, index: 0).rows.first?.values == [.primitive(.string("public")), .primitive(.string("private"))])
        await #expect(throws: Error.self) {
            _ = try await SQLBlockExecutor(context: materializedContext).execute(SQLBlock(steps: [
                .statement(.ddl(.refreshMaterializedView(name: TableRef("secured_materialized"), withData: true)))]), options: options())
        }
    }

    @Test("Retained SQL catalogs reject new physical snapshots after DROP while pre-DDL snapshots and logical renames stay compatible")
    func stalePhysicalSourceAdmission() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table(), insert([[.int(7), .string("old")]])])
        let old = container.acquirePublishedSchemaLease()
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let resolution = try container.testBaseContext().makeSQLReadResolution(searchPath: .default, workMeter: meter)
        let pinned = try container.engine.createOwnedTransaction()
        do {
            try await resolution.validatePhysicalSource(container: container, transaction: pinned)
            _ = try await run(container, [.ddl(.alterTable(SQLAlterTable(name: TableRef("items"),
                actions: [.renameTable(to: "renamed_items")])))])
            try await DatabaseSchemaExecutionScope.$container.withValue(container) {
                try await DatabaseSchemaExecutionScope.$lease.withValue(old) {
                    let retainedRows = try await select(container, "items")
                    #expect(retainedRows.rows.first?.values == [.primitive(.int32(7)), .primitive(.string("old"))])
                }
            }
            _ = try await run(container, [.ddl(.dropTable(names: [TableRef("renamed_items")], ifExists: false, behavior: .restrict))])
            try await resolution.validatePhysicalSource(container: container, transaction: pinned)
            let failure = DatabaseSchemaPublicationError.generationConflict(expected: old.generation, actual: container.schemaGeneration)
            await #expect(throws: failure) {
                try await DatabaseSchemaExecutionScope.$container.withValue(container) {
                    try await DatabaseSchemaExecutionScope.$lease.withValue(old) {
                        _ = try await select(container, "items")
                    }
                }
            }
            try await pinned.cancel()
        } catch {
            let primary = error
            do { try await pinned.cancel() }
            catch { throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
            throw primary
        }
    }

    private func open() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s15-view-tests", revision: 1), schema: schema), security: .testingDisabled)
    }
    private func table() -> QueryStatement {
        .ddl(.createTable(SQLCreateTable(name: TableRef("items"), source: .columns([
            SQLColumnDefinition(name: "a", type: .integer), SQLColumnDefinition(name: "b", type: .text)], constraints: []))))
    }
    private func insert(_ values: [[Expression]]) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), columns: ["a", "b"], source: .values(values)))
    }
    private func view(_ name: String, projection: [ProjectionItem]) -> QueryStatement {
        .ddl(.createView(SQLCreateView(name: TableRef(name), query: SelectQuery(projection: projection, source: .table(TableRef("items"))))))
    }
    private func run(_ container: DBContainer, _ statements: [QueryStatement]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: statements.map { .statement($0) }), options: options())
    }
    private func select(_ container: DBContainer, _ name: String) async throws -> QueryResponse {
        try rows(await run(container, [.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef(name))))]), index: 0)
    }
    private func rows(_ result: SQLBlockResult, index: Int) throws -> QueryResponse {
        guard case .rows(let rows) = result.results[index] else { throw SQLBlockExecutionError.unsupportedStatement }; return rows
    }
    private func options(searchPath: SQLSearchPath = .default) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 16, maximumRetainedBytes: 16_777_216, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), searchPath: searchPath)
    }
}
