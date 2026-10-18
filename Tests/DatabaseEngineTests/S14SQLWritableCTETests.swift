import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S14StatementItem: SecurityPolicy {
    #Directory<S14StatementItem>("s14-statement-items")
    var id: String = ""
    var title: String = ""
    @Restricted(read: .roles(["secret-reader"]))
    var secret: String = ""
    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsUpdate(from resource: borrowing Self, to newResource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
}

@Persistable
private struct S14ArchiveItem {
    #Directory<S14ArchiveItem>("s14-statement-archive")
    var id: String = ""
    var title: String = ""
}

@Suite("S14 writable CTE statement snapshot", .timeLimit(.minutes(1)))
struct S14SQLWritableCTETests {
    @Test("A base read sees the old image while RETURNING exposes the new image")
    func baseOldReturningNew() async throws {
        let container = try await open(); defer { await container.shutdown() }
        try await seed(container)
        let source = DataSource.join(JoinClause(type: .cross,
            left: .aliased(.table(TableRef(S14StatementItem.persistableType)), name: "base"), right: .table(TableRef("changed"))))
        let body = SelectQuery(projection: [item("title", table: "base"), item("title", table: "changed")], source: source)
        let result = try await execute(writer(body: .select(body)), container)
        #expect(try rows(result).rows.first?.values == [.primitive(.string("old")), .primitive(.string("old!"))])
        #expect(try await title(container) == "old!")
    }

    @Test("An unreferenced writable definition still performs its complete effect")
    func unusedWriterRunsOnce() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        _ = try await execute(writer(body: .select(constant())), container)
        #expect(try await title(container) == "old!")
    }

    @Test("LIMIT zero does not suppress a writable definition")
    func limitZeroStillWrites() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let body = SelectQuery(projection: [.wildcard()], source: .table(TableRef("changed")),
            pagination: QueryPagination(limit: .count(.fixed(0), withTies: false)))
        let result = try await execute(writer(body: .select(body)), container)
        #expect(try rows(result).rows.isEmpty)
        #expect(try await title(container) == "old!")
    }

    @Test("Two consumers share one completed RETURNING relation")
    func repeatedReferenceRunsOnce() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let body = SelectQuery(projection: [item("title", table: "a"), item("title", table: "b")],
            source: .join(JoinClause(type: .cross, left: .aliased(.table(TableRef("changed")), name: "a"),
                right: .aliased(.table(TableRef("changed")), name: "b"))))
        let result = try await execute(writer(body: .select(body)), container)
        #expect(try rows(result).rows.first?.values == [.primitive(.string("old!")), .primitive(.string("old!"))])
        #expect(try await title(container) == "old!")
    }

    @Test("DELETE RETURNING supplies an INSERT SELECT in the same transaction")
    func returningFeedsInsert() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let deleted = QueryStatement.delete(DeleteQuery(target: TableRef(S14StatementItem.persistableType),
            returning: [.column("id"), .column("title")]))
        let inserted = QueryStatement.insert(InsertQuery(target: TableRef(S14ArchiveItem.persistableType),
            columns: ["id", "title"], source: .select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("moved"))))))
        let result = try await execute(.with(SQLWithStatement(statements: [SQLNamedStatement(name: "moved", statement: deleted)], body: inserted)), container)
        guard case .command(.insert, let affected) = result else { Issue.record("Expected INSERT command"); return }
        #expect(affected == 1)
        #expect(try await title(container) == nil)
        #expect(try await container.testBaseContext().model(for: "one", as: S14ArchiveItem.self)?.title == "old")
    }

    @Test("A later selected failure rolls the complete block back")
    func laterFailureRollsBackWholeBlock() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let failure = SelectQuery(projection: [.expression(.divide(.int(1), .int(0)))], source: .values([[]]))
        await #expect(throws: Error.self) {
            _ = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
                SQLBlock(steps: [.statement(writer(body: .select(constant()))), .statement(.select(failure))]), options: options())
        }
        #expect(try await title(container) == "old")
    }

    @Test("Reading a writable definition without RETURNING fails before any write")
    func unreadableNoReturningFailsBeforeEffects() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let update = QueryStatement.update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
            assignments: [Assignment(column: "title", value: .string("wrong"))]))
        let statement = QueryStatement.with(SQLWithStatement(statements: [SQLNamedStatement(name: "changed", statement: update)],
            body: .select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("changed"))))))
        await #expect(throws: SQLStatementReadError.unreadableRelation("changed")) { _ = try await execute(statement, container) }
        #expect(try await title(container) == "old")
    }

    @Test("Empty RETURNING preserves duplicate labels, types and affected count")
    func emptyReturningRetainsDuplicateColumnsAndCount() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let statement = QueryStatement.update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
            assignments: [Assignment(column: "title", value: .string("unused"))], filter: .bool(false),
            returning: [.expression(.col("title"), alias: "same"), .expression(.col("title"), alias: "same")]))
        let result = try rows(await execute(statement, container))
        #expect(result.rows.isEmpty)
        #expect(result.affectedRows == 0)
        #expect(result.columns.map(\.name) == ["same", "same"])
        #expect(result.columns.allSatisfy { $0.type == .primitive(.string, isArray: false) })
    }

    @Test("WITH RECURSIVE permits a nonrecursive writer beside a recursive SELECT")
    func recursiveSelectSiblingWithNonrecursiveWriter() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let seed = SelectQuery(projection: [.expression(.int(1), alias: "n")], source: .values([[]]))
        let step = SelectQuery(projection: [.expression(.add(.col("n"), .int(1)))], source: .table(TableRef("numbers")),
            filter: .lessThan(.col("n"), .int(2)))
        let recursive = SelectQuery(projection: [.wildcard()], source: .setOperation(operator: .union, quantifier: .all, left: seed, right: step))
        let update = QueryStatement.update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
            assignments: [Assignment(column: "title", value: .string("done"))], returning: [.column("title")]))
        let body = SelectQuery(projection: [item("title", table: "changed"), item("n", table: "numbers")],
            source: .join(JoinClause(type: .cross, left: .table(TableRef("changed")), right: .table(TableRef("numbers")))))
        let result = try rows(await execute(.with(SQLWithStatement(recursive: true, statements: [
            SQLNamedStatement(name: "changed", statement: update),
            SQLNamedStatement(name: "numbers", columns: ["n"], statement: .select(recursive))], body: .select(body))), container))
        #expect(result.rows.count == 2)
        #expect(result.rows.allSatisfy { $0.values.first == .primitive(.string("done")) })
        #expect(try await title(container) == "done")
    }

    @Test("A forbidden field in a captured source rejects the statement before effects")
    func forbiddenReadColumnRejectsBeforeEffects() async throws {
        let container = try await open(enforced: true); defer { await container.shutdown() }; try await seed(container)
        let body = SelectQuery(projection: [.column("secret")], source: .table(TableRef(S14StatementItem.persistableType)))
        await #expect(throws: Error.self) { _ = try await execute(writer(body: .select(body)), container) }
        #expect(try await title(container) == "old")
        let allowed = SelectQuery(projection: [.column("title")], source: .table(TableRef(S14StatementItem.persistableType)))
        _ = try await execute(writer(body: .select(allowed)), container)
        #expect(try await title(container) == "old!")
    }

    @Test("A writable definition can depend on a later SELECT under WITH RECURSIVE")
    func recursiveForwardSelectDependency() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let value = SelectQuery(projection: [.column("value")], source: .table(TableRef("input")))
        let update = QueryStatement.update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
            assignments: [Assignment(column: "title", value: .subquery(value))], returning: [.column("title")]))
        let input = SelectQuery(projection: [.expression(.string("forward"), alias: "value")], source: .values([[]]))
        let statement = QueryStatement.with(SQLWithStatement(recursive: true, statements: [
            SQLNamedStatement(name: "changed", statement: update), SQLNamedStatement(name: "input", statement: .select(input))],
            body: .select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("changed"))))))
        #expect(try rows(await execute(statement, container)).rows.first?.values == [.primitive(.string("forward"))])
        #expect(try await title(container) == "forward")
    }

    @Test("Sibling writers may change disjoint physical rows of the same table")
    func siblingDisjointRowsShareOldSnapshot() async throws {
        let container = try await open(); defer { await container.shutdown() }; try await seed(container)
        let context = container.testBaseContext()
        try context.insert(S14StatementItem(id: "two", title: "old", secret: "hidden")); try await context.save()
        func update(_ identifier: String, value: String) -> QueryStatement {
            .update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
                assignments: [Assignment(column: "title", value: .string(value))],
                filter: .equal(.col("id"), .string(identifier)), returning: [.column("title")]))
        }
        let statement = QueryStatement.with(SQLWithStatement(statements: [
            SQLNamedStatement(name: "first", statement: update("one", value: "first")),
            SQLNamedStatement(name: "second", statement: update("two", value: "second"))],
            body: .select(SelectQuery(projection: [.column("title")], source: .table(TableRef(S14StatementItem.persistableType))))))
        let result = try rows(await execute(statement, container))
        #expect(result.rows.count == 2)
        #expect(result.rows.allSatisfy { $0.values == [.primitive(.string("old"))] })
        #expect(try await context.model(for: "one", as: S14StatementItem.self)?.title == "first")
        #expect(try await context.model(for: "two", as: S14StatementItem.self)?.title == "second")
    }

    #if MultiBase
    @Test("Write admission covers mutation reads while independent SELECT still requires read")
    func writeAdmissionKeepsIndependentReadBoundary() async throws {
        let container = try await open(enforced: true)
        defer { await container.shutdown() }
        try await seed(container)
        let authorization = AuthorizationContext.authenticated(Principal(identifier: "statement-writer"))
        try await container.grantTestBaseAccess(to: .principal("statement-writer"), access: .write)
        let executor = SQLBlockExecutor(context: container.testBaseContext(authorization: authorization))
        func update(_ value: String) -> QueryStatement {
            .update(UpdateQuery(target: TableRef(S14StatementItem.persistableType),
                assignments: [Assignment(column: "title", value: .string(value))], returning: [.column("title")]))
        }
        let direct = try await executor.execute(SQLBlock(steps: [.statement(update("direct"))]), options: options())
        #expect(try rows(direct.results[0]).rows.first?.values == [.primitive(.string("direct"))])
        #expect(try rows(direct.results[0]).affectedRows == 1)
        let composed = QueryStatement.with(SQLWithStatement(statements: [
            SQLNamedStatement(name: "input", statement: .select(constant()))], body: update("composed")))
        let result = try await executor.execute(SQLBlock(steps: [.statement(composed)]), options: options())
        #expect(try rows(result.results[0]).rows.first?.values == [.primitive(.string("composed"))])
        let denied = DatabaseGrantAuthorizationError.denied(resource: .base(try TestBaseEnvironment.id()), required: .read)
        await #expect(throws: denied) {
            _ = try await executor.execute(SQLBlock(steps: [
                .statement(update("rolled-back")), .statement(.select(constant()))]), options: options())
        }
        #expect(try await title(container) == "composed")
        await #expect(throws: denied) {
            _ = try await executor.execute(SQLBlock(steps: [
                .statement(writer(body: .select(constant())))]), options: options())
        }
        #expect(try await title(container) == "composed")
    }
    #endif

    private func open(enforced: Bool = false) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [S14StatementItem.schemaEntity, S14ArchiveItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()), runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s14-writable-cte", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S14StatementItem.self), DatabaseFrameworkRuntime.entity(S14ArchiveItem.self)],
                authorizationPolicies: enforced ? [AuthorizationPolicyHandler(S14StatementItem.self)] : []),
            security: enforced ? .enabled() : .testingDisabled)
    }
    private func seed(_ container: DBContainer) async throws {
        let context = container.testBaseContext()
        try context.insert(S14StatementItem(id: "one", title: "old", secret: "hidden")); try await context.save()
    }
    private func title(_ container: DBContainer) async throws -> String? {
        let query = SelectQuery(projection: [.column("title")], source: .table(TableRef(S14StatementItem.persistableType)))
        let output = try rows(await execute(.select(query), container))
        guard case .primitive(.string(let value))? = output.rows.first?.values.first else { return nil }
        return value
    }
    private func execute(_ statement: QueryStatement, _ container: DBContainer) async throws -> SQLStatementResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(statement)]), options: options()).results[0]
    }
    private func writer(body: QueryStatement) -> QueryStatement {
        .with(SQLWithStatement(statements: [SQLNamedStatement(name: "changed", statement: .update(UpdateQuery(
            target: TableRef(S14StatementItem.persistableType),
            assignments: [Assignment(column: "title", value: .concatenate(.col("title"), .string("!")))],
            returning: [.column("title")])) )], body: body))
    }
    private func constant() -> SelectQuery { SelectQuery(projection: [.expression(.int(1))], source: .values([[]])) }
    private func item(_ name: String, table: String) -> ProjectionItem { .expression(.column(ColumnRef(table: table, column: name))) }
    private func rows(_ result: SQLStatementResult) throws -> QueryResponse {
        guard case .rows(let value) = result else { throw SQLBlockExecutionError.unsupportedStatement }; return value
    }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 1_000,
            maximumDepth: 16, maximumRetainedBytes: 4_194_304, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
}
