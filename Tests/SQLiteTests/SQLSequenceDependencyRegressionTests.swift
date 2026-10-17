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

@Suite("S13 stable sequence default dependencies", .timeLimit(.minutes(1)), .heartbeat)
struct SQLSequenceDependencyRegressionTests {
    @Test("Literal defaults retain sequence identity across caller paths, DROP, rollback and file reopen", arguments: [false, true])
    func stableDefaultDependencies(unknownLiteral: Bool) async throws {
        let database = try SQLiteTestDatabase(prefix: "s13-sequence-dependencies")
        defer { database.remove() }
        let first = try await open(database)
        defer { await first.shutdown() }
        let original = try catalog(unknownLiteral: unknownLiteral)
        try await publish(original, in: first, key: "initial")
        _ = try await execute(first, [insert("first")])
        #expect(try await values("first", in: first) == row(10, 100))
        await first.shutdown()

        let reopened = try await restore(database)
        defer { await reopened.shutdown() }
        #expect(reopened.sqlCatalog == original)
        _ = try await execute(reopened, [insert("reopened")])
        #expect(try await values("reopened", in: reopened) == row(11, 101))
        // An unrelated DROP sees neither public nor app in its caller path.
        _ = try await execute(reopened, [.sequence(.drop(id: "unrelated-id", cascade: false))],
            searchPath: SQLSearchPath(entries: [.schema("other")]))
        for id in ["app-sequence-id", "public-sequence-id"] {
            await #expect(throws: SQLSequenceExecutionError.dependentIdentity(id)) {
                try await execute(reopened, [.sequence(.drop(id: id, cascade: false))])
            }
        }
        let beforeCascade = reopened.sqlCatalog
        _ = try await execute(reopened, [.savepoint("before"),
            .sequence(.drop(id: "app-sequence-id", cascade: true)), .rollbackTo("before")])
        #expect(reopened.sqlCatalog == beforeCascade)
        _ = try await execute(reopened, [insert("rollback")])
        #expect(try await values("rollback", in: reopened) == row(12, 102))

        _ = try await execute(reopened, [.sequence(.drop(id: "app-sequence-id", cascade: true))])
        let retained = try #require(reopened.sqlCatalog.relations.first)
        #expect(retained.columns[1].policy == .input(defaultExpression: nil))
        #expect(retained.columns[1].backing == original.relations[0].columns[1].backing)
        #expect(retained.columns[2...].elementsEqual(original.relations[0].columns[2...]))
        #expect(retained.constraints == original.relations[0].constraints)
        _ = try await execute(reopened, [insert("cascade")])
        #expect(try await values("cascade", in: reopened) == row(nil, 103))
        let afterCascade = reopened.sqlCatalog
        await reopened.shutdown()

        let restoredCascade = try await restore(database)
        defer { await restoredCascade.shutdown() }
        #expect(restoredCascade.sqlCatalog == afterCascade)
        _ = try await execute(restoredCascade, [insert("restored-cascade")])
        #expect(try await values("restored-cascade", in: restoredCascade) == row(nil, 104))
        await #expect(throws: SQLSequenceExecutionError.dependentIdentity("public-sequence-id")) {
            try await execute(restoredCascade, [.sequence(.drop(id: "public-sequence-id", cascade: false))])
        }
        await #expect(throws: SQLSequenceExecutionError.unknownSequence("app.dep")) {
            try await execute(restoredCascade, [.statement(.select(SelectQuery(projection: [
                .expression(.function(FunctionCall(name: "nextval", arguments: [.string("app.dep")])))] , source: .values([[]]))))])
        }
    }

    @Test("Missing declaration targets and unchanged-policy retargeting fail without publishing")
    func publicationRejectsMissingOrRetargetedConstant() async throws {
        let database = try SQLiteTestDatabase(prefix: "s13-sequence-admission")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let admitted = try catalog(unknownLiteral: true)
        try await publish(admitted, in: container, key: "initial")
        let generation = container.schemaGeneration
        let missing = try SQLCatalog(namespaces: admitted.namespaces, relations: admitted.relations,
            sequences: admitted.sequences.filter { $0.id != "app-sequence-id" })
        await #expect(throws: SQLSequenceExecutionError.unknownSequence("dep")) {
            try await publish(missing, in: container, key: "missing")
        }
        let replacement = try SQLSequence(id: "replacement-id", namespaceID: "app-id", name: "dep",
            start: 500, increment: 1, minimum: 1, maximum: 1000)
        let retargeted = try SQLCatalog(namespaces: admitted.namespaces, relations: admitted.relations,
            sequences: admitted.sequences.filter { $0.id != "app-sequence-id" } + [replacement])
        await #expect(throws: SQLConstraintError.invalidDeclaration(relation: "items-id",
            reason: "unchanged default cannot retarget a sequence identity")) {
            try await publish(retargeted, in: container, key: "retargeted")
        }
        #expect(container.schemaGeneration == generation)
        #expect(container.sqlCatalog == admitted)
        _ = try await execute(container, [insert("survives")])
        #expect(try await values("survives", in: container) == row(10, 100))
    }

    @Test("Referential SET DEFAULT uses the declaring sequence identity")
    func referentialSetDefaultUsesBoundIdentity() async throws {
        let database = try SQLiteTestDatabase(prefix: "s13-sequence-set-default")
        defer { database.remove() }
        let container = try await open(database)
        defer { await container.shutdown() }
        let original = try catalog(unknownLiteral: true)
        let declaration = original.relations[0]
        let unique = try SQLConstraint(attributes: SQLConstraintAttributes(id: "stable-unique", name: "stable_unique"),
            kind: .unique(columnIDs: ["stable"], nulls: .distinct))
        let foreign = try SQLConstraint(attributes: SQLConstraintAttributes(id: "own-fk", name: "own_fk"),
            kind: .foreignKey(SQLForeignKey(columnIDs: ["own"], referencedRelationID: declaration.id,
                referencedColumnIDs: ["stable"], onDelete: .setDefault)))
        let relation = try SQLRelation(id: declaration.id, namespaceID: declaration.namespaceID,
            name: declaration.name, entityName: declaration.entityName, columns: declaration.columns,
            primaryKeyColumnIDs: declaration.primaryKeyColumnIDs, constraints: [unique, foreign])
        try await publish(SQLCatalog(namespaces: original.namespaces, relations: [relation],
            sequences: original.sequences), in: container, key: "initial")
        for (id, own, stable) in [("parent", Expression.literal(.null), Int64(9)),
                                  ("destination", .literal(.null), 10), ("child", .int(9), 777)] {
            _ = try await execute(container, [.statement(.insert(InsertQuery(target: table,
                columns: ["id", "own", "other", "stable"],
                source: .values([[.string(id), own, .literal(.null), .int(stable)]]))))])
        }
        _ = try await execute(container, [.statement(.delete(DeleteQuery(target: table,
            filter: .equal(.col("id"), .string("parent")))))])
        #expect(try await values("child", in: container) == [.primitive(.int64(10)), .primitive(.null), .primitive(.int64(777))])
    }

    private var table: TableRef { TableRef(schema: "app", table: "items") }

    private func catalog(unknownLiteral: Bool) throws -> SQLCatalog {
        func next(_ name: String) -> Expression {
            .function(FunctionCall(name: "nextval", arguments: [unknownLiteral ? .literal(.sqlUnknownString(name)) : .string(name)]))
        }
        let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "own", name: "own", backing: .field(number: 2), policy: .input(defaultExpression: next("dep"))),
            SQLColumn(id: "other", name: "other", backing: .field(number: 3), policy: .input(defaultExpression: next("public.dep"))),
            SQLColumn(id: "stable", name: "stable", backing: .field(number: 4), policy: .input(defaultExpression: .int(777)))]
        let check = try SQLConstraint(attributes: SQLConstraintAttributes(id: "positive", name: "positive"),
            kind: .check(.greaterThan(.col("stable"), .int(0))))
        let relation = try SQLRelation(id: "items-id", namespaceID: "app-id", name: "items", entityName: "SequenceItem",
            columns: columns, primaryKeyColumnIDs: ["id"], constraints: [check])
        return try SQLCatalog(namespaces: [SQLNamespace(id: "public-id", name: "public"),
            SQLNamespace(id: "app-id", name: "app"), SQLNamespace(id: "other-id", name: "other")], relations: [relation],
            sequences: [SQLSequence(id: "app-sequence-id", namespaceID: "app-id", name: "dep", start: 10,
                increment: 1, minimum: 1, maximum: 1000),
                SQLSequence(id: "public-sequence-id", namespaceID: "public-id", name: "dep", start: 100,
                    increment: 1, minimum: 1, maximum: 1000),
                SQLSequence(id: "unrelated-id", namespaceID: "public-id", name: "unrelated", start: 1,
                    increment: 1, minimum: 1, maximum: 1000)])
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        let fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "own", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "other", fieldNumber: 3, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "stable", fieldNumber: 4, type: .int64, isOptional: true, queryType: .sql(.bigint))]
        let schema = try Schema(entities: [Schema.Entity(name: "SequenceItem", identifierType: .string,
            fields: fields, directoryComponents: [.staticPath("s13-sequence-dependencies")])])
        return try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity:
                .init(identifier: "s13-sequence-dependencies", revision: 1), schema: schema), security: .testingDisabled)
    }

    private func restore(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity:
                .init(identifier: "s13-sequence-dependencies", revision: 1), schema: schema)
        }
    }

    private func publish(_ catalog: SQLCatalog, in container: DBContainer, key: String) async throws {
        _ = try await container.publishSQLCatalog(catalog, expectedGeneration: container.schemaGeneration,
            idempotencyKey: key, authorization: TestBaseEnvironment.authorization)
    }

    private func insert(_ id: String) -> SQLBlockStep {
        .statement(.insert(InsertQuery(target: table, columns: ["id"], source: .values([[.string(id)]]))))
    }

    private func execute(_ container: DBContainer, _ steps: [SQLBlockStep],
        searchPath: SQLSearchPath = .default
    ) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps),
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000,
                maximumDepth: 16, maximumRetainedBytes: 4 * 1_024 * 1_024,
                maximumWorkUnits: 1_000_000, maximumReadPageRows: 128),
                searchPath: searchPath))
    }

    private func values(_ id: String, in container: DBContainer) async throws -> [QueryValue] {
        let result = try await execute(container, [.statement(.select(SelectQuery(projection: [
            .column("own"), .column("other"), .column("stable")], source: .table(table),
            filter: .equal(.col("id"), .string(id)))))])
        guard case .rows(let response) = result.results.first,
              let row = response.rows.first else { throw SQLSequenceExecutionError.invalidPersistedState }
        return row.values
    }

    private func row(_ own: Int64?, _ other: Int64) -> [QueryValue] {
        [.primitive(own.map(FieldValue.int64) ?? .null), .primitive(.int64(other)), .primitive(.int64(777))]
    }
}
#endif
