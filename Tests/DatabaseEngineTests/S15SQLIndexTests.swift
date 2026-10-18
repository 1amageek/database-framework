import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S15 transactional SQL indexes", .timeLimit(.minutes(1)))
struct S15SQLIndexTests {
    @Test("Composite unique indexes build existing rows and participate in inferred conflict targets")
    func compositeBuildAndConflict() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 2), insert("two", 1, 3), create("pair", [.col("a"), .col("b")], unique: true)], container)
        await #expect(throws: SQLConstraintError.self) { _ = try await block([insert("duplicate", 1, 2)], container) }
        _ = try await block([insert("ignored", 1, 2, conflict: .columns(["b", "a"], where: nil)), insert("three", 1, 4)], container)
        #expect(try await ids(container) == ["one", "three", "two"])
    }

    @Test("Duplicate backfill failure leaves data, catalog and generation unchanged")
    func duplicateBuildRollback() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 1), insert("two", 1, 2)], container)
        let generation = container.schemaGeneration
        await #expect(throws: SQLConstraintError.self) {
            _ = try await block([create("unique_a", [.col("a")], unique: true)], container)
        }
        #expect(container.schemaGeneration == generation)
        #expect(container.acquirePublishedSchemaLease().sqlCatalog.catalog.indexes.isEmpty)
        #expect(try await ids(container) == ["one", "two"])
        _ = try await block([create("nonunique", [.col("a")]), insert("three", 1, 3)], container)
        #expect(try await ids(container) == ["one", "three", "two"])
    }

    @Test("Partial membership is maintained and unsafe predicates retain full table results")
    func partialScanAndMaintenance() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let positive = Expression.greaterThan(.col("b"), .int(0))
        _ = try await block([insert("outside", 7, -1), insert("inside", 7, 1),
            create("positive_a", [.col("a")], unique: true, predicate: positive)], container)
        #expect(try await ids(container, filter: positive) == ["inside"])
        #expect(try await ids(container, filter: .lessThan(.col("b"), .int(0))) == ["outside"])
        #expect(try await ids(container) == ["inside", "outside"])
        func parameterizedIDs(_ floor: Int64) async throws -> [String] {
            let query = SelectQuery(projection: [.expression(.col("id"))], source: .table(TableRef("items")),
                filter: .greaterThan(.col("b"), .parameter(name: "floor")), orderBy: [SortKey(.col("id"))])
            let result = try await block([.statement(.select(query))], container,
                parameters: [QueryParameter(position: 1, name: "floor", value: .primitive(.int64(floor)))])
            guard case .rows(let rows) = result.results.first else { throw StorageFrameError.invalidValue }
            return try rows.rows.map {
                guard case .primitive(.string(let id)) = $0.values[0] else { throw StorageFrameError.invalidValue }
                return id
            }
        }
        #expect(try await parameterizedIDs(-2) == ["inside", "outside"])
        _ = try await block([create("full_b", [.col("b")])], container)
        #expect(try await parameterizedIDs(0) == ["inside"])
        _ = try await block([ddl(.dropIndex(names: [TableRef("full_b")], ifExists: false, behavior: .restrict))], container)
        _ = try await block([insert("ignored", 7, 1, conflict: .columns(["a"], where: positive))], container)
        await #expect(throws: SQLMutationError.self) {
            _ = try await block([insert("wrong-target", 7, 1, conflict: .columns(["a"], where: nil))], container)
        }
        await #expect(throws: SQLConstraintError.self) {
            _ = try await block([.statement(.update(UpdateQuery(target: TableRef("items"),
                assignments: [Assignment(column: "b", value: .int(2))], filter: .equal(.col("id"), .string("outside")))))], container)
        }
        #expect(try await ids(container, filter: positive) == ["inside"])
        _ = try await block([.statement(.delete(DeleteQuery(target: TableRef("items"), filter: .equal(.col("id"), .string("inside"))))),
            .statement(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "b", value: .int(2))],
                filter: .equal(.col("id"), .string("outside")))))], container)
        #expect(try await ids(container, filter: positive) == ["outside"])

        let narrowed = try await open()
        defer { await narrowed.shutdown() }
        let definition = SelectQuery(projection: [.expression(.col("a"))], source: .table(TableRef("items")))
        _ = try await block([ddl(.createView(SQLCreateView(name: TableRef("narrow"), query: definition))),
            ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("materialized"),
                query: definition, withData: false))), create("unreferenced_b", [.col("b")])], narrowed)
        func selectedValues(_ name: String) async throws -> [[QueryValue]] {
            let result = try await block([.statement(.select(SelectQuery(projection: [.wildcard()],
                source: .table(TableRef(name)), orderBy: [SortKey(.col("a"))])))], narrowed)
            guard case .rows(let rows) = result.results.first else { throw StorageFrameError.invalidValue }
            #expect(rows.columns.map { $0.name } == ["a"])
            return rows.rows.map { $0.values }
        }
        #expect(try await selectedValues("narrow").isEmpty)
        _ = try await block([ddl(.refreshMaterializedView(name: TableRef("materialized"), withData: true))], narrowed)
        #expect(try await selectedValues("materialized").isEmpty)
        _ = try await block([insert("first", 11, -1), insert("second", 22, 1)], narrowed)
        #expect(try await selectedValues("narrow") == [[.primitive(.int64(11))], [.primitive(.int64(22))]])
        _ = try await block([ddl(.refreshMaterializedView(name: TableRef("materialized"), withData: true))], narrowed)
        #expect(try await selectedValues("materialized") == [[.primitive(.int64(11))], [.primitive(.int64(22))]])
        _ = try await block([ddl(.dropIndex(names: [TableRef("unreferenced_b")], ifExists: false, behavior: .restrict)),
            create("unreferenced_predicate_b", [.col("a")], predicate: positive)], narrowed)
        #expect(try await selectedValues("narrow") == [[.primitive(.int64(11))], [.primitive(.int64(22))]])
        _ = try await block([ddl(.refreshMaterializedView(name: TableRef("materialized"), withData: true))], narrowed)
        #expect(try await selectedValues("materialized") == [[.primitive(.int64(11))], [.primitive(.int64(22))]])
    }

    @Test("Expression unique indexes support untargeted DO NOTHING and SQL NULL distinctness")
    func expressionAndNulls() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let lower = Expression.function(FunctionCall(name: "lower", arguments: [.col("tag")]))
        _ = try await block([create("folded", [lower], unique: true), insert("one", 1, 1, tag: "Alpha"),
            insert("ignored", 2, 2, tag: "ALPHA", doNothing: true), insert("null-one", nil, nil, tag: nil),
            insert("null-two", nil, nil, tag: nil)], container)
        #expect(try await ids(container) == ["null-one", "null-two", "one"])
        _ = try await block([ddl(.dropIndex(names: [TableRef("folded")], ifExists: false, behavior: .restrict)),
            .statement(.delete(DeleteQuery(target: TableRef("items"), filter: .equal(.col("id"), .string("null-two"))))),
            create("null_equal", [.col("a")], unique: true, nulls: .notDistinct)], container)
        _ = try await block([insert("ignored-null", nil, 4, doNothing: true)], container)
        await #expect(throws: SQLConstraintError.self) { _ = try await block([insert("duplicate-null", nil, 4)], container) }
        #expect(try await ids(container) == ["null-one", "one"])
    }

    @Test("Savepoint rollback restores dropped and created physical generations")
    func savepointIndexLifecycle() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 1), create("unique_a", [.col("a")], unique: true)], container)
        let original = try #require(container.acquirePublishedSchemaLease().sqlCatalog.catalog.indexes.first)
        _ = try await block([.savepoint("before"),
            ddl(.dropIndex(names: [TableRef("unique_a")], ifExists: false, behavior: .restrict)),
            create("temporary_b", [.col("b")]), insert("rolled-back", 1, 2), .rollbackTo("before")], container)
        #expect(container.acquirePublishedSchemaLease().sqlCatalog.catalog.indexes == [original])
        #expect(try await ids(container) == ["one"])
        await #expect(throws: SQLConstraintError.self) { _ = try await block([insert("duplicate", 1, 3)], container) }
        _ = try await block([ddl(.dropIndex(names: [TableRef(schema: "public", table: "unique_a")], ifExists: false, behavior: .restrict)),
            insert("allowed", 1, 3)], container)
        #expect(try await ids(container) == ["allowed", "one"])
    }

    @Test("A retained index catalog cannot read a retired generation from a newer storage snapshot")
    func retiredIndexReadFails() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 2), create("indexed", [.col("a")])], container)
        let lease = container.acquirePublishedSchemaLease()
        let policy = DatabaseReadPolicy(schemaLease: lease, authorization: TestBaseEnvironment.authorization)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let resolution = try SQLReadResolution.make(policy: policy, searchPath: .default, workMeter: meter)
        let options = ReadExecutionContext(monotonicClock: container.monotonicClock, workMeter: meter).withSQLResolution(resolution)
        let query = SelectQuery(projection: [.wildcard()], source: .table(TableRef("items")))
        let requirement = try DatabaseReadPolicy.listRequirement(entityName: "Item", selectQuery: query,
            schema: lease.schema, sqlResolution: resolution)
        _ = try await block([ddl(.dropIndex(names: [TableRef("indexed")], ifExists: false, behavior: .restrict))], container)
        let context = container.testBaseContext()
        await #expect(throws: DatabaseSchemaPublicationError.generationConflict(expected: container.schemaGeneration, actual: lease.generation)) {
            _ = try await context.withStorageAccess(requiredAccess: .read, configuration: .readOnly) { access in
                let transaction = DatabaseReadTransaction(storageAccess: .admittedReadAccess(access))
                _ = try await context.sqlIndexSourceRows(tableRef: TableRef("items"), options: options,
                    transaction: transaction, authorizationQuery: query, authorizationRequirement: requirement)
                return true
            }
        }
        #expect(try await ids(container) == ["one"])
    }

    @Test("Partial index binding and conflict inference survive column rename")
    func partialRename() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 1), create("positive_a", [.col("a")], unique: true,
            predicate: .greaterThan(.col("b"), .int(0))),
            ddl(.alterTable(SQLAlterTable(name: TableRef("items"), actions: [.renameColumn(name: "b", to: "active")])))], container)
        let predicate = Expression.greaterThan(.col("active"), .int(0))
        _ = try await block([.statement(.insert(InsertQuery(target: TableRef("items"), columns: ["id", "a", "active"],
            source: .values([[.string("ignored"), .int(1), .int(1)]]), onConflict: .doNothing,
            conflictTarget: .columns(["a"], where: predicate))))], container)
        #expect(try await ids(container, filter: predicate) == ["one"])
        #expect(try await ids(container) == ["one"])
    }

    @Test("Rich expression keys use SQL identity during persisted build")
    func richSemanticKeys() async throws {
        let cases: [(Expression, String, String)] = [
            (.cast(.col("tag"), targetType: .char(length: nil)), "x", "x  "),
            (.cast(.col("tag"), targetType: .jsonb), "{\"a\":1,\"b\":2}", "{\"b\":2.0,\"a\":1.00}"),
            (.array([.col("a"), .col("b")]), "first", "second"),
            (.row([.col("a"), .col("b")]), "first", "second")
        ]
        for (expression, first, second) in cases {
            let container = try await open()
            defer { await container.shutdown() }
            _ = try await block([insert("one", 1, 2, tag: first), insert("two", 1, 2, tag: second)], container)
            await #expect(throws: SQLConstraintError.self) {
                _ = try await block([create("rich_unique", [expression], unique: true)], container)
            }
            #expect(container.acquirePublishedSchemaLease().sqlCatalog.catalog.indexes.isEmpty)
            #expect(try await ids(container) == ["one", "two"])
        }
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([insert("one", 1, 2), insert("two", 2, 3),
            .statement(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "ts",
                value: .cast(.string("2024-01-01 00:00:00+00"), targetType: .timestamp(withTimeZone: true)))])))], container)
        await #expect(throws: SQLConstraintError.self) {
            _ = try await block([create("temporal_unique", [.col("ts")], unique: true)], container)
        }
        await #expect(throws: SQLConstraintError.self) {
            _ = try await block([create("unstable", [.cast(.col("tag"), targetType: .timestamp(withTimeZone: true))])], container)
        }
        #expect(container.acquirePublishedSchemaLease().sqlCatalog.catalog.indexes.isEmpty)
    }

    @Test("Index catalog codecs preserve complete declarations and reject truncated frames")
    func catalogCodec() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        _ = try await block([create("partial", [.col("a"), .col("b")], unique: true,
            predicate: .greaterThan(.col("a"), .int(0)), nulls: .notDistinct)], container)
        let snapshot = container.acquirePublishedSchemaLease().sqlCatalog
        let bytes = try SQLCatalogCodec.encode(snapshot)
        #expect(try SQLCatalogCodec.decode(bytes).catalog == snapshot.catalog)
        for count in 0..<bytes.count {
            #expect(throws: (any Error).self) { _ = try SQLCatalogCodec.decode(bytes.prefix(count)) }
        }
    }

    @Test("Foreign keys bind standalone unique indexes and DROP CASCADE preserves child data")
    func foreignKeyIndexDependency() async throws {
        let container = try await open()
        defer { await container.shutdown() }
        let child = SQLCreateTable(name: TableRef("children"), source: .columns([
            SQLColumnDefinition(name: "id", type: .text), SQLColumnDefinition(name: "parent", type: .bigint)
        ], constraints: [SQLTableConstraint(name: "parent_fk", kind: .foreignKey(SQLForeignKeyDefinition(
            columns: ["parent"], referencedTable: TableRef("items"), referencedColumns: ["a"])))]))
        func childInsert(_ id: String, _ parent: Int64) -> SQLBlockStep {
            .statement(.insert(InsertQuery(target: TableRef("children"), columns: ["id", "parent"],
                source: .values([[.string(id), .int(parent)]]))))
        }
        _ = try await block([insert("parent", 7, 1), create("parent_a", [.col("a")], unique: true),
            ddl(.createTable(child)), childInsert("child", 7)], container)
        let catalog = container.acquirePublishedSchemaLease().sqlCatalog.catalog
        let index = try #require(catalog.indexes.first { $0.name == "parent_a" })
        let relation = try #require(catalog.relations.first { $0.name == "children" })
        let constraint = try #require(relation.constraints.first { $0.name == "parent_fk" })
        guard case .foreignKey(let foreign) = constraint.kind else { throw StorageFrameError.invalidValue }
        #expect(foreign.referencedUniqueIndexID == index.id)
        await #expect(throws: SQLConstraintError.self) { _ = try await block([childInsert("invalid", 99)], container) }
        await #expect(throws: SQLDDLExecutionError.dependentObject("parent_fk")) {
            _ = try await block([ddl(.dropIndex(names: [TableRef("parent_a")], ifExists: false, behavior: .restrict))], container)
        }
        _ = try await block([ddl(.dropIndex(names: [TableRef("parent_a")], ifExists: false, behavior: .cascade)),
            childInsert("now-valid", 99)], container)
        let remaining = container.acquirePublishedSchemaLease().sqlCatalog.catalog
        #expect(remaining.indexes.isEmpty)
        #expect(remaining.relations.first { $0.id == relation.id }?.constraints.isEmpty == true)
        let result = try await block([.statement(.select(SelectQuery(projection: [.expression(.col("id"))],
            source: .table(TableRef("children")), orderBy: [SortKey(.col("id"))])))], container)
        guard case .rows(let rows) = result.results.first else { throw StorageFrameError.invalidValue }
        #expect(rows.rows.map { $0.values } == [[.primitive(.string("child"))], [.primitive(.string("now-valid"))]])
        #expect(try await ids(container) == ["parent"])
    }

    private func create(_ name: String, _ expressions: [Expression], unique: Bool = false,
        predicate: Expression? = nil, nulls: SQLNullUniqueness = .distinct) -> SQLBlockStep {
        ddl(.createIndex(SQLCreateIndex(name: TableRef(name), table: TableRef("items"),
            keys: expressions.map { SortKey($0) }, predicate: predicate, unique: unique, nulls: nulls)))
    }

    private func ddl(_ statement: SQLDDLStatement) -> SQLBlockStep {
        .statement(.ddl(statement))
    }

    private func insert(_ id: String, _ a: Int64?, _ b: Int64?, tag: String? = "tag",
        conflict: SQLConflictTarget? = nil, doNothing: Bool = false) -> SQLBlockStep {
        .statement(.insert(InsertQuery(target: TableRef("items"), columns: ["id", "a", "b", "tag"],
            source: .values([[.string(id), a.map(Expression.int) ?? .null,
                b.map(Expression.int) ?? .null, tag.map(Expression.string) ?? .null]]),
            onConflict: doNothing || conflict != nil ? .doNothing : nil, conflictTarget: conflict)))
    }

    private func block(_ steps: [SQLBlockStep], _ container: DBContainer,
        parameters: [QueryParameter] = []) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps), parameters: parameters,
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(
                maximumEntries: 10_000, maximumDepth: 8, maximumRetainedBytes: 8_000_000,
                maximumWorkUnits: 1_000_000, maximumReadPageRows: 128)))
    }

    private func ids(_ container: DBContainer, filter: Expression? = nil) async throws -> [String] {
        let result = try await block([.statement(.select(SelectQuery(projection: [.expression(.col("id"))],
            source: .table(TableRef("items")), filter: filter, orderBy: [SortKey(.col("id"))])))], container)
        guard case .rows(let response) = result.results.first else { throw StorageFrameError.invalidValue }
        return try response.rows.map { row in
            guard case .primitive(.string(let id)) = row.values[0] else { throw StorageFrameError.invalidValue }
            return id
        }
    }

    private func open() async throws -> DBContainer {
        let fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "a", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "b", fieldNumber: 3, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "tag", fieldNumber: 4, type: .string, isOptional: true, queryType: .sql(.text)),
            FieldSchema(name: "ts", fieldNumber: 5, type: .timestamp, isOptional: true, queryType: .sql(.timestamp(withTimeZone: true)))]
        let schema = try Schema(entities: [Schema.Entity(name: "Item", identifierType: .string, fields: fields,
            directoryComponents: [.staticPath("s15-sql-index-items")])], version: .init(1, 0, 0))
        let container = try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s15-sql-index-tests", revision: 1), schema: schema),
            security: .testingDisabled)
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item",
            columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "a", name: "a", fieldNumber: 2),
                SQLColumn(id: "b", name: "b", fieldNumber: 3), SQLColumn(id: "tag", name: "tag", fieldNumber: 4),
                SQLColumn(id: "ts", name: "ts", fieldNumber: 5)])
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [relation]),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "s15-sql-index-initial", authorization: TestBaseEnvironment.authorization)
        return container
    }
}
