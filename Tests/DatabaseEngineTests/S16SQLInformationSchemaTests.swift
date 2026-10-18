import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import QueryAST
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S16MetadataSource: SecurityPolicy {
    #Directory<S16MetadataSource>("s16-metadata-source")
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

@Suite("S16 information schema", .timeLimit(.minutes(1)))
struct S16SQLInformationSchemaTests {
    @Test("Every empty virtual relation has stable typed columns and no physical entity")
    func emptyTypedRelations() async throws {
        let container = try await open(); defer { await container.shutdown() }
        for descriptor in SQLInformationSchemaRelation.allCases {
            let result = try await select(container, descriptor.rawValue)
            #expect(result.rows.isEmpty)
            #expect(result.columns == (try descriptor.columns))
        }
        #expect(container.schema.entities.isEmpty)
        #expect(!container.sqlCatalog.namespaces.contains { $0.name == "information_schema" })
        await #expect(throws: Error.self) { _ = try await select(container, "not_a_relation") }
    }

    @Test("Own DDL and savepoint restoration use the current catalog candidate")
    func ownDDLAndRollback() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let query = QueryStatement.select(metadata("tables", projection: [.column("table_name")]))
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .statement(table("kept")), .savepoint("before"), .statement(table("abandoned")),
            .statement(query), .rollbackTo("before"), .statement(query)]), options: options())
        #expect(Set(try response(result, 3).rows.map { $0.values }) == Set([
            [.primitive(.string("kept"))], [.primitive(.string("abandoned"))]]))
        #expect(try response(result, 5).rows.map(\.values) == [[.primitive(.string("kept"))]])
        #expect(try await select(container, "tables", table: "kept").rows.count == 1)
        #expect(try await select(container, "tables", table: "abandoned").rows.isEmpty)
        await #expect(throws: Error.self) {
            _ = try await run(container, [table("failed"), .select(SelectQuery(
                projection: [.expression(.divide(.int(1), .int(0)))], source: .values([[]])))])
        }
        #expect(try await select(container, "tables", table: "failed").rows.isEmpty)
    }

    @Test("Dynamic columns retain order, types, defaults, generated and identity sequence attributes")
    func columnsAndSequences() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let definition = SQLCreateTable(name: TableRef("typed"), source: .columns([
            SQLColumnDefinition(name: "identity", type: .integer, policy: .identity(generation: .always, options: SQLSequenceOptions(start: 8, increment: 3))),
            SQLColumnDefinition(name: "value", type: .varchar(length: 12), policy: .input(defaultExpression: .string("ready"))),
            SQLColumnDefinition(name: "computed", type: .integer, policy: .generated(.add(.col("identity"), .int(1)), stored: false))], constraints: []))
        _ = try await run(container, [.ddl(.createTable(definition)),
            .ddl(.createSequence(SQLCreateSequence(name: TableRef("counting"), options: SQLSequenceOptions(type: .smallint, start: 4, increment: 2))))])
        let columns = try records(await select(container, "columns", table: "typed"))
        let identity = try #require(columns.first { $0["column_name"] == .primitive(.string("identity")) })
        #expect(identity["ordinal_position"] == .primitive(.int32(1)))
        #expect(identity["data_type"] == .primitive(.string("integer")))
        #expect(identity["identity_generation"] == .primitive(.string("ALWAYS")))
        #expect(identity["identity_start"] == .primitive(.string("8")))
        #expect(identity["identity_increment"] == .primitive(.string("3")))
        let value = try #require(columns.first { $0["column_name"] == .primitive(.string("value")) })
        #expect(value["character_maximum_length"] == .primitive(.int32(12)))
        // Literal.string is typed TEXT; canonical SQL preserves its declared type.
        #expect(value["column_default"] == .primitive(.string("CAST('ready' AS TEXT)")))
        let generated = try #require(columns.first { $0["column_name"] == .primitive(.string("computed")) })
        #expect(generated["is_generated"] == .primitive(.string("ALWAYS")))
        #expect(generated["generation_expression"] != .primitive(.null))
        let sequences = try records(await select(container, "sequences"))
        let sequence = try #require(sequences.first { $0["sequence_name"] == .primitive(.string("counting")) })
        #expect(sequence["data_type"] == .primitive(.string("smallint")))
        #expect(sequence["start_value"] == .primitive(.string("4")))
        #expect(sequence["numeric_precision"] == .primitive(.int32(16)))
    }

    @Test("Constraint rows expose real primary, unique, not-null, check and foreign-key bindings")
    func constraintsAndKeys() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let parent = SQLCreateTable(name: TableRef("parents"), source: .columns([
            SQLColumnDefinition(name: "a", type: .integer), SQLColumnDefinition(name: "b", type: .integer)], constraints: [
            SQLTableConstraint(name: "parent_key", kind: .primaryKey(columns: ["a", "b"]))]))
        let child = SQLCreateTable(name: TableRef("children"), source: .columns([
            SQLColumnDefinition(name: "a", type: .integer), SQLColumnDefinition(name: "b", type: .integer)], constraints: [
            SQLTableConstraint(name: "child_unique", kind: .unique(columns: ["a", "b"], nulls: .notDistinct)),
            SQLTableConstraint(name: "required_a", kind: .notNull(column: "a")),
            SQLTableConstraint(name: "positive", kind: .check(.greaterThan(.col("a"), .int(0)))),
            SQLTableConstraint(name: "parent_reference", timing: .deferrable(initiallyDeferred: true), kind: .foreignKey(
                SQLForeignKeyDefinition(columns: ["b", "a"], referencedTable: TableRef("parents"), referencedColumns: ["b", "a"],
                    match: .full, onUpdate: .cascade, onDelete: .restrict)))]))
        _ = try await run(container, [.ddl(.createTable(parent)), .ddl(.createTable(child))])
        let constraints = try records(await select(container, "table_constraints", table: "children"))
        #expect(constraints.count == 4)
        for (name, kind) in [("child_unique", "UNIQUE"), ("required_a", "CHECK"),
            ("positive", "CHECK"), ("parent_reference", "FOREIGN KEY")] {
            let named = constraints.filter { $0["constraint_name"] == .primitive(.string(name)) }
            #expect(named.count == 1)
            #expect(named.first?["constraint_type"] == .primitive(.string(kind)))
        }
        let reference = try #require(constraints.first { $0["constraint_name"] == .primitive(.string("parent_reference")) })
        #expect(reference["is_deferrable"] == .primitive(.string("YES")))
        #expect(reference["initially_deferred"] == .primitive(.string("YES")))
        let keys = try records(await select(container, "key_column_usage", table: "children"))
            .filter { $0["constraint_name"] == .primitive(.string("parent_reference")) }
        #expect(keys.count == 2)
        #expect(keys.first?["column_name"] == .primitive(.string("b")))
        #expect(keys.first?["position_in_unique_constraint"] == .primitive(.int32(2)))
        let references = try records(await select(container, "referential_constraints"))
        #expect(references.count == 1)
        #expect(references.first?["unique_constraint_name"] == .primitive(.string("parent_key")))
        #expect(references.first?["match_option"] == .primitive(.string("FULL")))
        #expect(references.first?["update_rule"] == .primitive(.string("CASCADE")))
        #expect(references.first?["delete_rule"] == .primitive(.string("RESTRICT")))
    }

    @Test("Ordinary views expose canonical definitions while materialized objects and backing names stay absent")
    func viewsAndMaterializedExclusion() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let definition = SelectQuery(projection: [.column("a")], source: .table(TableRef("items")))
        _ = try await run(container, [table("items"),
            .ddl(.createView(SQLCreateView(name: TableRef("visible_view"), query: definition))),
            .ddl(.createMaterializedView(SQLCreateMaterializedView(name: TableRef("materialized"), query: definition)))])
        let views = try records(await select(container, "views"))
        #expect(views.count == 1)
        #expect(views.first?["table_name"] == .primitive(.string("visible_view")))
        #expect(views.first?["view_definition"] == .primitive(.string(try SQLSerializer().serialize(container.sqlCatalog.views[0].definition.query))))
        let tables = try records(await select(container, "tables"))
        #expect(Set(tables.compactMap { $0["table_name"] }) == Set(["items", "visible_view"].map { QueryValue.primitive(.string($0)) }))
        #expect(try await select(container, "columns", table: "materialized").rows.isEmpty)
        #expect(try await select(container, "columns", table: "visible_view").rows.count == 1)
        let backingID = try #require(container.sqlCatalog.materializedViews.first?.backingRelationID)
        let backing = try #require(container.sqlCatalog.relations.first { $0.id == backingID })
        #expect(try await select(container, "columns", table: backing.name).rows.isEmpty)
    }

    @Test("Existing principal and field policies hide static metadata and dependent view definitions")
    func metadataVisibility() async throws {
        let schema = try Schema(entities: [S16MetadataSource.schemaEntity])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s16-metadata-security", revision: 1),
            entityRuntimes: [DatabaseFrameworkRuntime.entity(S16MetadataSource.self)],
            authorizationPolicies: [AuthorizationPolicyHandler(S16MetadataSource.self)])
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        #if MultiBase
        // DDL setup needs persisted Grants; entity and field policies remain enabled.
        try await container.grantTestBaseAccess(to: .principal("metadata-reader"), access: .all)
        try await container.grantTestDatabaseAccess(to: .principal("metadata-reader"), access: .administer)
        #endif
        func authorization(_ roles: Set<String>) -> AuthorizationContext {
            .authenticated(Principal(identifier: "metadata-reader", roles: roles.union(["admin", "test-runner"])))
        }
        let full = authorization(["source-reader", "secret-reader"])
        let view = QueryStatement.ddl(.createView(SQLCreateView(name: TableRef("secret_view"),
            query: SelectQuery(projection: [.column("secret")], source: .table(TableRef(S16MetadataSource.persistableType))))))
        let metadataView = QueryStatement.ddl(.createView(SQLCreateView(name: TableRef("column_catalog"),
            query: metadata("columns", projection: [.column("table_name"), .column("column_name")], table: S16MetadataSource.persistableType))))
        _ = try await run(container, [view, metadataView, .ddl(.createSequence(SQLCreateSequence(name: TableRef("secret_sequence"),
            ownedBy: .column(table: TableRef(S16MetadataSource.persistableType), column: "secret"))))], authorization: full)
        let fieldLimited = authorization(["source-reader"])
        let visibleColumns = try records(await select(container, "columns", table: S16MetadataSource.persistableType, authorization: fieldLimited))
        #expect(Set(visibleColumns.compactMap { $0["column_name"] }) == Set(["id", "title"].map { QueryValue.primitive(.string($0)) }))
        let visibleViews = try records(await select(container, "views", authorization: fieldLimited))
        #expect(visibleViews.count == 1)
        #expect(visibleViews.first?["table_name"] == .primitive(.string("column_catalog")))
        let selectedView = try response(await run(container, [.select(SelectQuery(projection: [.wildcard()],
            source: .table(TableRef("column_catalog"))))], authorization: fieldLimited), 0)
        #expect(Set(records(selectedView).compactMap { $0["column_name"] }) == Set(["id", "title"].map { QueryValue.primitive(.string($0)) }))
        #expect(try await select(container, "tables", table: "secret_view", authorization: fieldLimited).rows.isEmpty)
        #expect(try await select(container, "columns", table: S16MetadataSource.persistableType, authorization: authorization([])).rows.isEmpty)
        #expect(try await select(container, "tables", table: S16MetadataSource.persistableType, authorization: full).rows.count == 1)
        #expect(try await select(container, "views", authorization: full).rows.count == 2)
        #expect(try await select(container, "sequences", authorization: fieldLimited).rows.isEmpty)
        #expect(try await select(container, "sequences", authorization: full).rows.count == 1)
    }

    @Test("Policy operational failures propagate instead of producing empty metadata")
    func operationalPolicyFailure() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table("items")])
        let old = container.acquirePublishedSchemaLease()
        let generation = DatabaseSchemaGeneration(identifier: old.generation, fingerprint: old.fingerprint,
            indexPhysicalFingerprint: old.indexPhysicalFingerprint, executionRuntimeFingerprint: old.executionRuntimeFingerprint,
            schema: old.schema, sqlCatalog: old.sqlCatalog, runtimeConfiguration: old.runtimeConfiguration,
            indexPhysicalLayouts: old.indexPhysicalLayouts, directoryLayers: old.directoryLayers,
            securityDelegate: S16MetadataFailureDelegate())
        let lease = DatabaseSchemaLease(generation, token: DatabaseSchemaLeaseToken {})
        let policy = DatabaseReadPolicy(schemaLease: lease, authorization: .anonymous)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let resolution = try SQLReadResolution.make(policy: policy, searchPath: .default, workMeter: meter)
        #expect(throws: S16MetadataFailure.unavailable) {
            try SQLInformationSchemaRelation.tables.forEachRow(resolution: resolution) { _ in Issue.record("Unexpected row") }
        }
    }

    @Test("Virtual sources use ordinary filtering and explicit search paths without physical storage")
    func filteringAndSearchPath() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table("items"), table("other")])
        let query = SelectQuery(projection: [.column("table_name")], source: .table(TableRef("tables")),
            filter: .equal(.col("table_name"), .string("items")))
        let result = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(.select(query))]), options: options(searchPath: SQLSearchPath(entries: [.schema("information_schema")])))
        #expect(try response(result, 0).rows.map(\.values) == [[.primitive(.string("items"))]])
        #expect(!container.schema.entities.contains { $0.name == "tables" })
        _ = try await run(container, [table("tables"), .ddl(.createView(SQLCreateView(name: TableRef("captured"),
            query: SelectQuery(projection: [.column("a")], source: .table(TableRef("tables"))))))])
        let captured = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(.select(SelectQuery(projection: [.wildcard()],
                source: .table(TableRef(schema: "public", table: "captured")))))]),
            options: options(searchPath: SQLSearchPath(entries: [.schema("information_schema")])))
        #expect(try response(captured, 0).columns.map(\.name) == ["a"])
    }

    @Test("Durable metadata views retain typed virtual sources and frozen source ordinals")
    func durableMetadataView() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let query = metadata("columns", projection: [.column("column_name")], table: "items")
        let countQuery = SelectQuery(projection: [.expression(.aggregate(.count(nil, distinct: false)), alias: "total")],
            source: .table(TableRef(schema: "information_schema", table: "tables")))
        _ = try await run(container, [table("items"),
            .ddl(.createView(SQLCreateView(name: TableRef("metadata_columns"), query: query))),
            .ddl(.createView(SQLCreateView(name: TableRef("metadata_count"), query: countQuery)))])
        let view = try #require(container.sqlCatalog.views.first { $0.name == "metadata_columns" })
        let binding = try #require(view.definition.bindings.first)
        #expect(binding.kind == .informationSchema)
        #expect(binding.relationID == "columns")
        #expect(binding.columns.map(\.id) == ["table_name", "column_name"])
        #expect(!container.schema.entities.contains { $0.name == "metadata_columns" })
        let rows = try await SQLBlockExecutor(context: container.testBaseContext()).execute(
            SQLBlock(steps: [.statement(.select(SelectQuery(projection: [.wildcard()],
                source: .table(TableRef(schema: "public", table: "metadata_columns")))))]),
            options: options(searchPath: SQLSearchPath(entries: [.schema("absent")])) )
        #expect(Set(try response(rows, 0).rows.map(\.values)) == Set([
            [.primitive(.string("a"))], [.primitive(.string("b"))]]))
        let count = try response(await run(container, [.select(SelectQuery(projection: [.wildcard()], source: .table(TableRef("metadata_count"))))]), 0)
        #expect(count.rows.first?.values == [.primitive(.int64(3))])
        let countBinding = try #require(container.sqlCatalog.views.first { $0.name == "metadata_count" }?.definition.bindings.first)
        #expect(countBinding.kind == .informationSchema)
        #expect(countBinding.columns.isEmpty)
    }

    @Test("Virtual source kinds persist in the digest and invalid descriptor columns fail candidate admission")
    func durableBindingAdmissionAndCodec() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table("items"), .ddl(.createView(SQLCreateView(name: TableRef("catalog_view"),
            query: metadata("tables", projection: [.column("table_name")]))))])
        let lease = container.acquirePublishedSchemaLease()
        let encoded = try SQLCatalogCodec.encode(lease.sqlCatalog)
        let restored = try SQLCatalogCodec.decode(encoded).snapshot(physicalSchema: lease.schema,
            runtimeConfiguration: lease.runtimeConfiguration)
        #expect(restored.digest == lease.sqlCatalog.digest)
        #expect(restored.catalog.views.first?.definition.bindings.first?.kind == .informationSchema)
        var oldVersion = Array(encoded)
        oldVersion[4] = 3; oldVersion[5] = 0
        #expect(throws: StorageFrameError.unsupportedVersion(3)) { _ = try SQLCatalogCodec.decode(ByteString(oldVersion)) }
        let view = try #require(lease.sqlCatalog.catalog.views.first)
        let source = try #require(view.definition.bindings.first)
        for column: SQLViewColumnBinding in [
            try SQLViewColumnBinding(id: "missing", name: "table_name", type: .sql(.text)),
            try SQLViewColumnBinding(id: "table_name", name: "table_name", type: .sql(.integer))] {
            let invalid = try SQLViewSourceBinding(kind: .informationSchema, schema: source.schema,
                table: source.table, relationID: source.relationID, columns: [column])
            let definition = try SQLViewDefinition(query: view.definition.query,
                columns: view.definition.columns, bindings: [invalid])
            let replacement = try SQLView(id: view.id, namespaceID: view.namespaceID, name: view.name, definition: definition)
            let catalog = try SQLCatalog(namespaces: lease.sqlCatalog.catalog.namespaces,
                relations: lease.sqlCatalog.catalog.relations, views: [replacement])
            #expect(throws: SQLViewExecutionError.self) {
                _ = try lease.sqlCatalog.replacingCatalog(catalog, physicalSchema: lease.schema, runtimeConfiguration: lease.runtimeConfiguration)
            }
        }
        #expect(container.sqlCatalog == lease.sqlCatalog.catalog)
    }

    @Test("Canonical view definitions reject request capacity before row emission")
    func serializedDefinitionAdmissionBeforeEmission() async throws {
        let container = try await open(); defer { await container.shutdown() }
        let query = SelectQuery(projection: [.expression(.string(String(repeating: "a", count: 1_048_576)), alias: "payload")],
            source: .values([[]]))
        _ = try await run(container, [.ddl(.createView(SQLCreateView(name: TableRef("large_definition"), query: query)))])
        #expect(container.sqlCatalog.views.count == 1)
        let maximumBytes: UInt64 = 128 * 1_024
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: maximumBytes),
            monotonicClock: container.monotonicClock)
        let resolution = try SQLReadResolution.make(policy: container.testBaseContext().readPolicy(),
            searchPath: .default, workMeter: meter)
        let initialRetainedBytes = meter.retainedIntermediateBytes
        var didEmit = false
        do {
            try SQLInformationSchemaRelation.views.forEachRow(resolution: resolution) { row in
                didEmit = true
                let values = row.values
                let footprint = try CanonicalRelationalFootprintMeter.positionalValueAliasArrayFootprint(
                    count: values.count, valueAt: { values[$0] }, annotations: [:], version: nil,
                    workMeter: meter, stage: .bindingCandidate)
                let reservation = try meter.reserveIntermediate(rows: footprint.rows, bytes: footprint.bytes, at: .bindingCandidate)
                reservation.release()
            }
            Issue.record("Oversized canonical definition was admitted")
        } catch {
            #expect(SQLDiagnostics.diagnostic(for: error).sqlState == "54000")
            let failure = try #require(error as? SQLExecutionFailure)
            guard case .outputLimit = try #require(failure.underlyingError as? SQLSerializationError) else {
                Issue.record("Request capacity failure lost its canonical serialization cause")
                return
            }
        }
        #expect(!didEmit)
        #expect(meter.retainedIntermediateBytes == initialRetainedBytes)
        // This is admitted capacity, not measured allocation.
        #expect(meter.peakIntermediateBytes <= maximumBytes)

        let retainingMeter = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 2 * 1_024 * 1_024),
            monotonicClock: container.monotonicClock)
        let retainingResolution = try SQLReadResolution.make(policy: container.testBaseContext().readPolicy(),
            searchPath: .default, workMeter: retainingMeter)
        let initialOwnerBytes = retainingMeter.retainedIntermediateBytes
        var retained: SQLInformationSchemaRow?
        try SQLInformationSchemaRelation.views.forEachRow(resolution: retainingResolution) { row in retained = row }
        #expect(retained?.values.contains { value in
            if case .primitive(.string(let text)) = value { return text.utf8.count > 1_048_576 }
            return false
        } == true)
        #expect(retainingMeter.retainedIntermediateBytes > initialOwnerBytes)
        retained = nil
        #expect(retainingMeter.retainedIntermediateBytes == initialOwnerBytes)
        #expect(throws: S16MetadataFailure.unavailable) {
            try SQLInformationSchemaRelation.views.forEachRow(resolution: retainingResolution) { _ in
                throw S16MetadataFailure.unavailable
            }
        }
        #expect(retainingMeter.retainedIntermediateBytes == initialOwnerBytes)
    }

    @Test("Metadata traversal enforces exact work boundaries and cancellation before emission")
    func workAdmissionAndCancellation() async throws {
        let container = try await open(); defer { await container.shutdown() }
        _ = try await run(container, [table("items")])
        let policy = try container.testBaseContext().readPolicy()
        func resolution(_ budget: ExecutionBudget) throws -> SQLReadResolution {
            try SQLReadResolution.make(policy: policy, searchPath: .default,
                workMeter: DatabaseWorkMeter(budget: budget, monotonicClock: container.monotonicClock))
        }
        let baseline = try resolution(ExecutionBudget())
        var count = 0
        try SQLInformationSchemaRelation.tables.forEachRow(resolution: baseline) { _ in count += 1 }
        #expect(count == 1)
        let consumed = baseline.workMeter.consumedWorkUnits
        #expect(consumed > 0)
        let exact = try resolution(ExecutionBudget(maximumWorkUnits: consumed))
        try SQLInformationSchemaRelation.tables.forEachRow(resolution: exact) { _ in }
        let onePast = try resolution(ExecutionBudget(maximumWorkUnits: consumed - 1))
        #expect(throws: DatabaseWorkLimitError.self) {
            try SQLInformationSchemaRelation.tables.forEachRow(resolution: onePast) { _ in }
        }
        let cancelled = try resolution(ExecutionBudget())
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try SQLInformationSchemaRelation.tables.forEachRow(resolution: cancelled) { _ in Issue.record("Cancelled metadata emitted") }
        }
        await #expect(throws: Error.self) { try await task.value }
    }

    private func open() async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s16-information-schema", revision: 1), schema: schema), security: .testingDisabled)
    }
    private func table(_ name: String) -> QueryStatement {
        .ddl(.createTable(SQLCreateTable(name: TableRef(name), source: .columns([
            SQLColumnDefinition(name: "a", type: .integer), SQLColumnDefinition(name: "b", type: .text)], constraints: []))))
    }
    private func metadata(_ name: String, projection: [ProjectionItem] = [.wildcard()], table: String? = nil) -> SelectQuery {
        SelectQuery(projection: projection, source: .table(TableRef(schema: "information_schema", table: name)),
            filter: table.map { .equal(.col("table_name"), .string($0)) })
    }
    private func select(_ container: DBContainer, _ name: String, table: String? = nil,
        authorization: AuthorizationContext? = nil) async throws -> QueryResponse {
        try response(await run(container, [.select(metadata(name, table: table))], authorization: authorization), 0)
    }
    private func run(_ container: DBContainer, _ statements: [QueryStatement], authorization: AuthorizationContext? = nil) async throws -> SQLBlockResult {
        let context = authorization.map { container.testBaseContext(authorization: $0) } ?? container.testBaseContext()
        return try await SQLBlockExecutor(context: context).execute(SQLBlock(steps: statements.map { .statement($0) }), options: options())
    }
    private func response(_ result: SQLBlockResult, _ index: Int) throws -> QueryResponse {
        guard case .rows(let value) = result.results[index] else { throw SQLBlockExecutionError.unsupportedStatement }
        return value
    }
    private func records(_ response: QueryResponse) -> [[String: QueryValue]] {
        response.rows.map { row in Dictionary(uniqueKeysWithValues: response.columns.enumerated().map { ($0.element.name, row.values[$0.offset]) }) }
    }
    private func options(searchPath: SQLSearchPath = .default) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 16, maximumRetainedBytes: 16_777_216, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), searchPath: searchPath)
    }
}

private enum S16MetadataFailure: Error, Equatable { case unavailable }
private struct S16MetadataFailureDelegate: DataStoreSecurityDelegate {
    func evaluateList(entity: String, limit: Int?, offset: Int?, orderBy: [String]?) throws { throw S16MetadataFailure.unavailable }
    func evaluateFieldRead(entity: String, fields: Set<String>) throws {}
    func evaluateGet(_ resource: borrowing PersistedModel, fields: Set<String>?) throws {}
    func evaluateCreate(_ resource: borrowing PersistedModel) throws {}
    func evaluateUpdate(_ resource: borrowing PersistedModel, newResource: borrowing PersistedModel) throws {}
    func evaluateDelete(_ resource: borrowing PersistedModel) throws {}
}
