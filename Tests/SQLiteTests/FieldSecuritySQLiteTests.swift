#if SQLITE
import Database
@_spi(Testing) import DatabaseEngine
import DatabaseRuntime
import SQLiteStorage
import StorageKit
import TestHeartbeat
import TestSupport
import Testing

@Persistable
private struct FieldSecuritySQLiteRecord: SecurityPolicy {
    #Directory<FieldSecuritySQLiteRecord>(
        "field-security",
        "records"
    )
    #Index(
        .bitmap(
            name: "field_security_secret",
            field: \FieldSecuritySQLiteRecord.secret
        )
    )

    var id: String = "record"
    var title: String = ""

    @Restricted(read: .roles(["security"]), write: .roles(["security"]))
    var secret: String = ""

    static func permitsRead(
        of resource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        guard context.isAuthenticated else { return false }
        if context.principal?.roles.contains("deny-secret-order") == true,
           query.orderBy?.contains("secret") == true { return false }
        if context.principal?.roles.contains("bounded") == true {
            guard let limit = query.limit else { return false }
            return limit <= 1
        }
        return true
    }

    static func permitsCreate(
        _ newResource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsUpdate(
        from resource: borrowing FieldSecuritySQLiteRecord,
        to newResource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsDelete(
        _ resource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }
}

@Suite("SQLite field security execution", .serialized, .heartbeat)
struct FieldSecuritySQLiteTests {
    private let principalID = "field-security-principal"

    @Test("Canonical query authorizes every observed field before execution")
    func queryAuthorizesEveryObservedField() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        var record = FieldSecuritySQLiteRecord(title: "Visible")
        record.secret = "classified"
        try writer.insert(record)
        try await writer.save()

        let reader = context(in: container)
        let publicResult = try await reader.query(
            SelectQuery(
                projection: [.column("title")],
                source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType))
            )
        )
        #expect(try publicResult.rows.map {
            try $0.primitiveFields(columns: publicResult.columns)
        } == [["title": .string("Visible")]])

        let orderRestricted = context(in: container, roles: ["security", "deny-secret-order"])
        let orderedSource = DataSource.aliased(.table(TableRef(FieldSecuritySQLiteRecord.persistableType)), name: "r")
        let selectedSecret = Expression.field(.wholeRow("r"), name: "secret")
        let ordered = SelectQuery(projection: [.expression(.col("r", "title"))], source: orderedSource)
        #expect(try await orderRestricted.query(ordered.replacing(orderBy: [SortKey(.col("r", "title"))])).rows.count == 1)
        for expression: Expression in [.col("r", "secret"), selectedSecret,
            .field(.col("r"), name: "secret"), .wholeRow("r"), .col("r")] {
            await #expect(throws: SecurityError.self) {
                try await orderRestricted.query(ordered.replacing(orderBy: [SortKey(expression)]))
            }
        }
        let projectedSecret = ordered.replacing(projection: [.expression(selectedSecret, alias: "sort_value")])
        for key: Expression in [.col("sort_value"), .literal(.int(1))] {
            await #expect(throws: SecurityError.self) {
                try await orderRestricted.query(projectedSecret.replacing(orderBy: [SortKey(key)]))
            }
        }
        let derivedSecret = DataSource.aliased(.subquery(SelectQuery(projection: [.column("secret")],
            source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)))), name: "d")
        let mixed = ordered.replacing(source: .join(JoinClause(type: .cross,
            left: orderedSource, right: derivedSecret)))
        #expect(try await orderRestricted.query(mixed.replacing(orderBy: [SortKey(.col("r", "title"))])).rows.count == 1)
        for key: Expression in [.field(.wholeRow("d"), name: "secret"),
            .field(.col("d"), name: "secret"), .wholeRow("d"), .col("d", "secret")] {
            await #expect(throws: SecurityError.self) {
                try await orderRestricted.query(mixed.replacing(orderBy: [SortKey(key)]))
            }
        }
        let derivedProjection = mixed.replacing(projection: [
            .expression(.field(.wholeRow("d"), name: "secret"), alias: "sort_value")])
        for key: Expression in [.col("sort_value"), .literal(.int(1))] {
            await #expect(throws: SecurityError.self) {
                try await orderRestricted.query(derivedProjection.replacing(orderBy: [SortKey(key)]))
            }
        }
        // Names that only binding resolves, such as a table function column
        // or a scalar subquery's expanded output, keep their ordering dependencies.
        let table = "\"\(FieldSecuritySQLiteRecord.persistableType)\""
        for sql in [
            "SELECT (SELECT * FROM unnest(ARRAY[r.secret]) AS u(x)) FROM \(table) r ORDER BY x",
            "SELECT r.title FROM \(table) r, unnest(ARRAY[r.secret]) AS u(x) ORDER BY x",
        ] {
            await #expect(throws: SecurityError.self, "\(sql)") { try await orderRestricted.executeSQL(sql) }
        }
        #expect(try await orderRestricted.executeSQL(
            "SELECT (SELECT * FROM unnest(ARRAY[r.title]) AS u(x)) FROM \(table) r ORDER BY x").rows.count == 1)
        // Ordering in a query enclosing the read reaches the reading entity's
        // list decision. A derived source's outputs conservatively carry every
        // field it reads, so the permitted controls read only title.
        let blockOptions = SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(
            maximumEntries: 1000, maximumDepth: 16, maximumRetainedBytes: 8_000_000,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
        for block in ["CREATE VIEW secret_view AS SELECT secret FROM \(table);",
            "CREATE VIEW title_view AS SELECT title FROM \(table);",
            "CREATE TABLE ordering_items(value INTEGER); INSERT INTO ordering_items VALUES (1);"] {
            await #expect(throws: Never.self, "\(block)") { _ = try await writer.executeSQLBlock(block, options: blockOptions) }
        }
        for sql in [
            "SELECT d.secret FROM (SELECT secret FROM \(table)) d ORDER BY d.secret",
            "SELECT q.x FROM (SELECT (SELECT * FROM unnest(ARRAY[r.secret]) AS u(x)) FROM \(table) r) q ORDER BY q.x",
            "WITH c AS (SELECT secret FROM \(table)) SELECT secret FROM c ORDER BY secret",
            "SELECT secret FROM secret_view ORDER BY secret",
            "SELECT value FROM ordering_items ORDER BY (SELECT secret FROM \(table) LIMIT 1)",
        ] {
            await #expect(throws: SecurityError.self, "\(sql)") { try await orderRestricted.executeSQL(sql) }
        }
        for sql in [
            "SELECT d.title FROM (SELECT title FROM \(table)) d ORDER BY d.title",
            "WITH c AS (SELECT title FROM \(table)) SELECT title FROM c ORDER BY title",
            "SELECT title FROM title_view ORDER BY title",
            "SELECT value FROM ordering_items ORDER BY (SELECT title FROM \(table) LIMIT 1)",
        ] {
            do { #expect(try await orderRestricted.executeSQL(sql).rows.count == 1, "\(sql)") }
            catch { Issue.record("\(sql) was denied: \(error)") }
        }

        let boundedReader = context(in: container, roles: ["bounded"])
        let boundedQuery = SelectQuery(projection: [.column("title")],
            source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
            pagination: QueryPagination(limit: .count(.fixed(1), withTies: false)))
        #expect(try await boundedReader.query(boundedQuery).rows.count == 1)
        let dynamic = boundedQuery.replacing(pagination: QueryPagination(
            limit: .count(.expression(.literal(.int(1))), withTies: false)))
        await #expect(throws: SecurityError.self) { try await boundedReader.query(dynamic) }
        // Division would fail if a count subquery ran before its list grant.
        let countQuery = SelectQuery(projection: [.expression(.divide(
            .literal(.int(1)), .literal(.int(0))))], source: .values([[]]))
        let subqueryCount = boundedQuery.replacing(pagination: QueryPagination(
            limit: .count(.expression(.subquery(countQuery)), withTies: false)))
        await #expect(throws: SecurityError.self) { try await boundedReader.query(subqueryCount) }
        #expect(try await reader.query(dynamic).rows.count == 1)

        let fieldNames = try FieldSecuritySQLiteRecord.schemaEntity.allFields
        let titleIndex = try #require(fieldNames.firstIndex(of: "title"))
        let columnAliases = fieldNames.map { "left_" + $0 }
            + fieldNames.prefix(titleIndex + 1).map { "right_" + $0 }
        let joined = DataSource.join(JoinClause(type: .inner,
            left: .aliased(.table(TableRef(FieldSecuritySQLiteRecord.persistableType)), name: "left"),
            right: .aliased(.table(TableRef(FieldSecuritySQLiteRecord.persistableType)), name: "right"),
            condition: .on(.bool(true))))
        let aliasQuery = SelectQuery(projection: [.expression(.col("joined", "right_title"))],
            source: .aliased(.aliased(joined, name: "inner", columns: columnAliases), name: "joined"))
        let aliasedResult = try await reader.query(aliasQuery)
        #expect(try aliasedResult.rows.map { try $0.primitiveFields(columns: aliasedResult.columns) }
            == [["right_title": .string("Visible")]])
        let bareAliasJSON = "SELECT ROW_TO_JSON(r) FROM \"\(FieldSecuritySQLiteRecord.persistableType)\" AS r"
        await #expect(throws: FieldSecurityError.self) { try await reader.executeSQL(bareAliasJSON) }
        let authorizedJSON = try await writer.executeSQL(bareAliasJSON)
        let authorizedRow = try #require(authorizedJSON.rows.first)
        let authorizedValue = try #require(authorizedRow.values.first)
        guard case .json(let wholeRowJSON) = authorizedValue else {
            Issue.record("Expected authorized whole-row JSON")
            return
        }
        #expect(wholeRowJSON.bytes.withUnsafeBytes { String(decoding: $0, as: UTF8.self) }
            .contains("\"secret\":\"classified\""))
        let priorityNames = fieldNames.enumerated().map { index, name in index == titleIndex ? "r" : name }
        let priorityQuery = SelectQuery(projection: [.expression(.col("r"))],
            source: .aliased(.table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
                name: "r", columns: priorityNames))
        let priorityResult = try await reader.query(priorityQuery)
        #expect(priorityResult.rows.map { $0.values } == [[.primitive(.string("Visible"))]])

        let secretIndex = try #require(fieldNames.firstIndex(of: "secret"))
        let secretAlias = secretIndex <= titleIndex ? "right_secret" : "secret"
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(aliasQuery.replacing(projection: [.expression(.col("joined", secretAlias))]))
        }

        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: [.wildcard()],
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType))
                )
            )
        }

        let restrictedBitmap = try Bitmap(
            FieldSecuritySQLiteRecord.fields.secret,
            equals: "classified"
        )
        let publicRank = FusionInput(
            operation: .order([
                SortKey(.col("title"), direction: .ascending),
                SortKey(.col("id"), direction: .ascending),
            ]),
            scoring: .position,
            requirement: .candidates
        )
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: [.column("title")],
                    source: .table(
                        TableRef(FieldSecuritySQLiteRecord.persistableType)
                    ),
                    accessPath: .fusion(
                        FusionSource(
                            stages: [
                                FusionStageSource(
                                    inputs: [restrictedBitmap.fusionInput]
                                ),
                                FusionStageSource(
                                    inputs: [publicRank]
                                )
                            ]
                        )
                    )
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: [.column("title")],
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
                    filter: .equal(
                        .column(ColumnRef(column: "secret")),
                        .literal(.string("classified"))
                    )
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: [.column("title")],
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
                    orderBy: [SortKey(.column(ColumnRef(column: "secret")))]
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.model(
                for: record.id,
                as: FieldSecuritySQLiteRecord.self
            )
        }
    }

    @Test("Canonical mutation authorizes restricted field changes")
    func mutationAuthorizesRestrictedFieldChanges() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        var original = FieldSecuritySQLiteRecord(title: "Original")
        original.secret = "classified"
        try writer.insert(original)
        try await writer.save()

        let employee = context(in: container)
        var renamed = original
        renamed.title = "Renamed"
        try employee.update(renamed)
        try await employee.save()

        var changedSecret = renamed
        changedSecret.secret = "changed"
        try employee.update(changedSecret)
        await #expect(throws: FieldSecurityError.self) {
            try await employee.save()
        }

        var unauthorizedCreate = FieldSecuritySQLiteRecord(title: "Other")
        unauthorizedCreate.id = "other"
        unauthorizedCreate.secret = "classified"
        let createContext = context(in: container)
        try createContext.insert(unauthorizedCreate)
        await #expect(throws: FieldSecurityError.self) {
            try await createContext.save()
        }
    }

    #if MultiBase
    @Test("A transaction binding rejects another authorization context")
    func transactionBindingIsSessionBound() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        let employee = context(in: container)

        await #expect(throws: DatabaseTransactionError.invalidOperationContext) {
            try await writer.withTransaction { _ in
                _ = try await employee.query(
                    SelectQuery(
                        projection: [.column("title")],
                        source: .table(
                            TableRef(FieldSecuritySQLiteRecord.persistableType)
                        )
                    )
                )
            }
        }
    }
    #endif

    private func makeContainer() async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        #if MultiBase
        let baseID = try Base.ID("field-security")
        let configuration = DBConfiguration(
            testingName: "field-security",
            storageTopology: try .testing(storageEngine: engine),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            testingBaseID: baseID,
            testingPrincipal: Principal(
                identifier: principalID,
                roles: ["security"]
            )
        )
        #else
        let configuration = try DBConfiguration(
            name: "field-security",
            storageEngine: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock()
        )
        #endif
        return try await DBContainer.open(
            for: try Schema(
                entities: [try FieldSecuritySQLiteRecord.schemaEntity],
                version: .init(1, 0, 0)
            ),
            configuration: configuration,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        FieldSecuritySQLiteRecord.self
                    )
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(FieldSecuritySQLiteRecord.self)
                ],
                // SQL-created relations are unrestricted so ordering tests can
                // read the record through them.
                sqlAuthorizationPolicyFactory: { entity, _, _ in
                    AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
                }
            ),
            security: .enabled()
        )
    }

    private func context(
        in container: DBContainer,
        roles: Set<String> = []
    ) -> DatabaseContext {
        let authorization = AuthorizationContext.authenticated(
            Principal(
                identifier: principalID,
                roles: roles.union(["admin"])
            )
        )
        #if MultiBase
        do {
            return container.session(authorization: authorization)
                .base(try Base.ID("field-security"))
                .newContext()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
        #else
        return container.newContext(authorization: authorization)
        #endif
    }
}
#endif
