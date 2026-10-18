import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S11SecuredCatalogItem: SecurityPolicy {
    #Directory<S11SecuredCatalogItem>("s11-secured-catalog")
    var id: String = ""
    var title: String = ""
    @Restricted(read: .roles(["secret"]), write: .roles(["secret"]))
    var secret: String = ""

    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("reader") == true
    }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("reader") == true
    }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool {
        context.principal?.roles.contains("secret") == true
    }
    static func permitsUpdate(from resource: borrowing Self, to updated: borrowing Self,
                              in context: borrowing AuthorizationContext) -> Bool { false }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { false }
}

@Suite("S11 public SQL catalog execution")
struct S11SQLCatalogExecutionTests {
    @Test("Qualified lookup, ordered paths and current-user expansion select actual backing rows")
    func namespaceResolution() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await saveRows(in: container)
        try await publish(namespaceCatalog(), in: container, key: "namespaces")
        let context = container.testBaseContext()
        #expect(try await values(context, "SELECT code FROM items") == [[.primitive(.string("private"))]])
        #expect(try await values(context, "SELECT code FROM public.items") == [
            [.primitive(.string("same"))], [.primitive(.string("same"))]])
        let path = try SQLSearchPath(entries: [.schema("absent"), .schema("public"), .currentUser])
        #expect(try await values(context, "SELECT code FROM items", searchPath: path) == [
            [.primitive(.string("same"))], [.primitive(.string("same"))]])
        let page = try await context.executeSQL("SELECT code FROM items",
            options: ReadExecutionOptions(pageSize: 1, continuationSnapshotIsStable: true), searchPath: path)
        let continuation = try #require(page.continuation)
        let next = try await context.executeSQL("SELECT code FROM items",
            options: ReadExecutionOptions(pageSize: 1, continuation: continuation, continuationSnapshotIsStable: true),
            searchPath: path)
        #expect(next.rows.map(\.values) == [[.primitive(.string("same"))]])
        do {
            _ = try await context.executeSQL("SELECT code || ' changed' AS code FROM items",
                options: ReadExecutionOptions(pageSize: 1, continuation: continuation, continuationSnapshotIsStable: true),
                searchPath: path)
            Issue.record("Expected a changed logical projection to reject the continuation")
        } catch CanonicalReadError.invalidContinuation {}
        do {
            _ = try await context.executeSQL("SELECT code FROM items", options: ReadExecutionOptions(pageSize: 1,
                continuation: continuation, continuationSnapshotIsStable: true))
            Issue.record("Expected a changed search path to reject the continuation")
        } catch CanonicalReadError.invalidContinuation {}
        #if !MultiBase
        let anonymous = container.testBaseContext(authorization: .anonymous)
        #expect(try await values(anonymous, "SELECT code FROM items") == [
            [.primitive(.string("same"))], [.primitive(.string("same"))]])
        #endif
    }

    @Test("Missing qualified names cannot fall back to a physical entity or visible CTE")
    func qualifiedFailureIsTerminal() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await publish(namespaceCatalog(), in: container, key: "terminal")
        let context = container.testBaseContext()
        let missingNamespace = "SELECT * FROM absent.\"S11PhysicalA\""
        do {
            _ = try await context.executeSQL(missingNamespace)
            Issue.record("Expected exact namespace failure")
        } catch let failure as SQLBindingSourceFailure {
            guard case .catalog(.missingNamespace("absent")) = failure.cause else {
                Issue.record("Expected missingNamespace, got \(failure.cause)"); return
            }
            #expect(failure.position == "SELECT * FROM ".utf8.count)
        }
        let missingRelation = "WITH \"S11PhysicalA\" AS (SELECT 7 AS code) SELECT * FROM public.\"S11PhysicalA\""
        do {
            _ = try await context.executeSQL(missingRelation)
            Issue.record("Expected exact relation failure")
        } catch let failure as SQLBindingSourceFailure {
            guard case .catalog(.missingRelation(namespace: "public", name: "S11PhysicalA")) = failure.cause else {
                Issue.record("Expected missingRelation, got \(failure.cause)"); return
            }
            #expect(failure.position == missingRelation.utf8.count - "public.\"S11PhysicalA\"".utf8.count)
        }
        let emptyPath = try SQLSearchPath(entries: [])
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await context.executeSQL("SELECT * FROM items", searchPath: emptyPath)
        }
    }

    @Test("Only unqualified relation references are shadowed by a CTE")
    func cteShadowing() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await saveRows(in: container)
        try await publish(namespaceCatalog(), in: container, key: "cte")
        let context = container.testBaseContext()
        #expect(try await values(context, "WITH items AS (SELECT 'cte' AS code) SELECT code FROM items")
            == [[.primitive(.string("cte"))]])
        #expect(try await values(context, "WITH items AS (SELECT 'cte' AS code) SELECT code FROM public.items")
            == [[.primitive(.string("same"))], [.primitive(.string("same"))]])
    }

    @Test("Logical order, rename and hidden physical identity survive direct, paged and join reads")
    func logicalColumnMapping() async throws {
        #if FullTextIndexes
        let container = try await makeTextIndexedContainer()
        #else
        let container = try await makeContainer()
        #endif
        defer { await container.shutdown() }
        try await saveRows(in: container)
        try await publish(mappedCatalog(), in: container, key: "mapped")
        let context = container.testBaseContext()
        let response = try await context.executeSQL("SELECT * FROM inventory ORDER BY quantity")
        #expect(response.columns.map(\.name) == ["quantity", "label", "area"])
        #expect(response.rows.map(\.values) == [
            [.primitive(.int64(10)), .primitive(.string("same")), .primitive(.string("east"))],
            [.primitive(.int64(20)), .primitive(.string("same")), .primitive(.string("east"))]])
        let page = try await context.executeSQL("SELECT label, quantity FROM inventory WHERE quantity >= 10 ORDER BY quantity",
            options: ReadExecutionOptions(pageSize: 1))
        #expect(page.rows.map(\.values) == [[.primitive(.string("same")), .primitive(.int64(10))]])
        #expect(try await values(context,
            "SELECT a.label, b.quantity FROM inventory a JOIN inventory b ON a.quantity < b.quantity")
            == [[.primitive(.string("same")), .primitive(.int64(20))]])
        #expect(try await values(context,
            "WITH selected AS (SELECT label, quantity FROM inventory) SELECT label FROM selected WHERE quantity = 20")
            == [[.primitive(.string("same"))]])
        await #expect(throws: SQLBindingSourceFailure.self) { try await context.executeSQL("SELECT id FROM inventory") }
        let physical = try await context.query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S11PhysicalA"))))
        #expect(physical.rows.count == 2)
        let initialQuery = SelectQuery(projection: [.column("label"), .column("quantity")],
            source: .table(TableRef("inventory")), accessPath: .fusion(FusionSource(stages: [
                FusionStageSource(inputs: [FusionInput(operation: .filter(
                    .greaterThanOrEqual(.column(ColumnRef("quantity")), .literal(.int(10)))))]),
                FusionStageSource(inputs: [FusionInput(operation: .order([
                    SortKey(.column(ColumnRef("quantity")), direction: .descending)
                ]), scoring: .position, requirement: .candidates)])
            ])))
        let initialOptions = ReadExecutionOptions()
        let initialMeter = DatabaseWorkMeter(budget: initialOptions.budget, monotonicClock: container.monotonicClock)
        let initialExecution = ReadExecutionContext(options: initialOptions, monotonicClock: container.monotonicClock,
            workMeter: initialMeter)
        let initialResult = try await context.indexQueryContext.withSession(workMeter: initialMeter) { session in
            let resolution = try session.makeSQLReadResolution(searchPath: .default, workMeter: initialMeter)
            return try await session.executeCanonical(initialQuery, execution: initialExecution.withSQLResolution(resolution))
        }
        #expect(initialResult.columns.map(\.name) == ["label", "quantity"])
        #expect(initialResult.rows.map(\.values) == [
            [.primitive(.string("same")), .primitive(.int64(20))],
            [.primitive(.string("same")), .primitive(.int64(10))]])
        #if FullTextIndexes
        let searchStage = FusionStageSource(inputs: [FusionInput(operation: .index(FusionIndexSource(
            selection: .named(name: "s11_catalog_code", type: .text(.fullText)),
            referencedFields: [FieldIdentity(name: "code", number: 2)],
            parameters: ["fieldName": .string("code"), "terms": .array([.string("same")]), "matchMode": .string("all")])),
            scoring: .annotation(name: "score", order: .higherIsBetter))])
        let physicalFusion = SelectQuery(projection: [.wildcard()],
            source: .table(TableRef("S11PhysicalA")),
            accessPath: .fusion(FusionSource(stages: [searchStage, FusionStageSource(inputs: [
                FusionInput(operation: .filter(.greaterThanOrEqual(.column(ColumnRef("score")), .literal(.int(10)))),
                    requirement: .candidates)
            ])])), orderBy: [SortKey(.column(ColumnRef("score")), direction: .descending)])
        let expected: [[QueryValue]] = [
            [.primitive(.string("same")), .primitive(.int64(20))],
            [.primitive(.string("same")), .primitive(.int64(10))]]
        let physicalResult = try await context.query(physicalFusion)
        let physicalData = physicalResult.rows.map(\.values)
        let physicalExpected: [[QueryValue]] = [
            [.primitive(.string("b")), .primitive(.string("same")), .primitive(.string("east")), .primitive(.int64(20))],
            [.primitive(.string("a")), .primitive(.string("same")), .primitive(.string("east")), .primitive(.int64(10))]]
        try #require(physicalData == physicalExpected)
        print("S11_R2_PHYSICAL_FUSION_SANITY_PASS")
        let fusionQuery = SelectQuery(projection: [.column("label"), .column("quantity")],
            source: .table(TableRef("inventory")),
            accessPath: .fusion(FusionSource(stages: [searchStage, FusionStageSource(inputs: [
                FusionInput(operation: .filter(.greaterThanOrEqual(.column(ColumnRef("quantity")), .literal(.int(10)))),
                    requirement: .candidates)
            ])])), orderBy: [SortKey(.column(ColumnRef("quantity")), direction: .descending)])
        let options = ReadExecutionOptions()
        let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: container.monotonicClock)
        let execution = ReadExecutionContext(options: options, monotonicClock: container.monotonicClock, workMeter: meter)
        let fusion = try await context.indexQueryContext.withSession(workMeter: meter) { session in
            let resolution = try session.makeSQLReadResolution(searchPath: .default, workMeter: meter)
            return try await session.executeCanonical(fusionQuery, execution: execution.withSQLResolution(resolution))
        }
        #expect(fusion.rows.map(\.values) == expected)
        #endif
    }

    @Test("No-key, arbitrary-key and ordered composite-key metadata do not replace physical row identity")
    func keyMetadataAndDuplicateRows() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await saveRows(in: container)
        let columns = try [SQLColumn(id: "code", name: "code", fieldNumber: 2),
            SQLColumn(id: "region", name: "region", fieldNumber: 3)]
        let namespaces = try [SQLNamespace(id: "public", name: "public")]
        let keys: [[String]?] = [nil, ["code"], ["region", "code"]]
        for (index, key) in keys.enumerated() {
            let catalog = try SQLCatalog(namespaces: namespaces, relations: [SQLRelation(id: "keyed", namespaceID: "public",
                name: "keyed", entityName: "S11PhysicalA", columns: columns, primaryKeyColumnIDs: key)])
            if index == 1 {
                let generation = container.schemaGeneration
                await #expect {
                    try await publish(catalog, in: container, key: "key-\(index)")
                } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .unique && actualRelation == "keyed" && actualConstraint == "keyed_pkey"
        }
                #expect(container.schemaGeneration == generation)
                let replacement = try PersistedModel(entity: "S11PhysicalA", fields: [
                    PersistableField(number: 1, name: "id", value: .string("b")),
                    PersistableField(number: 2, name: "code", value: .string("other")),
                    PersistableField(number: 3, name: "region", value: .string("east")),
                    PersistableField(number: 4, name: "score", value: .int64(20))])
                try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
                    try await transaction.savePersistedModel(replacement, precondition: .exists)
                }
            }
            try await publish(catalog, in: container, key: "key-\(index)")
            #expect(container.sqlCatalog.relations[0].primaryKeyColumnIDs == key)
            let context = container.testBaseContext()
            let expected: [[QueryValue]] = [
                [.primitive(.string(index == 0 ? "same" : "other")), .primitive(.string("east"))],
                [.primitive(.string("same")), .primitive(.string("east"))]]
            #expect(try await values(context, "SELECT * FROM keyed ORDER BY code") == expected)
            #expect(try await values(context, "SELECT DISTINCT * FROM keyed ORDER BY code")
                == (index == 0 ? [expected[0]] : expected))
            await #expect(throws: SQLBindingSourceFailure.self) {
                try await context.executeSQL("SELECT id FROM keyed")
            }
        }
        let physical = try await container.testBaseContext().query(SelectQuery(projection: [.column("id")],
            source: .table(TableRef("S11PhysicalA")), orderBy: [SortKey(.column(ColumnRef("id")))]))
        #expect(physical.rows.map(\.values) == [[.primitive(.string("a"))], [.primitive(.string("b"))]])
    }

    @Test("Byte-distinct normalized-equivalent columns remain distinct in projections and wildcards")
    func exactColumnIdentity() async throws {
        let container = try await unicodeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let reversed = try PersistedModel(entity: "S11PhysicalA", fields: [
            PersistableField(number: 1, name: "id", value: .string("unicode-second")),
            PersistableField(number: 2, name: "code", value: .string("right")),
            PersistableField(number: 3, name: "region", value: .string("left")),
            PersistableField(number: 4, name: "score", value: .int64(2))])
        try await context.withTransaction(configuration: .batch) { transaction in
            try await transaction.savePersistedModel(reversed, precondition: .notExists)
        }
        let response = try await context.executeSQL("SELECT \"é\", \"e\u{0301}\" FROM exact ORDER BY \"é\"")
        expectExactNames(response, ["é", "e\u{0301}"])
        #expect(response.rows.map(\.values) == [
            [.primitive(.string("left")), .primitive(.string("right"))],
            [.primitive(.string("right")), .primitive(.string("left"))]])
        let decomposedOrder = try await context.executeSQL("SELECT \"é\", \"e\u{0301}\" FROM exact ORDER BY \"e\u{0301}\"")
        expectExactNames(decomposedOrder, ["é", "e\u{0301}"])
        #expect(decomposedOrder.rows.map(\.values) == [
            [.primitive(.string("right")), .primitive(.string("left"))],
            [.primitive(.string("left")), .primitive(.string("right"))]])
        let wildcard = try await context.executeSQL("SELECT e.* FROM exact e ORDER BY \"é\"")
        expectExactNames(wildcard, ["é", "e\u{0301}"])
        #expect(wildcard.rows.map(\.values) == response.rows.map(\.values))
        for sql in [
            "SELECT \"é\" AS x, \"e\u{0301}\" AS x FROM exact ORDER BY x",
            "SELECT \"é\" || '' AS x, \"e\u{0301}\" || '' AS x FROM exact ORDER BY x",
        ] {
            do {
                _ = try await context.executeSQL(sql)
                Issue.record("Expected distinct selected source columns to make the duplicate output alias ambiguous")
            } catch let failure as SQLBindingSourceFailure {
                guard case .read(.expressionEvaluation(.ambiguousColumn("x"), _)) = failure.cause else {
                    Issue.record("Expected typed duplicate output alias ambiguity, got \(failure.cause)")
                    continue
                }
            }
        }
        for sql in [
            "SELECT \"é\" AS x, \"é\" AS x FROM exact ORDER BY x",
            "SELECT \"é\" || '' AS x, \"é\" || '' AS x FROM exact ORDER BY x",
        ] {
            #expect(try await values(context, sql) == [
                [.primitive(.string("left")), .primitive(.string("left"))],
                [.primitive(.string("right")), .primitive(.string("right"))]])
        }
        await #expect(throws: SQLBindingSourceFailure.self) {
            try await context.executeSQL("SELECT \"é\" FROM exact a CROSS JOIN exact b")
        }
    }

    @Test("Byte-distinct aliases, USING and NATURAL joins preserve selected names and values")
    func exactJoinIdentity() async throws {
        let container = try await unicodeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let aliases = try await context.executeSQL("SELECT \"é\".*, \"e\u{0301}\".* FROM exact \"é\" CROSS JOIN exact \"e\u{0301}\"")
        expectExactNames(aliases, ["é", "e\u{0301}", "é", "e\u{0301}"])
        #expect(aliases.rows.map(\.values) == [[.primitive(.string("left")), .primitive(.string("right")),
            .primitive(.string("left")), .primitive(.string("right"))]])
        let natural = try await context.executeSQL("SELECT * FROM (SELECT \"é\" FROM exact) a NATURAL JOIN (SELECT \"e\u{0301}\" FROM exact) b")
        expectExactNames(natural, ["é", "e\u{0301}"])
        #expect(natural.rows.map(\.values) == [[.primitive(.string("left")), .primitive(.string("right"))]])
        let using = try await context.executeSQL("SELECT * FROM exact a JOIN exact b USING (\"é\")")
        expectExactNames(using, ["é", "e\u{0301}", "e\u{0301}"])
        #expect(using.rows.map(\.values) == [[.primitive(.string("left")), .primitive(.string("right")), .primitive(.string("right"))]])
    }

    @Test("Byte-distinct CTE names and correlated frames do not normalize or shadow each other")
    func exactCTEAndCorrelationIdentity() async throws {
        let container = try await unicodeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        #expect(try await values(context,
            "WITH \"é\" AS (SELECT \"é\" AS v FROM exact), \"e\u{0301}\" AS (SELECT \"e\u{0301}\" AS v FROM exact) SELECT a.v, b.v FROM \"é\" a CROSS JOIN \"e\u{0301}\" b")
            == [[.primitive(.string("left")), .primitive(.string("right"))]])
        #expect(try await values(context,
            "SELECT (SELECT \"é\".\"é\" || \"e\u{0301}\".\"e\u{0301}\" FROM exact \"e\u{0301}\") FROM exact \"é\"")
            == [[.primitive(.string("leftright"))]])
    }

    @Test("Catalog publication advances execution identity, preserves data, and respects old leases")
    func publicationAndLeases() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await saveRows(in: container)
        try await publish(mappedCatalog(), in: container, key: "before")
        let oldGeneration = container.schemaGeneration
        let oldFingerprint = container.schemaFingerprint
        let renamed = try mappedCatalog(relationName: "renamed", labelName: "description")
        let publication = try await container.withSchemaLease { lease in
            #expect(lease.generation == oldGeneration)
            let result = try await container.publishSQLCatalog(renamed, expectedGeneration: oldGeneration,
                idempotencyKey: "rename", authorization: TestBaseEnvironment.authorization)
            #expect(try await values(container.testBaseContext(), "SELECT label FROM inventory").count == 2)
            #expect(container.sqlCatalog.relations[0].name == "inventory")
            return result
        }
        #expect(publication.generation == oldGeneration + 1)
        #expect(container.schemaFingerprint == oldFingerprint)
        #expect(container.sqlCatalog == renamed)
        #expect(try await values(container.testBaseContext(), "SELECT description FROM renamed").count == 2)
        await #expect(throws: DatabaseSchemaPublicationError.generationConflict(
            expected: oldGeneration, actual: publication.generation)) {
            try await container.publishSQLCatalog(mappedCatalog(), expectedGeneration: oldGeneration,
                idempotencyKey: "stale", authorization: TestBaseEnvironment.authorization)
        }
        let retry = try await container.publishSQLCatalog(renamed, expectedGeneration: oldGeneration,
            idempotencyKey: "rename", authorization: TestBaseEnvironment.authorization)
        #expect(retry.generation == publication.generation)
        await #expect(throws: DatabaseSchemaPublicationError.idempotencyKeyReused("rename")) {
            try await container.publishSQLCatalog(mappedCatalog(), expectedGeneration: publication.generation,
                idempotencyKey: "rename", authorization: TestBaseEnvironment.authorization)
        }
        #expect(container.sqlCatalog == renamed)
        #expect(container.schemaGeneration == publication.generation)
        let sql = "SELECT description FROM renamed ORDER BY quantity"
        let page = try await container.testBaseContext().executeSQL(sql,
            options: ReadExecutionOptions(pageSize: 1, continuationSnapshotIsStable: true))
        let continuation = try #require(page.continuation)
        let changedCatalog = try SQLCatalog(namespaces: renamed.namespaces + [SQLNamespace(id: "unused", name: "unused")],
            relations: renamed.relations)
        try await publish(changedCatalog, in: container, key: "new-catalog-generation")
        do {
            _ = try await container.testBaseContext().executeSQL(sql, options: ReadExecutionOptions(pageSize: 1,
                continuation: continuation, continuationSnapshotIsStable: true))
            Issue.record("Expected the same SELECT to reject the previous catalog generation's continuation")
        } catch CanonicalReadError.invalidContinuation {}
        #expect(try await values(container.testBaseContext(), sql).count == 2)
    }

    @Test("A prepared SELECT retains its path and rejects a different schema generation")
    func preparedGenerationMismatch() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        try await saveRows(in: container)
        try await publish(namespaceCatalog(), in: container, key: "prepare")
        let context = container.testBaseContext()
        let options = ReadExecutionOptions()
        let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: container.monotonicClock)
        let execution = ReadExecutionContext(options: options, monotonicClock: container.monotonicClock, workMeter: meter)
        let path = try SQLSearchPath(entries: [.schema("public")])
        let prepared = try await context.indexQueryContext.withSession(workMeter: meter) { session in
            let prepared = try await context.prepareSQLSelectForCanonicalExecution(
                SelectQuery(projection: [.column("code")], source: .table(TableRef("items"))),
                workMeter: meter, session: session, structuralLimits: .default, searchPath: path)
            let result = try await prepared.execute(in: session, execution: execution)
            #expect(result.rows.map(\.values) == [[.primitive(.string("same"))], [.primitive(.string("same"))]])
            return prepared
        }
        try await publish(mappedCatalog(), in: container, key: "after-prepare")
        await #expect(throws: SQLReadResolutionError.generationMismatch) {
            try await context.indexQueryContext.withSession(workMeter: meter) { session in
                try await prepared.execute(in: session, execution: execution)
            }
        }
        #expect(try await values(context, "SELECT label FROM inventory").count == 2)
    }

    @Test("Logical renaming never bypasses physical entity and field authorization")
    func physicalAuthorization() async throws {
        let entity = try S11SecuredCatalogItem.schemaEntity
        let container = try await DBContainer.open(for: Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s11-catalog-security", revision: 1),
                entityRuntimes: [EntityRuntimeDefinition(schemaDriven: entity).registration()],
                authorizationPolicies: [AuthorizationPolicyHandler(S11SecuredCatalogItem.self)]), security: .enabled())
        defer { await container.shutdown() }
        let writer = container.testBaseContext(authorization: authorization(roles: ["reader", "secret"]))
        var item = S11SecuredCatalogItem(id: "protected", title: "visible")
        item.secret = "classified"
        try writer.insert(item)
        try await writer.save()
        let old = try #require(container.sqlCatalog.relations.first)
        let renamed = try SQLRelation(id: old.id, namespaceID: old.namespaceID, name: "documents", entityName: old.entityName,
            columns: old.columns.map { column in
                try SQLColumn(id: column.id, name: column.name == "secret" ? "public_name" : column.name,
                    backing: column.backing, policy: column.policy)
            }, primaryKeyColumnIDs: old.primaryKeyColumnIDs,
            primaryKeyConstraint: old.primaryKeyConstraint, constraints: old.constraints)
        try await publish(SQLCatalog(namespaces: container.sqlCatalog.namespaces, relations: [renamed]),
            in: container, key: "security")
        let reader = container.testBaseContext(authorization: authorization(roles: ["reader"]))
        #expect(try await values(reader, "SELECT title FROM documents") == [[.primitive(.string("visible"))]])
        await #expect(throws: FieldSecurityError.self) { try await reader.executeSQL("SELECT public_name FROM documents") }
        await #expect(throws: FieldSecurityError.self) { try await reader.executeSQL("SELECT title FROM documents ORDER BY public_name") }
        let denied = container.testBaseContext(authorization: authorization(roles: ["secret"]))
        await #expect(throws: SecurityError.self) { try await denied.executeSQL("SELECT title FROM documents") }
        #expect(try await values(writer, "SELECT public_name FROM documents") == [[.primitive(.string("classified"))]])
        #expect(try await values(reader, "SELECT title AS visible FROM documents ORDER BY visible")
            == [[.primitive(.string("visible"))]])
        #expect(try await values(reader, "SELECT title FROM documents ORDER BY 1")
            == [[.primitive(.string("visible"))]])
        await #expect(throws: FieldSecurityError.self) {
            try await reader.executeSQL("WITH documents AS (SELECT 'safe' AS public_name) SELECT public_name FROM public.documents")
        }
        let exact = try SQLRelation(id: renamed.id, namespaceID: renamed.namespaceID, name: renamed.name,
            entityName: renamed.entityName, columns: renamed.columns.map { column in
                let name = column.fieldNumber == 2 ? "é" : column.fieldNumber == 3 ? "e\u{0301}" : column.name
                return try SQLColumn(id: column.id, name: name, backing: column.backing, policy: column.policy)
            }, primaryKeyColumnIDs: renamed.primaryKeyColumnIDs,
            primaryKeyConstraint: renamed.primaryKeyConstraint, constraints: renamed.constraints)
        try await publish(SQLCatalog(namespaces: container.sqlCatalog.namespaces, relations: [exact]),
            in: container, key: "security-exact")
        #expect(try await values(reader, "SELECT \"é\".\"é\" FROM documents \"é\" CROSS JOIN documents \"e\u{0301}\"")
            == [[.primitive(.string("visible"))]])
        await #expect(throws: FieldSecurityError.self) {
            try await reader.executeSQL("SELECT \"e\u{0301}\".\"e\u{0301}\" FROM documents \"é\" CROSS JOIN documents \"e\u{0301}\"")
        }
        let title = try #require(exact.columns.first { $0.fieldNumber == 2 })
        let subset = try SQLRelation(id: exact.id, namespaceID: exact.namespaceID, name: exact.name,
            entityName: exact.entityName, columns: [title],
            fieldNumberHighWaterMark: exact.fieldNumberHighWaterMark)
        try await publish(SQLCatalog(namespaces: container.sqlCatalog.namespaces, relations: [subset]),
            in: container, key: "security-subset")
        let wildcard = try await reader.executeSQL("SELECT * FROM documents ORDER BY 1")
        expectExactNames(wildcard, ["é"])
        #expect(wildcard.rows.map(\.values) == [[.primitive(.string("visible"))]])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [entity("S11PhysicalA"), entity("S11PhysicalB")])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "s11-public-catalog", revision: 1), schema: schema), security: .testingDisabled)
    }

    #if FullTextIndexes
    private func makeTextIndexedContainer() async throws -> DBContainer {
        let physical = try entity("S11PhysicalA")
        let index = try IndexDescriptor(entityName: physical.name,
            declaration: .text(name: "s11_catalog_code", fields: [FieldIdentity(name: "code", number: 2)],
                mode: .fullText(tokenizer: .simple)), fieldSchemas: physical.fields)
        let indexed = try Schema.Entity(name: physical.name, identifierType: physical.identifierType,
            fields: physical.fields, directoryComponents: physical.directoryComponents, indexes: [index])
        let schema = try Schema(entities: [indexed, entity("S11PhysicalB")])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s11-public-catalog-fulltext", revision: 1), schema: schema),
            security: .testingDisabled)
    }
    #endif

    private func entity(_ name: String) throws -> Schema.Entity {
        try Schema.Entity(name: name, identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "code", fieldNumber: 2, type: .string),
            FieldSchema(name: "region", fieldNumber: 3, type: .string),
            FieldSchema(name: "score", fieldNumber: 4, type: .int64)], directoryComponents: [.staticPath(name)])
    }

    private func saveRows(in container: DBContainer) async throws {
        let models = try [("S11PhysicalA", "a", "same", Int64(10)), ("S11PhysicalA", "b", "same", Int64(20)),
            ("S11PhysicalB", "c", "private", Int64(30))].map { name, id, code, score in
                try PersistedModel(entity: name, fields: [
                    PersistableField(number: 1, name: "id", value: .string(id)),
                    PersistableField(number: 2, name: "code", value: .string(code)),
                    PersistableField(number: 3, name: "region", value: .string("east")),
                    PersistableField(number: 4, name: "score", value: .int64(score))])
            }
        try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            for model in models { try await transaction.savePersistedModel(model, precondition: .notExists) }
        }
    }

    private func namespaceCatalog() throws -> SQLCatalog {
        let columns = try [SQLColumn(id: "code", name: "code", fieldNumber: 2)]
        return try SQLCatalog(namespaces: [SQLNamespace(id: "p", name: "public"), SQLNamespace(id: "u", name: "test-runner")],
            relations: [SQLRelation(id: "a", namespaceID: "p", name: "items", entityName: "S11PhysicalA", columns: columns),
                SQLRelation(id: "b", namespaceID: "u", name: "items", entityName: "S11PhysicalB", columns: columns)])
    }

    private func mappedCatalog(relationName: String = "inventory", labelName: String = "label") throws -> SQLCatalog {
        try SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [
            SQLRelation(id: "inventory", namespaceID: "public", name: relationName, entityName: "S11PhysicalA", columns: [
                SQLColumn(id: "quantity", name: "quantity", fieldNumber: 4),
                SQLColumn(id: "label", name: labelName, fieldNumber: 2),
                SQLColumn(id: "area", name: "area", fieldNumber: 3)])])
    }

    private func unicodeContainer() async throws -> DBContainer {
        let container = try await makeContainer()
        do {
            let model = try PersistedModel(entity: "S11PhysicalA", fields: [
                PersistableField(number: 1, name: "id", value: .string("unicode")),
                PersistableField(number: 2, name: "code", value: .string("left")),
                PersistableField(number: 3, name: "region", value: .string("right")),
                PersistableField(number: 4, name: "score", value: .int64(1))])
            try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
                try await transaction.savePersistedModel(model, precondition: .notExists)
            }
            try await publish(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [
                SQLRelation(id: "exact", namespaceID: "public", name: "exact", entityName: "S11PhysicalA", columns: [
                    SQLColumn(id: "left", name: "é", fieldNumber: 2),
                    SQLColumn(id: "right", name: "e\u{0301}", fieldNumber: 3)])]), in: container, key: "unicode")
            return container
        } catch {
            await container.shutdown()
            throw error
        }
    }

    private func publish(_ catalog: SQLCatalog, in container: DBContainer, key: String) async throws {
        _ = try await container.publishSQLCatalog(catalog, expectedGeneration: container.schemaGeneration,
            idempotencyKey: key, authorization: TestBaseEnvironment.authorization)
    }

    private func values(_ context: DatabaseContext, _ sql: String, searchPath: SQLSearchPath = .default) async throws -> [[QueryValue]] {
        try await context.executeSQL(sql, searchPath: searchPath).rows.map(\.values)
    }

    private func expectExactNames(_ response: QueryResponse, _ expected: [String]) {
        #expect(response.columns.count == expected.count)
        for (column, name) in zip(response.columns, expected) { #expect(column.name.utf8.elementsEqual(name.utf8)) }
    }

    private func authorization(roles: Set<String>) -> AuthorizationContext {
        .authenticated(Principal(identifier: "test-runner", roles: roles.union(["test-runner", "admin"])))
    }
}
