#if SQLITE
import Database
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import StorageKit
import TestHeartbeat
import TestSupport
import Testing

@Persistable
private struct SchemaDrivenRuntimeAccount {
    #Directory<SchemaDrivenRuntimeAccount>(
        "schema-driven-runtime",
        "accounts"
    )
    #Index(
        .ordered(
            name: "schema_driven_runtime_account_email",
            keys: [.ascending(\SchemaDrivenRuntimeAccount.email)], unique: false))

    var id: String = ""
    var email: String = ""
    var age: Int64 = 0
    var nickname: String?
}

@Persistable
private struct SchemaDrivenIndexParityEntity {
    #Directory<SchemaDrivenIndexParityEntity>(
        "schema-driven-runtime",
        "index-parity"
    )

    var id: String
    var email: String
    var body: String
    var embedding: Vector
    var location: GeographicPoint
    var score: Int64
    var region: String
    var status: String
    var country: String
    var city: String
    var name: String
    var source: String
    var relation: String
    var target: String
    var amount: Int64
    var customerID: String
    var latency: Double
    var note: String?

    #Index(
        .ordered(
            name: "parity_scalar", keys: [.ascending(\SchemaDrivenIndexParityEntity.email)],
            unique: false))
    #Index(
        .vector(
            name: "parity_vector",
            embedding: \SchemaDrivenIndexParityEntity.embedding,
            dimensions: 3
        ))
    #Index(
        .text(
            name: "parity_fulltext", fields: [\SchemaDrivenIndexParityEntity.body],
            mode: .fullText(
                tokenizer: .simple, storePositions: true, ngramSize: 3, minimumTermLength: 2)))
    #Index(
        .spatial(
            name: "parity_spatial",
            location: \SchemaDrivenIndexParityEntity.location
        ))
    #Index(
        .rank(
            name: "parity_rank",
            score: \SchemaDrivenIndexParityEntity.score
        ))
    #Index(
        .aggregate(
            name: "parity_sum", function: .sum,
        groupBy: [.ascending(\SchemaDrivenIndexParityEntity.region)],
        value: \SchemaDrivenIndexParityEntity.amount))
    #Index(
        .aggregate(
            name: "parity_distinct", function: .approximateDistinct(precision: 14),
        groupBy: [.ascending(\SchemaDrivenIndexParityEntity.region)],
        value: \SchemaDrivenIndexParityEntity.customerID))
    #Index(
        .aggregate(
            name: "parity_percentile", function: .percentile(compression: 100),
        groupBy: [.ascending(\SchemaDrivenIndexParityEntity.region)],
        value: \SchemaDrivenIndexParityEntity.latency))
    #Index(
        .bitmap(
            name: "parity_bitmap",
            field: \SchemaDrivenIndexParityEntity.status
        ))
    #Index(
        .leaderboard(
            name: "parity_leaderboard",
            groupBy: [.ascending(\SchemaDrivenIndexParityEntity.region)],
            score: \SchemaDrivenIndexParityEntity.score,
            window: .daily,
            windowCount: 2
        ))
    #Index(
        .ordered(
            name: "parity_ordered_compound",
            keys: [
                .ascending(\SchemaDrivenIndexParityEntity.city),
                .ascending(\SchemaDrivenIndexParityEntity.country),
                .ascending(\SchemaDrivenIndexParityEntity.name),
            ]))
    #Index(
        .graph(
            name: "parity_graph",
            definition: .property(
                source: \SchemaDrivenIndexParityEntity.source,
                label: .field(\SchemaDrivenIndexParityEntity.relation),
                target: \SchemaDrivenIndexParityEntity.target, graph: nil, strategy: .adjacency)
        ))
}

@Persistable
private struct SchemaDrivenVersionCapabilityEntity {
    #Directory<SchemaDrivenVersionCapabilityEntity>(
        "schema-driven-runtime",
        "version-capability"
    )
    #Index(
        .history(
            name: "version_capability", version: \SchemaDrivenVersionCapabilityEntity.id,
            retention: .keepAll))

    var id: String
}

@Suite("Schema-driven runtime SQLite parity", .serialized, .heartbeat)
struct SchemaDrivenRuntimeSQLiteTests {
    private struct RelativeIndexEntry: Equatable {
        let key: ByteString
        let value: ByteString
    }

    @Test("Schema-driven CRUD and scalar query match compiled runtime")
    func canonicalCRUDMatchesCompiledRuntime() async throws {
        let schema = try makeSchema()
        let compiled = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenRuntimeAccount.self
                    )
                ]
            )
        )
        let schemaDriven = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                schema: schema
            )
        )
        defer {
            await compiled.shutdown()
            await schemaDriven.shutdown()
        }

        let alice = try persistedAccount(
            id: "account-alice",
            email: "alice@example.com",
            age: 31,
            nickname: .string("Al")
        )
        let bob = try persistedAccount(
            id: "account-bob",
            email: "bob@example.com",
            age: 17,
            nickname: .null
        )
        for container in [compiled, schemaDriven] {
            try await save([alice, bob], in: container)
        }

        let stableInitialPage = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .literal(
                        .langLiteral(
                            value: "Hello",
                            language: "EN-US"
                        )
                    ),
                    alias: "label"
                )
            ]),
            source: .table(
                TableRef(SchemaDrivenRuntimeAccount.persistableType)
            )
        )
        let stablePageOptions = ReadExecutionOptions(
            pageSize: 10,
            continuationSnapshotIsStable: true
        )
        let compiledStablePage = try await compiled.testBaseContext().query(
            stableInitialPage,
            options: stablePageOptions
        )
        let schemaDrivenStablePage = try await schemaDriven.testBaseContext()
            .query(
                stableInitialPage,
                options: stablePageOptions
            )
        expectEqual(compiledStablePage, schemaDrivenStablePage)
        #expect(compiledStablePage.rows.count == 2)
        #expect(compiledStablePage.continuation == nil)
        guard case .rdfTerm(.literal(let literal))? =
                compiledStablePage.rows[0].fields["label"] else {
            Issue.record("Expected a canonical language literal")
            return
        }
        #expect(literal.languageTag?.rawValue == "en-us")

        let adults = SelectQuery(
            projection: .all,
            source: .table(TableRef(SchemaDrivenRuntimeAccount.persistableType)),
            filter: .greaterThanOrEqual(
                .column(ColumnRef(column: "age")),
                .literal(.int(18))
            ),
            orderBy: [
                SortKey(
                    .column(ColumnRef(column: "email")),
                    direction: .ascending
                )
            ]
        )
        let compiledAdults = try await compiled.testBaseContext().query(adults)
        let schemaDrivenAdults = try await schemaDriven.testBaseContext().query(adults)
        expectEqual(compiledAdults, schemaDrivenAdults)

        let indexedLookup = SelectQuery(
            projection: .all,
            source: .table(TableRef(SchemaDrivenRuntimeAccount.persistableType)),
            filter: .equal(
                .column(ColumnRef(column: "email")),
                .literal(.string("alice@example.com"))
            )
        )
        let compiledLookup = try await compiled.testBaseContext().query(indexedLookup)
        let schemaDrivenLookup = try await schemaDriven.testBaseContext().query(indexedLookup)
        expectEqual(compiledLookup, schemaDrivenLookup)

        let updatedAlice = try persistedAccount(
            id: "account-alice",
            email: "alice+updated@example.com",
            age: 32,
            nickname: .string("Alice")
        )
        for container in [compiled, schemaDriven] {
            try await container.testBaseContext().withTransaction(
                configuration: .batch
            ) { transaction in
                try await transaction.savePersistedModel(
                    updatedAlice,
                    precondition: .exists
                )
                try await transaction.deletePersistedModel(
                    bob,
                    precondition: .exists
                )
            }
        }

        let allRows = SelectQuery(
            projection: .all,
            source: .table(TableRef(SchemaDrivenRuntimeAccount.persistableType)),
            orderBy: [
                SortKey(
                    .column(ColumnRef(column: "id")),
                    direction: .ascending
                )
            ]
        )
        let compiledRows = try await compiled.testBaseContext().query(allRows)
        let schemaDrivenRows = try await schemaDriven.testBaseContext().query(allRows)
        expectEqual(compiledRows, schemaDrivenRows)
        #expect(compiledRows.rows.count == 1)
        #expect(
            compiledRows.rows.first?.fields["email"]
                == .string("alice+updated@example.com")
        )
    }

    @Test("Schema-driven adaptation applies defaults and rejects invalid canonical values")
    func defaultsAreAppliedAndInvalidCanonicalValuesAreRejected() throws {
        let entity = try SchemaDrivenRuntimeAccount.schemaEntity
        let runtime = EntityRuntimeDefinition(schemaDriven: entity)
            .registration()

        let defaulted = try runtime.persistedModel(
            from: try FieldObject([
                (key: "id", value: .string("missing-age")),
                (key: "email", value: .string("person@example.com")),
            ])
        )
        #expect(defaulted.value(forFieldNamed: "age") == .int64(0))

        do {
            _ = try runtime.persistedModel(
                from: try FieldObject([
                    (key: "id", value: .string("wrong-type")),
                    (key: "email", value: .string("person@example.com")),
                    (key: "age", value: .string("thirty")),
                ])
            )
            Issue.record("Expected a field type failure")
        } catch SchemaDrivenEntityRuntimeError.invalidFieldValue(
            let entityName,
            let field,
            _
        ) {
            #expect(entityName == SchemaDrivenRuntimeAccount.persistableType)
            #expect(field == "age")
        }

        do {
            _ = try runtime.persistedModel(
                from: try FieldObject([
                    (key: "id", value: .string("unknown-field")),
                    (key: "email", value: .string("person@example.com")),
                    (key: "age", value: .int64(30)),
                    (key: "undeclared", value: .bool(true)),
                ])
            )
            Issue.record("Expected an unknown field failure")
        } catch SchemaDrivenEntityRuntimeError.unknownField(
            let entityName,
            let field
        ) {
            #expect(entityName == SchemaDrivenRuntimeAccount.persistableType)
            #expect(field == "undeclared")
        }
    }

    @Test("Applied schema and canonical runtime restore after SQLite restart")
    func appliedSchemaRestoresAfterRestart() async throws {
        let database = try SQLiteTestDatabase(
            prefix: "schema-driven-runtime-restore"
        )
        defer { database.remove() }
        let emptySchema = try Schema(
            entities: [],
            version: Schema.Version(0, 0, 0)
        )
        let targetSchema = try makeSchema()
        let first = try await DBContainer.openRestoringSchema(
            configuration: DBConfiguration.testing(
                storageEngine: try SQLiteStorageEngine(
                    configuration: .file(database.path)
                )
            ),
            security: .testingDisabled
        ) { schema in
            try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                schema: schema)
        }

        let publication: DatabaseSchemaPublicationResult
        do {
            #expect(first.schema == emptySchema)
            let manifest = SchemaManifest(schema: targetSchema)
            publication = try await first.publishSchema(
                targetSchema,
                fingerprint: manifest.fingerprint(),
                expectedFingerprint: first.schemaFingerprint,
                idempotencyKey: "sqlite-schema-restore",
                authorization: TestBaseEnvironment.authorization,
                runtimeConfiguration: try DatabaseFrameworkRuntime
                    .configuration(
                        executionIdentity: DatabaseExecutionRuntimeIdentity(
                            identifier: "database-tests",
                            revision: 1
                        ),
                        schema: targetSchema
                    )
            )
            try await save(
                [
                    try persistedAccount(
                        id: "persisted-across-restart",
                        email: "restart@example.com",
                        age: 44,
                        nickname: .null
                    )
                ],
                in: first
            )
            await first.shutdown()
        } catch {
            await first.shutdown()
            throw error
        }

        let reopened = try await DBContainer.openRestoringSchema(
            configuration: DBConfiguration.testing(
                storageEngine: try SQLiteStorageEngine(
                    configuration: .file(database.path)
                )
            ),
            security: .testingDisabled
        ) { schema in
            try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                schema: schema)
        }
        defer { await reopened.shutdown() }

        #expect(reopened.schema == targetSchema)
        #expect(reopened.schemaFingerprint == publication.fingerprint)
        #expect(reopened.schemaGeneration == publication.generation)
        let response = try await reopened.testBaseContext().query(
            SelectQuery(
                projection: .all,
                source: .table(
                    TableRef(SchemaDrivenRuntimeAccount.persistableType)
                ),
                filter: .equal(
                    .column(ColumnRef(column: "email")),
                    .literal(.string("restart@example.com"))
                )
            )
        )
        #expect(response.rows.count == 1)
        #expect(
            response.rows.first?.fields["id"]
                == .string("persisted-across-restart")
        )
    }

    @Test("Every SQLite-compatible built-in index writes identical canonical bytes")
    func indexMaintenanceMatchesCompiledRuntime() async throws {
        let schema = try Schema(
            entities: [try SchemaDrivenIndexParityEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let compiled = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenIndexParityEntity.self
                    )
                ]
            )
        )
        let schemaDriven = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                schema: schema
            )
        )
        defer {
            await compiled.shutdown()
            await schemaDriven.shutdown()
        }

        let inserted = try PersistedModel(parityEntity(revision: 1))
        for container in [compiled, schemaDriven] {
            try await container.testBaseContext().withTransaction(
                configuration: .batch
            ) { transaction in
                try await transaction.savePersistedModel(
                    inserted,
                    precondition: .notExists
                )
            }
        }
        try await expectIndexBytesEqual(compiled, schemaDriven)

        let updated = try PersistedModel(parityEntity(revision: 2))
        for container in [compiled, schemaDriven] {
            try await container.testBaseContext().withTransaction(
                configuration: .batch
            ) { transaction in
                try await transaction.savePersistedModel(
                    updated,
                    precondition: .exists
                )
            }
        }
        try await expectIndexBytesEqual(compiled, schemaDriven)

        for container in [compiled, schemaDriven] {
            try await container.testBaseContext().withTransaction(
                configuration: .batch
            ) { transaction in
                try await transaction.deletePersistedModel(
                    updated,
                    precondition: .exists
                )
            }
        }
        try await expectIndexBytesEqual(compiled, schemaDriven)
    }

    @Test("Version indexes are rejected before publication on SQLite")
    func versionIndexRequiresVersionstampedMutations() async throws {
        let schema = try Schema(
            entities: [try SchemaDrivenVersionCapabilityEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let configurations = [
            try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenVersionCapabilityEntity.self
                    )
                ]
            ),
            try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                schema: schema),
        ]

        for runtimeConfiguration in configurations {
            do {
                _ = try await makeContainer(
                    schema: schema,
                    runtimeConfiguration: runtimeConfiguration
                )
                Issue.record("Expected SQLite to reject a version index")
            } catch DatabaseRuntimeConfigurationError
                .unsupportedStorageCapability(
                    let source,
                    let indexName,
                    let indexType,
                    let capability
                ) {
                #expect(
                    source == .entity(
                        SchemaDrivenVersionCapabilityEntity.persistableType
                    )
                )
                #expect(indexName == "version_capability")
                #expect(indexType == .history)
                #expect(capability == .versionstampedMutations)
            }
        }
    }

    private func makeSchema() throws -> Schema {
        try Schema(
            entities: [try SchemaDrivenRuntimeAccount.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
    }

    private func parityEntity(
        revision: Int
    ) throws -> SchemaDrivenIndexParityEntity {
        SchemaDrivenIndexParityEntity(
            id: "parity-entity",
            email: revision == 1 ? "first@example.com" : "second@example.com",
            body: revision == 1
                ? "canonical runtime first revision"
                : "canonical runtime second revision",
            embedding: try Vector(
                float32: revision == 1
                    ? [0.1, 0.2, 0.3]
                    : [0.4, 0.5, 0.6]
            ),
            location: try GeographicPoint(
                latitude: revision == 1 ? 35.6812 : 34.6937,
                longitude: revision == 1 ? 139.7671 : 135.5023
            ),
            score: Int64(100 * revision),
            region: revision == 1 ? "east" : "west",
            status: revision == 1 ? "active" : "archived",
            country: "JP",
            city: revision == 1 ? "Tokyo" : "Osaka",
            name: "Canonical",
            source: revision == 1 ? "node-a" : "node-b",
            relation: "connects",
            target: revision == 1 ? "node-b" : "node-c",
            amount: Int64(1_000 * revision),
            customerID: revision == 1 ? "customer-a" : "customer-b",
            latency: Double(revision) * 12.5,
            note: revision == 1 ? nil : "updated"
        )
    }

    private func expectIndexBytesEqual(
        _ compiled: DBContainer,
        _ schemaDriven: DBContainer
    ) async throws {
        for descriptor in try SchemaDrivenIndexParityEntity.indexDescriptors {
            let compiledEntries = try await relativeIndexEntries(
                in: compiled,
                indexName: descriptor.name
            )
            let schemaDrivenEntries = try await relativeIndexEntries(
                in: schemaDriven,
                indexName: descriptor.name
            )
            #expect(
                compiledEntries == schemaDrivenEntries,
                "Canonical index bytes differ for \(descriptor.name)"
            )
        }
    }

    private func relativeIndexEntries(
        in container: DBContainer,
        indexName: String
    ) async throws -> [RelativeIndexEntry] {
        try await container.withTestBaseOperation {
        let entitySubspace = try await container.resolveDirectory(
            for: SchemaDrivenIndexParityEntity.self
        )
        let indexSubspace = try IndexLifecycleStore(
                container: container,
                subspace: entitySubspace
            ).indexSubspace(for: indexName)
        let range = indexSubspace.range()
        return try await container.engine.withTransaction { transaction in
            try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                snapshot: true
            ).map { key, value in
                RelativeIndexEntry(
                    key: key[(key.startIndex + indexSubspace.prefix.count)..<key.endIndex],
                    value: value
                )
            }
        }
        }
    }

    private func makeContainer(
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) async throws -> DBContainer {
        try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: runtimeConfiguration,
            security: .testingDisabled
        )
    }

    private func persistedAccount(
        id: String,
        email: String,
        age: Int64,
        nickname: FieldValue
    ) throws -> PersistedModel {
        try PersistedModel(
            entity: SchemaDrivenRuntimeAccount.persistableType,
            fields: [
                try PersistableField(number: 1, name: "id", value: .string(id)),
                try PersistableField(number: 2, name: "email", value: .string(email)),
                try PersistableField(number: 3, name: "age", value: .int64(age)),
                try PersistableField(number: 4, name: "nickname", value: nickname),
            ]
        )
    }

    private func save(
        _ models: [PersistedModel],
        in container: DBContainer
    ) async throws {
        try await container.testBaseContext().withTransaction(
            configuration: .batch
        ) { transaction in
            for model in models {
                try await transaction.savePersistedModel(
                    model,
                    precondition: .notExists
                )
            }
        }
    }

    private func expectEqual(
        _ left: QueryResponse,
        _ right: QueryResponse
    ) {
        #expect(left.rows == right.rows)
        #expect(left.continuation == right.continuation)
        #expect(left.metadata == right.metadata)
        #expect(left.affectedRows == right.affectedRows)
    }
}
#endif
