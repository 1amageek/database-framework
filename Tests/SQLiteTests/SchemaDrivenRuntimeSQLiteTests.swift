#if SQLITE
import Database
import DatabaseEngine
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
        .scalar,
        fields: [\SchemaDrivenRuntimeAccount.email],
        name: "schema_driven_runtime_account_email"
    )

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
        .scalar,
        fields: [\SchemaDrivenIndexParityEntity.email],
        name: "parity_scalar"
    )
    #Index(
        .vector(dimensions: 3),
        embedding: \SchemaDrivenIndexParityEntity.embedding,
        name: "parity_vector"
    )
    #Index(
        .fullText(tokenizer: .simple),
        fields: [\SchemaDrivenIndexParityEntity.body],
        name: "parity_fulltext"
    )
    #Index(
        .spatial(),
        location: \SchemaDrivenIndexParityEntity.location,
        name: "parity_spatial"
    )
    #Index(
        .rank,
        field: \SchemaDrivenIndexParityEntity.score,
        name: "parity_rank"
    )
    #Index(
        .sum,
        groupBy: [\SchemaDrivenIndexParityEntity.region],
        value: \SchemaDrivenIndexParityEntity.amount,
        name: "parity_sum"
    )
    #Index(
        .distinct(),
        groupBy: [\SchemaDrivenIndexParityEntity.region],
        value: \SchemaDrivenIndexParityEntity.customerID,
        name: "parity_distinct"
    )
    #Index(
        .percentile(),
        groupBy: [\SchemaDrivenIndexParityEntity.region],
        value: \SchemaDrivenIndexParityEntity.latency,
        name: "parity_percentile"
    )
    #Index(
        .bitmap,
        field: \SchemaDrivenIndexParityEntity.status,
        name: "parity_bitmap"
    )
    #Index(
        .timeWindowLeaderboard(window: .daily, windowCount: 2),
        groupBy: [\SchemaDrivenIndexParityEntity.region],
        field: \SchemaDrivenIndexParityEntity.score,
        name: "parity_leaderboard"
    )
    #Index(
        .permuted(.swapping(0, 1, size: 3)),
        fields: [
            \SchemaDrivenIndexParityEntity.country,
            \SchemaDrivenIndexParityEntity.city,
            \SchemaDrivenIndexParityEntity.name,
        ],
        name: "parity_permuted"
    )
    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \SchemaDrivenIndexParityEntity.source,
        edge: \SchemaDrivenIndexParityEntity.relation,
        to: \SchemaDrivenIndexParityEntity.target,
        name: "parity_graph"
    )
}

@Persistable
private struct SchemaDrivenVersionCapabilityEntity {
    #Directory<SchemaDrivenVersionCapabilityEntity>(
        "schema-driven-runtime",
        "version-capability"
    )
    #Index(
        .version(strategy: .keepAll),
        field: \SchemaDrivenVersionCapabilityEntity.id,
        name: "version_capability"
    )

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
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenRuntimeAccount.self
                    ),
                ]
            )
        )
        let schemaDriven = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
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
                ),
            ]
        )
        let compiledAdults = try await compiled.newContext().query(adults)
        let schemaDrivenAdults = try await schemaDriven.newContext().query(adults)
        expectEqual(compiledAdults, schemaDrivenAdults)

        let indexedLookup = SelectQuery(
            projection: .all,
            source: .table(TableRef(SchemaDrivenRuntimeAccount.persistableType)),
            filter: .equal(
                .column(ColumnRef(column: "email")),
                .literal(.string("alice@example.com"))
            )
        )
        let compiledLookup = try await compiled.newContext().query(indexedLookup)
        let schemaDrivenLookup = try await schemaDriven.newContext().query(indexedLookup)
        expectEqual(compiledLookup, schemaDrivenLookup)

        let updatedAlice = try persistedAccount(
            id: "account-alice",
            email: "alice+updated@example.com",
            age: 32,
            nickname: .string("Alice")
        )
        for container in [compiled, schemaDriven] {
            try await container.newContext().withTransaction(
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
                ),
            ]
        )
        let compiledRows = try await compiled.newContext().query(allRows)
        let schemaDrivenRows = try await schemaDriven.newContext().query(allRows)
        expectEqual(compiledRows, schemaDrivenRows)
        #expect(compiledRows.rows.count == 1)
        #expect(
            compiledRows.rows.first?.fields["email"]
                == .string("alice+updated@example.com")
        )
    }

    @Test("Schema-driven adaptation rejects invalid canonical values")
    func invalidCanonicalValuesAreRejected() throws {
        let entity = try SchemaDrivenRuntimeAccount.schemaEntity
        let runtime = EntityRuntimeDefinition(schemaDriven: entity)
            .registration()

        do {
            _ = try runtime.persistedModel(
                from: try FieldObject([
                    (key: "id", value: .string("missing-age")),
                    (key: "email", value: .string("person@example.com")),
                ])
            )
            Issue.record("Expected a missing required field failure")
        } catch SchemaDrivenEntityRuntimeError.missingRequiredField(
            let entityName,
            let field
        ) {
            #expect(entityName == SchemaDrivenRuntimeAccount.persistableType)
            #expect(field == "age")
        }

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
            security: .disabled
        ) { schema in
            try DatabaseFrameworkRuntime.configuration(schema: schema)
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
                runtimeConfiguration: try DatabaseFrameworkRuntime
                    .configuration(schema: targetSchema)
            )
            try await save(
                [
                    try persistedAccount(
                        id: "persisted-across-restart",
                        email: "restart@example.com",
                        age: 44,
                        nickname: .null
                    ),
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
            security: .disabled
        ) { schema in
            try DatabaseFrameworkRuntime.configuration(schema: schema)
        }
        defer { await reopened.shutdown() }

        #expect(reopened.schema == targetSchema)
        #expect(reopened.schemaFingerprint == publication.fingerprint)
        #expect(reopened.schemaGeneration == publication.generation)
        let response = try await reopened.newContext().query(
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
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenIndexParityEntity.self
                    ),
                ]
            )
        )
        let schemaDriven = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            )
        )
        defer {
            await compiled.shutdown()
            await schemaDriven.shutdown()
        }

        let inserted = try PersistedModel(parityEntity(revision: 1))
        for container in [compiled, schemaDriven] {
            try await container.newContext().withTransaction(
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
            try await container.newContext().withTransaction(
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
            try await container.newContext().withTransaction(
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
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenVersionCapabilityEntity.self
                    ),
                ]
            ),
            try DatabaseFrameworkRuntime.configuration(schema: schema),
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
                    let kindIdentifier,
                    let capability
                ) {
                #expect(
                    source == .entity(
                        SchemaDrivenVersionCapabilityEntity.persistableType
                    )
                )
                #expect(indexName == "version_capability")
                #expect(kindIdentifier == "version")
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
        let entitySubspace = try await container.resolveDirectory(
            for: SchemaDrivenIndexParityEntity.self
        )
        let indexSubspace = entitySubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
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

    private func makeContainer(
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) async throws -> DBContainer {
        try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: runtimeConfiguration,
            security: .disabled
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
        try await container.newContext().withTransaction(
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
