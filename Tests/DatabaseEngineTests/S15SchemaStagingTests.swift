#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

private enum S15SchemaFailure: Error { case rejected }

@Suite("S15 physical schema candidate staging", .timeLimit(.minutes(1)))
struct S15SchemaStagingTests {
    @Test("Physical rewrite is visible to later statements and commits with its schema")
    func rewriteAndJournalFollowCandidate() async throws {
        let container = try await s15SchemaContainer()
        defer { await container.shutdown() }
        let meter = s15SchemaMeter()
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: s15SchemaLimits(), workMeter: meter) { transaction in
            let old = try s15SchemaModel()
            try await transaction.savePersistedModel(old, precondition: .notExists)
            let original = await transaction.currentSQLSchemaLease()
            let identity = try original.runtimeConfiguration.entityRuntimes.registration(named: "S15Item")!.identity(for: old)
            let oldEntity = try #require(original.schema.entity(named: "S15Item"))
            let numericEntity = try Schema.Entity(name: oldEntity.name, identifierType: oldEntity.identifierType,
                fields: oldEntity.fields.map { field in
                    field.fieldNumber == 2 ? FieldSchema(name: field.name, fieldNumber: field.fieldNumber, type: .int64) : field
                }, directoryComponents: oldEntity.directoryComponents, directoryLayer: oldEntity.directoryLayer)
            let numericCandidate = try container.prepareSQLSchemaCandidate(
                schema: Schema(entities: [numericEntity]), catalog: original.sqlCatalog.catalog,
                sourceLease: original, workMeter: meter)
            _ = try await transaction.stageSQLSchemaCandidate(numericCandidate,
                expectedGeneration: original.generation, idempotencyKey: "convert-value",
                rewrites: [SQLSchemaRewrite(entityName: "S15Item") { old in
                    let converted = try old.fields.map { field in
                        field.number == 2 ? try PersistableField(number: field.number, name: field.name, value: .int64(6)) : field
                    }
                    return try PersistedModel(entity: old.entity, fields: converted)
                }])
            let numeric = try await transaction.fetchPersistedModel(identifiedBy: identity)
            #expect(numeric?.value(forFieldNamed: "value") == .int64(6))
            let source = await transaction.currentSQLSchemaLease()
            let target = try s15SchemaTarget(source: source)
            let candidate = try container.prepareSQLSchemaCandidate(schema: target.schema,
                catalog: target.catalog, sourceLease: source, workMeter: meter)
            _ = try await transaction.stageSQLSchemaCandidate(candidate,
                expectedGeneration: source.generation, idempotencyKey: "drop-value",
                rewrites: [SQLSchemaRewrite(entityName: "S15Item") { old in
                    try PersistedModel(entity: old.entity, fields: old.fields.filter { $0.number == 1 })
                }])
            let row = try await transaction.fetchPersistedModel(identifiedBy: identity)
            #expect(row?.value(forFieldNamed: "value") == nil)
            try await transaction.withStatementScope {
                #expect(container.schema.entity(named: "S15Item")?.fields.count == 1)
            }
            let rewritten = await transaction.currentSQLSchemaLease()
            let added = try Schema.Entity(name: "S15Added", identifierType: .string,
                fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
                directoryComponents: [.staticPath("s15-added")])
            let derived = try SQLCatalogSnapshot.derivedRelation(added, namespaceID: "public")
            let checked = try SQLRelation(id: derived.id, namespaceID: derived.namespaceID,
                name: derived.name, entityName: derived.entityName, columns: derived.columns,
                constraints: [SQLConstraint(attributes: SQLConstraintAttributes(id: "new-check", name: "new_check"),
                    kind: .check(.literal(.bool(true))))])
            let checkedCandidate = try container.prepareSQLSchemaCandidate(
                schema: Schema(entities: rewritten.schema.entities + [added]),
                catalog: SQLCatalog(namespaces: rewritten.sqlCatalog.catalog.namespaces,
                    relations: rewritten.sqlCatalog.catalog.relations + [checked]),
                sourceLease: rewritten, workMeter: meter)
            _ = try await transaction.stageSQLSchemaCandidate(checkedCandidate,
                expectedGeneration: rewritten.generation, idempotencyKey: "new-table-check", rewrites: [])
            try await transaction.withStatementScope {
                #expect(container.schema.entity(named: "S15Added") == added)
            }
        }
        #expect(container.schema.entity(named: "S15Item")?.fields.count == 1)
        #expect(container.schema.entity(named: "S15Added") != nil)
        #expect(container.acquirePublishedSchemaLease().sqlCatalog.sqlOwnedEntityNames == ["S15Added"])
        #expect(container.sqlCatalog.relations.first?.fieldNumberHighWaterMark == 2)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("A physical rewrite requires complete coverage and rolls back to its checkpoint")
    func checkpointRestoresPhysicalContractAndRows() async throws {
        let container = try await s15SchemaContainer()
        defer { await container.shutdown() }
        let meter = s15SchemaMeter()
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: s15SchemaLimits(), workMeter: meter) { transaction in
            let old = try s15SchemaModel()
            try await transaction.savePersistedModel(old, precondition: .notExists)
            let checkpoint = try await transaction.createCheckpoint()
            let source = await transaction.currentSQLSchemaLease()
            let target = try s15SchemaTarget(source: source)
            let candidate = try container.prepareSQLSchemaCandidate(schema: target.schema,
                catalog: target.catalog, sourceLease: source, workMeter: meter)
            _ = try await transaction.stageSQLSchemaCandidate(candidate,
                expectedGeneration: source.generation, idempotencyKey: "discard",
                rewrites: [SQLSchemaRewrite(entityName: "S15Item") { old in
                    try PersistedModel(entity: old.entity, fields: old.fields.filter { $0.number == 1 })
                }])
            try await transaction.rollback(to: checkpoint)
            let restored = await transaction.currentSQLSchemaLease()
            #expect(restored === source)
            let identity = try source.runtimeConfiguration.entityRuntimes.registration(named: "S15Item")!.identity(for: old)
            let row = try await transaction.fetchPersistedModel(identifiedBy: identity)
            #expect(row?.value(forFieldNamed: "value") == .string("before"))
            try await transaction.release(checkpoint)
        }
        #expect(container.schema.entity(named: "S15Item")?.fields.count == 2)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Conversion failure leaves durable schema and source rows unchanged")
    func conversionFailureRollsBack() async throws {
        let container = try await s15SchemaContainer()
        defer { await container.shutdown() }
        let source = container.acquirePublishedSchemaLease()
        let meter = s15SchemaMeter()
        await #expect(throws: S15SchemaFailure.self) {
            try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: s15SchemaLimits(), workMeter: meter) { transaction in
                try await transaction.savePersistedModel(s15SchemaModel(), precondition: .notExists)
                let attemptSource = await transaction.currentSQLSchemaLease()
                let target = try s15SchemaTarget(source: attemptSource)
                let candidate = try container.prepareSQLSchemaCandidate(schema: target.schema,
                    catalog: target.catalog, sourceLease: attemptSource, workMeter: meter)
                _ = try await transaction.stageSQLSchemaCandidate(candidate,
                    expectedGeneration: attemptSource.generation, idempotencyKey: "failure",
                    rewrites: [SQLSchemaRewrite(entityName: "S15Item") { _ in throw S15SchemaFailure.rejected }])
            }
        }
        #expect(container.schema == source.schema)
        #expect(container.sqlCatalog == source.sqlCatalog.catalog)
        #expect(meter.retainedIntermediateBytes == 0)
    }
    @Test("Catching conversion failure cannot commit staged target metadata")
    func caughtFailureStillAborts() async throws {
        let container = try await s15SchemaContainer()
        defer { await container.shutdown() }
        let before = container.acquirePublishedSchemaLease()
        let meter = s15SchemaMeter()
        await #expect(throws: DatabaseTransactionError.self) {
            try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: s15SchemaLimits(), workMeter: meter) { transaction in
                try await transaction.savePersistedModel(s15SchemaModel(), precondition: .notExists)
                let source = await transaction.currentSQLSchemaLease()
                let target = try s15SchemaTarget(source: source)
                let candidate = try container.prepareSQLSchemaCandidate(schema: target.schema,
                    catalog: target.catalog, sourceLease: source, workMeter: meter)
                do {
                    _ = try await transaction.stageSQLSchemaCandidate(candidate,
                        expectedGeneration: source.generation, idempotencyKey: "caught-failure",
                        rewrites: [SQLSchemaRewrite(entityName: "S15Item") { _ in throw S15SchemaFailure.rejected }])
                    Issue.record("Expected the physical conversion to fail")
                } catch S15SchemaFailure.rejected {}
            }
        }
        #expect(container.schema == before.schema)
        #expect(container.schemaGeneration == before.generation)
        #expect(meter.retainedIntermediateBytes == 0)
    }

}

private func s15SchemaContainer() async throws -> DBContainer {
    let entity = try Schema.Entity(name: "S15Item", identifierType: .string,
        fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: .string)],
        directoryComponents: [.staticPath("s15-items")])
    let schema = try Schema(entities: [entity])
    return try await DBContainer.open(for: schema,
        configuration: .testing(storageEngine: InMemoryEngine()),
        runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
            executionIdentity: .init(identifier: "s15-schema", revision: 1), schema: schema),
        security: .testingDisabled)
}

private func s15SchemaTarget(source: DatabaseSchemaLease) throws -> (schema: Schema, catalog: SQLCatalog) {
    let old = try #require(source.schema.entities.first)
    let entity = try Schema.Entity(name: old.name, identifierType: old.identifierType,
        fields: old.fields.filter { $0.fieldNumber == 1 }, directoryComponents: old.directoryComponents,
        directoryLayer: old.directoryLayer)
    let relation = try #require(source.sqlCatalog.catalog.relations.first)
    let target = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
        name: relation.name, entityName: relation.entityName,
        columns: relation.columns.filter { $0.fieldNumber == 1 },
        primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
        primaryKeyConstraint: relation.primaryKeyConstraint,
        fieldNumberHighWaterMark: relation.fieldNumberHighWaterMark)
    return (try Schema(entities: [entity]), try SQLCatalog(namespaces: source.sqlCatalog.catalog.namespaces,
        relations: [target]))
}

private func s15SchemaModel() throws -> PersistedModel {
    try PersistedModel(entity: "S15Item", fields: [
        PersistableField(number: 1, name: "id", value: .string("one")),
        PersistableField(number: 2, name: "value", value: .string("before"))])
}

private func s15SchemaMeter() -> DatabaseWorkMeter {
    DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 8 * 1_024 * 1_024),
        monotonicClock: TestProcessMonotonicClock())
}

private func s15SchemaLimits() throws -> TransactionCheckpointLimits {
    try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 16,
        maximumRetainedBytes: 8 * 1_024 * 1_024, maximumWorkUnits: 100_000, maximumReadPageRows: 128)
}
#endif
