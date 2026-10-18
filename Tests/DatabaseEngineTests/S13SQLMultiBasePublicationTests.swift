#if MultiBase
@_spi(Testing) @_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Suite("S13 atomic MultiBase SQL integrity publication", .timeLimit(.minutes(1)))
struct S13SQLMultiBasePublicationTests {
    @Test("Invalid rows in the second retained root reject publication atomically")
    func secondRootRejectsAndPreservesFloor() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        try await save("first", value: 1, base: fixture.first, container: container)
        try await save("bad", value: -1, base: fixture.second, container: container)
        let old = container.acquirePublishedSchemaLease()
        await #expect {
            _ = try await publish(positiveCatalog(), key: "invalid-second", container: container)
        } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .check && actualRelation == "items" && actualConstraint == "positive"
        }
        #expect(container.schemaGeneration == old.generation)
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        #expect(try await durableCatalog(container) == old.sqlCatalog.catalog)
        try await withLease(old, container: container) {
            try await save("still-admitted", value: -2, base: fixture.first, container: container)
        }
        #expect(try await value("still-admitted", base: fixture.first, container: container) == -2)
    }

    @Test("Valid roots publish together and reject a writer retaining old integrity rules")
    func validRootsAndOldWriter() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(localEngine: engine)
        let container = fixture.container
        defer { await container.shutdown() }
        try await save("first", value: 1, base: fixture.first, container: container)
        try await save("second", value: 2, base: fixture.second, container: container)
        let old = container.acquirePublishedSchemaLease()
        let barrier = engine.control.suspendNextMutatingCommit()
        let writer = Task {
            try await withLease(old, container: container) {
                try await save("racing-stale", value: -3, base: fixture.second, container: container)
            }
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: writer)
        let published: DatabaseSchemaPublicationResult
        do {
            published = try await publish(positiveCatalog(), key: "valid-roots", container: container)
        } catch {
            writer.cancel()
            barrier.release()
            _ = await writer.result
            await monitor.value
            throw error
        }
        barrier.release()
        await #expect(throws: DatabaseSchemaPublicationError.generationConflict(expected: published.generation, actual: old.generation)) {
            try await writer.value
        }
        await monitor.value
        #expect(try await value("racing-stale", base: fixture.second, container: container) == nil)
        #expect(published.generation == old.generation + 1)
        #expect(try await durableCatalog(container) == container.sqlCatalog)
        await #expect(throws: DatabaseSchemaPublicationError.generationConflict(expected: published.generation, actual: old.generation)) {
            try await withLease(old, container: container) {
                try await save("stale", value: -1, base: fixture.second, container: container)
            }
        }
        #expect(try await value("stale", base: fixture.second, container: container) == nil)
        try await save("current", value: 3, base: fixture.second, container: container)
    }

    @Test("A retired retained root participates in integrity validation")
    func retiredRootParticipates() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        try await save("bad", value: -1, base: fixture.second, container: container)
        let record = try await container.controlTransactionExecutor.withTransaction(configuration: .readOnly,
            clock: container.monotonicClock) { transaction in
            try #require(try await container.baseCatalog.load(fixture.second, transaction: transaction))
        }
        _ = try await container.retireBase(fixture.second, expectedRevision: record.revision)
        await #expect {
            _ = try await publish(positiveCatalog(), key: "retired-invalid", container: container)
        } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .check && actualRelation == "items" && actualConstraint == "positive"
        }
    }

    @Test("A harmless rename preserves mutation through an old catalog lease")
    func renamePreservesOldLease() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        _ = try await publish(catalog(name: "renamed_items"), key: "rename", container: container)
        try await withLease(old, container: container) {
            try await save("old-lease", value: -1, base: fixture.second, container: container)
        }
        #expect(try await value("old-lease", base: fixture.second, container: container) == -1)
    }

    @Test("Foreign retained roots explicitly reject integrity publication before metadata writes")
    func foreignRootRejects() async throws {
        let fixture = try await makeFixture(foreignSecond: true)
        let container = fixture.container
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        await #expect(throws: StorageError.unsupportedOperation("SQL integrity publication requires all retained roots in one transaction domain")) {
            _ = try await publish(positiveCatalog(), key: "foreign-rejected", container: container)
        }
        #expect(container.schemaGeneration == old.generation)
        #expect(try await durableCatalog(container) == old.sqlCatalog.catalog)
        try await save("foreign-still-valid", value: -1, base: fixture.second, container: container)
    }

    @Test("New foreign roots cannot bypass an existing SQL integrity writer floor")
    func futureForeignRootRejects() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        _ = try await publish(positiveCatalog(), key: "floor", container: container)
        let third = try Base.ID("third")
        await #expect(throws: StorageError.unsupportedOperation("SQL integrity publication requires all retained roots in one transaction domain")) {
            _ = try await container.provisionBase(third, placementID: fixture.foreignPlacement,
                initialGrants: grants(third), expectedRevision: 0)
        }
        let absent = try await container.controlTransactionExecutor.withTransaction(configuration: .readOnly,
            clock: container.monotonicClock) { transaction in
            try await container.baseCatalog.load(third, transaction: transaction) == nil
        }
        #expect(absent)
        _ = try await publish(catalog(), key: "remove-obligations", container: container)
        _ = try await container.provisionBase(third, placementID: fixture.foreignPlacement,
            initialGrants: grants(third), expectedRevision: 0)
    }

    @Test("Staging checks every root, owns its floor, and rolls back candidate admission")
    func stagingAndRollback() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        try await save("bad", value: -1, base: fixture.second, container: container)
        let old = container.acquirePublishedSchemaLease()
        let work = meter()
        try await context(fixture.first, container: container).withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: work) { transaction in
            let checkpoint = try await transaction.createCheckpoint()
            await #expect {
                _ = try await transaction.stageSQLCatalog(positiveCatalog(), expectedGeneration: old.generation,
                    idempotencyKey: "staged-rejected")
            } throws: { error in
            guard let failure = error as? SQLConstraintError,
                  case .violation(let kind, let actualRelation, let actualConstraint, _, _, _) = failure else { return false }
            return kind == .check && actualRelation == "items" && actualConstraint == "positive"
        }
            try await transaction.rollback(to: checkpoint)
            try await transaction.release(checkpoint)
        }
        #expect(container.schemaGeneration == old.generation)
        // Remove the violating row, then stage and discard an integrity floor.
        try await save("bad", value: 1, base: fixture.second, container: container)
        try await context(fixture.first, container: container).withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: meter()) { transaction in
            let checkpoint = try await transaction.createCheckpoint()
            _ = try await transaction.stageSQLCatalog(positiveCatalog(), expectedGeneration: old.generation,
                idempotencyKey: "staged-discarded")
            try await transaction.rollback(to: checkpoint)
            try await transaction.release(checkpoint)
        }
        try await withLease(old, container: container) {
            try await save("after-rollback", value: -1, base: fixture.first, container: container)
        }
        try await save("after-rollback", value: 1, base: fixture.first, container: container)
        let runtime = try #require(container.runtimeConfiguration.entityRuntimes.registration(named: "Item"))
        let stagedModel = try runtime.persistedModel(from: FieldObject([("id", .string("own-floor-write")), ("value", .int64(2))]))
        try await context(fixture.first, container: container).withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: meter()) { transaction in
            let first = try await transaction.stageSQLCatalog(positiveCatalog(), expectedGeneration: old.generation,
                idempotencyKey: "staged-own-floor")
            _ = try await transaction.stageSQLCatalog(positiveCatalog(name: "staged_items"), expectedGeneration: first.generation,
                idempotencyKey: "staged-rename")
            try await transaction.savePersistedModel(stagedModel, precondition: .none)
        }
        #expect(try await value("own-floor-write", base: fixture.first, container: container) == 2)
        #expect(container.schemaGeneration == old.generation + 2)
        #expect(container.sqlCatalog.relations[0].name == "staged_items")
        #expect(work.retainedIntermediateBytes == 0)
    }

    @Test("Global and every Base require exact-resource administration Grants")
    func administrationAuthority() async throws {
        let fixture = try await makeFixture()
        let container = fixture.container
        defer { await container.shutdown() }
        let globalOnly = AuthorizationContext.authenticated(Principal(identifier: "global-only"))
        let baseOnly = AuthorizationContext.authenticated(Principal(identifier: "base-only"))
        try await container.controlTransactionExecutor.withTransaction(configuration: .batch,
            clock: container.monotonicClock) { transaction in
            let databaseGrants = try await container.databaseGrantStore.direct(transaction: transaction)
            _ = try await container.databaseGrantStore.grant(Security.Grant(subject: .principal("global-only"),
                resource: .database, access: .administer), expectedRevision: databaseGrants.revision, transaction: transaction)
            for id in [fixture.first, fixture.second] {
                let lease = try container.acquireBaseSchemaMaintenanceLease(id)
                defer { lease.finish() }
                let store = DatabaseGrantStore(resource: .base(id), root: lease.systemRoot)
                let baseGrants = try await store.direct(transaction: transaction)
                _ = try await store.grant(Security.Grant(subject: .principal("base-only"), resource: .base(id), access: .administer),
                    expectedRevision: baseGrants.revision, transaction: transaction)
            }
        }
        await #expect(throws: DatabaseGrantAuthorizationError.denied(resource: .base(fixture.first), required: .administer)) {
            _ = try await container.publishSQLCatalog(positiveCatalog(), expectedGeneration: container.schemaGeneration,
                idempotencyKey: "global-only", authorization: globalOnly)
        }
        await #expect(throws: DatabaseGrantAuthorizationError.denied(resource: .database, required: .administer)) {
            _ = try await container.publishSQLCatalog(positiveCatalog(), expectedGeneration: container.schemaGeneration,
                idempotencyKey: "base-only", authorization: baseOnly)
        }
        _ = try await publish(positiveCatalog(), key: "all-authorized", container: container)
    }

    private struct Fixture {
        let container: DBContainer
        let first: Base.ID
        let second: Base.ID
        let foreignPlacement: Base.Placement.ID
    }

    private func makeFixture(localEngine: any StorageEngine = InMemoryEngine(),
        foreignSecond: Bool = false) async throws -> Fixture {
        let localID = try DatabaseStorageDomain.ID("local")
        let foreignID = try DatabaseStorageDomain.ID("foreign")
        let localPlacement = try Base.Placement.ID("local")
        let foreignPlacement = try Base.Placement.ID("foreign")
        let first = try Base.ID("first")
        let second = try Base.ID("second")
        let topology = try DatabaseStorageTopology(controlDomainID: localID, domains: [
            DatabaseStorageDomain(id: localID, rootPath: ["s13-local"], storageEngine: localEngine),
            DatabaseStorageDomain(id: foreignID, rootPath: ["s13-foreign"], storageEngine: InMemoryEngine())
        ], placements: [DatabaseStoragePlacement(id: localPlacement, domainID: localID),
            DatabaseStoragePlacement(id: foreignPlacement, domainID: foreignID)], defaultPlacementID: localPlacement)
        let schema = try Schema(entities: [Schema.Entity(name: "Item", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "value", fieldNumber: 2, type: .int64, queryType: .sql(.bigint))],
            directoryComponents: [.staticPath("s13-publication")])], version: .init(1, 0, 0))
        let configuration = DBConfiguration(testingName: "s13-publication", storageTopology: topology,
            monotonicClock: TestProcessMonotonicClock(), wallClock: FixedTestWallClock(),
            testingBaseID: first, testingPrincipal: Principal(identifier: "test-runner", roles: ["test-runner", "admin"]))
        let container = try await DBContainer.open(testing: schema, configuration: configuration,
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s13-publication", revision: 1),
                schema: schema), security: .testingDisabled)
        _ = try await publish(catalog(), key: "initial", container: container)
        _ = try await container.provisionBase(second, placementID: foreignSecond ? foreignPlacement : localPlacement,
            initialGrants: grants(second), expectedRevision: 0)
        return Fixture(container: container, first: first, second: second, foreignPlacement: foreignPlacement)
    }

    private func catalog(name: String = "items", constraints: [SQLConstraint] = []) throws -> SQLCatalog {
        try SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [SQLRelation(id: "items",
            namespaceID: "public", name: name, entityName: "Item", columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
                SQLColumn(id: "value", name: "value", fieldNumber: 2)], constraints: constraints)])
    }

    private func positiveCatalog(name: String = "items") throws -> SQLCatalog {
        try catalog(name: name, constraints: [SQLConstraint(attributes: SQLConstraintAttributes(id: "positive", name: "positive"),
            kind: .check(.greaterThan(.col("value"), .int(0))))])
    }

    private func grants(_ id: Base.ID) -> [Security.Grant] {
        [Security.Grant(subject: .principal("test-runner"), resource: .base(id), access: .all)]
    }

    private func context(_ base: Base.ID, container: DBContainer) -> DatabaseContext {
        container.session(authorization: TestBaseEnvironment.authorization).base(base).newContext()
    }

    private func save(_ id: String, value: Int64, base: Base.ID, container: DBContainer) async throws {
        let runtime = try #require(container.runtimeConfiguration.entityRuntimes.registration(named: "Item"))
        let model = try runtime.persistedModel(from: FieldObject([("id", .string(id)), ("value", .int64(value))]))
        try await context(base, container: container).withTransaction(configuration: .batch) { transaction in
            try await transaction.savePersistedModel(model, precondition: .none)
        }
    }

    private func value(_ id: String, base: Base.ID, container: DBContainer) async throws -> Int64? {
        try await context(base, container: container).withTransaction(configuration: .readOnly) { transaction in
            guard let model = try await transaction.loadPersistedModel(entity: "Item", id: Tuple(id), partition: nil),
                case .int64(let value) = model.value(forFieldNamed: "value") else { return nil }
            return value
        }
    }

    private func publish(_ catalog: SQLCatalog, key: String, container: DBContainer) async throws -> DatabaseSchemaPublicationResult {
        try await container.publishSQLCatalog(catalog, expectedGeneration: container.schemaGeneration,
            idempotencyKey: key, authorization: TestBaseEnvironment.authorization)
    }

    private func durableCatalog(_ container: DBContainer) async throws -> SQLCatalog? {
        try await container.controlTransactionExecutor.withTransaction(configuration: .readOnly,
            clock: container.monotonicClock) { transaction in
            try await SQLCatalogStore(root: container.metadataSubspace).load(transaction: transaction)?.catalog
        }
    }

    private func withLease(_ lease: DatabaseSchemaLease, container: DBContainer,
        _ operation: @Sendable () async throws -> Void) async throws {
        try await DatabaseSchemaExecutionScope.$container.withValue(container) {
            try await DatabaseSchemaExecutionScope.$lease.withValue(lease) {
                try await operation()
            }
        }
    }

    private func meter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1_000, maximumWorkUnits: 100_000,
            maximumIntermediateRows: 1_000, maximumIntermediateBytes: 8_000_000), monotonicClock: TestProcessMonotonicClock())
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 8, maximumRetainedBytes: 1_000_000,
            maximumWorkUnits: 100_000, maximumReadPageRows: 128)
    }
}
#endif
