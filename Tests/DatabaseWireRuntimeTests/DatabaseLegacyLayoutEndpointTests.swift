#if MultipleBases
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseEngine
@testable import DatabaseWireRuntime
import DatabaseFoundation
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit
import TestSupport
import Testing

@Suite("Legacy layout server execution", .serialized)
struct DatabaseLegacyLayoutEndpointTests {
    @Test("Changed legacy layout fingerprint is rejected before job creation")
    func changedFingerprintIsRejected() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        var changedBytes = Array(fixture.layoutFingerprint)
        changedBytes[0] ^= 0xff
        let changedFingerprint = try DatabaseLayoutFingerprint(changedBytes)
        let idempotencyKey = "legacy-layout-invalid-fingerprint"

        let result: Result<
            BaseExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.baseExecute,
            requestID: 1,
            target: .database,
            metadata: OperationRequestMetadata(
                idempotencyKey: idempotencyKey
            ),
            request: BaseExecuteOperation.Request(
                invocation: .legacyMigrationApply(
                    baseID: fixture.baseID,
                    placementID: fixture.placementID,
                    initialGrants: fixture.initialGrants,
                    expectedLayoutFingerprint: changedFingerprint,
                    expectedRevision: 0,
                    idempotencyKey: idempotencyKey
                )
            ),
            fixture: fixture
        )

        guard case .failure(let error) = result else {
            Issue.record("Expected changed legacy layout to be rejected")
            return
        }
        #expect(error.code == "LAYOUT_FINGERPRINT_CONFLICT")
        #expect(fixture.container.layoutStatus == .migrationRequired)
    }

    @Test("Legacy migration resumes from its durable checkpoint after runtime recreation")
    func migrationResumesAfterRuntimeRecreation() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let fingerprint = try DatabaseLayoutFingerprint(
            fixture.layoutFingerprint
        )
        let idempotencyKey = "legacy-layout-runtime-recreation"
        let start: Result<
            BaseExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.baseExecute,
            requestID: 1,
            target: .database,
            metadata: OperationRequestMetadata(
                idempotencyKey: idempotencyKey
            ),
            request: BaseExecuteOperation.Request(
                invocation: .legacyMigrationApply(
                    baseID: fixture.baseID,
                    placementID: fixture.placementID,
                    initialGrants: fixture.initialGrants,
                    expectedLayoutFingerprint: fingerprint,
                    expectedRevision: 0,
                    idempotencyKey: idempotencyKey
                )
            ),
            fixture: fixture
        )
        guard case .success(.job(let job)) = start else {
            Issue.record("Expected a durable migration job")
            return
        }

        try await fixture.runtime.runScheduledWork()
        let checkpoint: Result<
            JobStatusOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.jobStatus,
            requestID: 2,
            target: .database,
            request: JobStatusOperation.Request(job: job),
            fixture: fixture
        )
        guard case .success(let checkpointStatus) = checkpoint else {
            Issue.record("Expected the first durable migration checkpoint")
            return
        }
        #expect(checkpointStatus.state == .pending)
        #expect(checkpointStatus.completedWorkUnits == 1)

        let recreatedRuntime = try await makeRuntime(
            container: fixture.container,
            authorization: fixture.authorization
        )
        var terminalStatus: JobStatusOperation.Response?
        for requestID in 3..<132 {
            try await recreatedRuntime.runScheduledWork()
            let status: Result<
                JobStatusOperation.Response,
                RemoteOperationError
            > = try await invoke(
                DatabaseOperations.jobStatus,
                requestID: UInt64(requestID),
                target: .database,
                request: JobStatusOperation.Request(job: job),
                fixture: fixture,
                runtime: recreatedRuntime
            )
            guard case .success(let response) = status else {
                Issue.record("Expected resumed migration job status")
                return
            }
            if response.state == .succeeded
                || response.state == .failed
                || response.state == .cancelled {
                terminalStatus = response
                break
            }
        }
        let completedStatus = try #require(terminalStatus)
        #expect(completedStatus.state == .succeeded)
        #expect(completedStatus.executionCount > 1)
        #expect(fixture.container.layoutStatus == .current)
    }

    @Test("Runtime remains control-only and migrates through the durable job path")
    func controlOnlyRuntimeMigratesThroughDurableJob() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        let deniedQuery: Result<
            QueryExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.queryExecute,
            requestID: 1,
            target: .base(fixture.baseID),
            request: QueryExecuteOperation.Request(
                input: .text(
                    language: .sql,
                    statement: "SELECT * FROM LegacyEntity"
                )
            ),
            fixture: fixture
        )
        guard case .failure(let migrationRequired) = deniedQuery else {
            Issue.record("Expected the data operation to be rejected")
            return
        }
        #expect(migrationRequired.code == "MIGRATION_REQUIRED")

        let planResult: Result<
            BaseExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.baseExecute,
            requestID: 2,
            target: .database,
            request: BaseExecuteOperation.Request(
                invocation: .legacyMigrationPlan(
                    baseID: fixture.baseID,
                    placementID: fixture.placementID,
                    initialGrants: fixture.initialGrants
                )
            ),
            fixture: fixture
        )
        guard case .success(.plan(let plan)) = planResult,
              let fingerprint = plan.layoutFingerprint else {
            Issue.record("Expected a legacy migration plan")
            return
        }
        #expect(plan.action == .migrateLegacyLayout)
        #expect(fingerprint.bytes == fixture.layoutFingerprint)

        let idempotencyKey = "legacy-layout-endpoint-migration"
        let applyResult: Result<
            BaseExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.baseExecute,
            requestID: 3,
            target: .database,
            metadata: OperationRequestMetadata(
                idempotencyKey: idempotencyKey
            ),
            request: BaseExecuteOperation.Request(
                invocation: .legacyMigrationApply(
                    baseID: fixture.baseID,
                    placementID: fixture.placementID,
                    initialGrants: fixture.initialGrants,
                    expectedLayoutFingerprint: fingerprint,
                    expectedRevision: 0,
                    idempotencyKey: idempotencyKey
                )
            ),
            fixture: fixture
        )
        guard case .success(.job(let job)) = applyResult else {
            Issue.record(
                "Expected a persistent migration job, received: \(applyResult)"
            )
            return
        }

        var terminalStatus: JobStatusOperation.Response?
        for requestID in 4..<132 {
            try await fixture.runtime.runScheduledWork()
            let statusResult: Result<
                JobStatusOperation.Response,
                RemoteOperationError
            > = try await invoke(
                DatabaseOperations.jobStatus,
                requestID: UInt64(requestID),
                target: .database,
                request: JobStatusOperation.Request(job: job),
                fixture: fixture
            )
            guard case .success(let status) = statusResult else {
                Issue.record("Expected the migration job status")
                return
            }
            if status.state == .succeeded
                || status.state == .failed
                || status.state == .cancelled {
                terminalStatus = status
                break
            }
        }
        let status = try #require(terminalStatus)
        #expect(status.state == .succeeded)
        #expect(fixture.container.layoutStatus == .current)

        let record = try await fixture.container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try #require(
                try await fixture.container.baseCatalog.load(
                    fixture.baseID,
                    transaction: transaction.storageAccess
                )
            )
        }
        #expect(record.lifecycle == .active)
        #expect(record.revision == 2)
        let oldValue = try await fixture.engine.withTransaction { transaction in
            try await transaction.getValue(
                for: fixture.legacyKey,
                snapshot: true
            )
        }
        #expect(oldValue == nil)
    }

    private struct Fixture {
        let engine: InMemoryEngine
        let container: DBContainer
        let runtime: DatabaseOperationRuntime
        let authorization: AuthorizationContext
        let baseID: Base.ID
        let placementID: Base.Placement.ID
        let initialGrants: [Security.Grant]
        let layoutFingerprint: ByteString
        let legacyKey: ByteString
    }

    private actor Scheduler: DatabaseJobScheduler {
        func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
            _ = timestamp
        }
    }

    private func makeFixture() async throws -> Fixture {
        let engine = InMemoryEngine()
        let clock = TestProcessMonotonicClock()
        _ = try await DatabaseFormatCatalog(
            database: engine,
            root: Subspace(),
            clock: clock
        ).installIfEmptyOrValidate(.v1(itemStorage: .v1))
        let legacyRoot = Subspace(prefix: Tuple([
            "_database-framework", "rdf-graph-store", Int64(1),
        ]).pack())
        let legacyKey = legacyRoot.pack(Tuple("endpoint-fixture"))
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(utf8: "legacy-endpoint-value"),
                for: legacyKey
            )
        }

        let domainID = try DatabaseStorageDomain.ID("primary")
        let placementID = try Base.Placement.ID("default")
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["database", "main"],
                    storageEngine: engine
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: placementID
        )
        let schema = try Schema(
            entities: [],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(
                storageTopology: topology,
                monotonicClock: clock,
                wallClock: RealtimeDatabaseWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            ),
            security: .testingDisabled
        )
        let authorization = AuthorizationContext.authenticated(
            Principal(identifier: "migration-operator", roles: ["admin"])
        )
        let baseID = try Base.ID("migrated")
        let initialGrants = [
            Security.Grant(
                subject: .principal("migration-operator"),
                resource: .base(baseID),
                access: .all
            ),
        ]
        let inventory = try await container.legacyLayoutInventory()
        let layoutFingerprint = try await container.legacyLayoutFingerprint(
            inventory: inventory
        )
        let runtime = try await makeRuntime(
            container: container,
            authorization: authorization
        )
        return Fixture(
            engine: engine,
            container: container,
            runtime: runtime,
            authorization: authorization,
            baseID: baseID,
            placementID: placementID,
            initialGrants: initialGrants,
            layoutFingerprint: layoutFingerprint,
            legacyKey: legacyKey
        )
    }

    private func makeRuntime(
        container: DBContainer,
        authorization: AuthorizationContext
    ) async throws -> DatabaseOperationRuntime {
        let identifierGenerator = RandomDatabaseUUIDGenerator()
        let jobFactory = try DatabasePersistentJobServiceFactory(
            registry: DatabaseResumableOperationRegistry(operations: []),
            identifierGenerator: identifierGenerator,
            storageLimits: DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_576
            )
        )
        return try await DatabaseOperationRuntime(
            container: container,
            configuration: try DatabaseOperationRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "legacy-test"),
                serviceFactory: AnyDatabaseOperationServiceFactory(
                    PersistentLifecycleTestServiceFactory(
                        jobFactory: jobFactory
                    )
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock()
            ),
            hostServices: try testJobHostServices(
                scheduler: Scheduler(),
                identifierGenerator: identifierGenerator,
                authorization: authorization
            )
        )
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        requestID: UInt64,
        target: DatabaseOperationTarget,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        request: Request,
        fixture: Fixture,
        runtime: DatabaseOperationRuntime? = nil
    ) async throws -> Result<Response, RemoteOperationError> {
        let bytes = try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            target: target,
            metadata: metadata,
            request: request
        )
        return try DatabaseWireDecoder().decodeResponse(
            operation,
            from: try await (runtime ?? fixture.runtime).execute(
                bytes,
                context: DatabaseRequestExecutionContext(
                    authorization: fixture.authorization,
                    jobAuthorizationReference:
                        try TestDatabaseJobAuthorizationValidator.reference()
                )
            ),
            matching: requestID
        )
    }
}

final class PersistentLifecycleTestServiceFactory:
    DatabaseOperationServiceFactory,
    Sendable
{
    let jobFactory: DatabasePersistentJobServiceFactory

    init(jobFactory: DatabasePersistentJobServiceFactory) {
        self.jobFactory = jobFactory
    }

    func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        let unavailable = PersistentLifecycleUnavailableService()
        return DatabaseOperationServices(
            graphOperations: GraphOperationServices(
                statementExecutor: CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: context.runtimeLimits
                ),
                algorithm: AnyDatabaseGraphAlgorithmService(unavailable),
                ontology: AnyDatabaseOntologyService(unavailable),
                shacl: AnyDatabaseSHACLService(unavailable)
            ),
            readCommandRegistry: try DatabaseReadCommandRegistry(commands: []),
            writeCommandRegistry: try DatabaseWriteCommandRegistry(commands: []),
            maintenanceService: AnyDatabaseMaintenanceService(unavailable),
            jobService: try await jobFactory.makeJobService(context: context)
        )
    }
}

struct PersistentLifecycleUnavailableService:
    DatabaseGraphAlgorithmService,
    DatabaseOntologyService,
    DatabaseSHACLService,
    DatabaseMaintenanceService
{
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response {
        _ = request
        _ = context
        throw PersistentLifecycleUnavailableError()
    }

    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        _ = request
        _ = context
        throw PersistentLifecycleUnavailableError()
    }

    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecutionResult {
        _ = request
        _ = context
        throw PersistentLifecycleUnavailableError()
    }

    func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult {
        _ = request
        _ = context
        throw PersistentLifecycleUnavailableError()
    }
}

struct PersistentLifecycleUnavailableError: Error {}
#endif
