import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseOperations
import DatabaseTypes
import DatabaseWire
import StorageKit
import Synchronization
import Testing

private let maintenanceJobTestStorageLimits =
    DatabasePersistentJobStorageLimits(maximumStorageValueBytes: 1_048_576)
private let maintenanceJobStartRequirement = DatabaseOperationRequirement(
    acceptedTargets: [.database, .base],
    access: .administer,
    transaction: .write,
    baseAdmission: .activeData
)

@Suite("Database maintenance operation service", .serialized)
struct DatabaseMaintenanceOperationServiceTests {
    @Test("Migration status and bounded execution use the compiled plan")
    func migrationsReportAndExecuteExactStages() async throws {
        let engine = InMemoryEngine()
        let target = try await DBContainer.open(
            for: MaintenanceSchemaV3.self,
            migrationPlan: MaintenanceMigrationPlan.self,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(CatalogPartitionedEntity.self)]
            ),
            security: .testingDisabled
        )
        try await target.installTestBaseSchemaSnapshot(
            for: MaintenanceSchemaV1.versionIdentifier
        )
        let maintenanceContext = try await makeMaintenanceServiceContext(
            container: target
        )
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )

        let statusResponse = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .migrationStatus
            ),
            context: try operationContext(container: target)
        ).response
        guard case .migrationStatus(let status) = statusResponse else {
            Issue.record("Expected migration status")
            return
        }
        #expect(status.currentVersion == Schema.Version(1, 0, 0))
        #expect(status.targetVersion == Schema.Version(3, 0, 0))
        #expect(status.pendingMigrationIdentifiers == [
            "migration:1.0.0->2.0.0",
            "migration:2.0.0->3.0.0",
        ])

        let firstRequest = migrationRequest()
        let first = try await service.execute(
            firstRequest,
            context: try operationContext(
                container: target,
                request: firstRequest,
                idempotencyKey: "migration-first"
            )
        ).response
        let firstResult = try executionResult(first)
        #expect(firstResult.kind == .migrations)
        #expect(firstResult.completedWorkUnits == 1)
        #expect(!firstResult.isComplete)
        let continuation = try #require(firstResult.continuation)
        #expect(try await target.testBaseCurrentSchemaVersion() == Schema.Version(2, 0, 0))

        let secondRequest = MaintenanceExecuteOperation.Request(
            invocation: firstRequest.invocation,
            continuation: continuation,
            budget: firstRequest.budget
        )
        let second = try await service.execute(
            secondRequest,
            context: try operationContext(
                container: target,
                request: secondRequest,
                idempotencyKey: "migration-second"
            )
        ).response
        let secondResult = try executionResult(second)
        #expect(secondResult.completedWorkUnits == 2)
        #expect(secondResult.isComplete)
        #expect(secondResult.continuation == nil)
        #expect(try await target.testBaseCurrentSchemaVersion() == Schema.Version(3, 0, 0))
    }

    @Test("Persistent migration job resumes the compiled plan")
    func persistentMigrationJobResumes() async throws {
        let engine = InMemoryEngine()
        let target = try await DBContainer.open(
            for: MaintenanceSchemaV3.self,
            migrationPlan: MaintenanceMigrationPlan.self,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(CatalogPartitionedEntity.self)]
            ),
            security: .testingDisabled
        )
        try await target.installTestBaseSchemaSnapshot(
            for: MaintenanceSchemaV1.versionIdentifier
        )
        let maintenanceContext = try await makeMaintenanceServiceContext(
            container: target
        )
        let scheduler = RecordingScheduler()
        let service = try await makeMaintenanceJobService(
            maintenanceContext: maintenanceContext,
            scheduler: scheduler,
            identifiers: FixedIdentifierGenerator()
        )
        let nestedRequest = migrationRequest()
        let startRequest = JobStartOperation.Request(
            target: try testDataRootTarget(),
            operation: JobOperations.maintenance.identifier,
            requestPayload: try encodeMaintenanceRequest(nestedRequest),
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext.testDataRoot(
            container: target,
            requirement: maintenanceJobStartRequirement,
            requestID: 45,
            metadata: OperationRequestMetadata(
                traceID: "migration-job",
                idempotencyKey: "migration-job"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobStartRequest(startRequest),
            wireLimits: .default
        )
        let started = try await service.start(
            startRequest,
            context: context
        ).response

        try await service.runScheduledWork()
        let firstSlice = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(firstSlice.state == .pending)
        #expect(firstSlice.completedWorkUnits == 1)
        #expect(try await target.testBaseCurrentSchemaVersion() == Schema.Version(2, 0, 0))

        try await service.runScheduledWork()
        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .succeeded(
            _,
            let responsePayloadPage,
            _,
            _,
            let continuation
        ) = result else {
            Issue.record("Expected migration job to complete")
            return
        }
        #expect(continuation == nil)
        let response = try decodeMaintenanceResponse(responsePayloadPage)
        let execution = try executionResult(response)
        #expect(execution.kind == .migrations)
        #expect(execution.completedWorkUnits == 2)
        #expect(execution.isComplete)
        #expect(try await target.testBaseCurrentSchemaVersion() == Schema.Version(3, 0, 0))
        #expect(await scheduler.count() >= 2)
    }

    @Test("Index status pages every registered dynamic partition")
    func indexStatusPagesDynamicPartitions() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertEntity(
            id: "first",
            tenant: "tenant-a",
            value: "alpha",
            into: maintenanceContext.container
        )
        try await insertEntity(
            id: "second",
            tenant: "tenant-b",
            value: "beta",
            into: maintenanceContext.container
        )
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )
        let budget = ExecutionBudget(
            maximumRows: 1,
            maximumWorkUnits: 1,
            timeoutMilliseconds: 1_000
        )
        let first = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: nil,
                    index: nil,
                    partitions: FieldObject()
                ),
                budget: budget
            ),
            context: try operationContext(container: maintenanceContext.container)
        ).response
        guard case .indexStatus(let firstPage) = first else {
            Issue.record("Expected an index status page")
            return
        }
        let firstIndexes = try firstPage.materializedIndexes(maximumCount: 1)
        #expect(firstIndexes.count == 1)
        let continuation = try #require(firstPage.continuation)

        let second = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: nil,
                    index: nil,
                    partitions: FieldObject()
                ),
                continuation: continuation,
                budget: budget
            ),
            context: try operationContext(container: maintenanceContext.container)
        ).response
        guard case .indexStatus(let secondPage) = second else {
            Issue.record("Expected a second index status page")
            return
        }
        let secondIndexes = try secondPage.materializedIndexes(maximumCount: 1)
        #expect(secondIndexes.count == 1)
        #expect(secondPage.continuation == nil)
        let allPartitions = [
            firstIndexes[0].partitions,
            secondIndexes[0].partitions,
        ]
        #expect(Set(allPartitions.compactMap { $0["tenantID"] }) == [
            FieldValue.string("tenant-a"),
            FieldValue.string("tenant-b"),
        ])
    }

    @Test("Index status continuation is bound to its filters")
    func indexStatusContinuationRejectsDifferentFilters() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertEntity(
            id: "first",
            tenant: "tenant-a",
            value: "alpha",
            into: maintenanceContext.container
        )
        try await insertEntity(
            id: "second",
            tenant: "tenant-b",
            value: "beta",
            into: maintenanceContext.container
        )
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )
        let budget = ExecutionBudget(
            maximumRows: 1,
            maximumWorkUnits: 1,
            timeoutMilliseconds: 1_000
        )
        let first = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: CatalogPartitionedEntity.persistableType,
                    index: nil,
                    partitions: FieldObject()
                ),
                budget: budget
            ),
            context: try operationContext(container: maintenanceContext.container)
        ).response
        guard case .indexStatus(let page) = first else {
            Issue.record("Expected an index status page")
            return
        }
        let continuation = try #require(page.continuation)

        await #expect(throws: DatabaseMaintenanceRuntimeError.self) {
            try await service.execute(
                MaintenanceExecuteOperation.Request(
                    invocation: .indexStatus(
                        entity: CatalogPartitionedEntity.persistableType,
                        index: "catalog_value",
                        partitions: FieldObject()
                    ),
                    continuation: continuation,
                    budget: budget
                ),
                context: try operationContext(container: maintenanceContext.container)
            )
        }
    }

    @Test("Direct index rebuild resumes with an opaque continuation")
    func directIndexRebuildResumes() async throws {
        let engine = InMemoryEngine()
        let initialMaintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        try await insertEntities(into: initialMaintenanceContext.container)
        let partitions = try tenantPartition("tenant-a")
        let identifiers = FixedIdentifierGenerator()
        let firstService = DatabaseMaintenanceOperationService(
            context: initialMaintenanceContext.serviceContext,
            identifierGenerator: identifiers
        )
        let firstRequest = rebuildRequest(partitions: partitions)

        let firstResponse = try await firstService.execute(
            firstRequest,
            context: try operationContext(
                container: initialMaintenanceContext.container,
                request: firstRequest,
                idempotencyKey: "direct-rebuild-1"
            )
        ).response
        let firstResult = try executionResult(firstResponse)
        #expect(firstResult.kind == .indexRebuild)
        #expect(firstResult.completedWorkUnits == 1)
        #expect(!firstResult.isComplete)
        let continuation = try #require(firstResult.continuation)

        let building = try await indexStatus(
            service: firstService,
            container: initialMaintenanceContext.container,
            partitions: partitions
        )
        #expect(building.state == .building)
        #expect(building.indexedEntityCount == 1)

        let recreatedMaintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        let recreatedService = DatabaseMaintenanceOperationService(
            context: recreatedMaintenanceContext.serviceContext,
            identifierGenerator: identifiers
        )
        let secondRequest = MaintenanceExecuteOperation.Request(
            invocation: firstRequest.invocation,
            continuation: continuation,
            budget: firstRequest.budget
        )
        let secondResponse = try await recreatedService.execute(
            secondRequest,
            context: try operationContext(
                container: recreatedMaintenanceContext.container,
                request: secondRequest,
                idempotencyKey: "direct-rebuild-2"
            )
        ).response
        let secondResult = try executionResult(secondResponse)
        #expect(secondResult.kind == .indexRebuild)
        #expect(secondResult.completedWorkUnits == 2)
        #expect(secondResult.isComplete)
        #expect(secondResult.continuation == nil)

        let completed = try await indexStatus(
            service: recreatedService,
            container: recreatedMaintenanceContext.container,
            partitions: partitions
        )
        #expect(completed.state == .ready)
        #expect(completed.indexedEntityCount == 2)
    }

    @Test("Persistent maintenance job resumes the real index runtime")
    func persistentIndexRebuildJobResumes() async throws {
        let engine = InMemoryEngine()
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        try await insertEntities(into: maintenanceContext.container)
        let partitions = try tenantPartition("tenant-a")
        let nestedRequest = rebuildRequest(partitions: partitions)
        let nestedPayload = try encodeMaintenanceRequest(nestedRequest)
        let scheduler = RecordingScheduler()
        let identifiers = FixedIdentifierGenerator()
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation()
                ),
            ]
        )
        let factory = try DatabasePersistentJobServiceFactory(
            registry: registry,
            identifierGenerator: identifiers,
            storageLimits: maintenanceJobTestStorageLimits
        )
        let firstService = try await factory.makeJobService(
            context: maintenanceContext.serviceContext.withHostServices(
                try testJobHostServices(scheduler: scheduler)
            )
        )
        let startRequest = JobStartOperation.Request(
            target: try testDataRootTarget(),
            operation: JobOperations.maintenance.identifier,
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1
        )
        let startContext = DatabaseOperationContext.testDataRoot(
            container: maintenanceContext.container,
            requirement: maintenanceJobStartRequirement,
            requestID: 41,
            metadata: OperationRequestMetadata(
                traceID: "maintenance-job",
                idempotencyKey: "maintenance-job"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobStartRequest(startRequest),
            wireLimits: .default
        )

        let started = try await firstService.start(
            startRequest,
            context: startContext
        )
        try await firstService.runScheduledWork()
        let pending = try await firstService.status(
            JobStatusOperation.Request(job: started.response.job),
            context: startContext
        )
        #expect(pending.state == .pending)
        #expect(pending.completedWorkUnits == 1)

        let recreatedService = try await factory.makeJobService(
            context: maintenanceContext.serviceContext.withHostServices(
                try testJobHostServices(scheduler: scheduler)
            )
        )
        try await recreatedService.runScheduledWork()
        let result = try await recreatedService.result(
            JobResultOperation.Request(job: started.response.job),
            context: startContext
        )
        guard case .succeeded(
            let job,
            let responsePayloadPage,
            let totalResponseBytes,
            _,
            let continuation
        ) = result else {
            Issue.record("Expected a successful maintenance job")
            return
        }
        #expect(job == started.response.job)
        #expect(totalResponseBytes == UInt64(responsePayloadPage.count))
        #expect(continuation == nil)
        let response = try decodeMaintenanceResponse(responsePayloadPage)
        let execution = try executionResult(response)
        #expect(execution.kind == .indexRebuild)
        #expect(execution.completedWorkUnits == 2)
        #expect(execution.isComplete)
        #expect(await scheduler.count() >= 2)

        let maintenance = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: identifiers
        )
        let completed = try await indexStatus(
            service: maintenance,
            container: maintenanceContext.container,
            partitions: partitions
        )
        #expect(completed.state == .ready)
        #expect(completed.indexedEntityCount == 2)
    }

    @Test("Direct compaction requires the persistent job boundary")
    func directCompactionRequiresPersistentJob() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )
        let request = compactionRequest()

        await #expect(
            throws: DatabaseMaintenanceRuntimeError.compactionRequiresJob
        ) {
            try await service.execute(
                request,
                context: try operationContext(
                    container: maintenanceContext.container,
                    request: request,
                    idempotencyKey: "direct-compaction"
                )
            )
        }
    }

    @Test("Persistent compaction commits each physical slice with its job state")
    func persistentCompactionCommitsWithJobState() async throws {
        let engine = try await ControlledCompactionStorageEngine(
            behavior: .twoSlices
        )
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        let service = try await makeMaintenanceJobService(
            maintenanceContext: maintenanceContext,
            scheduler: RecordingScheduler(),
            identifiers: FixedIdentifierGenerator()
        )
        let nestedRequest = compactionRequest()
        let startRequest = JobStartOperation.Request(
            target: try testDataRootTarget(),
            operation: JobOperations.maintenance.identifier,
            requestPayload: try encodeMaintenanceRequest(nestedRequest),
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext.testDataRoot(
            container: maintenanceContext.container,
            requirement: maintenanceJobStartRequirement,
            requestID: 44,
            metadata: OperationRequestMetadata(
                traceID: "compaction-job",
                idempotencyKey: "compaction-job"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobStartRequest(startRequest),
            wireLimits: .default
        )
        let started = try await service.start(
            startRequest,
            context: context
        ).response

        try await service.runScheduledWork()
        let firstSlice = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        let firstMarker = try await StorageTransactionExecutor(
            engine: maintenanceContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await transaction.getValue(
                for: ControlledCompactionStorageEngine.markerKey,
                snapshot: true
            )
        }
        #expect(firstSlice.state == .pending)
        #expect(firstSlice.completedWorkUnits == 1)
        #expect(firstMarker == [1])

        try await service.runScheduledWork()
        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .succeeded(
            _,
            let responsePayloadPage,
            let totalResponseBytes,
            _,
            let continuation
        ) = result else {
            Issue.record("Expected compaction to complete")
            return
        }
        #expect(totalResponseBytes == UInt64(responsePayloadPage.count))
        #expect(continuation == nil)
        let response = try decodeMaintenanceResponse(responsePayloadPage)
        let execution = try executionResult(response)
        let finalMarker = try await StorageTransactionExecutor(
            engine: maintenanceContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await transaction.getValue(
                for: ControlledCompactionStorageEngine.markerKey,
                snapshot: true
            )
        }
        #expect(execution.kind == .compaction)
        #expect(execution.completedWorkUnits == 2)
        #expect(execution.isComplete)
        #expect(finalMarker == [2])
    }

    @Test("Compaction rolls back physical work when continuation encoding fails")
    func compactionRollsBackWhenContinuationEncodingFails() async throws {
        let limits = try DatabaseWireLimits(
            maximumFrameBytes: 16 * 1_024,
            maximumStringBytes: 1_024,
            maximumByteStringBytes: 128,
            maximumCollectionCount: 1_024,
            maximumNestingDepth: 32,
            maximumObjectCount: 4_096
        )
        let engine = try await ControlledCompactionStorageEngine(
            behavior: .oversizedContinuation(byteCount: 256)
        )
        let maintenanceContext = try await makeMaintenanceServiceContext(
            engine: engine,
            wireLimits: limits
        )
        let service = try await makeMaintenanceJobService(
            maintenanceContext: maintenanceContext,
            scheduler: RecordingScheduler(),
            identifiers: FixedIdentifierGenerator(),
            storageLimits: DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_576,
                maximumSpecificationBytes: 8 * 1_024,
                maximumPlanBytes: 8 * 1_024,
                maximumStateBytes: 16 * 1_024,
                maximumOperationStateBytes: 4 * 1_024,
                maximumUnsuccessfulOutcomeBytes: 4 * 1_024,
                maximumResultBytes: 4 * 1_024,
                resultChunkBytes: 4 * 1_024
            )
        )
        let nestedRequest = compactionRequest()
        let nestedPayload = try encodeMaintenanceRequest(
            nestedRequest,
            limits: limits
        )
        let startRequest = JobStartOperation.Request(
            target: try testDataRootTarget(),
            operation: JobOperations.maintenance.identifier,
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 1,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 1
            )
        )
        let context = DatabaseOperationContext.testDataRoot(
            container: maintenanceContext.container,
            requirement: maintenanceJobStartRequirement,
            requestID: 45,
            metadata: OperationRequestMetadata(
                traceID: "compaction-rollback",
                idempotencyKey: "compaction-rollback"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobStartRequest(
                startRequest,
                limits: limits
            ),
            wireLimits: limits
        )
        let started = try await service.start(
            startRequest,
            context: context
        ).response

        try await service.runScheduledWork()
        try await service.runScheduledWork()

        let completedStatus = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(completedStatus.state == .failed)
        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .failed = result else {
            Issue.record("Expected oversized continuation to fail the job")
            return
        }
        let marker = try await StorageTransactionExecutor(
            engine: maintenanceContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await transaction.getValue(
                for: ControlledCompactionStorageEngine.markerKey,
                snapshot: true
            )
        }
        #expect(marker == nil)
    }

    @Test("Cancelling a partial rebuild enters a terminal failed index state")
    func cancellationMarksPartialRebuildFailed() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertEntities(into: maintenanceContext.container)
        let partitions = try tenantPartition("tenant-a")
        let nestedPayload = try encodeMaintenanceRequest(
            rebuildRequest(partitions: partitions)
        )
        let identifiers = FixedIdentifierGenerator()
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation()
                ),
            ]
        )
        let factory = try DatabasePersistentJobServiceFactory(
            registry: registry,
            identifierGenerator: identifiers,
            storageLimits: maintenanceJobTestStorageLimits
        )
        let scheduler = RecordingScheduler()
        let authorizationValidator = try TestDatabaseJobAuthorizationValidator()
        let service = try await factory.makeJobService(
            context: maintenanceContext.serviceContext.withHostServices(
                DatabaseOperationHostServices(
                    jobScheduler: AnyDatabaseJobScheduler(scheduler),
                    jobAuthorizationValidator:
                        AnyDatabaseJobAuthorizationValidator(
                            authorizationValidator
                        )
                )
            )
        )
        let startRequest = JobStartOperation.Request(
            target: try testDataRootTarget(),
            operation: JobOperations.maintenance.identifier,
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext.testDataRoot(
            container: maintenanceContext.container,
            requirement: maintenanceJobStartRequirement,
            requestID: 42,
            metadata: OperationRequestMetadata(
                traceID: "maintenance-cancel",
                idempotencyKey: "maintenance-cancel"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobStartRequest(startRequest),
            wireLimits: .default
        )
        let started = try await service.start(startRequest, context: context)
        try await service.runScheduledWork()

        let cancelRequest = JobCancelOperation.Request(
            job: started.response.job
        )
        let cancelContext = DatabaseOperationContext.testDataRoot(
            container: maintenanceContext.container,
            requirement: .canonical(for: .jobCancel),
            requestID: 43,
            metadata: OperationRequestMetadata(
                traceID: "maintenance-cancel",
                idempotencyKey: "maintenance-cancel-request"
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeJobCancelRequest(cancelRequest),
            wireLimits: .default
        )
        let cancelled = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(cancelled.accepted)
        #expect(cancelled.state == .committingUnsuccessfulOutcome)
        await authorizationValidator.revoke(
            try TestDatabaseJobAuthorizationValidator.reference()
        )
        try await service.runScheduledWork()

        let maintenance = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: identifiers
        )
        let status = try await indexStatus(
            service: maintenance,
            container: maintenanceContext.container,
            partitions: partitions
        )
        #expect(status.state == .failed)
        #expect(status.indexedEntityCount == 1)
        #expect(status.detail == "cancelled")
    }

    private func makeMaintenanceServiceContext(
        engine: any StorageEngine,
        wireLimits: DatabaseWireLimits = .default
    ) async throws -> MaintenanceServiceContext {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try CatalogPartitionedEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(CatalogPartitionedEntity.self)]
            ),
            security: .testingDisabled
        )
        return try await makeMaintenanceServiceContext(
            container: container,
            wireLimits: wireLimits
        )
    }

    private func makeMaintenanceServiceContext(
        container: DBContainer,
        wireLimits: DatabaseWireLimits = .default
    ) async throws -> MaintenanceServiceContext {
        let stateStore = DatabaseMutationStateStore(
            container: container
        )
        return MaintenanceServiceContext(
            container: container,
            serviceContext: DatabaseOperationServiceContext(
                container: container,
                stateStore: stateStore,
                coordinator: DatabaseTransactionalOperationCoordinator(
                    stateStore: stateStore
                ),
                runtimeLimits: .default,
                wireLimits: wireLimits,
                clock: AnyDatabaseWallClock(try FixedClock())
            )
        )
    }

    private func makeMaintenanceJobService<
        Scheduler: DatabaseJobScheduler,
        Identifiers: DatabaseUUIDGenerator
    >(
        maintenanceContext: MaintenanceServiceContext,
        scheduler: Scheduler,
        identifiers: Identifiers,
        storageLimits: DatabasePersistentJobStorageLimits =
            maintenanceJobTestStorageLimits
    ) async throws -> AnyDatabaseJobService {
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation(
                        runtimeLimits:
                            maintenanceContext.serviceContext.runtimeLimits
                    )
                ),
            ]
        )
        let factory = try DatabasePersistentJobServiceFactory(
            registry: registry,
            identifierGenerator: identifiers,
            storageLimits: storageLimits
        )
        return try await factory.makeJobService(
            context: maintenanceContext.serviceContext.withHostServices(
                try testJobHostServices(scheduler: scheduler)
            )
        )
    }

    private func insertEntities(into container: DBContainer) async throws {
        let context = container.testBaseContext()
        var first = CatalogPartitionedEntity()
        first.id = "first"
        first.tenantID = "tenant-a"
        first.value = "alpha"
        var second = CatalogPartitionedEntity()
        second.id = "second"
        second.tenantID = "tenant-a"
        second.value = "beta"
        try context.insert(first)
        try context.insert(second)
        try await context.save()
    }

    private func insertEntity(
        id: String,
        tenant: String,
        value: String,
        into container: DBContainer
    ) async throws {
        let context = container.testBaseContext()
        var entity = CatalogPartitionedEntity()
        entity.id = id
        entity.tenantID = tenant
        entity.value = value
        try context.insert(entity)
        try await context.save()
    }

    private func rebuildRequest(
        partitions: FieldObject
    ) -> MaintenanceExecuteOperation.Request {
        MaintenanceExecuteOperation.Request(
            invocation: .rebuildIndex(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                batchSize: 1
            ),
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1_000
            )
        )
    }

    private func compactionRequest() -> MaintenanceExecuteOperation.Request {
        MaintenanceExecuteOperation.Request(
            invocation: .compact,
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1_000
            )
        )
    }

    private func migrationRequest() -> MaintenanceExecuteOperation.Request {
        MaintenanceExecuteOperation.Request(
            invocation: .runMigrations(
                targetVersion: Schema.Version(3, 0, 0)
            ),
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1_000
            )
        )
    }

    private func tenantPartition(
        _ tenant: String
    ) throws -> FieldObject {
        try FieldObject([
            (key: "tenantID", value: .string(tenant)),
        ])
    }

    private func operationContext(
        container: DBContainer
    ) throws -> DatabaseOperationContext {
        return DatabaseOperationContext.testDataRoot(
            container: container,
            requirement: .canonical(for: .maintenanceExecute),
            requestID: 40,
            metadata: OperationRequestMetadata(traceID: "maintenance-direct"),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: [],
            wireLimits: .default
        )
    }

    private func operationContext(
        container: DBContainer,
        request: MaintenanceExecuteOperation.Request,
        idempotencyKey: String
    ) throws -> DatabaseOperationContext {
        return DatabaseOperationContext.testDataRoot(
            container: container,
            requirement: .canonical(for: .maintenanceExecute),
            requestID: 40,
            metadata: OperationRequestMetadata(
                traceID: "maintenance-direct",
                idempotencyKey: idempotencyKey
            ),
            authorization: TestBaseEnvironment.authorization,
            jobAuthorizationReference:
                try TestDatabaseJobAuthorizationValidator.reference(),
            requestPayload: try encodeMaintenanceRequest(request),
            wireLimits: .default
        )
    }

    private func encodeMaintenanceRequest(
        _ request: MaintenanceExecuteOperation.Request,
        limits: DatabaseWireLimits = .default
    ) throws -> ByteString {
        try DatabaseWireEncoder(limits: limits).encodeRequestPayload(
            DatabaseOperationCatalog.maintenanceExecute,
            request: request
        )
    }

    private func decodeMaintenanceResponse(
        _ payload: ByteString,
        limits: DatabaseWireLimits = .default
    ) throws -> MaintenanceExecuteOperation.Response {
        try DatabaseWireDecoder(limits: limits).decodeResponsePayload(
            DatabaseOperationCatalog.maintenanceExecute,
            from: payload
        )
    }

    private func encodeJobStartRequest(
        _ request: JobStartOperation.Request,
        limits: DatabaseWireLimits = .default
    ) throws -> ByteString {
        try DatabaseWireEncoder(limits: limits).encodeRequestPayload(
            DatabaseOperationCatalog.jobStart,
            request: request
        )
    }

    private func encodeJobCancelRequest(
        _ request: JobCancelOperation.Request,
        limits: DatabaseWireLimits = .default
    ) throws -> ByteString {
        try DatabaseWireEncoder(limits: limits).encodeRequestPayload(
            DatabaseOperationCatalog.jobCancel,
            request: request
        )
    }

    private func executionResult(
        _ response: MaintenanceExecuteOperation.Response
    ) throws -> MaintenanceExecuteOperation.ExecutionResult {
        guard case .execution(let result) = response else {
            throw MaintenanceScenarioError.unexpectedResponse
        }
        return result
    }

    private func indexStatus(
        service: DatabaseMaintenanceOperationService,
        container: DBContainer,
        partitions: FieldObject
    ) async throws -> MaintenanceExecuteOperation.IndexStatus {
        let response = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: CatalogPartitionedEntity.persistableType,
                    index: "catalog_value",
                    partitions: partitions
                ),
                budget: ExecutionBudget(
                    maximumRows: 10,
                    maximumWorkUnits: 10,
                    timeoutMilliseconds: 1_000
                )
            ),
            context: try operationContext(container: container)
        ).response
        guard case .indexStatus(let page) = response else {
            throw MaintenanceScenarioError.unexpectedResponse
        }
        let indexes = try page.materializedIndexes(maximumCount: 1)
        guard indexes.count == 1, let status = indexes.first else {
            throw MaintenanceScenarioError.unexpectedResponse
        }
        return status
    }

    private struct MaintenanceServiceContext: Sendable {
        let container: DBContainer
        let serviceContext: DatabaseOperationServiceContext
    }

    private final class FixedClock: WallClock, Sendable {
        private let value: Timestamp

        init() throws {
            value = Timestamp(secondsSinceUnixEpoch: 1_000)
        }

        var now: Timestamp { value }
    }

    private actor RecordingScheduler: DatabaseJobScheduler {
        private var timestamps: [Timestamp] = []

        func ensureWakeUp(
            noLaterThan timestamp: Timestamp
        ) async throws {
            timestamps.append(timestamp)
        }

        func count() -> Int {
            timestamps.count
        }
    }

    private final class FixedIdentifierGenerator: DatabaseUUIDGenerator, Sendable {
        private let value = Mutex<UInt64>(1)

        func generate() -> DatabaseTypes.UUID {
            value.withLock { current in
                defer { current += 1 }
                return DatabaseTypes.UUID(high: 0, low: current)
            }
        }
    }

    private enum MaintenanceScenarioError: Error {
        case unexpectedResponse
    }
}

private enum MaintenanceSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try CatalogPartitionedEntity.schemaEntity]
        }
    }
}

private enum MaintenanceSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try CatalogPartitionedEntity.schemaEntity]
        }
    }
}

private enum MaintenanceSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try CatalogPartitionedEntity.schemaEntity]
        }
    }
}

private enum MaintenanceInitialMigrationPlan: SchemaMigrationPlan {
    static let schemas: [any VersionedSchema.Type] = [
        MaintenanceSchemaV1.self,
    ]
    static let stages: [MigrationStage] = []
}

private enum MaintenanceMigrationPlan: SchemaMigrationPlan {
    static let schemas: [any VersionedSchema.Type] = [
        MaintenanceSchemaV1.self,
        MaintenanceSchemaV2.self,
        MaintenanceSchemaV3.self,
    ]
    static let stages: [MigrationStage] = [
        .lightweight(
            fromVersion: MaintenanceSchemaV1.self,
            toVersion: MaintenanceSchemaV2.self
        ),
        .lightweight(
            fromVersion: MaintenanceSchemaV2.self,
            toVersion: MaintenanceSchemaV3.self
        ),
    ]
}
