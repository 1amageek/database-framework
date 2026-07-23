import Core
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseValue
import DatabaseWire
import StorageKit
import Synchronization
import Testing

@Suite("Database maintenance operation service", .serialized)
struct DatabaseMaintenanceOperationServiceTests {
    @Test("Migration status and bounded execution use the compiled plan")
    func migrationsReportAndExecuteExactStages() async throws {
        let engine = InMemoryEngine()
        let initial = try await DBContainer(
            for: MaintenanceSchemaV1.self,
            migrationPlan: MaintenanceInitialMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        try await initial.migrateIfNeeded()

        let target = try await DBContainer(
            for: MaintenanceSchemaV3.self,
            migrationPlan: MaintenanceMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
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
            context: operationContext(container: target)
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
        #expect(try await target.getCurrentSchemaVersion() == Schema.Version(2, 0, 0))

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
        #expect(try await target.getCurrentSchemaVersion() == Schema.Version(3, 0, 0))
    }

    @Test("Persistent migration job resumes the compiled plan")
    func persistentMigrationJobResumes() async throws {
        let engine = InMemoryEngine()
        let initial = try await DBContainer(
            for: MaintenanceSchemaV1.self,
            migrationPlan: MaintenanceInitialMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        try await initial.migrateIfNeeded()

        let target = try await DBContainer(
            for: MaintenanceSchemaV3.self,
            migrationPlan: MaintenanceMigrationPlan.self,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
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
            operation: try DatabaseMaintenanceJobDescriptor
                .jobOperationIdentifier(),
            requestPayload: try DatabaseEnvelopeCodec.encode(nestedRequest),
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext(
            container: target,
            requestID: 45,
            metadata: DatabaseRequestMetadata(
                traceID: "migration-job",
                idempotencyKey: "migration-job"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(startRequest)
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
        #expect(try await target.getCurrentSchemaVersion() == Schema.Version(2, 0, 0))

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
        let response = try DatabaseEnvelopeCodec.decode(
            MaintenanceExecuteOperation.Response.self,
            from: responsePayloadPage
        )
        let execution = try executionResult(response)
        #expect(execution.kind == .migrations)
        #expect(execution.completedWorkUnits == 2)
        #expect(execution.isComplete)
        #expect(try await target.getCurrentSchemaVersion() == Schema.Version(3, 0, 0))
        #expect(await scheduler.count() >= 2)
    }

    @Test("Index status pages every registered dynamic partition")
    func indexStatusPagesDynamicPartitions() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertRecord(
            id: "first",
            tenant: "tenant-a",
            value: "alpha",
            into: maintenanceContext.container
        )
        try await insertRecord(
            id: "second",
            tenant: "tenant-b",
            value: "beta",
            into: maintenanceContext.container
        )
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )
        let budget = DatabaseExecutionBudget(
            maximumRows: 1,
            maximumWorkUnits: 1,
            timeoutMilliseconds: 1_000
        )
        let first = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: nil,
                    index: nil,
                    partitions: []
                ),
                budget: budget
            ),
            context: operationContext(container: maintenanceContext.container)
        ).response
        guard case .indexStatus(let firstPage) = first else {
            Issue.record("Expected an index status page")
            return
        }
        #expect(firstPage.indexes.count == 1)
        let continuation = try #require(firstPage.continuation)

        let second = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: nil,
                    index: nil,
                    partitions: []
                ),
                continuation: continuation,
                budget: budget
            ),
            context: operationContext(container: maintenanceContext.container)
        ).response
        guard case .indexStatus(let secondPage) = second else {
            Issue.record("Expected a second index status page")
            return
        }
        #expect(secondPage.indexes.count == 1)
        #expect(secondPage.continuation == nil)
        let allPartitions = firstPage.indexes[0].partitions
            + secondPage.indexes[0].partitions
        #expect(Set(allPartitions.map(\.value)) == [
            DatabaseValue.string("tenant-a"),
            DatabaseValue.string("tenant-b"),
        ])
    }

    @Test("Index status continuation is bound to its filters")
    func indexStatusContinuationRejectsDifferentFilters() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertRecord(
            id: "first",
            tenant: "tenant-a",
            value: "alpha",
            into: maintenanceContext.container
        )
        try await insertRecord(
            id: "second",
            tenant: "tenant-b",
            value: "beta",
            into: maintenanceContext.container
        )
        let service = DatabaseMaintenanceOperationService(
            context: maintenanceContext.serviceContext,
            identifierGenerator: FixedIdentifierGenerator()
        )
        let budget = DatabaseExecutionBudget(
            maximumRows: 1,
            maximumWorkUnits: 1,
            timeoutMilliseconds: 1_000
        )
        let first = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: CatalogPartitionedRecord.persistableType,
                    index: nil,
                    partitions: []
                ),
                budget: budget
            ),
            context: operationContext(container: maintenanceContext.container)
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
                        entity: CatalogPartitionedRecord.persistableType,
                        index: "catalog_value",
                        partitions: []
                    ),
                    continuation: continuation,
                    budget: budget
                ),
                context: operationContext(container: maintenanceContext.container)
            )
        }
    }

    @Test("Direct index rebuild resumes with an opaque continuation")
    func directIndexRebuildResumes() async throws {
        let engine = InMemoryEngine()
        let initialMaintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        try await insertRecords(into: initialMaintenanceContext.container)
        let partitions = [try tenantPartition("tenant-a")]
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
        #expect(building.indexedRecordCount == 1)

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
        #expect(completed.indexedRecordCount == 2)
    }

    @Test("Persistent maintenance job resumes the real index runtime")
    func persistentIndexRebuildJobResumes() async throws {
        let engine = InMemoryEngine()
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: engine)
        try await insertRecords(into: maintenanceContext.container)
        let partitions = [try tenantPartition("tenant-a")]
        let nestedRequest = rebuildRequest(partitions: partitions)
        let nestedPayload = try DatabaseEnvelopeCodec.encode(nestedRequest)
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
            scheduler: scheduler,
            clock: FixedClock(),
            identifierGenerator: identifiers
        )
        let firstService = try await factory.makeJobService(
            context: maintenanceContext.serviceContext
        )
        let startRequest = JobStartOperation.Request(
            operation: try DatabaseMaintenanceJobDescriptor
                .jobOperationIdentifier(),
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1
        )
        let startContext = DatabaseOperationContext(
            container: maintenanceContext.container,
            requestID: 41,
            metadata: DatabaseRequestMetadata(
                traceID: "maintenance-job",
                idempotencyKey: "maintenance-job"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(startRequest)
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
            context: maintenanceContext.serviceContext
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
        let response = try DatabaseEnvelopeCodec.decode(
            MaintenanceExecuteOperation.Response.self,
            from: responsePayloadPage
        )
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
        #expect(completed.indexedRecordCount == 2)
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
            operation: try DatabaseMaintenanceJobDescriptor
                .jobOperationIdentifier(),
            requestPayload: try DatabaseEnvelopeCodec.encode(nestedRequest),
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext(
            container: maintenanceContext.container,
            requestID: 44,
            metadata: DatabaseRequestMetadata(
                traceID: "compaction-job",
                idempotencyKey: "compaction-job"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(startRequest)
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
        let firstMarker = try await maintenanceContext.container.engine.withTransaction(
            configuration: .readOnly
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
        let response = try DatabaseEnvelopeCodec.decode(
            MaintenanceExecuteOperation.Response.self,
            from: responsePayloadPage
        )
        let execution = try executionResult(response)
        let finalMarker = try await maintenanceContext.container.engine.withTransaction(
            configuration: .readOnly
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
            maximumFrameBytes: 4_096,
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
            identifiers: FixedIdentifierGenerator()
        )
        let nestedRequest = compactionRequest()
        let nestedPayload = try DatabaseEnvelopeCodec.encode(
            nestedRequest,
            limits: limits
        )
        let startRequest = JobStartOperation.Request(
            operation: try DatabaseMaintenanceJobDescriptor
                .jobOperationIdentifier(),
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 1,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 1
            )
        )
        let context = DatabaseOperationContext(
            container: maintenanceContext.container,
            requestID: 45,
            metadata: DatabaseRequestMetadata(
                traceID: "compaction-rollback",
                idempotencyKey: "compaction-rollback"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(
                startRequest,
                limits: limits
            )
        )
        let started = try await service.start(
            startRequest,
            context: context
        ).response

        try await service.runScheduledWork()

        let terminalStatus = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(terminalStatus.state == .failed)
        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .failed = result else {
            Issue.record("Expected oversized continuation to fail the job")
            return
        }
        let marker = try await maintenanceContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(
                for: ControlledCompactionStorageEngine.markerKey,
                snapshot: true
            )
        }
        #expect(marker == nil)
    }

    @Test("Cancelling a partial rebuild records a terminal failed index state")
    func cancellationMarksPartialRebuildFailed() async throws {
        let maintenanceContext = try await makeMaintenanceServiceContext(engine: InMemoryEngine())
        try await insertRecords(into: maintenanceContext.container)
        let partitions = [try tenantPartition("tenant-a")]
        let nestedPayload = try DatabaseEnvelopeCodec.encode(
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
            scheduler: RecordingScheduler(),
            clock: FixedClock(),
            identifierGenerator: identifiers
        )
        let service = try await factory.makeJobService(
            context: maintenanceContext.serviceContext
        )
        let startRequest = JobStartOperation.Request(
            operation: try DatabaseMaintenanceJobDescriptor
                .jobOperationIdentifier(),
            requestPayload: nestedPayload,
            maximumSliceWorkUnits: 1
        )
        let context = DatabaseOperationContext(
            container: maintenanceContext.container,
            requestID: 42,
            metadata: DatabaseRequestMetadata(
                traceID: "maintenance-cancel",
                idempotencyKey: "maintenance-cancel"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(startRequest)
        )
        let started = try await service.start(startRequest, context: context)
        try await service.runScheduledWork()

        let cancelRequest = JobCancelOperation.Request(
            job: started.response.job
        )
        let cancelContext = DatabaseOperationContext(
            container: maintenanceContext.container,
            requestID: 43,
            metadata: DatabaseRequestMetadata(
                traceID: "maintenance-cancel",
                idempotencyKey: "maintenance-cancel-request"
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(cancelRequest)
        )
        let cancelled = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(cancelled.accepted)
        #expect(cancelled.state == .cancelled)

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
        #expect(status.indexedRecordCount == 1)
        #expect(status.detail == "cancelled")
    }

    private func makeMaintenanceServiceContext(
        engine: any StorageEngine,
        wireLimits: DatabaseWireLimits = .default
    ) async throws -> MaintenanceServiceContext {
        let container = try await DBContainer(
            for: Schema(
                [CatalogPartitionedRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
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
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        return MaintenanceServiceContext(
            container: container,
            serviceContext: DatabaseServerServiceContext(
                container: container,
                stateStore: stateStore,
                coordinator: DatabaseTransactionalOperationCoordinator(
                    stateStore: stateStore
                ),
                runtimeLimits: .default,
                wireLimits: wireLimits
            )
        )
    }

    private func makeMaintenanceJobService<
        Scheduler: DatabaseJobScheduler,
        Identifiers: DatabaseUUIDGenerator
    >(
        maintenanceContext: MaintenanceServiceContext,
        scheduler: Scheduler,
        identifiers: Identifiers
    ) async throws -> AnyDatabaseJobService {
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation(
                        wireLimits: maintenanceContext.serviceContext.wireLimits
                    )
                ),
            ]
        )
        let factory = try DatabasePersistentJobServiceFactory(
            registry: registry,
            scheduler: scheduler,
            clock: FixedClock(),
            identifierGenerator: identifiers
        )
        return try await factory.makeJobService(
            context: maintenanceContext.serviceContext
        )
    }

    private func insertRecords(into container: DBContainer) async throws {
        let context = container.newContext()
        var first = CatalogPartitionedRecord()
        first.id = "first"
        first.tenantID = "tenant-a"
        first.value = "alpha"
        var second = CatalogPartitionedRecord()
        second.id = "second"
        second.tenantID = "tenant-a"
        second.value = "beta"
        context.insert(first)
        context.insert(second)
        try await context.save()
    }

    private func insertRecord(
        id: String,
        tenant: String,
        value: String,
        into container: DBContainer
    ) async throws {
        let context = container.newContext()
        var record = CatalogPartitionedRecord()
        record.id = id
        record.tenantID = tenant
        record.value = value
        context.insert(record)
        try await context.save()
    }

    private func rebuildRequest(
        partitions: [DatabaseObjectField]
    ) -> MaintenanceExecuteOperation.Request {
        MaintenanceExecuteOperation.Request(
            invocation: .rebuildIndex(
                entity: CatalogPartitionedRecord.persistableType,
                index: "catalog_value",
                partitions: partitions,
                batchSize: 1
            ),
            budget: DatabaseExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1_000
            )
        )
    }

    private func compactionRequest() -> MaintenanceExecuteOperation.Request {
        MaintenanceExecuteOperation.Request(
            invocation: .compact,
            budget: DatabaseExecutionBudget(
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
            budget: DatabaseExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 1_000
            )
        )
    }

    private func tenantPartition(
        _ tenant: String
    ) throws -> DatabaseObjectField {
        let schema = try #require(CatalogPartitionedRecord.fieldSchemas.first {
            $0.name == "tenantID"
        })
        let number = try #require(UInt32(exactly: schema.fieldNumber))
        return DatabaseObjectField(
            number: number,
            name: "tenantID",
            value: .string(tenant)
        )
    }

    private func operationContext(
        container: DBContainer
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 40,
            metadata: DatabaseRequestMetadata(traceID: "maintenance-direct"),
            requestPayload: []
        )
    }

    private func operationContext(
        container: DBContainer,
        request: MaintenanceExecuteOperation.Request,
        idempotencyKey: String
    ) throws -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 40,
            metadata: DatabaseRequestMetadata(
                traceID: "maintenance-direct",
                idempotencyKey: idempotencyKey
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(request)
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
        partitions: [DatabaseObjectField]
    ) async throws -> MaintenanceExecuteOperation.IndexStatus {
        let response = try await service.execute(
            MaintenanceExecuteOperation.Request(
                invocation: .indexStatus(
                    entity: CatalogPartitionedRecord.persistableType,
                    index: "catalog_value",
                    partitions: partitions
                ),
                budget: DatabaseExecutionBudget(
                    maximumRows: 10,
                    maximumWorkUnits: 10,
                    timeoutMilliseconds: 1_000
                )
            ),
            context: operationContext(container: container)
        ).response
        guard case .indexStatus(let page) = response,
              page.indexes.count == 1,
              let status = page.indexes.first else {
            throw MaintenanceScenarioError.unexpectedResponse
        }
        return status
    }

    private struct MaintenanceServiceContext: Sendable {
        let container: DBContainer
        let serviceContext: DatabaseServerServiceContext
    }

    private final class FixedClock: DatabaseWallClock, Sendable {
        func now() -> DatabaseTimestamp {
            DatabaseTimestamp(secondsSinceUnixEpoch: 1_000)
        }
    }

    private actor RecordingScheduler: DatabaseJobScheduler {
        private var timestamps: [DatabaseTimestamp] = []

        func schedule(at timestamp: DatabaseTimestamp) async throws {
            timestamps.append(timestamp)
        }

        func count() -> Int {
            timestamps.count
        }
    }

    private final class FixedIdentifierGenerator: DatabaseUUIDGenerator, Sendable {
        private let value = Mutex<UInt64>(1)

        func generate() -> DatabaseUUID {
            value.withLock { current in
                defer { current += 1 }
                return DatabaseUUID(high: 0, low: current)
            }
        }
    }

    private enum MaintenanceScenarioError: Error {
        case unexpectedResponse
    }
}

private enum MaintenanceSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static let models: [any Persistable.Type] = [
        CatalogPartitionedRecord.self,
    ]
}

private enum MaintenanceSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static let models: [any Persistable.Type] = [
        CatalogPartitionedRecord.self,
    ]
}

private enum MaintenanceSchemaV3: VersionedSchema {
    static let versionIdentifier = Schema.Version(3, 0, 0)
    static let models: [any Persistable.Type] = [
        CatalogPartitionedRecord.self,
    ]
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
