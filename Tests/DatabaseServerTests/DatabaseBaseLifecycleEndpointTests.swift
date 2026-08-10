import DatabaseKit
import DatabaseRuntime
@testable import DatabaseEngine
@testable import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit
import TestSupport
import Testing

@Suite("Base lifecycle server execution", .serialized)
struct DatabaseBaseLifecycleEndpointTests {
    @Test("Deletion job remains authorized by its exact marker after Grant removal")
    func deletionJobFinishesAfterGrantRemoval() async {
        var stage = "fixture creation"
        do {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        stage = "Base seed"
        let context = fixture.container.testBaseContext()
        var entity = DatabaseEndpointEntity()
        entity.id = "delete-me"
        entity.title = "must be removed"
        entity.priority = 1
        do {
            try context.insert(entity)
            try await context.save()
        } catch {
            Issue.record("Base seed failed: \(error)")
            return
        }

        stage = "Base retirement"
        let current: DatabaseBaseRecord
        let retired: DatabaseBaseRecord
        do {
            current = try await record(fixture)
            retired = try await fixture.container.retireBase(
                fixture.baseID,
                expectedRevision: current.revision
            )
        } catch {
            Issue.record("Base retirement failed: \(error)")
            return
        }
        let idempotencyKey = "base-delete-endpoint"
        stage = "deletion job start"
        let start: Result<BaseExecuteOperation.Response, RemoteOperationError>
        stage = "deletion prepare slice"
        do {
            start = try await invoke(
                DatabaseOperations.baseExecute,
                requestID: 1,
                target: .base(fixture.baseID),
                metadata: OperationRequestMetadata(
                    idempotencyKey: idempotencyKey
                ),
                request: BaseExecuteOperation.Request(
                    invocation: .delete(
                        expectedRevision: retired.revision,
                        idempotencyKey: idempotencyKey
                    )
                ),
                authorization: TestBaseEnvironment.authorization,
                fixture: fixture
            )
        } catch {
            Issue.record("Deletion job start transport failed: \(error)")
            return
        }
        guard case .success(.job(let job)) = start else {
            Issue.record(
                "Expected a persistent Base deletion job, received: \(start)"
            )
            return
        }
        let owner = ByteString(job.jobID.bytes)

        do {
            try await fixture.runtime.runScheduledWork()
        } catch {
            Issue.record("Deletion prepare slice failed: \(error)")
            return
        }
        stage = "prepared deletion observation"
        #expect(try await record(fixture).lifecycle == .deleting)
        #expect(
            try await fixture.container.permitsBaseDeletionFinalization(
                fixture.baseID,
                owner: owner
            ) == false
        )

        stage = "deletion clear slice"
        do {
            try await fixture.runtime.runScheduledWork()
        } catch {
            Issue.record("Deletion clear slice failed: \(error)")
            return
        }
        #expect(
            try await fixture.container.permitsBaseDeletionFinalization(
                fixture.baseID,
                owner: owner
            )
        )

        stage = "intruder status authorization"
        let intruderStatus: Result<
            JobStatusOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.jobStatus,
            requestID: 2,
            target: .base(fixture.baseID),
            request: JobStatusOperation.Request(job: job),
            authorization: .authenticated(Principal(identifier: "intruder")),
            fixture: fixture
        )
        guard case .failure(let denial) = intruderStatus else {
            Issue.record("Expected a different principal to be denied")
            return
        }
        #expect(denial.category == .authorization)

        stage = "deletion finish slice"
        do {
            try await fixture.runtime.runScheduledWork()
        } catch {
            Issue.record("Deletion finish slice failed: \(error)")
            return
        }
        let tombstone = try await record(fixture)
        #expect(tombstone.lifecycle == .tombstone)
        let remainingIntent = try await fixture.container
            .withControlMetadataTransaction(configuration: .readOnly) {
                transaction in
                try await DatabaseBaseDeletionStore(
                    root: fixture.container.storageTopology.controlDomain.root,
                    collection: "intents"
                ).load(
                    fixture.baseID,
                    transaction: transaction.storageAccess
                )
            }
        #expect(remainingIntent == nil)

        stage = "owner job status"
        let status: Result<
            JobStatusOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.jobStatus,
            requestID: 3,
            target: .base(fixture.baseID),
            request: JobStatusOperation.Request(job: job),
            authorization: TestBaseEnvironment.authorization,
            fixture: fixture
        )
        guard case .success(let successfulStatus) = status else {
            Issue.record("Expected the owning principal to read job status")
            return
        }
        #expect(successfulStatus.state == .succeeded)

        stage = "owner job result"
        let result: Result<
            JobResultOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.jobResult,
            requestID: 4,
            target: .base(fixture.baseID),
            request: JobResultOperation.Request(job: job),
            authorization: TestBaseEnvironment.authorization,
            fixture: fixture
        )
        guard case .success(.succeeded(
            _,
            let responsePayload,
            _,
            _,
            nil
        )) = result else {
            Issue.record("Expected the complete deletion job result")
            return
        }
        let operationResponse = try DatabaseWireDecoder()
            .decodeResponsePayload(
                DatabaseOperations.baseExecute,
                from: responsePayload
            )
        guard case .base(let description) = operationResponse else {
            Issue.record("Expected the deleted Base description")
            return
        }
        #expect(description.lifecycle == .tombstone)

        stage = "tombstone query rejection"
        let unavailable: Result<
            QueryExecuteOperation.Response,
            RemoteOperationError
        > = try await invoke(
            DatabaseOperations.queryExecute,
            requestID: 5,
            target: .base(fixture.baseID),
            request: QueryExecuteOperation.Request(
                input: .text(
                    language: .sql,
                    statement: "SELECT * FROM DatabaseEndpointEntity"
                )
            ),
            authorization: TestBaseEnvironment.authorization,
            fixture: fixture
        )
        guard case .failure(let unavailableError) = unavailable else {
            Issue.record("Expected tombstoned Base data to be unavailable")
            return
        }
        #expect(unavailableError.code == "BASE_UNAVAILABLE")
        } catch {
            Issue.record("Unexpected error during \(stage): \(error)")
        }
    }

    private struct Fixture {
        let container: DBContainer
        let runtime: DatabaseServerRuntime
        let baseID: Base.ID
    }

    private actor Scheduler: DatabaseJobScheduler {
        func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
            _ = timestamp
        }
    }

    private func makeFixture() async throws -> Fixture {
        let schema = try Schema(
            entities: [try DatabaseEndpointEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        DatabaseEndpointEntity.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
        let identifierGenerator = RandomDatabaseUUIDGenerator()
        let jobFactory = try DatabasePersistentJobServiceFactory(
            registry: DatabaseResumableOperationRegistry(operations: []),
            identifierGenerator: identifierGenerator,
            storageLimits: DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_576
            )
        )
        let runtime = try await DatabaseServerRuntime(
            container: container,
            configuration: try DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "lifecycle-test"),
                serviceFactory: AnyDatabaseServerServiceFactory(
                    PersistentLifecycleTestServiceFactory(
                        jobFactory: jobFactory
                    )
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock()
            ),
            hostServices: DatabaseServerHostServices(
                jobScheduler: Scheduler(),
                identifierGenerator: identifierGenerator
            )
        )
        return Fixture(
            container: container,
            runtime: runtime,
            baseID: try TestBaseEnvironment.id()
        )
    }

    private func record(_ fixture: Fixture) async throws -> DatabaseBaseRecord {
        try await fixture.container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try #require(
                try await fixture.container.baseCatalog.load(
                    fixture.baseID,
                    transaction: transaction.storageAccess
                )
            )
        }
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        requestID: UInt64,
        target: DatabaseOperationTarget,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        request: Request,
        authorization: AuthorizationContext,
        fixture: Fixture
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
            from: try await fixture.runtime.execute(
                bytes,
                context: DatabaseRequestExecutionContext(
                    authorization: authorization
                )
            ),
            matching: requestID
        )
    }
}
