#if SQLITE
@testable import DatabaseEngine
import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Persistable(type: "RestartSchemaBuildAccount")
private struct RestartSchemaBuildAccountV1 {
    #Directory<RestartSchemaBuildAccountV1>(
        "schema-index-job-restart",
        "accounts"
    )

    var id: String = ""
    var email: String = ""
}

@Persistable(type: "RestartSchemaBuildAccount")
private struct RestartSchemaBuildAccountV2 {
    #Directory<RestartSchemaBuildAccountV2>(
        "schema-index-job-restart",
        "accounts"
    )
    #Index(
        .scalar,
        fields: [\RestartSchemaBuildAccountV2.email],
        name: "restart_schema_build_account_email"
    )

    var id: String = ""
    var email: String = ""
}

@Suite("Schema index job SQLite restart", .serialized)
struct SchemaIndexJobRestartSQLiteTests {
    @Test("published schema and pending index job resume after process restart")
    func pendingBuildResumesAfterRestart() async throws {
        let database = try SQLiteTestDatabase(
            prefix: "schema-index-job-restart"
        )
        defer { database.remove() }
        let initialSchema = try Schema(
            entities: [try RestartSchemaBuildAccountV1.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let targetSchema = try Schema(
            entities: [try RestartSchemaBuildAccountV2.schemaEntity],
            version: Schema.Version(2, 0, 0)
        )
        let first = try await DBContainer.open(
            for: initialSchema,
            configuration: DBConfiguration.testing(
                storageEngine: try SQLiteStorageEngine(
                    configuration: .file(database.path)
                )
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        RestartSchemaBuildAccountV1.self
                    ),
                ]
            ),
            security: .disabled
        )
        let job: JobIdentity
        do {
            let firstRuntime = try await makeRuntime(container: first)
            let context = first.newContext()
            try context.insert(
                RestartSchemaBuildAccountV1(
                    id: "persisted-before-restart",
                    email: "restart@example.com"
                )
            )
            try await context.save()
            let response = try await invoke(
                DatabaseOperations.schemaExecute,
                request: SchemaExecuteOperation.Request(
                    invocation: .apply(
                        manifest: SchemaManifest(schema: targetSchema),
                        expectedFingerprint: first.schemaFingerprint,
                        idempotencyKey: "schema-index-job-restart"
                    )
                ),
                requestID: 1,
                runtime: firstRuntime
            )
            guard case .applied(let publication) = response,
                  let publishedJob = publication.job else {
                Issue.record("Expected a persistent schema index build job")
                await first.shutdown()
                return
            }
            job = publishedJob
            #expect(
                try await indexStatus(container: first).indexState
                    == .writeOnly
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
        #expect(try await indexStatus(container: reopened).indexState == .writeOnly)

        let restoredRuntime = try await makeRuntime(container: reopened)
        try await restoredRuntime.runScheduledWork()
        let status = try await invoke(
            DatabaseOperations.jobStatus,
            request: JobStatusOperation.Request(job: job),
            requestID: 2,
            runtime: restoredRuntime
        )
        #expect(status.state == .succeeded)
        let index = try await indexStatus(container: reopened)
        #expect(index.indexState == .readable)
        #expect(index.rebuildState?.indexedEntityCount == 1)
    }

    private func makeRuntime(
        container: DBContainer
    ) async throws -> DatabaseServerRuntime {
        let runtimeLimits = DatabaseRuntimeLimits.default
        let identifierGenerator = RandomDatabaseUUIDGenerator()
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                try AnyDatabaseResumableOperation(
                    DatabaseSchemaApplyResumableOperation(
                        runtimeLimits: runtimeLimits
                    )
                ),
                try AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation(
                        runtimeLimits: runtimeLimits
                    )
                ),
            ]
        )
        let serviceFactory = CanonicalDatabaseServerServiceFactory(
            maintenanceServiceFactory:
                DatabaseMaintenanceOperationServiceFactory(
                    identifierGenerator: identifierGenerator
                ),
            jobServiceFactory: try DatabasePersistentJobServiceFactory(
                registry: registry,
                identifierGenerator: identifierGenerator,
                storageLimits: DatabasePersistentJobStorageLimits(
                    maximumStorageValueBytes: 1_048_576
                )
            )
        )
        return try await DatabaseServerRuntime(
            container: container,
            configuration: try DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(
                    version: "schema-restart-test"
                ),
                serviceFactory: AnyDatabaseServerServiceFactory(
                    serviceFactory
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock(),
                schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory(
                    SchemaDrivenDatabaseRuntimeFactory()
                ),
                runtimeLimits: runtimeLimits
            ),
            hostServices: DatabaseServerHostServices(
                jobScheduler: SQLiteSchemaJobScheduler()
            )
        )
    }

    private func indexStatus(
        container: DBContainer
    ) async throws -> DatabaseIndexMaintenanceStatus {
        try await container.newContext().withTransaction { transaction in
            try await DatabaseIndexMaintenanceRuntime(
                container: container
            ).status(
                entity: RestartSchemaBuildAccountV2.persistableType,
                index: "restart_schema_build_account_email",
                partitions: FieldObject(),
                transaction: transaction.storageAccess
            )
        }
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        runtime: DatabaseServerRuntime
    ) async throws -> Response {
        let requestBytes = try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            metadata: OperationRequestMetadata(),
            request: request
        )
        let responseBytes = try await runtime.execute(
            requestBytes,
            context: DatabaseRequestExecutionContext(
                authorization: .anonymous
            )
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            operation,
            from: responseBytes,
            matching: requestID
        )
        switch response {
        case .success(let value):
            return value
        case .failure(let error):
            throw error
        }
    }
}

private actor SQLiteSchemaJobScheduler: DatabaseJobScheduler {
    func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
        _ = timestamp
    }
}
#endif
