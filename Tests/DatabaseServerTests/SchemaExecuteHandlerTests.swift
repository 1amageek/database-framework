@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Persistable
private struct SchemaExecuteAccount {
    #Directory<SchemaExecuteAccount>("schema-execute", "accounts")
    #Index(
        .scalar,
        fields: [\SchemaExecuteAccount.email],
        name: "schema_execute_account_email"
    )

    var id: String = ""
    var email: String = ""
}

@Persistable(type: "SchemaBuildAccount")
private struct SchemaBuildAccountV1 {
    #Directory<SchemaBuildAccountV1>("schema-execute", "build-accounts")

    var id: String = ""
    var email: String = ""
}

@Persistable(type: "SchemaBuildAccount")
private struct SchemaBuildAccountV2 {
    #Directory<SchemaBuildAccountV2>("schema-execute", "build-accounts")
    #Index(
        .scalar,
        fields: [\SchemaBuildAccountV2.email],
        name: "schema_build_account_email"
    )

    var id: String = ""
    var email: String = ""
}

@Persistable(type: "SchemaBuildTenantAccount")
private struct SchemaBuildTenantAccountV1 {
    #Directory<SchemaBuildTenantAccountV1>(
        "schema-execute",
        "tenants",
        \SchemaBuildTenantAccountV1.tenantID,
        "accounts",
        layer: .partition
    )

    var id: String = ""
    var tenantID: String = ""
    var email: String = ""
}

@Persistable(type: "SchemaBuildTenantAccount")
private struct SchemaBuildTenantAccountV2 {
    #Directory<SchemaBuildTenantAccountV2>(
        "schema-execute",
        "tenants",
        \SchemaBuildTenantAccountV2.tenantID,
        "accounts",
        layer: .partition
    )
    #Index(
        .scalar,
        fields: [\SchemaBuildTenantAccountV2.email],
        name: "schema_build_tenant_account_email"
    )

    var id: String = ""
    var tenantID: String = ""
    var email: String = ""
}

@Suite("Schema execute runtime", .serialized)
struct SchemaExecuteHandlerTests {
    @Test("plan, apply, replay, and request generation leases are coherent")
    func planApplyReplayAndLease() async throws {
        let initialSchema = try Schema(
            entities: [],
            version: Schema.Version(0, 0, 0)
        )
        let targetSchema = try Schema(
            entities: [try SchemaExecuteAccount.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await makeContainer(schema: initialSchema)
        defer { await container.shutdown() }
        let runtime = try await makeRuntime(container: container)
        let initialFingerprint = container.schemaFingerprint
        let targetManifest = SchemaManifest(schema: targetSchema)

        let capabilities = try await invoke(
            DatabaseOperations.capabilitiesDescribe,
            request: EmptyOperationPayload(),
            requestID: 1,
            runtime: runtime
        )
        #expect(capabilities.features.contains {
            $0.identifier == "schema.execute" && $0.version == 1
        })

        let planResponse = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .plan(
                    manifest: targetManifest,
                    expectedFingerprint: initialFingerprint
                )
            ),
            requestID: 2,
            runtime: runtime
        )
        guard case .plan(let plan) = planResponse else {
            Issue.record("Expected a schema plan response")
            return
        }
        let targetFingerprint = try targetManifestFingerprint(targetManifest)
        #expect(plan.currentFingerprint == initialFingerprint)
        #expect(plan.targetFingerprint == targetFingerprint)
        #expect(plan.compatibility == .initial)
        #expect(plan.issues.isEmpty)

        let applied = try await container.withSchemaLease { lease in
            #expect(lease.schema == initialSchema)
            let response = try await invoke(
                DatabaseOperations.schemaExecute,
                request: SchemaExecuteOperation.Request(
                    invocation: .apply(
                        manifest: targetManifest,
                        expectedFingerprint: initialFingerprint,
                        idempotencyKey: "schema-apply-1"
                    )
                ),
                requestID: 3,
                runtime: runtime
            )
            #expect(container.schema == initialSchema)
            return response
        }
        guard case .applied(let firstPublication) = applied else {
            Issue.record("Expected an applied schema response")
            return
        }
        #expect(container.schema == targetSchema)
        #expect(container.schemaGeneration == firstPublication.generation)

        let replay = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: targetManifest,
                    expectedFingerprint: initialFingerprint,
                    idempotencyKey: "schema-apply-1"
                )
            ),
            requestID: 4,
            runtime: runtime
        )
        #expect(replay == applied)

        let description = try await invoke(
            DatabaseOperations.schemaDescribe,
            request: EmptyOperationPayload(),
            requestID: 5,
            runtime: runtime
        )
        #expect(description.version == targetSchema.version)
        #expect(description.entities.map(\.name) == [SchemaExecuteAccount.persistableType])
    }

    @Test("Schema publication uses the latest generation behind a stale request lease")
    func publicationBypassesStaleRequestLease() async throws {
        let initialSchema = try Schema(
            entities: [],
            version: Schema.Version(0, 0, 0)
        )
        let firstSchema = try Schema(
            entities: [try SchemaExecuteAccount.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let secondSchema = try schemaAddingOptionalField(to: firstSchema)
        let container = try await makeContainer(schema: initialSchema)
        defer { await container.shutdown() }
        let coordinator = DatabaseSchemaCoordinator(
            container: container,
            runtimeFactory: AnyDatabaseSchemaRuntimeFactory(
                SchemaDrivenDatabaseRuntimeFactory()
            )
        )
        let initialFingerprint = container.schemaFingerprint.detached()
        let firstManifest = SchemaManifest(schema: firstSchema)
        let firstFingerprint = try firstManifest.fingerprint()
        let secondManifest = SchemaManifest(schema: secondSchema)

        let publication = try await container.withSchemaLease { lease in
            #expect(lease.schema == initialSchema)
            _ = try await coordinator.apply(
                manifest: firstManifest,
                expectedFingerprint: initialFingerprint,
                idempotencyKey: "stale-lease-first",
                context: DatabaseOperationContext(
                    container: container,
                    requestID: 6,
                    metadata: OperationRequestMetadata(
                        idempotencyKey: "stale-lease-first"
                    ),
                    requestPayload: ByteString()
                )
            )
            #expect(container.schema == initialSchema)
            let second = try await coordinator.apply(
                manifest: secondManifest,
                expectedFingerprint: firstFingerprint,
                idempotencyKey: "stale-lease-second",
                context: DatabaseOperationContext(
                    container: container,
                    requestID: 7,
                    metadata: OperationRequestMetadata(
                        idempotencyKey: "stale-lease-second"
                    ),
                    requestPayload: ByteString()
                )
            )
            #expect(container.schema == initialSchema)
            return second
        }

        #expect(container.schema == secondSchema)
        #expect(container.schemaFingerprint == publication.fingerprint)
        #expect(publication.previousFingerprint == firstFingerprint)
    }

    @Test("An old idempotency replay never republishes an obsolete generation")
    func oldIdempotencyReplayDoesNotRollBackGeneration() async throws {
        let initialSchema = try Schema(
            entities: [],
            version: Schema.Version(0, 0, 0)
        )
        let firstSchema = try Schema(
            entities: [try SchemaExecuteAccount.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let secondSchema = try schemaAddingOptionalField(to: firstSchema)
        let container = try await makeContainer(schema: initialSchema)
        defer { await container.shutdown() }
        let runtime = try await makeRuntime(container: container)
        let initialFingerprint = container.schemaFingerprint.detached()
        let firstManifest = SchemaManifest(schema: firstSchema)
        let firstFingerprint = try firstManifest.fingerprint()
        let secondManifest = SchemaManifest(schema: secondSchema)
        let secondFingerprint = try secondManifest.fingerprint()

        let firstResponse = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: firstManifest,
                    expectedFingerprint: initialFingerprint,
                    idempotencyKey: "obsolete-generation-first"
                )
            ),
            requestID: 8,
            runtime: runtime
        )
        let secondResponse = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: secondManifest,
                    expectedFingerprint: firstFingerprint,
                    idempotencyKey: "obsolete-generation-second"
                )
            ),
            requestID: 9,
            runtime: runtime
        )
        let replayResponse = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: firstManifest,
                    expectedFingerprint: initialFingerprint,
                    idempotencyKey: "obsolete-generation-first"
                )
            ),
            requestID: 10,
            runtime: runtime
        )
        guard case .applied(let first) = firstResponse,
              case .applied(let second) = secondResponse,
              case .applied(let replay) = replayResponse else {
            Issue.record("Expected applied schema responses")
            return
        }

        #expect(replay == first)
        #expect(container.schema == secondSchema)
        #expect(container.schemaFingerprint == secondFingerprint)
        #expect(container.schemaGeneration == second.generation)
        #expect(second.generation > first.generation)
    }

    @Test("fingerprint conflict and migration requirement remain typed failures")
    func failuresAreTyped() async throws {
        let initialSchema = try Schema(
            entities: [try SchemaExecuteAccount.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await makeContainer(schema: initialSchema)
        defer { await container.shutdown() }
        let runtime = try await makeRuntime(container: container)
        let wrongFingerprint = try SchemaManifest(
            schema: try Schema(entities: [], version: .init(0, 0, 0))
        ).fingerprint()

        let conflictingBytes = try await execute(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .plan(
                    manifest: SchemaManifest(schema: initialSchema),
                    expectedFingerprint: wrongFingerprint
                )
            ),
            requestID: 11,
            runtime: runtime
        )
        let conflicting = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.schemaExecute,
            from: conflictingBytes,
            matching: 11
        )
        guard case .failure(let conflict) = conflicting else {
            Issue.record("Expected a schema fingerprint conflict")
            return
        }
        #expect(conflict.category == .conflict)
        #expect(conflict.code == "SCHEMA_FINGERPRINT_CONFLICT")

        let requiredFieldSchema = try schemaAddingRequiredField(
            to: initialSchema
        )
        let migrationPlan = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .plan(
                    manifest: SchemaManifest(schema: requiredFieldSchema),
                    expectedFingerprint: container.schemaFingerprint
                )
            ),
            requestID: 12,
            runtime: runtime
        )
        guard case .plan(let plan) = migrationPlan else {
            Issue.record("Expected a migration plan")
            return
        }
        #expect(plan.compatibility == .requiresMigration)
        #expect(plan.issues.contains { $0.code == "required-field-added" })
    }

    @Test("index addition publishes write-only state and completes through its atomic persistent job")
    func indexAdditionUsesPersistentBuildJob() async throws {
        let initialSchema = try Schema(
            entities: [try SchemaBuildAccountV1.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let targetSchema = try Schema(
            entities: [try SchemaBuildAccountV2.schemaEntity],
            version: Schema.Version(2, 0, 0)
        )
        let container = try await DBContainer.open(
            for: initialSchema,
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaBuildAccountV1.self
                    ),
                ]
            ),
            security: .disabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.newContext()
        let first = SchemaBuildAccountV1(
            id: "account-1",
            email: "first@example.com"
        )
        let second = SchemaBuildAccountV1(
            id: "account-2",
            email: "second@example.com"
        )
        try context.insert(first)
        try context.insert(second)
        try await context.save()

        let manifest = SchemaManifest(schema: targetSchema)
        let response = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: manifest,
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-index-build"
                )
            ),
            requestID: 21,
            runtime: runtime
        )
        guard case .applied(let publication) = response,
              let job = publication.job else {
            Issue.record("Expected an applied schema with a persistent job")
            return
        }
        #expect(container.schema == targetSchema)

        let building = try await indexStatus(
            container: container,
            entity: SchemaBuildAccountV2.persistableType,
            index: "schema_build_account_email"
        )
        #expect(building.indexState == .writeOnly)

        try await runtime.runScheduledWork()
        let completed = try await invoke(
            DatabaseOperations.jobStatus,
            request: JobStatusOperation.Request(job: job),
            requestID: 22,
            runtime: runtime
        )
        #expect(completed.state == .succeeded)

        let ready = try await indexStatus(
            container: container,
            entity: SchemaBuildAccountV2.persistableType,
            index: "schema_build_account_email"
        )
        #expect(ready.indexState == .readable)
        #expect(ready.rebuildState?.indexedEntityCount == 2)

        let replay = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: manifest,
                    expectedFingerprint: try SchemaManifest(
                        schema: initialSchema
                    ).fingerprint(),
                    idempotencyKey: "schema-index-build"
                )
            ),
            requestID: 23,
            runtime: runtime
        )
        #expect(replay == response)
    }

    @Test("dynamic partitions stay write-only until every partition is backfilled")
    func dynamicIndexAdditionBuildsEveryPartition() async throws {
        let initialSchema = try Schema(
            entities: [try SchemaBuildTenantAccountV1.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let targetSchema = try Schema(
            entities: [try SchemaBuildTenantAccountV2.schemaEntity],
            version: Schema.Version(2, 0, 0)
        )
        let container = try await DBContainer.open(
            for: initialSchema,
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaBuildTenantAccountV1.self
                    ),
                ]
            ),
            security: .disabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.newContext()
        try context.insert(
            SchemaBuildTenantAccountV1(
                id: "account-a",
                tenantID: "tenant-a",
                email: "a@example.com"
            )
        )
        try context.insert(
            SchemaBuildTenantAccountV1(
                id: "account-b",
                tenantID: "tenant-b",
                email: "b@example.com"
            )
        )
        try await context.save()

        let response = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: SchemaManifest(schema: targetSchema),
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-dynamic-index-build"
                )
            ),
            requestID: 31,
            runtime: runtime
        )
        guard case .applied(let publication) = response,
              let job = publication.job else {
            Issue.record("Expected a dynamic schema index build job")
            return
        }

        let tenantA = try partitions(tenantID: "tenant-a")
        let tenantB = try partitions(tenantID: "tenant-b")
        #expect(
            try await indexStatus(
                container: container,
                entity: SchemaBuildTenantAccountV2.persistableType,
                index: "schema_build_tenant_account_email",
                partitions: tenantA
            ).indexState == .writeOnly
        )
        #expect(
            try await indexStatus(
                container: container,
                entity: SchemaBuildTenantAccountV2.persistableType,
                index: "schema_build_tenant_account_email",
                partitions: tenantB
            ).indexState == .writeOnly
        )

        try await runtime.runScheduledWork()
        let completed = try await invoke(
            DatabaseOperations.jobStatus,
            request: JobStatusOperation.Request(job: job),
            requestID: 32,
            runtime: runtime
        )
        #expect(completed.state == .succeeded)
        for tenant in [tenantA, tenantB] {
            let status = try await indexStatus(
                container: container,
                entity: SchemaBuildTenantAccountV2.persistableType,
                index: "schema_build_tenant_account_email",
                partitions: tenant
            )
            #expect(status.indexState == .readable)
            #expect(status.rebuildState?.indexedEntityCount == 1)
        }
    }

    @Test("cancelled schema index build is recoverable by idempotent re-apply")
    func cancelledIndexBuildCanBeReplaced() async throws {
        let initialSchema = try Schema(
            entities: [try SchemaBuildAccountV1.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let targetSchema = try Schema(
            entities: [try SchemaBuildAccountV2.schemaEntity],
            version: Schema.Version(2, 0, 0)
        )
        let container = try await DBContainer.open(
            for: initialSchema,
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaBuildAccountV1.self
                    ),
                ]
            ),
            security: .disabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.newContext()
        try context.insert(
            SchemaBuildAccountV1(
                id: "cancelled-account",
                email: "cancelled@example.com"
            )
        )
        try await context.save()

        let manifest = SchemaManifest(schema: targetSchema)
        let first = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: manifest,
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-index-cancelled"
                )
            ),
            requestID: 41,
            runtime: runtime
        )
        guard case .applied(let firstPublication) = first,
              let firstJob = firstPublication.job else {
            Issue.record("Expected the first schema index build job")
            return
        }
        let cancellation = try await invoke(
            DatabaseOperations.jobCancel,
            request: JobCancelOperation.Request(job: firstJob),
            requestID: 42,
            runtime: runtime,
            metadata: OperationRequestMetadata(
                idempotencyKey: "schema-index-cancel-job"
            )
        )
        #expect(cancellation.accepted)
        try await runtime.runScheduledWork()
        let cancelled = try await invoke(
            DatabaseOperations.jobStatus,
            request: JobStatusOperation.Request(job: firstJob),
            requestID: 43,
            runtime: runtime
        )
        #expect(cancelled.state == .cancelled)

        let replacement = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: manifest,
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-index-replacement"
                )
            ),
            requestID: 44,
            runtime: runtime
        )
        guard case .applied(let replacementPublication) = replacement,
              let replacementJob = replacementPublication.job else {
            Issue.record("Expected a replacement schema index build job")
            return
        }
        #expect(replacementJob != firstJob)

        try await runtime.runScheduledWork()
        let completed = try await invoke(
            DatabaseOperations.jobStatus,
            request: JobStatusOperation.Request(job: replacementJob),
            requestID: 45,
            runtime: runtime
        )
        #expect(completed.state == .succeeded)
        #expect(
            try await indexStatus(
                container: container,
                entity: SchemaBuildAccountV2.persistableType,
                index: "schema_build_account_email"
            ).indexState == .readable
        )
    }

    private func makeContainer(schema: Schema) async throws -> DBContainer {
        try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            ),
            security: .disabled
        )
    }

    private func makeRuntime(
        container: DBContainer
    ) async throws -> DatabaseServerRuntime {
        let unavailable = UnavailablePlatformServices()
        return try await DatabaseServerRuntime(
            container: container,
            configuration: try DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "schema-test"),
                serviceFactory: AnyDatabaseServerServiceFactory(
                    CanonicalDatabaseServerServiceFactory(
                        maintenanceServiceFactory: unavailable,
                        jobServiceFactory: unavailable
                    )
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock(),
                schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory(
                    SchemaDrivenDatabaseRuntimeFactory()
                )
            )
        )
    }

    private func makePersistentRuntime(
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
        let services = CanonicalDatabaseServerServiceFactory(
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
                identity: DatabaseRuntimeIdentity(version: "schema-test"),
                serviceFactory: AnyDatabaseServerServiceFactory(services),
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
                jobScheduler: SchemaTestJobScheduler()
            )
        )
    }

    private func indexStatus(
        container: DBContainer,
        entity: String,
        index: String,
        partitions: FieldObject = FieldObject()
    ) async throws -> DatabaseIndexMaintenanceStatus {
        try await container.newContext().withTransaction { transaction in
            try await DatabaseIndexMaintenanceRuntime(
                container: container
            ).status(
                entity: entity,
                index: index,
                partitions: partitions,
                transaction: transaction.storageAccess
            )
        }
    }

    private func partitions(tenantID: String) throws -> FieldObject {
        try FieldObject([
            (key: "tenantID", value: .string(tenantID)),
        ])
    }

    private func schemaAddingRequiredField(
        to schema: Schema
    ) throws -> Schema {
        let current = try #require(schema.entities.first)
        let added = FieldSchema(
            name: "requiredValue",
            fieldNumber: 3,
            type: .string,
            isOptional: false,
            isArray: false
        )
        return try Schema(
            entities: [
                try Schema.Entity(
                    name: current.name,
                    identifierType: current.identifierType,
                    fields: current.fields + [added],
                    directoryComponents: current.directoryComponents,
                    directoryLayer: current.directoryLayer,
                    indexes: current.indexes,
                    relationships: current.relationships,
                    fieldAccessRules: current.fieldAccessRules,
                    enumMetadata: current.enumMetadata,
                    ontology: current.ontology,
                    polymorphicMembership: current.polymorphicMembership
                ),
            ],
            version: Schema.Version(2, 0, 0)
        )
    }

    private func schemaAddingOptionalField(
        to schema: Schema
    ) throws -> Schema {
        let current = try #require(schema.entities.first)
        let added = FieldSchema(
            name: "optionalValue",
            fieldNumber: 3,
            type: .string,
            isOptional: true,
            isArray: false
        )
        return try Schema(
            entities: [
                try Schema.Entity(
                    name: current.name,
                    identifierType: current.identifierType,
                    fields: current.fields + [added],
                    directoryComponents: current.directoryComponents,
                    directoryLayer: current.directoryLayer,
                    indexes: current.indexes,
                    relationships: current.relationships,
                    fieldAccessRules: current.fieldAccessRules,
                    enumMetadata: current.enumMetadata,
                    ontology: current.ontology,
                    polymorphicMembership: current.polymorphicMembership
                ),
            ],
            version: Schema.Version(2, 0, 0)
        )
    }

    private func targetManifestFingerprint(
        _ manifest: SchemaManifest
    ) throws -> SchemaFingerprint {
        try manifest.fingerprint()
    }

    private func execute<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        runtime: DatabaseServerRuntime,
        metadata: OperationRequestMetadata = OperationRequestMetadata()
    ) async throws -> ByteString {
        let bytes = try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            metadata: metadata,
            request: request
        )
        return try await runtime.execute(
            bytes,
            context: DatabaseRequestExecutionContext(
                authorization: .anonymous
            )
        )
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        runtime: DatabaseServerRuntime,
        metadata: OperationRequestMetadata = OperationRequestMetadata()
    ) async throws -> Response {
        let response = try DatabaseWireDecoder().decodeResponse(
            operation,
            from: try await execute(
                operation,
                request: request,
                requestID: requestID,
                runtime: runtime,
                metadata: metadata
            ),
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

private actor SchemaTestJobScheduler: DatabaseJobScheduler {
    func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
        _ = timestamp
    }
}

private struct UnavailablePlatformServices:
    DatabaseMaintenanceServiceFactory,
    DatabaseMaintenanceService,
    DatabaseJobServiceFactory,
    DatabaseJobService {
    var jobOperations: [JobOperationIdentifier] { [] }

    func makeMaintenanceService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseMaintenanceService {
        _ = context
        return AnyDatabaseMaintenanceService(self)
    }

    func makeJobService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseJobService {
        _ = context
        return AnyDatabaseJobService(self)
    }

    func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult {
        _ = request
        _ = context
        throw UnavailablePlatformServiceError.unavailable
    }

    func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStartExecutionResult {
        _ = request
        _ = context
        throw UnavailablePlatformServiceError.unavailable
    }

    func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        _ = request
        _ = context
        throw UnavailablePlatformServiceError.unavailable
    }

    func result(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response {
        _ = request
        _ = context
        throw UnavailablePlatformServiceError.unavailable
    }

    func cancel(
        _ request: JobCancelOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobCancellationExecutionResult {
        _ = request
        _ = context
        throw UnavailablePlatformServiceError.unavailable
    }

    func runScheduledWork() async throws {
        throw UnavailablePlatformServiceError.unavailable
    }
}

private enum UnavailablePlatformServiceError: Error {
    case unavailable
}
