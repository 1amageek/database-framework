@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseWireRuntime
import DatabaseFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import TestSupport
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

        let accepted = try await container.withSchemaLease { lease in
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
        guard case .accepted(let job) = accepted else {
            Issue.record("Expected an accepted schema transition job")
            return
        }
        #expect(container.schema == initialSchema)
        let status = try await runUntilTerminal(
            job,
            runtime: runtime,
            firstRequestID: 4
        )
        #expect(status.state == .succeeded)
        let publication = try await schemaApplyResult(
            job,
            runtime: runtime,
            requestID: 80
        )
        #expect(container.schema == targetSchema)
        #expect(container.schemaGeneration == publication.generation)

        let replay = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: targetManifest,
                    expectedFingerprint: initialFingerprint,
                    idempotencyKey: "schema-apply-1"
                )
            ),
            requestID: 81,
            runtime: runtime
        )
        #expect(replay == accepted)

        let description = try await invoke(
            DatabaseOperations.schemaDescribe,
            request: EmptyOperationPayload(),
            requestID: 82,
            runtime: runtime
        )
        #expect(description.version == targetSchema.version)
        #expect(description.entities.map(\.name) == [SchemaExecuteAccount.persistableType])
    }

    @Test("Schema publication preserves an already acquired generation lease")
    func publicationPreservesStaleRequestLease() async throws {
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
        let runtime = try await makePersistentRuntime(container: container)
        let initialFingerprint = container.schemaFingerprint.detached()
        let firstManifest = SchemaManifest(schema: firstSchema)
        let firstFingerprint = try firstManifest.fingerprint()
        let secondManifest = SchemaManifest(schema: secondSchema)

        let firstPublication = try await container.withSchemaLease { lease in
            #expect(lease.schema == initialSchema)
            let response = try await invoke(
                DatabaseOperations.schemaExecute,
                request: SchemaExecuteOperation.Request(
                    invocation: .apply(
                        manifest: firstManifest,
                        expectedFingerprint: initialFingerprint,
                        idempotencyKey: "stale-lease-first"
                    )
                ),
                requestID: 6,
                runtime: runtime
            )
            guard case .accepted(let job) = response else {
                throw SchemaExecuteHandlerTestError.expectedAcceptedJob
            }
            #expect(container.schema == initialSchema)
            let status = try await runUntilTerminal(
                job,
                runtime: runtime,
                firstRequestID: 90
            )
            #expect(status.state == .succeeded)
            #expect(lease.schema == initialSchema)
            #expect(container.schema == initialSchema)
            return try await schemaApplyResult(
                job,
                runtime: runtime,
                requestID: 91
            )
        }
        #expect(container.schema == firstSchema)

        let secondResponse = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: secondManifest,
                    expectedFingerprint: firstFingerprint,
                    idempotencyKey: "stale-lease-second"
                )
            ),
            requestID: 7,
            runtime: runtime
        )
        guard case .accepted(let secondJob) = secondResponse else {
            Issue.record("Expected the second schema transition job")
            return
        }
        let secondStatus = try await runUntilTerminal(
            secondJob,
            runtime: runtime,
            firstRequestID: 92
        )
        #expect(secondStatus.state == .succeeded)
        let secondPublication = try await schemaApplyResult(
            secondJob,
            runtime: runtime,
            requestID: 93
        )

        #expect(container.schema == secondSchema)
        #expect(container.schemaFingerprint == secondPublication.fingerprint)
        #expect(firstPublication.fingerprint == firstFingerprint)
        #expect(secondPublication.previousFingerprint == firstFingerprint)
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
        guard case .accepted(let firstJob) = firstResponse else {
            Issue.record("Expected the first schema transition job")
            return
        }
        #expect(
            try await runUntilTerminal(
                firstJob,
                runtime: runtime,
                firstRequestID: 100
            ).state == .succeeded
        )
        let first = try await schemaApplyResult(
            firstJob,
            runtime: runtime,
            requestID: 101
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
        guard case .accepted(let secondJob) = secondResponse else {
            Issue.record("Expected the second schema transition job")
            return
        }
        #expect(
            try await runUntilTerminal(
                secondJob,
                runtime: runtime,
                firstRequestID: 102
            ).state == .succeeded
        )
        let second = try await schemaApplyResult(
            secondJob,
            runtime: runtime,
            requestID: 103
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

        #expect(replayResponse == firstResponse)
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
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.testBaseContext()
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
        guard case .accepted(let job) = response else {
            Issue.record("Expected an accepted schema transition job")
            return
        }
        #expect(container.schema == initialSchema)

        try await runtime.runScheduledWork()
        #expect(container.schema == initialSchema)
        try await runtime.runScheduledWork()
        let building = try await indexStatus(
            container: container,
            entity: SchemaBuildAccountV2.persistableType,
            index: "schema_build_account_email"
        )
        #expect(building.indexState == .writeOnly)
        #expect(container.schema == targetSchema)

        let completed = try await runUntilTerminal(
            job,
            runtime: runtime,
            firstRequestID: 22
        )
        #expect(completed.state == .succeeded)
        #expect(container.schema == targetSchema)

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
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.testBaseContext()
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
        guard case .accepted(let job) = response else {
            Issue.record("Expected a dynamic schema index build job")
            return
        }

        #expect(container.schema == initialSchema)
        try await runtime.runScheduledWork()
        #expect(container.schema == initialSchema)
        try await runtime.runScheduledWork()
        #expect(container.schema == targetSchema)
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

        let completed = try await runUntilTerminal(
            job,
            runtime: runtime,
            firstRequestID: 32
        )
        #expect(completed.state == .succeeded)
        #expect(container.schema == targetSchema)
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
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let runtime = try await makePersistentRuntime(container: container)
        let context = container.testBaseContext()
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
        guard case .accepted(let firstJob) = first else {
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
        let cancelled = try await runUntilTerminal(
            firstJob,
            runtime: runtime,
            firstRequestID: 43
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
        guard case .accepted(let replacementJob) = replacement else {
            Issue.record("Expected a replacement schema index build job")
            return
        }
        #expect(replacementJob != firstJob)

        let completed = try await runUntilTerminal(
            replacementJob,
            runtime: runtime,
            firstRequestID: 45
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

#if MultipleBases
    @Test("An accepted schema transition blocks Base lifecycle changes")
    func acceptedTransitionBlocksBaseLifecycle() async throws {
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
        let runtime = try await makePersistentRuntime(container: container)
        let base = try await baseRecord(container)
        let response = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: SchemaManifest(schema: targetSchema),
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-blocks-base-lifecycle"
                )
            ),
            requestID: 51,
            runtime: runtime
        )
        guard case .accepted(let job) = response else {
            Issue.record("Expected an accepted schema transition job")
            return
        }

        do {
            _ = try await container.retireBase(
                base.id,
                expectedRevision: base.revision
            )
            Issue.record("Expected the Base lifecycle change to be rejected")
        } catch DatabaseSchemaPublicationError.transitionInProgress(
            let activeJob
        ) {
            #expect(activeJob == job)
        }

        #expect(
            try await runUntilTerminal(
                job,
                runtime: runtime,
                firstRequestID: 52
            ).state == .succeeded
        )
    }

    @Test("Schema transition installs the current snapshot into retired Bases")
    func retiredBaseReceivesSchemaSnapshot() async throws {
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
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let current = try await baseRecord(container)
        let retired = try await container.retireBase(
            current.id,
            expectedRevision: current.revision
        )
        let runtime = try await makePersistentRuntime(container: container)
        let response = try await invoke(
            DatabaseOperations.schemaExecute,
            request: SchemaExecuteOperation.Request(
                invocation: .apply(
                    manifest: SchemaManifest(schema: targetSchema),
                    expectedFingerprint: container.schemaFingerprint,
                    idempotencyKey: "schema-retired-base"
                )
            ),
            requestID: 61,
            runtime: runtime
        )
        guard case .accepted(let job) = response else {
            Issue.record("Expected an accepted schema transition job")
            return
        }
        #expect(
            try await runUntilTerminal(
                job,
                runtime: runtime,
                firstRequestID: 62
            ).state == .succeeded
        )

        let lease = try container.acquireBaseSchemaMaintenanceLease(
            retired.id
        )
        let installedVersion = try await container.withBaseLease(lease) {
            try await container.getCurrentSchemaVersion()
        }
        #expect(installedVersion == targetSchema.version)

        let active = try await container.activateBase(
            retired.id,
            expectedRevision: retired.revision,
            authorization: TestBaseEnvironment.authorization
        )
        #expect(active.lifecycle == .active)
    }
#endif

    private func makeContainer(schema: Schema) async throws -> DBContainer {
        try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            ),
            security: .testingDisabled
        )
    }

#if MultipleBases
    private func baseRecord(
        _ container: DBContainer
    ) async throws -> DatabaseBaseRecord {
        let id = try TestBaseEnvironment.id()
        return try await container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try #require(
                try await container.baseCatalog.load(
                    id,
                    transaction: transaction.storageAccess
                )
            )
        }
    }
#endif

    private func makeRuntime(
        container: DBContainer
    ) async throws -> DatabaseOperationRuntime {
        try await makePersistentRuntime(container: container)
    }

    private func makePersistentRuntime(
        container: DBContainer
    ) async throws -> DatabaseOperationRuntime {
        let runtimeLimits = DatabaseRuntimeLimits.default
        let identifierGenerator = RandomDatabaseUUIDGenerator()
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                try AnyDatabaseResumableOperation(
                    DatabaseMaintenanceResumableOperation(
                        runtimeLimits: runtimeLimits
                    )
                ),
            ]
        )
        let services = CanonicalDatabaseOperationServiceFactory(
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
        return try await DatabaseOperationRuntime(
            container: container,
            configuration: try DatabaseOperationRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "schema-test"),
                serviceFactory: AnyDatabaseOperationServiceFactory(services),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock(),
                schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory(
                    SchemaDrivenDatabaseRuntimeFactory()
                ),
                runtimeLimits: runtimeLimits
            ),
            hostServices: try testJobHostServices(
                scheduler: SchemaTestJobScheduler()
            )
        )
    }

    private func runUntilTerminal(
        _ job: JobIdentity,
        runtime: DatabaseOperationRuntime,
        firstRequestID: UInt64
    ) async throws -> JobStatusOperation.Response {
        var requestID = firstRequestID
        for _ in 0..<128 {
            try await runtime.runScheduledWork()
            let status = try await invoke(
                DatabaseOperations.jobStatus,
                request: JobStatusOperation.Request(job: job),
                requestID: requestID,
                runtime: runtime
            )
            switch status.state {
            case .succeeded, .failed, .cancelled:
                return status
            case .pending, .running, .committingUnsuccessfulOutcome:
                break
            }
            requestID += 1
        }
        throw SchemaExecuteHandlerTestError.didNotReachTerminalState
    }

    private func schemaApplyResult(
        _ job: JobIdentity,
        runtime: DatabaseOperationRuntime,
        requestID: UInt64
    ) async throws -> SchemaExecuteOperation.Applied {
        let result = try await invoke(
            DatabaseOperations.jobResult,
            request: JobResultOperation.Request(job: job),
            requestID: requestID,
            runtime: runtime
        )
        guard case .succeeded(
            _,
            let responsePayload,
            _,
            _,
            nil
        ) = result else {
            throw SchemaExecuteHandlerTestError.expectedSuccessfulJobResult
        }
        let response = try DatabaseWireDecoder().decodeResponsePayload(
            DatabaseOperations.schemaExecute,
            from: responsePayload
        )
        guard case .applied(let publication) = response else {
            throw SchemaExecuteHandlerTestError.expectedAppliedResult
        }
        return publication
    }

    private func indexStatus(
        container: DBContainer,
        entity: String,
        index: String,
        partitions: FieldObject = FieldObject()
    ) async throws -> DatabaseIndexMaintenanceStatus {
        try await container.testBaseContext().withTransaction { transaction in
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
        runtime: DatabaseOperationRuntime,
        metadata: OperationRequestMetadata = OperationRequestMetadata()
    ) async throws -> ByteString {
        let bytes = try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            target: .database,
            metadata: metadata,
            request: request
        )
        return try await runtime.execute(
            bytes,
            context: DatabaseRequestExecutionContext(
                authorization: TestBaseEnvironment.authorization,
                jobAuthorizationReference:
                    try TestDatabaseJobAuthorizationValidator.reference()
            )
        )
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        runtime: DatabaseOperationRuntime,
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

private enum SchemaExecuteHandlerTestError: Error {
    case didNotReachTerminalState
    case expectedAcceptedJob
    case expectedSuccessfulJobResult
    case expectedAppliedResult
}
