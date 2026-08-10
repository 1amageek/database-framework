import DatabaseEngine
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

public actor DatabaseSchemaCoordinator {
    private let container: DBContainer
    private let runtimeFactory: AnyDatabaseSchemaRuntimeFactory
    private let jobService: AnyDatabaseJobService?

    public init(
        container: DBContainer,
        runtimeFactory: AnyDatabaseSchemaRuntimeFactory,
        jobService: AnyDatabaseJobService? = nil
    ) {
        self.container = container
        self.runtimeFactory = runtimeFactory
        self.jobService = jobService
    }

    public func plan(
        manifest: SchemaManifest,
        expectedFingerprint: SchemaFingerprint?
    ) async throws -> SchemaExecuteOperation.Plan {
        let prepared = try await prepare(
            manifest: manifest,
            expectedFingerprint: expectedFingerprint
        )
        return prepared.plan
    }

    public func apply(
        manifest: SchemaManifest,
        expectedFingerprint: SchemaFingerprint,
        idempotencyKey: String,
        context: DatabaseOperationContext
    ) async throws -> JobIdentity {
        let executor = try context.requireControlExecutor()
        let targetFingerprint = try manifest.fingerprint()
        if let existing = try await executor.withTransaction(
            requiredAccess: .administer,
            configuration: .readOnly,
            { transaction in
                try await executor.schemaApplication(
                    idempotencyKey: idempotencyKey,
                    transaction: transaction
                )
            }
        ) {
            try validate(
                existing,
                expectedFingerprint: expectedFingerprint,
                targetFingerprint: targetFingerprint
            )
            try await jobService?.recoverPersistentJobSchedule()
            return existing.job
        }
        let prepared = try await prepare(
            manifest: manifest,
            expectedFingerprint: expectedFingerprint
        )
        guard prepared.plan.compatibility != .requiresMigration else {
            throw DatabaseSchemaExecutionError.migrationRequired(
                prepared.plan.issues
            )
        }
        _ = prepared.runtimeConfiguration
        guard let persistentJobService = jobService else {
            throw DatabaseSchemaExecutionError.persistentJobServiceUnavailable
        }
        let startRequest = try DatabaseSchemaApplyResumableOperation.job()
            .makeStartRequest(
                SchemaExecuteOperation.Request(
                    invocation: .apply(
                        manifest: manifest,
                        expectedFingerprint: expectedFingerprint,
                        idempotencyKey: idempotencyKey
                    )
                ),
                target: .database,
                maximumSliceWorkUnits:
                    DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
            )
        let job = try await executor.withTransaction(
            requiredAccess: .administer,
            configuration: .batch
        ) { transaction in
            if let existing = try await executor.schemaApplication(
                idempotencyKey: idempotencyKey,
                transaction: transaction
            ) {
                try self.validate(
                    existing,
                    expectedFingerprint: expectedFingerprint,
                    targetFingerprint: targetFingerprint
                )
                return existing.job
            }
            let job = try await persistentJobService.createPersistentJob(
                startRequest,
                context: context,
                transaction: transaction
            )
            try await executor.insertSchemaApplication(
                DatabaseSchemaApplicationRecord(
                    idempotencyKey: idempotencyKey,
                    expectedFingerprint: expectedFingerprint,
                    targetFingerprint: targetFingerprint,
                    job: job
                ),
                transaction: transaction
            )
            return job
        }
        try await persistentJobService.recoverPersistentJobSchedule()
        return job
    }

    private nonisolated func validate(
        _ application: DatabaseSchemaApplicationRecord,
        expectedFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint
    ) throws {
        guard application.expectedFingerprint == expectedFingerprint,
              application.targetFingerprint == targetFingerprint else {
            throw DatabaseSchemaPublicationError.idempotencyKeyReused(
                application.idempotencyKey
            )
        }
    }

    private func prepare(
        manifest: SchemaManifest,
        expectedFingerprint: SchemaFingerprint?
    ) async throws -> PreparedSchemaChange {
        let publishedLease = container.acquirePublishedSchemaLease()
        let currentSchema = publishedLease.schema
        let currentFingerprint = publishedLease.fingerprint.detached()
        if let expectedFingerprint,
           expectedFingerprint != currentFingerprint {
            throw DatabaseSchemaPublicationError.fingerprintConflict(
                expected: expectedFingerprint,
                actual: currentFingerprint
            )
        }
        let targetFingerprint = try manifest.fingerprint()
        let analysis = DatabaseSchemaChangeAnalysis.analyze(
            current: currentSchema,
            target: manifest.schema
        )
        let runtimeConfiguration: DatabaseRuntimeConfiguration
        do {
            runtimeConfiguration = try await runtimeFactory
                .makeRuntimeConfiguration(for: manifest.schema)
            try container.validateSchemaGeneration(
                manifest.schema,
                runtimeConfiguration: runtimeConfiguration
            )
        } catch let error as DatabaseSchemaPublicationError {
            throw error
        } catch let error as DatabaseEngine.DatabaseRuntimeConfigurationError {
            if case .unsupportedStorageCapability(
                _,
                let indexName,
                let kindIdentifier,
                let capability
            ) = error {
                let capabilityName: String
                switch capability {
                case .versionstampedMutations:
                    capabilityName = "versionstampedMutations"
                }
                throw DatabaseSchemaExecutionError
                    .storageCapabilityUnavailable(
                        indexName: indexName,
                        kindIdentifier: kindIdentifier,
                        capability: capabilityName
                    )
            }
            throw DatabaseSchemaExecutionError.runtimeUnavailable
        } catch {
            throw DatabaseSchemaExecutionError.runtimeUnavailable
        }
        return PreparedSchemaChange(
            plan: SchemaExecuteOperation.Plan(
                currentFingerprint: currentFingerprint,
                targetFingerprint: targetFingerprint,
                compatibility: analysis.compatibility,
                issues: analysis.issues
            ),
            runtimeConfiguration: runtimeConfiguration
        )
    }
}

private struct PreparedSchemaChange: Sendable {
    let plan: SchemaExecuteOperation.Plan
    let runtimeConfiguration: DatabaseRuntimeConfiguration
}
