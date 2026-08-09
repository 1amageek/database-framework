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
    ) async throws -> SchemaExecuteOperation.Applied {
        let targetFingerprint = try manifest.fingerprint()
        if let stored = try await container.storedSchemaPublication(
            idempotencyKey: idempotencyKey,
            matching: targetFingerprint
        ) {
            return try await appliedResponse(for: stored)
        }
        let prepared = try await prepare(
            manifest: manifest,
            expectedFingerprint: nil
        )
        guard prepared.plan.compatibility != .requiresMigration else {
            throw DatabaseSchemaExecutionError.migrationRequired(
                prepared.plan.issues
            )
        }
        let jobStartRequest: JobStartOperation.Request?
        if prepared.requiresIndexBuildJob {
            jobStartRequest = try DatabaseSchemaApplyResumableOperation.job()
                .makeStartRequest(
                    SchemaExecuteOperation.Request(
                        invocation: .apply(
                            manifest: manifest,
                            expectedFingerprint: expectedFingerprint,
                            idempotencyKey: idempotencyKey
                        )
                    ),
                    maximumSliceWorkUnits:
                        DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits
                )
        } else {
            jobStartRequest = nil
        }
        let persistentJobService = jobService
        let publication = try await container.publishSchema(
            manifest.schema,
            fingerprint: prepared.plan.targetFingerprint,
            expectedFingerprint: expectedFingerprint,
            idempotencyKey: idempotencyKey,
            runtimeConfiguration: prepared.runtimeConfiguration,
            prepareIndexBuildJob: { transaction in
                guard let jobStartRequest else { return nil }
                guard let persistentJobService else {
                    throw DatabaseSchemaExecutionError
                        .persistentJobServiceUnavailable
                }
                return try await persistentJobService.createPersistentJob(
                    jobStartRequest,
                    context: context,
                    transaction: transaction
                )
            }
        )
        return try await appliedResponse(for: publication)
    }

    private func appliedResponse(
        for publication: DatabaseSchemaPublicationResult
    ) async throws -> SchemaExecuteOperation.Applied {
        if publication.job != nil {
            guard let persistentJobService = jobService else {
                throw DatabaseSchemaExecutionError
                    .persistentJobServiceUnavailable
            }
            try await persistentJobService.recoverPersistentJobSchedule()
        }
        return SchemaExecuteOperation.Applied(
            previousFingerprint: publication.previousFingerprint,
            fingerprint: publication.fingerprint,
            schemaVersion: publication.schemaVersion,
            generation: publication.generation,
            job: publication.job
        )
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
        let pending = try await container.pendingSchemaIndexBuilds(
            in: manifest.schema
        )
        let indexBuilds = DatabaseSchemaChangeAnalysis.mergedIndexBuilds(
            analyzed: analysis.indexBuilds,
            pending: pending,
            schema: manifest.schema
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
            runtimeConfiguration: runtimeConfiguration,
            requiresIndexBuildJob: !indexBuilds.isEmpty
        )
    }
}

private struct PreparedSchemaChange: Sendable {
    let plan: SchemaExecuteOperation.Plan
    let runtimeConfiguration: DatabaseRuntimeConfiguration
    let requiresIndexBuildJob: Bool
}
