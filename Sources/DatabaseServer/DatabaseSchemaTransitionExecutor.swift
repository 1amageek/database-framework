import DatabaseEngine
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

/// Owns the cross-domain sequencing required by one database-scoped schema
/// transition. Each Base callback receives only an executor fixed to that Base.
package final class DatabaseSchemaTransitionExecutor: Sendable {
    private let container: DBContainer
    private let authorization: AuthorizationContext
    private let runtimeFactory: AnyDatabaseSchemaRuntimeFactory

    package init(
        container: DBContainer,
        authorization: AuthorizationContext,
        runtimeFactory: AnyDatabaseSchemaRuntimeFactory
    ) {
        self.container = container
        self.authorization = authorization
        self.runtimeFactory = runtimeFactory
    }

    func withBase<Result: Sendable>(
        _ target: DatabaseSchemaApplyJobPlan.BaseTarget,
        _ operation: @Sendable @escaping (
            BaseOperationExecutor
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try container.acquireBaseSchemaMaintenanceLease(target.id)
        guard lease.placementGeneration == target.placementGeneration else {
            throw DatabaseSchemaApplyJobError.baseGenerationChanged(target.id)
        }
        return try await container.withBaseLease(lease) {
            let context = self.container.session(
                authorization: self.authorization
            ).base(target.id).newContext()
            return try await operation(
                BaseOperationExecutor(
                    baseID: target.id,
                    container: self.container,
                    authorization: self.authorization,
                    dataContext: context
                )
            )
        }
    }

    func stage(
        _ target: DatabaseSchemaApplyJobPlan.BaseTarget,
        previousSchema: Schema,
        targetSchema: Schema
    ) async throws {
        try await withBase(target) { executor in
            try await executor.withActiveDataTransaction(
                requiredAccess: .administer,
                configuration: .batch
            ) { transaction in
                _ = try await self.container.initializeNewSchemaIndexStates(
                    from: previousSchema,
                    to: targetSchema,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    func installSnapshot(
        _ target: DatabaseSchemaApplyJobPlan.BaseTarget,
        schema: Schema
    ) async throws {
        try await withBase(target) { executor in
            try await executor.withActiveDataTransaction(
                requiredAccess: .administer,
                configuration: .batch
            ) { transaction in
                try executor.installSchemaSnapshot(
                    schema,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    package func publish(
        manifest: SchemaManifest,
        expectedFingerprint: SchemaFingerprint,
        targetFingerprint: SchemaFingerprint,
        idempotencyKey: String
    ) async throws -> DatabaseSchemaPublicationResult {
        let runtimeConfiguration = try await runtimeFactory
            .makeRuntimeConfiguration(for: manifest.schema)
        try container.validateSchemaGeneration(
            manifest.schema,
            runtimeConfiguration: runtimeConfiguration
        )
        return try await container.publishSchema(
            manifest.schema,
            fingerprint: targetFingerprint,
            expectedFingerprint: expectedFingerprint,
            idempotencyKey: idempotencyKey,
            authorization: authorization,
            runtimeConfiguration: runtimeConfiguration
        )
    }

    package func finish(job: JobIdentity) async throws {
        try await container.withControlTransaction(
            requiredAccess: .administer,
            authorization: authorization,
            configuration: .batch
        ) { transaction in
            try await DatabaseSchemaApplicationStore(
                metadataSubspace: self.container.metadataSubspace
            ).finish(
                job: job,
                transaction: transaction.storageAccess
            )
        }
    }
}
