import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
@_spi(DatabaseOperations) import DatabaseWire

/// Package-internal capability for creating a job inside an existing database
/// transaction. Schema publication uses this boundary so its generation,
/// index lifecycle markers, and resumable job become durable atomically.
package protocol DatabasePersistentJobCreating: Sendable {
    func preparePersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> DatabasePreparedPersistentJob

    func storePreparedPersistentJob(
        _ prepared: DatabasePreparedPersistentJob,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity

    func createPersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity

    func recoverPersistentJobSchedule() async throws
}
