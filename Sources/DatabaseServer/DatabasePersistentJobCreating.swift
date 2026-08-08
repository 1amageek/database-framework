import DatabaseEngine
@_spi(DatabaseServer) import DatabaseWire

/// Package-internal capability for creating a job inside an existing database
/// transaction. Schema publication uses this boundary so its generation,
/// index lifecycle markers, and resumable job become durable atomically.
package protocol DatabasePersistentJobCreating: Sendable {
    func createPersistentJob(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> JobIdentity

    func recoverPersistentJobSchedule() async throws
}
