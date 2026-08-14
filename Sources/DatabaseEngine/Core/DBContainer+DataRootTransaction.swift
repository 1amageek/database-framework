import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one database transaction through the container-owned engine.
    /// When `MultipleBases` is enabled, the selected Base and its persisted
    /// Grant are resolved inside the same transaction attempt.
    package func withDatabaseTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        #if DATABASE_MULTIPLE_BASES
        let lease = try requireActiveDataRoot()
        let authorization = RequestAuthorization.context
        let selectedTransactionExecutor = lease.transactionExecutor
        #else
        _ = requiredAccess
        let selectedTransactionExecutor = transactionExecutor
        #endif
        return try await selectedTransactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { transaction in
            #if DATABASE_MULTIPLE_BASES
            try await DatabaseGrantStore(
                resource: lease.resource,
                root: lease.root
            ).require(
                requiredAccess,
                authorization: authorization,
                transaction: transaction
            )
            #endif
            return try await operation(transaction)
        }
    }
}
