import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one target-local storage attempt after evaluating the persisted
    /// Grant in that same transaction. The data root and request authorization
    /// must already be bound by the operation coordinator.
    package func withActiveDataRootTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try requireActiveDataRoot()
        let authorization = RequestAuthorization.context
        return try await lease.transactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { transaction in
            try await DatabaseGrantStore(
                resource: lease.resource,
                root: lease.root
            ).require(
                requiredAccess,
                authorization: authorization,
                transaction: transaction
            )
            return try await operation(transaction)
        }
    }
}
