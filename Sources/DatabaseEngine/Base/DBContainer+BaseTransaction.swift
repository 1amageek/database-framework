import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes an administrative Base transaction. The bound lease may refer
    /// to an inactive Base, but the transaction never grants data admission.
    package func withBaseAdministrationTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try requireBoundBaseLease()
        return try await lease.transactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            try await DatabaseGrantStore(
                resource: .base(lease.baseID),
                root: lease.root
            ).require(
                requiredAccess,
                authorization: authorization,
                transaction: storageAccess
            )
            let transaction = DatabaseTransaction(
                storageAccess: storageAccess,
                container: self
            )
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                do {
                    let result = try await operation(transaction)
                    try await transaction.prepareForCommit()
                    return result
                } catch {
                    await transaction.invalidate()
                    throw error
                }
            }
        }
    }

    /// Executes one Base-local storage attempt after evaluating the persisted
    /// Grant in that same transaction. The current Base lease and request
    /// authorization must already be bound by the operation coordinator.
    package func withActiveBaseTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try requireActiveBaseLease()
        let authorization = RequestAuthorization.context
        return try await lease.transactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { transaction in
            try await DatabaseGrantStore(
                resource: .base(lease.baseID),
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
