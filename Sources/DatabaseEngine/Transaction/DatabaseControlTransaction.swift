import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one transaction against the control domain for framework-owned
    /// metadata. Callers at a user request boundary must perform the target
    /// Grant check before invoking this method; this method is intentionally
    /// package-scoped and never creates an authorization bypass on its own.
    package func withControlMetadataTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await controlTransactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            let transaction = DatabaseTransaction(
                storageAccess: storageAccess,
                container: self
            )
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

    /// Executes a control-domain transaction after evaluating the persisted
    /// database Grant in that same transaction attempt.
    package func withControlTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await controlTransactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            try await self.databaseGrantStore.require(
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
}
