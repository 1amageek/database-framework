import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one transaction against the control domain for framework-owned
    /// metadata. Callers at a user request boundary must perform the target
    /// Grant check before invoking this method; this method is intentionally
    /// package-scoped and never creates an authorization bypass on its own.
    @_spi(DatabaseExecution)
    public func withControlMetadataTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        #if DATABASE_MULTI_BASE
        let selectedTransactionExecutor = controlTransactionExecutor
        #else
        let selectedTransactionExecutor = transactionExecutor
        #endif
        return try await selectedTransactionExecutor.withTransaction(
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

    #if DATABASE_MULTI_BASE
    /// Executes a control-domain transaction after evaluating the persisted
    /// database Grant in the same transaction attempt.
    @_spi(DatabaseExecution)
    public func withControlTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        let selectedTransactionExecutor = controlTransactionExecutor
        return try await selectedTransactionExecutor.withTransaction(
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
    #else
    /// Executes authorized control work against the ordinary database.
    /// The calling composition admits the operation while the framework binds
    /// its authorization context for entity and field policies.
    @_spi(DatabaseExecution)
    public func withControlTransaction<Result: Sendable>(
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await transactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
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
    #endif
}
