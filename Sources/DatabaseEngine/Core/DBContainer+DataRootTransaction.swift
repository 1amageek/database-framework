import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one database transaction through the container-owned engine.
    /// When `MultiBase` is enabled, the selected Base and its persisted
    /// Grant are resolved inside the same transaction attempt.
    package func withDatabaseTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        #if DATABASE_MULTI_BASE
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
            #if DATABASE_MULTI_BASE
            try await DatabaseGrantStore(
                resource: lease.resource,
                root: lease.root
            ).require(
                requiredAccess,
                authorization: authorization,
                transaction: transaction
            )
            #endif
            let operationScope = DatabaseReadScopeGate()
            let admittedTransaction = DataRootTransactionAccess.admitted(
                transaction,
                dataRoot: {
                    #if DATABASE_MULTI_BASE
                    lease.root
                    #else
                    self.databaseRoot
                    #endif
                }(),
                accessMode: requiredAccess == .read
                    ? .readOnly
                    : .readWrite,
                readScope: operationScope
            )
            let result: Result
            do {
                result = try await operation(admittedTransaction)
            } catch {
                let operationError = error
                do {
                    try await operationScope.closeAndWait()
                } catch let cleanupError as DatabaseReadScopeCleanupError {
                    admittedTransaction.revoke()
                    throw cleanupError.preserving(
                        operationError: operationError
                    )
                }
                admittedTransaction.revoke()
                throw operationError
            }
            do {
                try await operationScope.closeAndWait()
                admittedTransaction.revoke()
                return result
            } catch {
                admittedTransaction.revoke()
                throw error
            }
        }
    }
}
