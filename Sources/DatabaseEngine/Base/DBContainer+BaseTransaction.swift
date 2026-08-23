#if DATABASE_MULTI_BASE
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
            let admittedStorageAccess = requiredAccess == .read
                ? ReadAuthorizedTransactionAccess.admitted(storageAccess)
                : storageAccess
            let transaction = DatabaseTransaction(
                storageAccess: admittedStorageAccess,
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

#endif
