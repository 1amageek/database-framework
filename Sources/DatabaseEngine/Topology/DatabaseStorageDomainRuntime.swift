#if DATABASE_MULTIPLE_BASES
import StorageKit

/// Prepared execution state for one claimed storage domain.
package struct DatabaseStorageDomainRuntime: Sendable {
    package let id: DatabaseStorageDomain.ID
    package let namespacePath: [String]
    package let engine: any StorageEngine
    package let transactionExecutor: StorageTransactionExecutor
    package let root: Subspace
    package let transactionCapabilities: TransactionCapabilities
}
#endif
