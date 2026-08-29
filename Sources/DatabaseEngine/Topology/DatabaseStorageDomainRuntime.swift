#if DATABASE_MULTI_BASE
import StorageKit

/// Prepared execution state for one claimed storage domain.
///
/// `databaseRoot` is the Directory named by `rootPath` and `defaultTenant` is
/// the Default Partition of that root. Both are fixed for the container's
/// lifetime, so retaining them is sound. Base Partitions are not retained here:
/// `bases/<Base.ID>` is resolved inside the transaction that uses it, because
/// provisioning, moving, and deletion change which Partitions exist.
package struct DatabaseStorageDomainRuntime: Sendable {
    package let id: DatabaseStorageDomain.ID
    package let rootPath: [String]
    package let engine: any StorageEngine
    package let transactionExecutor: StorageTransactionExecutor
    package let databaseRoot: Directory
    package let defaultTenant: DatabaseTenantDirectories
    package let transactionCapabilities: TransactionCapabilities

    /// Directory access of this domain's engine.
    package var directoryAccess: any DirectoryAccess {
        engine.directoryAccess
    }

    /// Framework metadata Subspace of this domain's Default Partition.
    ///
    /// Domain-scoped records that must outlive a Base Partition, such as its
    /// deletion marker, belong here rather than inside `bases/<Base.ID>`.
    package var systemRoot: Subspace { defaultTenant.systemRoot }

    /// Directory the Default Partition's application binding starts from.
    package var dataDirectory: Directory { defaultTenant.data }
}
#endif
