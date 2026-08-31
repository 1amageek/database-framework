#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Immutable storage root retained for the duration of one data operation.
///
/// The root is the Base a session selected. Query, mutation, and index
/// execution consume this type and do not depend on Base lifecycle semantics.
///
/// Despite its name this value is an address record, not authority: it says
/// which Partition an operation resolves below, not that the operation may
/// read or write there. Authority over the keyspace is the StorageKit
/// `PartitionLease` a transaction attempt takes through `partitionAuthority`.
package struct DatabaseDataRootLease: Sendable {
    package let resource: Security.Resource
    package let domain: DatabaseStorageDomainRuntime
    package let tenant: DatabaseTenantDirectories
    package let generation: UInt64

    /// Framework metadata Subspace of this Tenant Partition.
    package var systemRoot: Subspace { tenant.systemRoot }

    /// Directory application `#Directory` binding starts from.
    package var dataDirectory: Directory { tenant.data }

    /// Stable identity of the Partition this operation addresses.
    ///
    /// A retained handle whose Partition was removed and recreated no longer
    /// matches this value, which is the generation `leasePartition` re-checks
    /// when the attempt takes authority.
    package var partitionRoot: Subspace { tenant.partitionRoot }

    package var transactionExecutor: StorageTransactionExecutor {
        domain.transactionExecutor
    }

    /// Authority selector for the Partition this operation addresses.
    package var partitionAuthority: DatabasePartitionAuthority {
        DatabasePartitionAuthority(
            engine: domain.engine,
            partition: tenant.partition
        )
    }
}

/// Request-local data root used by all relative storage paths.
package enum ActiveDatabaseDataRootContext {
    @TaskLocal package static var lease: DatabaseDataRootLease?
}
#endif
