#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Immutable storage root retained for the duration of one data operation.
///
/// The root represents the database itself in the single-root runtime and a
/// selected Base when the `MultiBase` trait is enabled. Query, mutation,
/// and index execution consume this type and do not depend on Base lifecycle
/// semantics.
package struct DatabaseDataRootLease: Sendable {
    package let resource: Security.Resource
    package let domain: DatabaseStorageDomainRuntime
    package let tenant: DatabaseTenantDirectories
    package let generation: UInt64

    /// Framework metadata Subspace of this Tenant Partition.
    package var systemRoot: Subspace { tenant.systemRoot }

    /// Directory application `#Directory` binding starts from.
    package var dataDirectory: Directory { tenant.data }

    /// Stable identity of the Partition this lease admits operations against.
    package var partitionRoot: Subspace { tenant.partitionRoot }

    package var transactionExecutor: StorageTransactionExecutor {
        domain.transactionExecutor
    }
}

/// Request-local data root used by all relative storage paths.
package enum ActiveDatabaseDataRootContext {
    @TaskLocal package static var lease: DatabaseDataRootLease?
}
#endif
