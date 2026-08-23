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
    package let root: Subspace
    package let generation: UInt64

    package var transactionExecutor: StorageTransactionExecutor {
        domain.transactionExecutor
    }
}

/// Revocable request-local binding for one selected data root.
package struct DatabaseDataRootExecutionBinding: Sendable {
    package let lease: DatabaseDataRootLease
    package let operationScope: DatabaseReadScopeGate

    package init(
        lease: DatabaseDataRootLease,
        operationScope: DatabaseReadScopeGate
    ) {
        self.lease = lease
        self.operationScope = operationScope
    }
}

/// Request-local data root used by all relative storage paths. The binding is
/// revocable so a child task cannot keep storage authority after the lexical
/// operation that selected the root has returned.
package enum ActiveDatabaseDataRootContext {
    @TaskLocal package static var binding: DatabaseDataRootExecutionBinding?

    package static var lease: DatabaseDataRootLease? {
        binding?.lease
    }
}
#endif
