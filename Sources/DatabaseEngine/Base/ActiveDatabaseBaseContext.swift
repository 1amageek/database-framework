#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Non-owning Base execution identity inherited through task-local context.
///
/// The lexical `withBaseLease` caller retains the counted owner. Child tasks
/// inherit only this immutable placement value and a revocable operation gate,
/// so retaining task-local context cannot delay Base retirement.
package struct DatabaseBaseExecutionBinding: Sendable {
    package let generation: DatabaseBaseGeneration
    package let permitsDataOperations: Bool
    package let permitsInactiveMaintenance: Bool
    package let operationScope: DatabaseReadScopeGate

    package init(
        lease: DatabaseBaseLease,
        operationScope: DatabaseReadScopeGate
    ) {
        self.generation = lease.generation
        self.permitsDataOperations = lease.permitsDataOperations
        self.permitsInactiveMaintenance = lease.permitsInactiveMaintenance
        self.operationScope = operationScope
    }

    package var baseID: Base.ID { generation.record.id }
    package var placementID: Base.Placement.ID {
        generation.record.placementID
    }
    package var placementGeneration: UInt64 {
        generation.record.placementGeneration
    }
    package var domainID: String { generation.domain.id.value }
    package var root: Subspace { generation.root }
    package var transactionExecutor: StorageTransactionExecutor {
        generation.domain.transactionExecutor
    }
    package var dataRoot: DatabaseDataRootLease {
        DatabaseDataRootLease(
            resource: .base(baseID),
            domain: generation.domain,
            root: generation.root,
            generation: generation.record.placementGeneration
        )
    }
}

/// Request-local immutable Base placement used by all relative data paths.
package enum ActiveDatabaseBaseContext {
    @TaskLocal package static var binding: DatabaseBaseExecutionBinding?
}

#endif
