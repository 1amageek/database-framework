#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit
import Synchronization

/// Retains one immutable Base generation and its admission lease.
public final class DatabaseBaseLease: Sendable {
    private let token: DatabaseBaseLeaseToken
    package let generation: DatabaseBaseGeneration
    package let permitsDataOperations: Bool
    package let permitsInactiveMaintenance: Bool

    package init(
        generation: DatabaseBaseGeneration,
        token: DatabaseBaseLeaseToken,
        permitsDataOperations: Bool = true,
        permitsInactiveMaintenance: Bool = false
    ) {
        self.generation = generation
        self.token = token
        self.permitsDataOperations = permitsDataOperations
        self.permitsInactiveMaintenance = permitsInactiveMaintenance
    }

    public var baseID: Base.ID { generation.record.id }
    public var placementID: Base.Placement.ID {
        generation.record.placementID
    }
    public var placementGeneration: UInt64 {
        generation.record.placementGeneration
    }
    public var domainID: String { generation.domain.id.value }
    package var systemRoot: Subspace { generation.tenant.systemRoot }
    package var transactionExecutor: StorageTransactionExecutor {
        generation.domain.transactionExecutor
    }

    package var dataRoot: DatabaseDataRootLease {
        DatabaseDataRootLease(
            resource: .base(baseID),
            domain: generation.domain,
            tenant: generation.tenant,
            generation: generation.record.placementGeneration
        )
    }

    /// Ends this admission lease at a point the holder chooses.
    ///
    /// A holder that owns transactions directly must end its lease after those
    /// transactions are terminal, so a Base lifecycle drain cannot complete
    /// while one is still open. Releasing the last reference instead would end
    /// the lease at an ARC release point the holder does not control. The
    /// token is exactly-once, so ending here and letting the last reference go
    /// cannot decrement twice. Every holder is inside this module, so the
    /// release stays `internal`: no caller outside it ends an admission lease.
    internal func finish() {
        token.finish()
    }
}

package final class DatabaseBaseLeaseToken: Sendable {
    private let didFinish = Mutex(false)
    private let finishOperation: @Sendable () -> Void

    package init(finishOperation: @escaping @Sendable () -> Void) {
        self.finishOperation = finishOperation
    }

    package func finish() {
        let shouldFinish = didFinish.withLock { didFinish in
            guard !didFinish else { return false }
            didFinish = true
            return true
        }
        if shouldFinish {
            finishOperation()
        }
    }

    deinit {
        finish()
    }
}

#endif
