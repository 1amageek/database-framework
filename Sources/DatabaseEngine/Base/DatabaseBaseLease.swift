#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit
import Synchronization

/// Retains one immutable Base generation and its admission lease.
///
/// Construction is the authority to admit work against a Base generation, so
/// it stays inside this module: the generation store and the Base lifecycle
/// paths that own the admission count are the only issuers. A `package`
/// initializer would let a sibling target read the generation off a counted
/// lease, build an uncounted one around a no-op token, release the counted
/// lease, and keep operating against a Base whose drain has already completed.
public final class DatabaseBaseLease: Sendable {
    private let token: DatabaseBaseLeaseToken
    internal let generation: DatabaseBaseGeneration
    package let permitsDataOperations: Bool
    package let permitsInactiveMaintenance: Bool

    internal init(
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

/// Carries the exactly-once release of one admission count.
///
/// The token is the counted half of a lease, so issuing one is the same
/// authority as issuing a lease and stays `internal` for the same reason.
internal final class DatabaseBaseLeaseToken: Sendable {
    private let didFinish = Mutex(false)
    private let finishOperation: @Sendable () -> Void

    internal init(finishOperation: @escaping @Sendable () -> Void) {
        self.finishOperation = finishOperation
    }

    internal func finish() {
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
