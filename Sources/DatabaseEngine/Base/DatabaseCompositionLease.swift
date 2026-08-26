#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit
import Synchronization

/// Retains one immutable Composition definition and every member Base lease
/// for the complete lifetime of a federated read.
package final class DatabaseCompositionLease: Sendable {
    package let selection: CompositionSelection
    package let resolution: CompositionResolution
    package let namedRecord: DatabaseCompositionRecord?
    package let members: [DatabaseBaseLease]

    package init(
        selection: CompositionSelection,
        resolution: CompositionResolution,
        namedRecord: DatabaseCompositionRecord?,
        members: [DatabaseBaseLease]
    ) {
        precondition(
            resolution.bases == members.map { $0.baseID },
            "Composition member leases must preserve canonical Base order"
        )
        self.selection = selection
        self.resolution = resolution
        self.namedRecord = namedRecord
        self.members = members
    }

    package func member(
        identifiedBy id: Base.ID
    ) -> DatabaseBaseLease? {
        members.first { $0.baseID == id }
    }

    /// Immutable placement generation for every member admitted by this
    /// execution. Remote adapters may bind durable result snapshots to these
    /// values without inventing a generation for a derived Composition.
    package var basePlacementGenerations: [Base.ID: UInt64] {
        Dictionary(
            uniqueKeysWithValues: members.map {
                ($0.baseID, $0.placementGeneration)
            }
        )
    }
}

/// Immutable Composition metadata that does not retain Base admission or
/// transaction lifecycle authority.
@_spi(DatabaseExecution)
public struct DatabaseCompositionReadMetadata: Sendable {
    public let resolution: CompositionResolution
    public let namedRecord: DatabaseCompositionRecord?
    public let basePlacementGenerations: [Base.ID: UInt64]

    init(lease: DatabaseCompositionLease) {
        self.resolution = lease.resolution
        self.namedRecord = lease.namedRecord
        self.basePlacementGenerations = lease.basePlacementGenerations
    }
}

package struct DatabaseCompositionMember: Sendable, Hashable {
    package let baseID: Base.ID
}

final class DatabaseCompositionReadSnapshotVault: Sendable {
    struct MemberAccess: Sendable {
        let lease: DatabaseBaseLease
        let transaction: DatabaseReadTransaction
        let operationLease: DatabaseReadScopeOperationLease
    }

    private struct State: Sendable {
        var isActive = true
        var activeOperationCount = 0
        var invalidationWaiters: [CheckedContinuation<Void, Never>] = []
        var lease: DatabaseCompositionLease?
        var transactions: [String: DatabaseReadTransaction]?
    }

    private let selection: CompositionSelection
    private let state: Mutex<State>

    init(
        lease: DatabaseCompositionLease,
        transactions: [String: DatabaseReadTransaction]
    ) {
        self.selection = lease.selection
        self.state = Mutex(
            State(lease: lease, transactions: transactions)
        )
    }

    func memberAccess(
        for member: DatabaseCompositionMember
    ) throws -> MemberAccess {
        try state.withLock { state in
            guard state.isActive,
                  let compositionLease = state.lease,
                  let transactions = state.transactions,
                  let lease = compositionLease.member(
                    identifiedBy: member.baseID
                  ),
                  let transaction = transactions[lease.domainID] else {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            let (count, overflow) = state.activeOperationCount
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            state.activeOperationCount = count
            return MemberAccess(
                lease: lease,
                transaction: transaction,
                operationLease: DatabaseReadScopeOperationLease(
                    scopeIdentity: ObjectIdentifier(self),
                    endOperation: { [self] in endOperation() }
                )
            )
        }
    }

    func invalidateAndDrain() async {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                state.isActive = false
                guard state.activeOperationCount > 0 else { return true }
                state.invalidationWaiters.append(continuation)
                return false
            }
            if resumeImmediately { continuation.resume() }
        }
        state.withLock { state in
            state.lease = nil
            state.transactions = nil
        }
    }

    private func endOperation() {
        let waiters = state.withLock { state in
            precondition(state.activeOperationCount > 0)
            state.activeOperationCount -= 1
            guard !state.isActive,
                  state.activeOperationCount == 0 else {
                return [CheckedContinuation<Void, Never>]()
            }
            let waiters = state.invalidationWaiters
            state.invalidationWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters { waiter.resume() }
    }
}

/// One request-local view of simultaneously open domain transactions. The
/// value may escape internally, but its vault is synchronously invalidated
/// before the owning transaction lifecycle advances.
package struct DatabaseCompositionReadSnapshot: Sendable {
    package let metadata: DatabaseCompositionReadMetadata
    package let members: [DatabaseCompositionMember]
    let sourceIdentity: DatabaseCompositionSourceIdentity
    let authorization: AuthorizationContext
    let vault: DatabaseCompositionReadSnapshotVault
    private let capturedReadPoints: [DomainReadPoint]

    init(
        lease: DatabaseCompositionLease,
        sourceIdentity: DatabaseCompositionSourceIdentity,
        authorization: AuthorizationContext,
        transactions: [String: DatabaseReadTransaction],
        readPoints: [DomainReadPoint]
    ) {
        self.metadata = DatabaseCompositionReadMetadata(lease: lease)
        self.members = lease.members.map {
            DatabaseCompositionMember(baseID: $0.baseID)
        }
        self.sourceIdentity = sourceIdentity
        self.authorization = authorization
        self.vault = DatabaseCompositionReadSnapshotVault(
            lease: lease,
            transactions: transactions
        )
        self.capturedReadPoints = readPoints
    }

    /// Captures exactly one backend read point for every simultaneously open
    /// physical domain. Domain ordering is canonical so the resulting value is
    /// stable for the complete federated read.
    package func readPoints() async throws -> [DomainReadPoint] {
        capturedReadPoints
    }
}

#endif
