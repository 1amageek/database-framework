import DatabaseTypes
import StorageKit
import Synchronization

/// Coordinates deterministic transaction failures and suspension boundaries.
public final class StorageTransactionControl: Sendable {
    enum CommitDirective: Sendable {
        case proceed
        case fail(StorageError)
        case suspend(StorageOperationBarrier)
    }

    private enum PendingCommitDirective: Sendable {
        case fail(StorageError)
        case suspend(StorageOperationBarrier)
    }

    private struct State: Sendable {
        var pendingCommitDirective: PendingCommitDirective?
        var rangeAdvanceBarriers: [StorageOperationBarrier] = []
        var interceptedMutationKeys: [ByteString] = []
        var suspendedRangeAdvanceCount = 0
        var maximumSuspendedRangeAdvanceCount = 0
    }

    private let state = Mutex(State())

    public init() {}

    public func failNextMutatingCommit(with error: StorageError) {
        state.withLock { state in
            precondition(state.pendingCommitDirective == nil)
            state.pendingCommitDirective = .fail(error)
        }
    }

    @discardableResult
    public func suspendNextMutatingCommit() -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { state in
            precondition(state.pendingCommitDirective == nil)
            state.pendingCommitDirective = .suspend(barrier)
        }
        return barrier
    }

    @discardableResult
    public func suspendNextRangeAdvance() -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { state in
            state.rangeAdvanceBarriers.append(barrier)
        }
        return barrier
    }

    public var lastInterceptedMutationKeys: [ByteString] {
        state.withLock { $0.interceptedMutationKeys }
    }

    public var maximumSuspendedRangeAdvanceCount: Int {
        state.withLock { $0.maximumSuspendedRangeAdvanceCount }
    }

    func commitDirective(
        hasMutations: Bool,
        mutationKeys: [ByteString]
    ) -> CommitDirective {
        guard hasMutations else {
            return .proceed
        }
        return state.withLock { state in
            guard let pending = state.pendingCommitDirective else {
                return .proceed
            }
            state.pendingCommitDirective = nil
            state.interceptedMutationKeys = mutationKeys
            switch pending {
            case .fail(let error):
                return .fail(error)
            case .suspend(let barrier):
                return .suspend(barrier)
            }
        }
    }

    func suspendRangeAdvanceIfRequested() async {
        let barrier = state.withLock { state -> StorageOperationBarrier? in
            guard !state.rangeAdvanceBarriers.isEmpty else {
                return nil
            }
            let barrier = state.rangeAdvanceBarriers.removeFirst()
            state.suspendedRangeAdvanceCount += 1
            state.maximumSuspendedRangeAdvanceCount = max(
                state.maximumSuspendedRangeAdvanceCount,
                state.suspendedRangeAdvanceCount
            )
            return barrier
        }
        guard let barrier else {
            return
        }
        await barrier.enterAndWait()
        state.withLock { state in
            precondition(state.suspendedRangeAdvanceCount > 0)
            state.suspendedRangeAdvanceCount -= 1
        }
    }
}
