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

    private struct PendingValueReadBarrier: Sendable {
        let key: ByteString?
        let barrier: StorageOperationBarrier
    }

    private struct State: Sendable {
        var pendingCommitDirective: PendingCommitDirective?
        var valueReadBarriers: [PendingValueReadBarrier] = []
        var boundedValueReadBarriers: [PendingValueReadBarrier] = []
        var rangeAdvanceBarriers: [StorageOperationBarrier] = []
        var interceptedMutationKeys: [ByteString] = []
        var suspendedValueReadCount = 0
        var maximumSuspendedValueReadCount = 0
        var suspendedRangeAdvanceCount = 0
        var maximumSuspendedRangeAdvanceCount = 0
        var valueReadCount = 0
        var boundedValueReadMaximums: [Int] = []
        var keyReadCount = 0
        var openedRangeCursorCount = 0
        var namespaceReadCount = 0
        var readVersionCount = 0
        var rangeMetadataReadCount = 0
        var finishedRangeCursorCount = 0
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

    /// Suspends one completed backend value read before the value crosses the
    /// controlled transaction boundary. This lets cancellation tests prove
    /// the caller's post-suspension checkpoint and retained-owner cleanup.
    @discardableResult
    public func suspendNextValueRead() -> StorageOperationBarrier {
        suspendNextValueRead(for: nil)
    }

    /// Suspends the next completed read of one exact storage key. Reads of
    /// framework metadata do not consume this barrier.
    @discardableResult
    public func suspendNextValueRead(
        for key: ByteString
    ) -> StorageOperationBarrier {
        suspendNextValueRead(for: Optional(key))
    }

    private func suspendNextValueRead(
        for key: ByteString?
    ) -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { state in
            state.valueReadBarriers.append(
                PendingValueReadBarrier(key: key, barrier: barrier)
            )
        }
        return barrier
    }

    /// Suspends a bounded read before it reaches the backend. This boundary
    /// lets concurrent-admission tests observe the exact maximum dispatched
    /// to each backend call, including a zero-byte allowance.
    @discardableResult
    public func suspendNextBoundedValueRead(
        for key: ByteString
    ) -> StorageOperationBarrier {
        let barrier = StorageOperationBarrier()
        state.withLock { state in
            state.boundedValueReadBarriers.append(
                PendingValueReadBarrier(key: key, barrier: barrier)
            )
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

    public var maximumSuspendedValueReadCount: Int {
        state.withLock { $0.maximumSuspendedValueReadCount }
    }

    public var dataReadOperationCount: Int {
        state.withLock {
            $0.valueReadCount
                + $0.keyReadCount
                + $0.openedRangeCursorCount
                + $0.namespaceReadCount
                + $0.readVersionCount
                + $0.rangeMetadataReadCount
        }
    }

    public var boundedValueReadMaximums: [Int] {
        state.withLock { $0.boundedValueReadMaximums }
    }

    public var openedRangeCursorCount: Int {
        state.withLock { $0.openedRangeCursorCount }
    }

    public var finishedRangeCursorCount: Int {
        state.withLock { $0.finishedRangeCursorCount }
    }

    func recordRangeCursorOpened() {
        state.withLock { $0.openedRangeCursorCount += 1 }
    }

    func recordValueRead() {
        state.withLock { $0.valueReadCount += 1 }
    }

    func recordBoundedValueRead(maximumByteCount: Int) {
        state.withLock { state in
            state.valueReadCount += 1
            state.boundedValueReadMaximums.append(maximumByteCount)
        }
    }

    func recordKeyRead() {
        state.withLock { $0.keyReadCount += 1 }
    }

    func recordNamespaceRead() {
        state.withLock { $0.namespaceReadCount += 1 }
    }

    func recordReadVersion() {
        state.withLock { $0.readVersionCount += 1 }
    }

    func recordRangeMetadataRead() {
        state.withLock { $0.rangeMetadataReadCount += 1 }
    }

    func recordRangeCursorFinished() {
        state.withLock { $0.finishedRangeCursorCount += 1 }
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

    func suspendValueReadIfRequested(for key: ByteString) async {
        let barrier = state.withLock { state -> StorageOperationBarrier? in
            guard let index = state.valueReadBarriers.firstIndex(
                where: { $0.key == nil || $0.key == key }
            ) else {
                return nil
            }
            let barrier = state.valueReadBarriers.remove(at: index).barrier
            state.suspendedValueReadCount += 1
            state.maximumSuspendedValueReadCount = max(
                state.maximumSuspendedValueReadCount,
                state.suspendedValueReadCount
            )
            return barrier
        }
        guard let barrier else {
            return
        }
        await barrier.enterAndWait()
        state.withLock { state in
            precondition(state.suspendedValueReadCount > 0)
            state.suspendedValueReadCount -= 1
        }
    }

    func suspendBoundedValueReadIfRequested(for key: ByteString) async {
        let barrier = state.withLock { state -> StorageOperationBarrier? in
            guard let index = state.boundedValueReadBarriers.firstIndex(
                where: { $0.key == key }
            ) else {
                return nil
            }
            let barrier = state.boundedValueReadBarriers.remove(at: index).barrier
            state.suspendedValueReadCount += 1
            state.maximumSuspendedValueReadCount = max(
                state.maximumSuspendedValueReadCount,
                state.suspendedValueReadCount
            )
            return barrier
        }
        guard let barrier else {
            return
        }
        await barrier.enterAndWait()
        state.withLock { state in
            precondition(state.suspendedValueReadCount > 0)
            state.suspendedValueReadCount -= 1
        }
    }
}
