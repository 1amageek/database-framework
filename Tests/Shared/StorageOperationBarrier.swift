import Synchronization

public enum StorageOperationBarrierError: Error, Equatable, Sendable {
    case operationCompletedBeforeEntry
}

/// A deterministic suspension boundary for storage behavior tests.
public final class StorageOperationBarrier: Sendable {
    private enum EntryAdmission {
        case entered
        case cancelled
        case waiting
    }

    private struct CancellableEntryWaiter: Sendable {
        let identifier: UInt64
        let continuation: CheckedContinuation<Void, any Error>
    }

    private struct State: Sendable {
        var entered = false
        var released = false
        var entryWaiters: [CheckedContinuation<Void, Never>] = []
        var cancellableEntryWaiters: [CancellableEntryWaiter] = []
        var cancelledEntryWaiterIdentifiers: Set<UInt64> = []
        var releaseWaiters: [CheckedContinuation<Void, Never>] = []
        var nextWaiterIdentifier: UInt64 = 0
    }

    private let state = Mutex(State())

    public init() {}

    public func waitUntilEntered() async {
        await withCheckedContinuation { continuation in
            let shouldResume = state.withLock { state in
                guard !state.entered else {
                    return true
                }
                state.entryWaiters.append(continuation)
                return false
            }
            if shouldResume {
                continuation.resume()
            }
        }
    }

    /// Waits for entry while allowing a structured race to cancel and join
    /// this waiter when the operation under test completes first.
    public func waitUntilEnteredOrCancellation() async throws {
        let identifier = state.withLock { state -> UInt64 in
            let identifier = state.nextWaiterIdentifier
            let (nextIdentifier, overflow) = identifier
                .addingReportingOverflow(1)
            precondition(!overflow, "Barrier waiter identifier overflow")
            state.nextWaiterIdentifier = nextIdentifier
            return identifier
        }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, any Error>) in
                let admission = state.withLock { state -> EntryAdmission in
                    if state.entered {
                        return .entered
                    }
                    if state.cancelledEntryWaiterIdentifiers.remove(
                        identifier
                    ) != nil {
                        return .cancelled
                    }
                    state.cancellableEntryWaiters.append(
                        CancellableEntryWaiter(
                            identifier: identifier,
                            continuation: continuation
                        )
                    )
                    return .waiting
                }
                switch admission {
                case .entered:
                    continuation.resume()
                case .cancelled:
                    continuation.resume(throwing: CancellationError())
                case .waiting:
                    break
                }
            }
        } onCancel: {
            let continuation = state.withLock {
                state -> CheckedContinuation<Void, any Error>? in
                if let index = state.cancellableEntryWaiters.firstIndex(
                    where: { $0.identifier == identifier }
                ) {
                    return state.cancellableEntryWaiters.remove(at: index)
                        .continuation
                }
                if !state.entered {
                    state.cancelledEntryWaiterIdentifiers.insert(identifier)
                }
                return Optional<CheckedContinuation<Void, any Error>>.none
            }
            continuation?.resume(throwing: CancellationError())
        }
    }

    /// Requires the controlled operation to enter this barrier before it
    /// completes. The returned monitor must be joined after the operation is
    /// released so this diagnostic task cannot outlive the test.
    public func waitUntilEntered<Success: Sendable>(
        beforeCompletionOf operation: Task<Success, any Error>
    ) async throws -> Task<Void, Never> {
        let entryWaiter = Task {
            try await waitUntilEnteredOrCancellation()
        }
        let completionMonitor = Task {
            _ = await operation.result
            entryWaiter.cancel()
        }
        do {
            try await entryWaiter.value
        } catch is CancellationError {
            switch await operation.result {
            case .success:
                throw StorageOperationBarrierError
                    .operationCompletedBeforeEntry
            case .failure(let error):
                throw error
            }
        }
        return completionMonitor
    }

    public func signalEntry() {
        let waiters = state.withLock { state in
            state.entered = true
            let waiters = state.entryWaiters
            state.entryWaiters.removeAll(keepingCapacity: false)
            let cancellableWaiters = state.cancellableEntryWaiters
            state.cancellableEntryWaiters.removeAll(keepingCapacity: false)
            state.cancelledEntryWaiterIdentifiers.removeAll(
                keepingCapacity: false
            )
            return (waiters, cancellableWaiters)
        }
        for waiter in waiters.0 {
            waiter.resume()
        }
        for waiter in waiters.1 {
            waiter.continuation.resume()
        }
    }

    public func release() {
        let waiters = state.withLock { state in
            guard !state.released else {
                return [CheckedContinuation<Void, Never>]()
            }
            state.released = true
            let waiters = state.releaseWaiters
            state.releaseWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    public func enterAndWait() async {
        signalEntry()

        await withCheckedContinuation { continuation in
            let shouldResume = state.withLock { state in
                guard !state.released else {
                    return true
                }
                state.releaseWaiters.append(continuation)
                return false
            }
            if shouldResume {
                continuation.resume()
            }
        }
    }
}
