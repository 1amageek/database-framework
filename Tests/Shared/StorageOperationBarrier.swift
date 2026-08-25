import Synchronization

/// A deterministic suspension boundary for storage behavior tests.
public final class StorageOperationBarrier: Sendable {
    private struct State: Sendable {
        var entered = false
        var released = false
        var entryWaiters: [CheckedContinuation<Void, Never>] = []
        var releaseWaiters: [CheckedContinuation<Void, Never>] = []
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

    public func signalEntry() {
        let waiters = state.withLock { state in
            state.entered = true
            let waiters = state.entryWaiters
            state.entryWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
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

    func enterAndWait() async {
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
