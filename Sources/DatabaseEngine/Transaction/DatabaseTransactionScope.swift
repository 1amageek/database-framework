import Synchronization

/// Owns admission and completion for capabilities issued by one logical
/// database transaction.
///
/// Closing a scope rejects work that has not started and waits for admitted
/// work to leave before the physical transaction can commit or be discarded.
package final class DatabaseTransactionScope: Sendable {
    private struct State: Sendable {
        var acceptsOperations = true
        var activeOperationCount = 0
        var closeWaiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    package init() {}

    package func enter() throws {
        try state.withLock { state in
            guard state.acceptsOperations else {
                throw DatabaseTransactionError.closed
            }
            guard state.activeOperationCount == 0 else {
                throw DatabaseTransactionError.concurrentOperation
            }
            state.activeOperationCount += 1
        }
    }

    package func leave() {
        let closeWaiters: [CheckedContinuation<Void, Never>] = state.withLock { state in
            precondition(state.activeOperationCount > 0)
            state.activeOperationCount -= 1
            guard !state.acceptsOperations,
                  state.activeOperationCount == 0 else {
                return []
            }
            let closeWaiters = state.closeWaiters
            state.closeWaiters.removeAll(keepingCapacity: false)
            return closeWaiters
        }
        for closeWaiter in closeWaiters {
            closeWaiter.resume()
        }
    }

    package func closeAndWait() async {
        await withCheckedContinuation { continuation in
            let resumesImmediately = state.withLock { state in
                state.acceptsOperations = false
                guard state.activeOperationCount > 0 else {
                    return true
                }
                state.closeWaiters.append(continuation)
                return false
            }
            if resumesImmediately {
                continuation.resume()
            }
        }
    }
}
