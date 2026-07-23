/// Owns admission and completion for capabilities issued by one logical
/// database transaction.
///
/// Closing a scope rejects work that has not started and waits for admitted
/// work to leave before the physical transaction can commit or be discarded.
package actor DatabaseTransactionScope {
    private var acceptsOperations = true
    private var activeOperationCount = 0
    private var closeWaiters: [CheckedContinuation<Void, Never>] = []

    package init() {}

    package func enter() throws {
        guard acceptsOperations else {
            throw DatabaseTransactionError.closed
        }
        guard activeOperationCount == 0 else {
            throw DatabaseTransactionError.concurrentOperation
        }
        activeOperationCount += 1
    }

    package func leave() {
        precondition(activeOperationCount > 0)
        activeOperationCount -= 1
        resumeCloseWaitersIfDrained()
    }

    package func closeAndWait() async {
        acceptsOperations = false
        if activeOperationCount > 0 {
            await withCheckedContinuation { continuation in
                closeWaiters.append(continuation)
            }
        }
    }

    private func resumeCloseWaitersIfDrained() {
        guard !acceptsOperations, activeOperationCount == 0 else {
            return
        }
        let waiters = closeWaiters
        closeWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
    }
}
