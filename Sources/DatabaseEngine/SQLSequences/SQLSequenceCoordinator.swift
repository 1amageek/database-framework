import StorageKit

/// Serializes SQL requests through allocation replay and authoritative close.
package actor SQLSequenceCoordinator {
    private struct Waiter {
        let id: UInt64
        let continuation: CheckedContinuation<Void, any Error>
    }
    private var occupied = false
    private var nextID: UInt64 = 0
    private var waiters: [Waiter] = []

    func enter(clock: any StorageMonotonicClock, deadline: TransactionExecutionDeadline) async throws {
        if Task.isCancelled { throw CancellationError() }
        guard clock.now < deadline.instant else { throw timeout(deadline) }
        if !occupied { occupied = true; return }
        let id = nextID
        let (next, overflow) = nextID.addingReportingOverflow(1)
        guard !overflow else { throw SQLSequenceExecutionError.replayDiverged }
        nextID = next
        let timer = Task {
            do {
                try await clock.sleep(until: deadline.instant)
                expire(id, error: timeout(deadline))
            } catch {
                if !Task.isCancelled { expire(id, error: error) }
            }
        }
        defer { timer.cancel() }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiters.append(Waiter(id: id, continuation: continuation))
            }
        } onCancel: {
            Task { await self.expire(id, error: CancellationError()) }
        }
        // If delivery raced cancellation/deadline, return the granted permit before failing.
        do {
            if Task.isCancelled { throw CancellationError() }
            guard clock.now < deadline.instant else { throw timeout(deadline) }
        } catch {
            leave()
            throw error
        }
    }

    func leave() {
        if waiters.isEmpty { occupied = false }
        else { waiters.removeFirst().continuation.resume() }
    }

    private func expire(_ id: UInt64, error: any Error) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        waiters.remove(at: index).continuation.resume(throwing: error)
    }

    private func timeout(_ deadline: TransactionExecutionDeadline) -> TransactionExecutionDeadlineExceeded {
        TransactionExecutionDeadlineExceeded(timeoutMilliseconds: UInt64(deadline.timeoutMilliseconds),
            source: .inheritedOperation)
    }
}
