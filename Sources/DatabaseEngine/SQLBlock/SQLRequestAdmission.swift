import StorageKit

/// Fair container admission: readers share a cohort; effects own one exclusive permit.
package actor SQLRequestAdmission {
    package enum Mode: Sendable { case shared, exclusive }
    package struct Permit: Sendable {
        fileprivate let id: UInt64
        package let mode: Mode
    }
    private struct Waiter {
        let permit: Permit
        let claim: DatabaseIntermediateReservation
        let continuation: CheckedContinuation<Permit, any Error>
    }
    private var nextID: UInt64 = 0
    private var active: [UInt64: DatabaseIntermediateReservation] = [:]
    private var exclusive = false
    private var waiters: [Waiter] = []

    func enter(_ mode: Mode, meter: DatabaseWorkMeter, clock: any StorageMonotonicClock,
               deadline: TransactionExecutionDeadline) async throws -> Permit {
        if Task.isCancelled { throw CancellationError() }
        guard clock.now < deadline.instant else { throw timeout(deadline) }
        let (next, overflow) = nextID.addingReportingOverflow(1)
        guard !overflow else { throw DatabaseTransactionError.invalidOperationContext }
        nextID = next
        let permit = Permit(id: next, mode: mode)
        let claim = try meter.reserveIntermediate(bytes: 512, at: .mutationPlanning)
        if waiters.isEmpty && !exclusive && (mode == .shared || active.isEmpty) {
            grant(permit, claim: claim)
            return permit
        }
        let timer = Task {
            do {
                try await clock.sleep(until: deadline.instant)
                expire(permit.id, error: timeout(deadline))
            } catch {
                if !Task.isCancelled { expire(permit.id, error: error) }
            }
        }
        defer { timer.cancel(); await timer.value }
        let acquired = try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiters.append(Waiter(permit: permit, claim: claim, continuation: continuation))
            }
        } onCancel: { Task { await self.expire(permit.id, error: CancellationError()) } }
        do {
            if Task.isCancelled { throw CancellationError() }
            guard clock.now < deadline.instant else { throw timeout(deadline) }
        } catch {
            leave(acquired)
            throw error
        }
        return acquired
    }

    func leave(_ permit: Permit) {
        guard let claim = active.removeValue(forKey: permit.id) else {
            preconditionFailure("An SQL admission permit must be returned exactly once")
        }
        claim.release()
        if permit.mode == .exclusive { exclusive = false }
        drain()
    }

    private func grant(_ permit: Permit, claim: DatabaseIntermediateReservation) {
        active[permit.id] = claim
        if permit.mode == .exclusive { exclusive = true }
    }

    private func drain() {
        guard !exclusive else { return }
        while let first = waiters.first {
            if first.permit.mode == .exclusive && !active.isEmpty { return }
            waiters.removeFirst()
            grant(first.permit, claim: first.claim)
            first.continuation.resume(returning: first.permit)
            if exclusive { return }
        }
    }

    private func expire(_ id: UInt64, error: any Error) {
        guard let index = waiters.firstIndex(where: { $0.permit.id == id }) else { return }
        let waiter = waiters.remove(at: index)
        waiter.claim.release()
        waiter.continuation.resume(throwing: error)
        drain()
    }

    private func timeout(_ deadline: TransactionExecutionDeadline) -> TransactionExecutionDeadlineExceeded {
        TransactionExecutionDeadlineExceeded(timeoutMilliseconds: UInt64(deadline.timeoutMilliseconds),
            source: .inheritedOperation)
    }
}
