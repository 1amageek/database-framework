import Synchronization

/// Atomically publishes immutable schema generations without invalidating old
/// request leases.
final class DatabaseSchemaGenerationStore: Sendable {
    private final class DrainWaiterIdentity: Sendable {}

    private struct DrainWaiter: Sendable {
        let identity: DrainWaiterIdentity
        let generation: UInt64
        let continuation: CheckedContinuation<Void, any Error>
    }

    private enum DrainRegistration {
        case waiting
        case completed
        case cancelled
    }

    private struct State: Sendable {
        var current: DatabaseSchemaGeneration
        var activeLeaseCounts: [UInt64: Int]
        var drainWaiters: [ObjectIdentifier: DrainWaiter]
    }

    private let state: Mutex<State>

    init(initial: DatabaseSchemaGeneration) {
        self.state = Mutex(
            State(
                current: initial,
                activeLeaseCounts: [:],
                drainWaiters: [:]
            )
        )
    }

    func acquire() -> DatabaseSchemaLease {
        let generation = state.withLock { state in
            let identifier = state.current.identifier
            let count = state.activeLeaseCounts[identifier, default: 0]
            let (incremented, overflow) = count.addingReportingOverflow(1)
            precondition(
                !overflow,
                "A schema generation lease count must not overflow"
            )
            state.activeLeaseCounts[identifier] = incremented
            return state.current
        }
        return DatabaseSchemaLease(
            generation,
            token: DatabaseSchemaLeaseToken { [self] in
                release(generation.identifier)
            }
        )
    }

    func publish(_ generation: DatabaseSchemaGeneration) {
        state.withLock { state in
            precondition(
                generation.identifier >= state.current.identifier,
                "Schema generations must not move backwards"
            )
            if generation.identifier == state.current.identifier {
                precondition(
                    generation.fingerprint == state.current.fingerprint
                        && generation.indexPhysicalFingerprint
                            == state.current.indexPhysicalFingerprint
                        && generation.executionRuntimeFingerprint
                            == state.current.executionRuntimeFingerprint,
                    "One schema generation identifier must have one identity"
                )
            }
            state.current = generation
        }
    }

    /// Waits until every lease older than `generation` has been released.
    /// New acquisitions can only bind the currently published generation, so
    /// no older lease can enter after the caller observes the publication.
    func waitUntilDrained(olderThan generation: UInt64) async throws {
        let identity = DrainWaiterIdentity()
        let identifier = ObjectIdentifier(identity)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let registration = state.withLock { state in
                    precondition(
                        state.current.identifier >= generation,
                        "A schema generation must be published before it can drain"
                    )
                    guard !Task.isCancelled else {
                        return DrainRegistration.cancelled
                    }
                    guard
                        Self.hasActiveLease(
                            olderThan: generation,
                            state: state
                        )
                    else {
                        return DrainRegistration.completed
                    }
                    state.drainWaiters[identifier] = DrainWaiter(
                        identity: identity,
                        generation: generation,
                        continuation: continuation
                    )
                    return DrainRegistration.waiting
                }
                switch registration {
                case .waiting:
                    break
                case .completed:
                    continuation.resume()
                case .cancelled:
                    continuation.resume(throwing: CancellationError())
                }
            }
        } onCancel: { [self] in
            let continuation = state.withLock { state in
                state.drainWaiters.removeValue(
                    forKey: identifier
                )?.continuation
            }
            continuation?.resume(throwing: CancellationError())
        }
        try ensureDatabaseTaskIsActive()
    }

    private func release(_ generation: UInt64) {
        let waiters = state.withLock { state in
            guard let count = state.activeLeaseCounts[generation], count > 0
            else {
                preconditionFailure(
                    "A schema generation lease must finish exactly once"
                )
            }
            if count == 1 {
                state.activeLeaseCounts.removeValue(forKey: generation)
            } else {
                state.activeLeaseCounts[generation] = count - 1
            }

            let waiters = state.drainWaiters
            var pending: [ObjectIdentifier: DrainWaiter] = [:]
            var ready: [CheckedContinuation<Void, any Error>] = []
            pending.reserveCapacity(waiters.count)
            ready.reserveCapacity(waiters.count)
            for (identifier, waiter) in waiters {
                if Self.hasActiveLease(
                    olderThan: waiter.generation,
                    state: state
                ) {
                    pending[identifier] = waiter
                } else {
                    ready.append(waiter.continuation)
                }
            }
            state.drainWaiters = pending
            return ready
        }
        for waiter in waiters {
            waiter.resume(returning: ())
        }
    }

    private static func hasActiveLease(
        olderThan generation: UInt64,
        state: State
    ) -> Bool {
        state.activeLeaseCounts.contains { entry in
            entry.key < generation && entry.value > 0
        }
    }
}
