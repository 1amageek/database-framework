import StorageKit
import Synchronization

/// Fixture time never advances; transaction deadline tasks still cancel and drain.
struct FrozenMonotonicClock: StorageMonotonicClock {
    var now: StorageInstant { StorageInstant(durationSinceReference: .zero) }

    func sleep(until deadline: StorageInstant) async throws(StorageClockError) {
        guard deadline > now else { return }
        await Wait().untilCancelled()
        throw .cancelled
    }

    func sleep(for duration: Duration) async throws(StorageClockError) {
        try await sleep(until: now.advanced(by: duration))
    }

    private final class Wait: Sendable {
        private struct State {
            var cancelled = false
            var continuation: CheckedContinuation<Void, Never>?
        }
        private let state = Mutex(State())

        func untilCancelled() async {
            await withTaskCancellationHandler {
                await withCheckedContinuation { continuation in
                    let cancelled = state.withLock { state in
                        if state.cancelled { return true }
                        state.continuation = continuation
                        return false
                    }
                    if cancelled { continuation.resume() }
                }
            } onCancel: {
                let continuation = self.state.withLock { state in
                    state.cancelled = true
                    let continuation = state.continuation
                    state.continuation = nil
                    return continuation
                }
                continuation?.resume()
            }
        }
    }
}
