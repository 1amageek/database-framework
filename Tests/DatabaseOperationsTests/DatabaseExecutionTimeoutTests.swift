import DatabaseEngine
import DatabaseWire
import Synchronization
import StorageKit
import Testing
@testable import DatabaseOperations
@testable import DatabaseOperationCore

@Suite("Database execution timeout admission")
struct DatabaseExecutionTimeoutTests {
    @Test("An expired deadline never starts the operation")
    func expiredDeadlineRejectsBeforeOperationStart() async throws {
        let starts = TimeoutOperationCounter()
        let clock = FixedTimeoutClock(
            now: StorageInstant(durationSinceReference: .milliseconds(1))
        )
        let deadline = StorageInstant(durationSinceReference: .zero)

        do {
            let _: Void = try await DatabaseExecutionTimeout.run(
                until: deadline,
                timeoutMilliseconds: .max,
                clock: clock
            ) {
                starts.increment()
            }
            Issue.record("Expected expired deadline rejection")
        } catch DatabaseOperationLimitError.executionTimedOut(
            let timeoutMilliseconds
        ) {
            #expect(timeoutMilliseconds == .max)
        }

        #expect(starts.value == 0)
    }

    @Test("A running metered loop exits when the timeout cancels it")
    func runningMeteredLoopStopsAtTimeout() async throws {
        let gate = TimeoutStartGate()
        let clock = CoordinatedTimeoutClock(gate: gate)
        let iterations = TimeoutOperationCounter()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: .max,
                timeoutMilliseconds: .max
            ),
            monotonicClock: clock
        )

        do {
            let _: Void = try await DatabaseExecutionTimeout.run(
                until: StorageInstant(durationSinceReference: .milliseconds(1)),
                timeoutMilliseconds: 1,
                clock: clock
            ) {
                try meter.checkpoint(at: .bindingCandidate)
                iterations.increment()
                await gate.markStarted()
                while true {
                    try meter.checkpoint(at: .bindingCandidate)
                    iterations.increment()
                }
            }
            Issue.record("Expected the running operation to time out")
        } catch DatabaseOperationLimitError.executionTimedOut(
            let timeoutMilliseconds
        ) {
            #expect(timeoutMilliseconds == 1)
        }

        #expect(iterations.value > 0)
    }
}

private struct FixedTimeoutClock: StorageMonotonicClock {
    let now: StorageInstant

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {}
}

private struct CoordinatedTimeoutClock: StorageMonotonicClock {
    let gate: TimeoutStartGate

    var now: StorageInstant {
        StorageInstant(durationSinceReference: .zero)
    }

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        await gate.waitUntilStarted()
    }
}

private actor TimeoutStartGate {
    private var isStarted = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func markStarted() {
        isStarted = true
        let currentWaiters = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in currentWaiters {
            waiter.resume()
        }
    }

    func waitUntilStarted() async {
        guard !isStarted else { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private final class TimeoutOperationCounter: Sendable {
    private let count = Mutex(0)

    var value: Int { count.withLock { $0 } }

    func increment() {
        count.withLock { $0 += 1 }
    }
}
