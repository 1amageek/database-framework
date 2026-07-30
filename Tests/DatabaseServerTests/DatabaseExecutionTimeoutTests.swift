import Synchronization
import StorageKit
import Testing
@testable import DatabaseServer

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
        } catch DatabaseRuntimeLimitError.executionTimedOut(
            let timeoutMilliseconds
        ) {
            #expect(timeoutMilliseconds == .max)
        }

        #expect(starts.value == 0)
    }
}

private struct FixedTimeoutClock: StorageMonotonicClock {
    let now: StorageInstant

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {}
}

private final class TimeoutOperationCounter: Sendable {
    private let count = Mutex(0)

    var value: Int { count.withLock { $0 } }

    func increment() {
        count.withLock { $0 += 1 }
    }
}
