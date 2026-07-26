#if !os(WASI)
import TestSupport
import Testing

@Suite("Serialized Scenario Access Gate")
struct SerializedScenarioAccessGateTests {
    @Test("Concurrent operations never overlap", .timeLimit(.minutes(1)))
    func concurrentOperationsNeverOverlap() async throws {
        let gate = SerializedScenarioAccessGate()
        let probe = ScenarioConcurrencyProbe()

        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<128 {
                group.addTask {
                    try await gate.withAccess {
                        await probe.enter()
                        await Task.yield()
                        await probe.leave()
                    }
                }
            }
            try await group.waitForAll()
        }

        let maximumConcurrentOperations = await probe.maximumConcurrentOperations
        #expect(maximumConcurrentOperations == 1)
    }

    @Test("Cancellation transfers ownership to the next waiter", .timeLimit(.minutes(1)))
    func cancellationTransfersOwnership() async throws {
        let gate = SerializedScenarioAccessGate()
        let firstOperation = ScenarioOperationBlocker()

        let first = Task {
            try await gate.withAccess {
                await firstOperation.waitForRelease()
            }
        }
        await firstOperation.waitUntilEntered()

        let cancelled = Task {
            try await gate.withAccess {}
        }
        let succeeding = Task {
            try await gate.withAccess { true }
        }

        while await gate.waitingRequestCount < 2 {
            await Task.yield()
        }
        cancelled.cancel()
        await firstOperation.release()

        try await first.value
        await #expect(throws: CancellationError.self) {
            try await cancelled.value
        }
        #expect(try await succeeding.value)
    }
}

private actor ScenarioConcurrencyProbe {
    private var concurrentOperations = 0
    private(set) var maximumConcurrentOperations = 0

    func enter() {
        concurrentOperations += 1
        maximumConcurrentOperations = max(
            maximumConcurrentOperations,
            concurrentOperations
        )
    }

    func leave() {
        concurrentOperations -= 1
    }
}

private actor ScenarioOperationBlocker {
    private var enteredContinuation: CheckedContinuation<Void, Never>?
    private var releaseContinuation: CheckedContinuation<Void, Never>?
    private var hasEntered = false
    private var isReleased = false

    func waitUntilEntered() async {
        guard !hasEntered else { return }
        await withCheckedContinuation { continuation in
            enteredContinuation = continuation
        }
    }

    func waitForRelease() async {
        hasEntered = true
        enteredContinuation?.resume()
        enteredContinuation = nil

        guard !isReleased else { return }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func release() {
        isReleased = true
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}
#endif
