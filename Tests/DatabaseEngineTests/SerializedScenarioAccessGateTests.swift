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

    @Test("Cancellation removes a waiter before the current owner releases", .timeLimit(.minutes(1)))
    func cancellationRemovesWaitingRequestImmediately() async throws {
        let gate = SerializedScenarioAccessGate()
        let firstOperation = ScenarioOperationBlocker()
        let cancellationFinished = ScenarioCompletionSignal()

        let first = Task {
            try await gate.withAccess {
                await firstOperation.waitForRelease()
            }
        }
        await firstOperation.waitUntilEntered()

        let cancelled = Task {
            do {
                try await gate.withAccess {}
                await cancellationFinished.signal()
            } catch {
                await cancellationFinished.signal()
                throw error
            }
        }
        let succeeding = Task {
            try await gate.withAccess { true }
        }

        while await gate.waitingRequestCount < 2 {
            await Task.yield()
        }
        cancelled.cancel()

        // Cancellation owns removal from the wait queue. Completion must not
        // depend on the current owner releasing the gate.
        await cancellationFinished.wait()
        #expect(await gate.waitingRequestCount == 1)

        await #expect(throws: CancellationError.self) {
            try await cancelled.value
        }

        await firstOperation.release()
        try await first.value
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

private actor ScenarioCompletionSignal {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isSignalled = false

    func wait() async {
        guard !isSignalled else { return }
        await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
    }

    func signal() {
        isSignalled = true
        continuation?.resume()
        continuation = nil
    }
}
#endif
