/// FIFO exclusivity gate for integration scenarios sharing one external
/// consistency domain.
package actor SerializedScenarioAccessGate {
    private var accessIsHeld = false
    private var waitingRequests: [CheckedContinuation<Void, Never>] = []

    package init() {}

    package var waitingRequestCount: Int {
        waitingRequests.count
    }

    package func withAccess<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await acquireAccess()
        defer { releaseAccess() }
        return try await operation()
    }

    private func acquireAccess() async throws {
        try Task.checkCancellation()

        guard accessIsHeld else {
            accessIsHeld = true
            return
        }

        await withCheckedContinuation { continuation in
            waitingRequests.append(continuation)
        }

        if Task.isCancelled {
            releaseAccess()
            throw CancellationError()
        }
    }

    private func releaseAccess() {
        guard !waitingRequests.isEmpty else {
            accessIsHeld = false
            return
        }

        let nextRequest = waitingRequests.removeFirst()
        nextRequest.resume()
    }
}
