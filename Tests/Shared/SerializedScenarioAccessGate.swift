/// FIFO exclusivity gate for integration scenarios sharing one external
/// consistency domain.
package actor SerializedScenarioAccessGate {
    private final class WaitingRequestIdentity: Sendable {}

    private struct WaitingRequest {
        let identity: WaitingRequestIdentity
        let continuation: CheckedContinuation<Void, any Error>
    }

    private var accessIsHeld = false
    private var waitingRequests: [WaitingRequest] = []

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

        let identity = WaitingRequestIdentity()
        try await withTaskCancellationHandler(
            operation: {
                try await waitForAccess(identity)
            },
            onCancel: {
                Task {
                    await self.cancelWaitingRequest(identity)
                }
            }
        )
        do {
            try Task.checkCancellation()
        } catch {
            releaseAccess()
            throw error
        }
    }

    private func waitForAccess(
        _ identity: WaitingRequestIdentity
    ) async throws {
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, any Error>) in
            guard !Task.isCancelled else {
                continuation.resume(throwing: CancellationError())
                return
            }
            waitingRequests.append(
                WaitingRequest(
                    identity: identity,
                    continuation: continuation
                )
            )
        }
    }

    private func cancelWaitingRequest(_ identity: WaitingRequestIdentity) {
        guard let index = waitingRequests.firstIndex(where: {
            $0.identity === identity
        }) else {
            return
        }
        let request = waitingRequests.remove(at: index)
        request.continuation.resume(throwing: CancellationError())
    }

    private func releaseAccess() {
        guard !waitingRequests.isEmpty else {
            accessIsHeld = false
            return
        }

        let nextRequest = waitingRequests.removeFirst()
        nextRequest.continuation.resume()
    }
}
