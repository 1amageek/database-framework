/// Serializes terminal cleanup so every invalidation caller observes the same
/// authoritative completion and no caller can outpace cursor shutdown.
actor FusionIndexReadSessionInvalidation {
    private enum State {
        case idle
        case running(Task<Void, any Error>)
        case finished((any Error)?)
    }

    private var state: State = .idle

    func run(
        _ operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        let task: Task<Void, any Error>
        switch state {
        case .idle:
            task = Task {
                try await operation()
            }
            state = .running(task)
        case .running(let existing):
            task = existing
        case .finished(.some(let error)):
            throw error
        case .finished(nil):
            return
        }

        do {
            try await task.value
            state = .finished(nil)
        } catch {
            state = .finished(error)
            throw error
        }
    }
}
