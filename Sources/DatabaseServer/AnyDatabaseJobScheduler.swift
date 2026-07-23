import DatabaseValue

/// Type-erased scheduler boundary used by long-lived database runtimes.
public final class AnyDatabaseJobScheduler: DatabaseJobScheduler, Sendable {
    private let ensureWakeUpNoLaterThan:
        @Sendable (DatabaseTimestamp) async throws -> Void

    public init<Scheduler: DatabaseJobScheduler>(_ scheduler: Scheduler) {
        self.ensureWakeUpNoLaterThan = { timestamp in
            try await scheduler.ensureWakeUp(noLaterThan: timestamp)
        }
    }

    public func ensureWakeUp(
        noLaterThan timestamp: DatabaseTimestamp
    ) async throws {
        try await ensureWakeUpNoLaterThan(timestamp)
    }
}
