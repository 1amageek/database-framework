import DatabaseValue

/// Type-erased scheduler boundary used by long-lived database runtimes.
public final class AnyDatabaseJobScheduler: DatabaseJobScheduler, Sendable {
    private let scheduleJob: @Sendable (DatabaseTimestamp) async throws -> Void

    public init<Scheduler: DatabaseJobScheduler>(_ scheduler: Scheduler) {
        self.scheduleJob = { timestamp in
            try await scheduler.schedule(at: timestamp)
        }
    }

    public func schedule(at timestamp: DatabaseTimestamp) async throws {
        try await scheduleJob(timestamp)
    }
}
