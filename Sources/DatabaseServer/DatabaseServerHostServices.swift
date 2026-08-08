/// Platform services injected by the host that owns a server runtime.
public struct DatabaseServerHostServices: Sendable {
    public let jobScheduler: AnyDatabaseJobScheduler?

    public init(jobScheduler: AnyDatabaseJobScheduler? = nil) {
        self.jobScheduler = jobScheduler
    }

    public init<Scheduler: DatabaseJobScheduler>(
        jobScheduler: Scheduler
    ) {
        self.jobScheduler = AnyDatabaseJobScheduler(jobScheduler)
    }

    public static let none = DatabaseServerHostServices()
}
