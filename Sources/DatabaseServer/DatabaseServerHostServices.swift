/// Platform services injected by the host that owns a server runtime.
public struct DatabaseServerHostServices: Sendable {
    public let jobScheduler: AnyDatabaseJobScheduler?
    public let identifierGenerator: AnyDatabaseUUIDGenerator?

    public init(
        jobScheduler: AnyDatabaseJobScheduler? = nil,
        identifierGenerator: AnyDatabaseUUIDGenerator? = nil
    ) {
        self.jobScheduler = jobScheduler
        self.identifierGenerator = identifierGenerator
    }

    public init<Scheduler: DatabaseJobScheduler>(
        jobScheduler: Scheduler
    ) {
        self.jobScheduler = AnyDatabaseJobScheduler(jobScheduler)
        self.identifierGenerator = nil
    }

    public init<Generator: DatabaseUUIDGenerator>(
        identifierGenerator: Generator,
        jobScheduler: AnyDatabaseJobScheduler? = nil
    ) {
        self.jobScheduler = jobScheduler
        self.identifierGenerator = AnyDatabaseUUIDGenerator(
            identifierGenerator
        )
    }

    public init<
        Scheduler: DatabaseJobScheduler,
        Generator: DatabaseUUIDGenerator
    >(
        jobScheduler: Scheduler,
        identifierGenerator: Generator
    ) {
        self.jobScheduler = AnyDatabaseJobScheduler(jobScheduler)
        self.identifierGenerator = AnyDatabaseUUIDGenerator(
            identifierGenerator
        )
    }

    public static let none = DatabaseServerHostServices()
}
