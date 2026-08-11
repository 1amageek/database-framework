import DatabaseEngine

/// Platform services injected by the host that owns an operation runtime.
public struct DatabaseHostServices: Sendable {
    public let jobScheduler: AnyDatabaseJobScheduler?
    public let identifierGenerator: AnyDatabaseUUIDGenerator?
    public let jobAuthorizationValidator: AnyDatabaseJobAuthorizationValidator?

    public init(
        jobScheduler: AnyDatabaseJobScheduler? = nil,
        identifierGenerator: AnyDatabaseUUIDGenerator? = nil,
        jobAuthorizationValidator: AnyDatabaseJobAuthorizationValidator? = nil
    ) {
        self.jobScheduler = jobScheduler
        self.identifierGenerator = identifierGenerator
        self.jobAuthorizationValidator = jobAuthorizationValidator
    }

    public init<Scheduler: DatabaseJobScheduler>(
        jobScheduler: Scheduler
    ) {
        self.jobScheduler = AnyDatabaseJobScheduler(jobScheduler)
        self.identifierGenerator = nil
        self.jobAuthorizationValidator = nil
    }

    public init<Generator: DatabaseUUIDGenerator>(
        identifierGenerator: Generator,
        jobScheduler: AnyDatabaseJobScheduler? = nil
    ) {
        self.jobScheduler = jobScheduler
        self.identifierGenerator = AnyDatabaseUUIDGenerator(
            identifierGenerator
        )
        self.jobAuthorizationValidator = nil
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
        self.jobAuthorizationValidator = nil
    }

    public static let none = DatabaseHostServices()
}
