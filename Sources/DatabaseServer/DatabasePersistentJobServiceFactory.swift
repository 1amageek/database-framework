public final class DatabasePersistentJobServiceFactory:
    DatabaseJobServiceFactory,
    Sendable {
    private let registry: DatabaseResumableOperationRegistry
    private let scheduler: AnyDatabaseJobScheduler
    private let clock: AnyDatabaseWallClock
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let errorMapper: AnyDatabaseErrorMapper
    private let configuration: DatabaseJobRuntimeConfiguration
    private let storageLimits: DatabasePersistentJobStorageLimits

    public init<
        Scheduler: DatabaseJobScheduler,
        Clock: DatabaseWallClock,
        IdentifierGenerator: DatabaseUUIDGenerator,
        ErrorMapper: DatabaseErrorMapper
    >(
        registry: DatabaseResumableOperationRegistry,
        scheduler: Scheduler,
        clock: Clock,
        identifierGenerator: IdentifierGenerator,
        errorMapper: ErrorMapper,
        configuration: DatabaseJobRuntimeConfiguration = .init(),
        storageLimits: DatabasePersistentJobStorageLimits = .init()
    ) throws {
        try configuration.validate()
        try storageLimits.validate()
        self.registry = registry
        self.scheduler = AnyDatabaseJobScheduler(scheduler)
        self.clock = AnyDatabaseWallClock(clock)
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
        self.configuration = configuration
        self.storageLimits = storageLimits
    }

    public convenience init<
        Scheduler: DatabaseJobScheduler,
        Clock: DatabaseWallClock,
        IdentifierGenerator: DatabaseUUIDGenerator
    >(
        registry: DatabaseResumableOperationRegistry,
        scheduler: Scheduler,
        clock: Clock,
        identifierGenerator: IdentifierGenerator,
        configuration: DatabaseJobRuntimeConfiguration = .init(),
        storageLimits: DatabasePersistentJobStorageLimits = .init()
    ) throws {
        try self.init(
            registry: registry,
            scheduler: scheduler,
            clock: clock,
            identifierGenerator: identifierGenerator,
            errorMapper: CanonicalDatabaseErrorMapper(),
            configuration: configuration,
            storageLimits: storageLimits
        )
    }

    public func makeJobService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseJobService {
        let store = try await DatabasePersistentJobStore(
            container: context.container,
            wireLimits: context.wireLimits,
            storageLimits: storageLimits
        )
        let runner = DatabasePersistentJobRunner(
            container: context.container,
            store: store,
            registry: registry,
            scheduler: scheduler,
            clock: clock,
            identifierGenerator: identifierGenerator,
            errorMapper: errorMapper,
            configuration: configuration,
            wireLimits: context.wireLimits,
            storageLimits: storageLimits,
            runnerID: identifierGenerator.generate()
        )
        let service = DatabasePersistentJobService(
            store: store,
            coordinator: context.coordinator,
            registry: registry,
            runner: runner,
            clock: clock,
            identifierGenerator: identifierGenerator,
            configuration: configuration,
            runtimeLimits: context.runtimeLimits,
            wireLimits: context.wireLimits,
            storageLimits: storageLimits
        )
        try await runner.recoverSchedule()
        return AnyDatabaseJobService(service)
    }
}
