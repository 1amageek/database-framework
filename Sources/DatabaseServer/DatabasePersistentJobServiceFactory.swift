public final class DatabasePersistentJobServiceFactory:
    DatabaseJobServiceFactory,
    Sendable {
    private let registry: DatabaseResumableOperationRegistry
    private let scheduler: AnyDatabaseJobScheduler
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let errorMapper: AnyDatabaseErrorMapper
    private let configuration: DatabaseJobRuntimeConfiguration
    private let storageLimits: DatabasePersistentJobStorageLimits

    public init<
        Scheduler: DatabaseJobScheduler,
        IdentifierGenerator: DatabaseUUIDGenerator,
        ErrorMapper: DatabaseErrorMapper
    >(
        registry: DatabaseResumableOperationRegistry,
        scheduler: Scheduler,
        identifierGenerator: IdentifierGenerator,
        errorMapper: ErrorMapper,
        storageLimits: DatabasePersistentJobStorageLimits,
        configuration: DatabaseJobRuntimeConfiguration = .init()
    ) throws {
        try configuration.validate()
        try storageLimits.validate()
        self.registry = registry
        self.scheduler = AnyDatabaseJobScheduler(scheduler)
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
        self.configuration = configuration
        self.storageLimits = storageLimits
    }

    public convenience init<
        Scheduler: DatabaseJobScheduler,
        IdentifierGenerator: DatabaseUUIDGenerator
    >(
        registry: DatabaseResumableOperationRegistry,
        scheduler: Scheduler,
        identifierGenerator: IdentifierGenerator,
        storageLimits: DatabasePersistentJobStorageLimits,
        configuration: DatabaseJobRuntimeConfiguration = .init()
    ) throws {
        try self.init(
            registry: registry,
            scheduler: scheduler,
            identifierGenerator: identifierGenerator,
            errorMapper: CanonicalDatabaseErrorMapper(),
            storageLimits: storageLimits,
            configuration: configuration
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
        let failureStoragePolicy = try DatabasePersistentJobFailureStoragePolicy(
            storageLimits: storageLimits,
            wireLimits: context.wireLimits
        )
        let runner = DatabasePersistentJobRunner(
            container: context.container,
            store: store,
            registry: registry,
            scheduler: scheduler,
            clock: context.clock,
            identifierGenerator: identifierGenerator,
            errorMapper: errorMapper,
            configuration: configuration,
            wireLimits: context.wireLimits,
            storageLimits: storageLimits,
            failureStoragePolicy: failureStoragePolicy,
            runnerID: identifierGenerator.generate()
        )
        let service = DatabasePersistentJobService(
            store: store,
            coordinator: context.coordinator,
            registry: registry,
            runner: runner,
            clock: context.clock,
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
