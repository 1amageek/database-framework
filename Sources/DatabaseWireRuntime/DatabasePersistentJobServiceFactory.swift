import DatabaseEngine

public final class DatabasePersistentJobServiceFactory:
    DatabaseJobServiceFactory,
    Sendable {
    private let registry: DatabaseResumableOperationRegistry
    private let identifierGenerator: AnyDatabaseUUIDGenerator
    private let errorMapper: AnyDatabaseErrorMapper
    private let configuration: DatabaseJobRuntimeConfiguration
    private let storageLimits: DatabasePersistentJobStorageLimits

    public init<
        IdentifierGenerator: DatabaseUUIDGenerator,
        ErrorMapper: DatabaseErrorMapper
    >(
        registry: DatabaseResumableOperationRegistry,
        identifierGenerator: IdentifierGenerator,
        errorMapper: ErrorMapper,
        storageLimits: DatabasePersistentJobStorageLimits,
        configuration: DatabaseJobRuntimeConfiguration = .init()
    ) throws {
        try configuration.validate()
        try storageLimits.validate()
        self.registry = registry
        self.identifierGenerator = AnyDatabaseUUIDGenerator(identifierGenerator)
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
        self.configuration = configuration
        self.storageLimits = storageLimits
    }

    public convenience init<
        IdentifierGenerator: DatabaseUUIDGenerator
    >(
        registry: DatabaseResumableOperationRegistry,
        identifierGenerator: IdentifierGenerator,
        storageLimits: DatabasePersistentJobStorageLimits,
        configuration: DatabaseJobRuntimeConfiguration = .init()
    ) throws {
        try self.init(
            registry: registry,
            identifierGenerator: identifierGenerator,
            errorMapper: CanonicalDatabaseErrorMapper(),
            storageLimits: storageLimits,
            configuration: configuration
        )
    }

    public func makeJobService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseJobService {
        guard let scheduler = context.hostServices.jobScheduler,
              let authorizationValidator = context.hostServices
                .jobAuthorizationValidator else {
            return AnyDatabaseJobService(UnavailableDatabaseJobService())
        }
        var registry = registry
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        registry = try registry.including(
            AnyDatabaseResumableOperation(
                DatabaseBaseLifecycleResumableOperation(
                    runtimeLimits: context.runtimeLimits
                )
            )
        ).including(
            AnyDatabaseResumableOperation(
                DatabaseLegacyLayoutMigrationResumableOperation(
                    runtimeLimits: context.runtimeLimits
                )
            )
        )
        #endif
        if let schemaRuntimeFactory = context.schemaRuntimeFactory {
            registry = try registry.including(
                AnyDatabaseResumableOperation(
                    DatabaseSchemaApplyResumableOperation(
                        runtimeFactory: schemaRuntimeFactory,
                        runtimeLimits: context.runtimeLimits
                    )
                )
            )
        }
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
            authorizationValidator: authorizationValidator,
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
        return AnyDatabaseJobService(persistent: service)
    }
}
