import DatabaseEngine
import DatabaseTypes

public final class DatabaseServerRuntime: Sendable {
    public let endpoint: DatabaseEndpoint
    private let jobService: AnyDatabaseJobService

    public init(
        container: DBContainer,
        configuration: DatabaseServerRuntimeConfiguration
    ) async throws {
        try configuration.runtimeLimits.validateConfiguration()
        try await container.migrateIfNeeded()
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore,
            runtimeLimits: configuration.runtimeLimits,
            wireLimits: configuration.wireLimits
        )
        let services = try await configuration.makeServices(
            context: DatabaseServerServiceContext(
                container: container,
                stateStore: stateStore,
                coordinator: coordinator,
                runtimeLimits: configuration.runtimeLimits,
                wireLimits: configuration.wireLimits
            )
        )
        let handlers = [
            AnyDatabaseOperationHandler(
                CapabilitiesDescribeHandler(
                    identity: configuration.identity,
                    jobOperations: services.jobService.jobOperations
                )
            ),
            AnyDatabaseOperationHandler(
                SchemaDescribeHandler()
            ),
            AnyDatabaseOperationHandler(
                QueryExecuteHandler(
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                MutationExecuteHandler(
                    stateStore: stateStore,
                    statementExecutor: services.statementExecutor,
                    runtimeLimits: configuration.runtimeLimits,
                    wireLimits: configuration.wireLimits
                )
            ),
            AnyDatabaseOperationHandler(
                GraphAlgorithmHandler(
                    service: services.graphAlgorithmService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                OntologyExecuteHandler(
                    service: services.ontologyService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                SHACLExecuteHandler(
                    service: services.shaclService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                CommandExecuteHandler(
                    readRegistry: services.readCommandRegistry,
                    writeRegistry: services.writeCommandRegistry,
                    coordinator: coordinator,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                MaintenanceExecuteHandler(
                    service: services.maintenanceService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                JobStartHandler(
                    service: services.jobService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                JobStatusHandler(service: services.jobService)
            ),
            AnyDatabaseOperationHandler(
                JobResultHandler(service: services.jobService)
            ),
            AnyDatabaseOperationHandler(
                JobCancelHandler(service: services.jobService)
            ),
        ]
        let registry = try DatabaseOperationRegistry(handlers: handlers)
        self.jobService = services.jobService
        self.endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: configuration.admissionPolicy,
            middlewares: configuration.middlewares,
            limits: configuration.wireLimits,
            errorMapper: configuration.errorMapper
        )
    }

    public func execute(_ bytes: ByteString) async throws -> ByteString {
        try await endpoint.execute(bytes)
    }

    public func runScheduledWork() async throws {
        try await jobService.runScheduledWork()
    }
}
