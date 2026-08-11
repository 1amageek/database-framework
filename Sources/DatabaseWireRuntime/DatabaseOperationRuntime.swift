import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public final class DatabaseOperationRuntime: Sendable {
    public let endpoint: DatabaseEndpoint
    private let jobService: AnyDatabaseJobService
    private let querySnapshotStore: DatabaseQuerySnapshotStore?

    public init(
        container: DBContainer,
        configuration: DatabaseOperationRuntimeConfiguration,
        hostServices: DatabaseHostServices = .none
    ) async throws {
        let stateStore = DatabaseMutationStateStore(
            container: container
        )
        let coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore,
            runtimeLimits: configuration.runtimeLimits,
            wireLimits: configuration.wireLimits
        )
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        let serviceContext = DatabaseOperationServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: configuration.runtimeLimits,
            wireLimits: configuration.wireLimits,
            clock: configuration.clock,
            graphOperationLimits: configuration.graphOperationLimits,
            schemaRuntimeFactory: configuration.schemaRuntimeFactory,
            hostServices: hostServices
        )
        #else
        let serviceContext = DatabaseOperationServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: configuration.runtimeLimits,
            wireLimits: configuration.wireLimits,
            clock: configuration.clock,
            schemaRuntimeFactory: configuration.schemaRuntimeFactory,
            hostServices: hostServices
        )
        #endif
        let services = try await configuration.makeServices(
            context: serviceContext
        )
        let includesJobs = !services.jobService.jobOperations.isEmpty
        let includesSchemaExecution = configuration.schemaRuntimeFactory != nil
            && includesJobs
        if includesSchemaExecution {
            let schemaJob = try DatabaseSchemaApplyResumableOperation.job()
                .identifier
            guard services.jobService.jobOperations.contains(schemaJob) else {
                throw DatabaseHostServiceError
                    .missingSchemaApplyJobOperation
            }
        }
        let advertisedOperations = DatabaseRuntimeCapabilityCatalog.operations(
            includesSchemaExecution: includesSchemaExecution,
            includesJobs: includesJobs
        )
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        let graphOperations = services.graphOperations
        #endif
        let querySnapshotStore: DatabaseQuerySnapshotStore?
        if let identifierGenerator = hostServices.identifierGenerator,
           let scheduler = hostServices.jobScheduler {
            querySnapshotStore = DatabaseQuerySnapshotStore(
                container: container,
                clock: configuration.clock,
                identifierGenerator: identifierGenerator,
                scheduler: scheduler,
                wireLimits: configuration.wireLimits
            )
        } else {
            querySnapshotStore = nil
        }
        let includesDurableQueryPaging = querySnapshotStore != nil
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        let includesDurableCompositionPaging = querySnapshotStore != nil
        #else
        let includesDurableCompositionPaging = false
        #endif
        var handlers = [
            AnyDatabaseOperationHandler(
                CapabilitiesDescribeHandler(
                    identity: configuration.identity,
                    jobOperations: services.jobService.jobOperations,
                    features: DatabaseRuntimeCapabilityCatalog.features(
                        includesSchemaExecution: includesSchemaExecution,
                        includesJobs: includesJobs,
                        includesDurableQueryPaging:
                            includesDurableQueryPaging,
                        includesDurableCompositionPaging:
                            includesDurableCompositionPaging
                    )
                )
            ),
            AnyDatabaseOperationHandler(
                SchemaDescribeHandler()
            ),
            AnyDatabaseOperationHandler(
                GrantExecuteHandler(
                    coordinator: coordinator,
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
        ]
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        handlers.append(contentsOf: [
            AnyDatabaseOperationHandler(
                BaseExecuteHandler(
                    coordinator: coordinator,
                    jobService: services.jobService,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                CompositionExecuteHandler(
                    coordinator: coordinator,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                QueryExecuteHandler(
                    runtimeLimits: configuration.runtimeLimits,
                    querySnapshotStore: querySnapshotStore
                )
            ),
        ])
        #else
        handlers.append(
            AnyDatabaseOperationHandler(
                QueryExecuteHandler(
                    runtimeLimits: configuration.runtimeLimits,
                    querySnapshotStore: querySnapshotStore
                )
            )
        )
        #endif
        if includesSchemaExecution,
           let schemaRuntimeFactory = configuration.schemaRuntimeFactory {
            handlers.append(
                AnyDatabaseOperationHandler(
                    SchemaExecuteHandler(
                        coordinator: DatabaseSchemaCoordinator(
                            container: container,
                            runtimeFactory: schemaRuntimeFactory,
                            jobService: services.jobService
                        )
                    )
                )
            )
        }
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        handlers.append(contentsOf: [
            AnyDatabaseOperationHandler(
                GraphAlgorithmHandler(
                    service: graphOperations.algorithm,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                OntologyExecuteHandler(
                    service: graphOperations.ontology,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
            AnyDatabaseOperationHandler(
                SHACLExecuteHandler(
                    service: graphOperations.shacl,
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
        ])
        #endif
        handlers.append(contentsOf: [
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
        ])
        if includesJobs {
            handlers.append(contentsOf: [
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
            ])
        }
        let registry = try DatabaseOperationRegistry(
            handlers: handlers,
            requiredOperations: advertisedOperations
        )
        self.jobService = services.jobService
        self.querySnapshotStore = querySnapshotStore
        self.endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: configuration.admissionPolicy,
            middlewares: configuration.middlewares,
            limits: configuration.wireLimits,
            errorMapper: configuration.errorMapper
        )
    }

    public func execute(
        _ bytes: ByteString,
        context: DatabaseRequestExecutionContext
    ) async throws -> ByteString {
        try await endpoint.execute(bytes, context: context)
    }

    public func runScheduledWork() async throws {
        try await querySnapshotStore?.cleanupExpired()
        try await jobService.runScheduledWork()
    }
}
