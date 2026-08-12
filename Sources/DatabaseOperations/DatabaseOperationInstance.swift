import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public final class DatabaseOperationInstance: Sendable {
    private actor Lifecycle {
        enum State: Equatable {
            case running
            case shuttingDown
            case shutDown
        }

        private var state = State.running
        private var activeOperationCount = 0
        private var drainWaiters: [CheckedContinuation<Void, Never>] = []
        private var shutdownWaiters: [CheckedContinuation<Void, Never>] = []

        func admit() throws(DatabaseOperationInstanceError) {
            guard state == .running else {
                throw .shuttingDown
            }
            activeOperationCount += 1
        }

        func release() {
            precondition(activeOperationCount > 0)
            activeOperationCount -= 1
            guard activeOperationCount == 0 else { return }
            let waiters = drainWaiters
            drainWaiters.removeAll(keepingCapacity: false)
            for waiter in waiters {
                waiter.resume()
            }
        }

        func beginShutdown() async -> Bool {
            switch state {
            case .running:
                state = .shuttingDown
                if activeOperationCount > 0 {
                    await withCheckedContinuation { continuation in
                        drainWaiters.append(continuation)
                    }
                }
                return true
            case .shuttingDown:
                await withCheckedContinuation { continuation in
                    shutdownWaiters.append(continuation)
                }
                return false
            case .shutDown:
                return false
            }
        }

        func finishShutdown() {
            precondition(state == .shuttingDown)
            precondition(activeOperationCount == 0)
            state = .shutDown
            let waiters = shutdownWaiters
            shutdownWaiters.removeAll(keepingCapacity: false)
            for waiter in waiters {
                waiter.resume()
            }
        }
    }

    private let container: DBContainer
    package let dispatcher: DatabaseOperationDispatcher
    private let jobService: AnyDatabaseJobService
    private let querySnapshotStore: DatabaseQuerySnapshotStore?
    private let lifecycle = Lifecycle()

    /// Transfers authoritative ownership of `container` to a new operation
    /// instance. A failed open shuts the container down before returning.
    public static func open(
        container: DBContainer,
        configuration: DatabaseOperationConfiguration,
        hostServices: DatabaseOperationHostServices = .none
    ) async throws -> DatabaseOperationInstance {
        do {
            return try await DatabaseOperationInstance(
                container: container,
                configuration: configuration,
                hostServices: hostServices
            )
        } catch {
            await container.shutdown()
            throw error
        }
    }

    private init(
        container: DBContainer,
        configuration: DatabaseOperationConfiguration,
        hostServices: DatabaseOperationHostServices
    ) async throws {
        // Internal continuation and durable-state payloads use the canonical
        // codec budget. Host-configured frame limits belong to the Wire edge
        // and are supplied only when a request is dispatched.
        let operationCodecLimits = DatabaseWireLimits.default
        let wallClock = AnyDatabaseWallClock(container.wallClock)
        let stateStore = DatabaseMutationStateStore(
            container: container
        )
        let coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore,
            runtimeLimits: configuration.runtimeLimits
        )
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        let serviceContext = DatabaseOperationServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            runtimeLimits: configuration.runtimeLimits,
            wireLimits: operationCodecLimits,
            clock: wallClock,
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
            wireLimits: operationCodecLimits,
            clock: wallClock,
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
        let advertisedOperations = DatabaseOperationCapabilityCatalog.operations(
            includesSchemaExecution: includesSchemaExecution,
            includesJobs: includesJobs
        )
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        let graphOperations = services.graphOperations
        #endif
        let querySnapshotStore: DatabaseQuerySnapshotStore?
        if let identifierGenerator = hostServices.identifierGenerator,
           let scheduler = hostServices.jobScheduler {
            querySnapshotStore = DatabaseQuerySnapshotStore(
                container: container,
                clock: wallClock,
                identifierGenerator: identifierGenerator,
                scheduler: scheduler,
                wireLimits: operationCodecLimits
            )
        } else {
            querySnapshotStore = nil
        }
        let includesDurableQueryPaging = querySnapshotStore != nil
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        let includesDurableCompositionPaging = querySnapshotStore != nil
        #else
        let includesDurableCompositionPaging = false
        #endif
        var handlers = [
            AnyDatabaseOperationHandler(
                CapabilitiesDescribeHandler(
                    identity: configuration.identity,
                    jobOperations: services.jobService.jobOperations,
                    features: DatabaseOperationCapabilityCatalog.features(
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
                    runtimeLimits: configuration.runtimeLimits
                )
            ),
        ]
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
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
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
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
        self.container = container
        self.dispatcher = DatabaseOperationDispatcher(
            container: container,
            registry: registry,
            admissionPolicy: configuration.admissionPolicy,
            middlewares: configuration.middlewares
        )
    }

    package func dispatch(
        _ request: DatabaseWireRequestEnvelope,
        context: DatabaseRequestExecutionContext,
        requestLimits: DatabaseWireLimits,
        responseLimits: DatabaseWireLimits
    ) async throws -> DatabaseOperationDispatchOutcome {
        try await lifecycle.admit()
        do {
            let outcome = try await dispatcher.execute(
                request,
                context: context,
                requestLimits: requestLimits,
                responseLimits: responseLimits
            )
            await lifecycle.release()
            return outcome
        } catch {
            await lifecycle.release()
            throw error
        }
    }

    public func runScheduledWork() async throws {
        try await lifecycle.admit()
        do {
            try await querySnapshotStore?.cleanupExpired()
            try await jobService.runScheduledWork()
            await lifecycle.release()
        } catch {
            await lifecycle.release()
            throw error
        }
    }

    public func shutdown() async {
        guard await lifecycle.beginShutdown() else { return }
        await container.shutdown()
        await lifecycle.finishShutdown()
    }
}

public enum DatabaseOperationInstanceError: Error, Sendable, Equatable {
    case shuttingDown
}
