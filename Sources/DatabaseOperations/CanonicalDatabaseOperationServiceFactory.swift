import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import GraphIndex
#endif
import StorageKit

public final class CanonicalDatabaseOperationServiceFactory:
    DatabaseOperationServiceFactory,
    Sendable {
    private let maintenanceServiceFactory: AnyDatabaseMaintenanceServiceFactory
    private let jobServiceFactory: AnyDatabaseJobServiceFactory
    private let readCommands: [AnyDatabaseReadCommand]
    private let writeCommands: [AnyDatabaseWriteCommand]
    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private let loadSource: AnySPARQLLoadSource
    private let functionRegistry: SPARQLFunctionRegistry
    #endif

    public init<
        MaintenanceFactory: DatabaseMaintenanceServiceFactory,
        JobFactory: DatabaseJobServiceFactory
    >(
        maintenanceServiceFactory: MaintenanceFactory,
        jobServiceFactory: JobFactory,
        readCommands: [AnyDatabaseReadCommand] = [],
        writeCommands: [AnyDatabaseWriteCommand] = []
    ) {
        self.maintenanceServiceFactory = AnyDatabaseMaintenanceServiceFactory(
            maintenanceServiceFactory
        )
        self.jobServiceFactory = AnyDatabaseJobServiceFactory(jobServiceFactory)
        self.readCommands = readCommands
        self.writeCommands = writeCommands
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        self.loadSource = .unconfigured
        self.functionRegistry = .empty
        #endif
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public init<
        MaintenanceFactory: DatabaseMaintenanceServiceFactory,
        JobFactory: DatabaseJobServiceFactory
    >(
        maintenanceServiceFactory: MaintenanceFactory,
        jobServiceFactory: JobFactory,
        readCommands: [AnyDatabaseReadCommand] = [],
        writeCommands: [AnyDatabaseWriteCommand] = [],
        loadSource: AnySPARQLLoadSource,
        functionRegistry: SPARQLFunctionRegistry = .empty
    ) {
        self.maintenanceServiceFactory = AnyDatabaseMaintenanceServiceFactory(
            maintenanceServiceFactory
        )
        self.jobServiceFactory = AnyDatabaseJobServiceFactory(jobServiceFactory)
        self.readCommands = readCommands
        self.writeCommands = writeCommands
        self.loadSource = loadSource
        self.functionRegistry = functionRegistry
    }
    #endif

    public func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        let readCommandRegistry = try DatabaseReadCommandRegistry(
            commands: readCommands
        )
        let writeCommandRegistry = try DatabaseWriteCommandRegistry(
            commands: writeCommands
        )
        let maintenanceService = try await maintenanceServiceFactory
            .makeMaintenanceService(context: context)
        let jobService = try await jobServiceFactory.makeJobService(
            context: context
        )

        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        let ontologyStore = try await DatabaseRDFDocumentStore(
            container: context.container,
            namespace: "ontology",
            wireLimits: context.wireLimits
        )
        let ontologyProcessor = DatabaseOntologyReasoningProcessor(
            documentStore: ontologyStore,
            container: context.container,
            clock: context.clock,
            monotonicClock: context.container.monotonicClock,
            wireLimits: context.wireLimits
        )
        let shaclStore = try await DatabaseRDFDocumentStore(
            container: context.container,
            namespace: "shacl",
            wireLimits: context.wireLimits
        )
        let shaclDataSourceResolver = SchemaDatabaseSHACLDataSourceResolver(
            container: context.container,
            stateStore: context.stateStore
        )
        let shaclProcessor = DatabaseSHACLValidationProcessor(
            documentStore: shaclStore,
            dataSourceResolver: shaclDataSourceResolver,
            wireLimits: context.wireLimits
        )

        return DatabaseOperationServices(
            graphOperations: GraphOperationServices(
                statementExecutor:
                    CanonicalDatabaseStatementMutationExecutor(
                        runtimeLimits: context.runtimeLimits,
                        loadSource: loadSource,
                        functionRegistry: functionRegistry,
                        graphOperationLimits: context.graphOperationLimits
                    ),
                algorithm: AnyDatabaseGraphAlgorithmService(
                    CanonicalDatabaseGraphAlgorithmService(
                        sourceResolver: SchemaDatabaseGraphSourceResolver(
                            container: context.container
                        ),
                        wireLimits: context.wireLimits
                    ),
                ),
                ontology: AnyDatabaseOntologyService(
                    CanonicalDatabaseOntologyService(
                        store: ontologyStore,
                        processor: ontologyProcessor,
                        coordinator: context.coordinator,
                        wireLimits: context.wireLimits
                    )
                ),
                shacl: AnyDatabaseSHACLService(
                    CanonicalDatabaseSHACLService(
                        store: shaclStore,
                        processor: shaclProcessor,
                        coordinator: context.coordinator,
                        wireLimits: context.wireLimits
                    )
                )
            ),
            readCommandRegistry: readCommandRegistry,
            writeCommandRegistry: writeCommandRegistry,
            maintenanceService: maintenanceService,
            jobService: jobService
        )
        #else
        return DatabaseOperationServices(
            statementExecutor: AnyDatabaseStatementMutationExecutor(
                CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: context.runtimeLimits
                )
            ),
            readCommandRegistry: readCommandRegistry,
            writeCommandRegistry: writeCommandRegistry,
            maintenanceService: maintenanceService,
            jobService: jobService
        )
        #endif
    }
}
