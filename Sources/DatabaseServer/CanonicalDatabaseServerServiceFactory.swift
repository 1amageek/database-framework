import DatabaseEngine
#if DATABASE_SERVER_GRAPH_INDEXES
import GraphIndex
import OntologyIndex
#endif
import StorageKit

public final class CanonicalDatabaseServerServiceFactory:
    DatabaseServerServiceFactory,
    Sendable {
    private let maintenanceServiceFactory: AnyDatabaseMaintenanceServiceFactory
    private let jobServiceFactory: AnyDatabaseJobServiceFactory
    private let readCommands: [AnyDatabaseReadCommand]
    private let writeCommands: [AnyDatabaseWriteCommand]
    #if DATABASE_SERVER_GRAPH_INDEXES
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
        #if DATABASE_SERVER_GRAPH_INDEXES
        self.loadSource = .unconfigured
        self.functionRegistry = .empty
        #endif
    }

    #if DATABASE_SERVER_GRAPH_INDEXES
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
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
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

        #if DATABASE_SERVER_GRAPH_INDEXES
        let ontologyStore = try await DatabaseRDFDocumentStore(
            container: context.container,
            namespace: "ontology",
            wireLimits: context.wireLimits
        )
        let ontologyIndexSubspace = try await context.container.engine
            .resolveOrCreateNamespace(
                path: ["database-framework", "ontology-index"]
            )
        let ontologyIndexStore = OntologyStore(
            subspace: OntologySubspace(base: ontologyIndexSubspace)
        )
        let ontologyProcessor = DatabaseOntologyReasoningProcessor(
            documentStore: ontologyStore,
            ontologyStore: ontologyIndexStore,
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
            stateStore: context.stateStore,
            ontologyStore: ontologyIndexStore
        )
        let shaclProcessor = DatabaseSHACLValidationProcessor(
            documentStore: shaclStore,
            dataSourceResolver: shaclDataSourceResolver,
            wireLimits: context.wireLimits
        )

        return DatabaseServerServices(
            graphOperations: GraphOperationServices(
                statementExecutor:
                    CanonicalDatabaseStatementMutationExecutor(
                        runtimeLimits: context.runtimeLimits,
                        graphStore: CanonicalRDFGraphStore(),
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
        return DatabaseServerServices(
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
