import GraphIndex
import OntologyIndex

public final class CanonicalDatabaseServerServiceFactory:
    DatabaseServerServiceFactory,
    Sendable {
    private let maintenanceServiceFactory: AnyDatabaseMaintenanceServiceFactory
    private let jobServiceFactory: AnyDatabaseJobServiceFactory
    private let readCommands: [AnyDatabaseReadCommand]
    private let writeCommands: [AnyDatabaseWriteCommand]
    private let loadSource: AnySPARQLLoadSource
    private let functionRegistry: SPARQLFunctionRegistry

    public init<
        MaintenanceFactory: DatabaseMaintenanceServiceFactory,
        JobFactory: DatabaseJobServiceFactory
    >(
        maintenanceServiceFactory: MaintenanceFactory,
        jobServiceFactory: JobFactory,
        readCommands: [AnyDatabaseReadCommand] = [],
        writeCommands: [AnyDatabaseWriteCommand] = [],
        loadSource: AnySPARQLLoadSource = .unconfigured,
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

    public func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices {
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
            statementExecutor: AnyDatabaseStatementMutationExecutor(
                CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: context.runtimeLimits,
                    loadSource: loadSource,
                    functionRegistry: functionRegistry
                )
            ),
            graphAlgorithmService: AnyDatabaseGraphAlgorithmService(
                CanonicalDatabaseGraphAlgorithmService(
                    sourceResolver: SchemaDatabaseGraphSourceResolver(
                        container: context.container
                    ),
                    wireLimits: context.wireLimits
                ),
            ),
            ontologyService: AnyDatabaseOntologyService(
                CanonicalDatabaseOntologyService(
                    store: ontologyStore,
                    processor: ontologyProcessor,
                    coordinator: context.coordinator,
                    wireLimits: context.wireLimits
                )
            ),
            shaclService: AnyDatabaseSHACLService(
                CanonicalDatabaseSHACLService(
                    store: shaclStore,
                    processor: shaclProcessor,
                    coordinator: context.coordinator,
                    wireLimits: context.wireLimits
                )
            ),
            readCommandRegistry: try DatabaseReadCommandRegistry(
                commands: readCommands
            ),
            writeCommandRegistry: try DatabaseWriteCommandRegistry(
                commands: writeCommands
            ),
            maintenanceService: try await maintenanceServiceFactory
                .makeMaintenanceService(context: context),
            jobService: try await jobServiceFactory.makeJobService(
                context: context
            )
        )
    }
}
