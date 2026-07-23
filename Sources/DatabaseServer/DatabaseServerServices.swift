public final class DatabaseServerServices: Sendable {
    public let statementExecutor: AnyDatabaseStatementMutationExecutor
    public let graphAlgorithmService: AnyDatabaseGraphAlgorithmService
    public let ontologyService: AnyDatabaseOntologyService
    public let shaclService: AnyDatabaseSHACLService
    public let readCommandRegistry: DatabaseReadCommandRegistry
    public let writeCommandRegistry: DatabaseWriteCommandRegistry
    public let maintenanceService: AnyDatabaseMaintenanceService
    public let jobService: AnyDatabaseJobService

    public init(
        statementExecutor: AnyDatabaseStatementMutationExecutor,
        graphAlgorithmService: AnyDatabaseGraphAlgorithmService,
        ontologyService: AnyDatabaseOntologyService,
        shaclService: AnyDatabaseSHACLService,
        readCommandRegistry: DatabaseReadCommandRegistry,
        writeCommandRegistry: DatabaseWriteCommandRegistry,
        maintenanceService: AnyDatabaseMaintenanceService,
        jobService: AnyDatabaseJobService
    ) {
        self.statementExecutor = statementExecutor
        self.graphAlgorithmService = graphAlgorithmService
        self.ontologyService = ontologyService
        self.shaclService = shaclService
        self.readCommandRegistry = readCommandRegistry
        self.writeCommandRegistry = writeCommandRegistry
        self.maintenanceService = maintenanceService
        self.jobService = jobService
    }
}
