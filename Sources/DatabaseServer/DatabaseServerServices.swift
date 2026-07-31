public final class DatabaseServerServices: Sendable {
    #if DATABASE_SERVER_GRAPH_INDEXES
    public let graphOperations: GraphOperationServices

    public var statementExecutor: AnyDatabaseStatementMutationExecutor {
        graphOperations.statementExecutor
    }
    #else
    public let statementExecutor: AnyDatabaseStatementMutationExecutor
    #endif
    public let readCommandRegistry: DatabaseReadCommandRegistry
    public let writeCommandRegistry: DatabaseWriteCommandRegistry
    public let maintenanceService: AnyDatabaseMaintenanceService
    public let jobService: AnyDatabaseJobService

    #if DATABASE_SERVER_GRAPH_INDEXES
    public init(
        graphOperations: GraphOperationServices,
        readCommandRegistry: DatabaseReadCommandRegistry,
        writeCommandRegistry: DatabaseWriteCommandRegistry,
        maintenanceService: AnyDatabaseMaintenanceService,
        jobService: AnyDatabaseJobService
    ) {
        self.graphOperations = graphOperations
        self.readCommandRegistry = readCommandRegistry
        self.writeCommandRegistry = writeCommandRegistry
        self.maintenanceService = maintenanceService
        self.jobService = jobService
    }
    #else
    public init(
        statementExecutor: AnyDatabaseStatementMutationExecutor,
        readCommandRegistry: DatabaseReadCommandRegistry,
        writeCommandRegistry: DatabaseWriteCommandRegistry,
        maintenanceService: AnyDatabaseMaintenanceService,
        jobService: AnyDatabaseJobService
    ) {
        self.statementExecutor = statementExecutor
        self.readCommandRegistry = readCommandRegistry
        self.writeCommandRegistry = writeCommandRegistry
        self.maintenanceService = maintenanceService
        self.jobService = jobService
    }
    #endif

    public func replacingCommandRegistries(
        read readCommandRegistry: DatabaseReadCommandRegistry,
        write writeCommandRegistry: DatabaseWriteCommandRegistry
    ) -> DatabaseServerServices {
        #if DATABASE_SERVER_GRAPH_INDEXES
        return DatabaseServerServices(
            graphOperations: graphOperations,
            readCommandRegistry: readCommandRegistry,
            writeCommandRegistry: writeCommandRegistry,
            maintenanceService: maintenanceService,
            jobService: jobService
        )
        #else
        return DatabaseServerServices(
            statementExecutor: statementExecutor,
            readCommandRegistry: readCommandRegistry,
            writeCommandRegistry: writeCommandRegistry,
            maintenanceService: maintenanceService,
            jobService: jobService
        )
        #endif
    }
}
