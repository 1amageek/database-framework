import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
public struct GraphOperationServices: Sendable {
    public let statementExecutor: AnyDatabaseStatementMutationExecutor
    public let algorithm: AnyDatabaseGraphAlgorithmService
    public let ontology: AnyDatabaseOntologyService
    public let shacl: AnyDatabaseSHACLService

    public init<StatementMutation: GraphStatementMutationExecutor>(
        statementExecutor: StatementMutation,
        algorithm: AnyDatabaseGraphAlgorithmService,
        ontology: AnyDatabaseOntologyService,
        shacl: AnyDatabaseSHACLService
    ) {
        self.statementExecutor = AnyDatabaseStatementMutationExecutor(
            statementExecutor
        )
        self.algorithm = algorithm
        self.ontology = ontology
        self.shacl = shacl
    }
}
#endif
