import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
@_spi(DatabaseOperations) import DatabaseWire

public protocol DatabaseOntologyService: Sendable {
    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult
}

#endif
