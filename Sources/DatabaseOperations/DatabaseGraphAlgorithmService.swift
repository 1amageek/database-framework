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

public protocol DatabaseGraphAlgorithmService: Sendable {
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response
}

#endif
