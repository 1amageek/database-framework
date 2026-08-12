import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseKit

public struct CanonicalPreparedStatementMutation: Sendable {
    enum Payload: Sendable {
        case statement(QueryStatement)
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        case sparql(PreparedSPARQLUpdateRequest)
        #endif
    }

    let payload: Payload
    let workMeter: DatabaseWorkMeter
    let structuralLimits: QueryStructuralLimits

    init(
        payload: Payload,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) {
        self.payload = payload
        self.workMeter = workMeter
        self.structuralLimits = structuralLimits
    }
}
