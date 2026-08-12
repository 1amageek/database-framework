import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

struct PreparedDatabaseOperation: Sendable {
    let requirement: DatabaseOperationRequirement
    let invoke: @Sendable (
        DatabaseOperationContext
    ) async throws -> DatabaseOperationResult
}
