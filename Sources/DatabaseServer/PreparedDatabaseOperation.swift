@_spi(DatabaseServer) import DatabaseWire

struct PreparedDatabaseOperation: Sendable {
    let requirement: DatabaseOperationRequirement
    let invoke: @Sendable (
        DatabaseOperationContext
    ) async throws -> DatabaseOperationResult
}
