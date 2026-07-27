@_spi(DatabaseServer) import DatabaseWire

/// Type-erased SHACL service for runtime composition.
public final class AnyDatabaseSHACLService: DatabaseSHACLService, Sendable {
    private let executeSHACL: @Sendable (
        SHACLExecuteOperation.Request,
        DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation>

    public init<Service: DatabaseSHACLService>(_ service: Service) {
        self.executeSHACL = { request, context in
            try await service.execute(request, context: context)
        }
    }

    public func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> DatabasePreparedOperationResponse<SHACLExecuteOperation> {
        try await executeSHACL(request, context)
    }
}
