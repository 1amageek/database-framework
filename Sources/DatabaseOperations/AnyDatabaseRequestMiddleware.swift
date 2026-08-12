import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

/// Type-erased database request middleware.
public final class AnyDatabaseRequestMiddleware:
    DatabaseRequestMiddleware,
    Sendable {
    private let handleRequest: @Sendable (
        DatabaseWireRequestEnvelope,
        DatabaseOperationContext,
        DatabaseRequestHandler
    ) async throws -> DatabaseOperationResult

    public init<Middleware: DatabaseRequestMiddleware>(
        _ middleware: Middleware
    ) {
        self.handleRequest = { request, context, next in
            try await middleware.handle(
                request: request,
                context: context,
                next: next
            )
        }
    }

    public func handle(
        request: DatabaseWireRequestEnvelope,
        context: DatabaseOperationContext,
        next: DatabaseRequestHandler
    ) async throws -> DatabaseOperationResult {
        try await handleRequest(request, context, next)
    }
}
