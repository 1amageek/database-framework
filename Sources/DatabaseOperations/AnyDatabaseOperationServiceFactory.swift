import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
/// Type-erased application service factory.
public final class AnyDatabaseOperationServiceFactory: Sendable {
    private let createServices: @Sendable (
        DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices

    public init<Factory: DatabaseOperationServiceFactory>(_ factory: Factory) {
        self.createServices = { context in
            try await factory.makeServices(context: context)
        }
    }

    public init(
        makeServices: @escaping @Sendable (
            DatabaseOperationServiceContext
        ) async throws -> DatabaseOperationServices
    ) {
        self.createServices = makeServices
    }

    public func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        try await createServices(context)
    }
}
