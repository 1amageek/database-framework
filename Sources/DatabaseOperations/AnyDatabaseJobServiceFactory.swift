import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
/// Type-erased persistent job service factory for runtime composition.
public final class AnyDatabaseJobServiceFactory: Sendable {
    private let createJobService: @Sendable (
        DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseJobService

    public init<Factory: DatabaseJobServiceFactory>(_ factory: Factory) {
        self.createJobService = { context in
            try await factory.makeJobService(context: context)
        }
    }

    public func makeJobService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseJobService {
        try await createJobService(context)
    }
}
