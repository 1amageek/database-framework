import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct JobCancelHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = JobCancelOperation

    private let service: AnyDatabaseJobService

    public init(service: AnyDatabaseJobService) {
        self.service = service
    }

    public func invoke(
        request: JobCancelOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        return try await service.cancel(request, context: context)
            .operationResult
    }
}
