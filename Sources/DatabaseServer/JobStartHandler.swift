import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct JobStartHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = JobStartOperation

    private let service: AnyDatabaseJobService
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        service: AnyDatabaseJobService,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.service = service
        self.runtimeLimits = runtimeLimits
    }

    public func requirement(
        for request: JobStartOperation.Request
    ) throws -> DatabaseOperationRequirement {
        DatabaseOperationRequirement(
            acceptedTargets: [.database, .base],
            access: .administer,
            transaction: .write,
            baseAdmission: try service.baseAdmission(for: request.operation)
        )
    }

    public func invoke(
        request: JobStartOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        guard request.maximumSliceWorkUnits > 0,
              request.maximumSliceWorkUnits <= runtimeLimits.maximumWorkUnits else {
            throw DatabaseRuntimeLimitError.invalidMaximumWorkUnits(
                requested: request.maximumSliceWorkUnits,
                maximum: runtimeLimits.maximumWorkUnits
            )
        }
        return try await service.start(request, context: context)
            .operationResult
    }
}
