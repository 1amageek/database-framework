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

public struct CapabilitiesDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = CapabilitiesDescribeOperation

    private let identity: DatabaseOperationIdentity
    private let jobOperations: [JobOperationIdentifier]
    private let features: [CapabilitiesDescribeOperation.Feature]

    public init(
        identity: DatabaseOperationIdentity,
        jobOperations: [JobOperationIdentifier]
    ) {
        self.init(
            identity: identity,
            jobOperations: jobOperations,
            features: DatabaseOperationCapabilityCatalog.features(
                includesSchemaExecution: false,
                includesJobs: !jobOperations.isEmpty
            )
        )
    }

    public init(
        identity: DatabaseOperationIdentity,
        jobOperations: [JobOperationIdentifier],
        features: [CapabilitiesDescribeOperation.Feature]
    ) {
        self.identity = identity
        self.jobOperations = jobOperations
        self.features = features
    }

    public func handle(
        _ request: EmptyOperationPayload,
        context: DatabaseOperationContext
    ) async throws -> CapabilitiesDescribeOperation.Response {
        _ = request
        return try await context.requireControlExecutor().withTransaction(
            requiredAccess: .read,
            configuration: .readOnly
        ) { _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: identity.version,
                features: features,
                jobOperations: jobOperations
            )
        }
    }
}
