import DatabaseEngine
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct CapabilitiesDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = CapabilitiesDescribeOperation

    private let identity: DatabaseRuntimeIdentity
    private let jobOperations: [JobOperationIdentifier]
    private let features: [CapabilitiesDescribeOperation.Feature]

    public init(
        identity: DatabaseRuntimeIdentity,
        jobOperations: [JobOperationIdentifier]
    ) {
        self.init(
            identity: identity,
            jobOperations: jobOperations,
            features: DatabaseRuntimeCapabilityCatalog.features(
                includesSchemaExecution: false,
                includesJobs: !jobOperations.isEmpty
            )
        )
    }

    public init(
        identity: DatabaseRuntimeIdentity,
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
