import DatabaseWire

public struct CapabilitiesDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = CapabilitiesDescribeOperation

    private let identity: DatabaseRuntimeIdentity
    private let jobOperations: [DatabaseJobOperationIdentifier]

    public init(
        identity: DatabaseRuntimeIdentity,
        jobOperations: [DatabaseJobOperationIdentifier]
    ) {
        self.identity = identity
        self.jobOperations = jobOperations
    }

    public func handle(
        _ request: DatabaseEmpty,
        context: DatabaseOperationContext
    ) async throws -> CapabilitiesDescribeOperation.Response {
        CapabilitiesDescribeOperation.Response(
            runtimeVersion: identity.version,
            features: DatabaseRuntimeCapabilityCatalog.features,
            jobOperations: jobOperations
        )
    }
}
