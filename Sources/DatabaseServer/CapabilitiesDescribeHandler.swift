@_spi(DatabaseServer) import DatabaseWire

public struct CapabilitiesDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = CapabilitiesDescribeOperation

    private let identity: DatabaseRuntimeIdentity
    private let jobOperations: [JobOperationIdentifier]

    public init(
        identity: DatabaseRuntimeIdentity,
        jobOperations: [JobOperationIdentifier]
    ) {
        self.identity = identity
        self.jobOperations = jobOperations
    }

    public func handle(
        _ request: EmptyOperationPayload,
        context: DatabaseOperationContext
    ) async throws -> CapabilitiesDescribeOperation.Response {
        CapabilitiesDescribeOperation.Response(
            runtimeVersion: identity.version,
            features: DatabaseRuntimeCapabilityCatalog.features,
            jobOperations: jobOperations
        )
    }
}
