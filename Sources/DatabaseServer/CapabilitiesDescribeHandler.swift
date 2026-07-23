import DatabaseWire

public struct CapabilitiesDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = CapabilitiesDescribeOperation

    private let descriptor: DatabaseRuntimeDescriptor
    private let jobOperations: [DatabaseJobOperationIdentifier]

    public init(
        descriptor: DatabaseRuntimeDescriptor,
        jobOperations: [DatabaseJobOperationIdentifier]
    ) {
        self.descriptor = descriptor
        self.jobOperations = jobOperations
    }

    public func handle(
        _ request: DatabaseEmpty,
        context: DatabaseOperationContext
    ) async throws -> CapabilitiesDescribeOperation.Response {
        CapabilitiesDescribeOperation.Response(
            runtimeVersion: descriptor.runtimeVersion,
            features: descriptor.features,
            jobOperations: jobOperations
        )
    }
}
