@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseCommandRequestContext: Sendable {
    public let requestID: UInt64
    public let metadata: OperationRequestMetadata

    public init(
        requestID: UInt64,
        metadata: OperationRequestMetadata
    ) {
        self.requestID = requestID
        self.metadata = metadata
    }
}
