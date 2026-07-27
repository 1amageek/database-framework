@_spi(DatabaseServer) import DatabaseWire

/// State-independent request metadata evaluated before extensible dispatch.
public struct DatabaseOperationAdmissionRequest: Sendable, Hashable {
    public let requestID: UInt64
    public let operation: DatabaseOperationIdentifier
    public let metadata: OperationRequestMetadata

    public init(
        requestID: UInt64,
        operation: DatabaseOperationIdentifier,
        metadata: OperationRequestMetadata
    ) {
        self.requestID = requestID
        self.operation = operation
        self.metadata = metadata
    }
}
