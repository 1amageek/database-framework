import DatabaseWire

/// State-independent request metadata evaluated before extensible dispatch.
public struct DatabaseOperationAdmissionRequest: Sendable, Hashable {
    public let requestID: UInt64
    public let operation: DatabaseOperationIdentifier
    public let metadata: DatabaseRequestMetadata

    public init(
        requestID: UInt64,
        operation: DatabaseOperationIdentifier,
        metadata: DatabaseRequestMetadata
    ) {
        self.requestID = requestID
        self.operation = operation
        self.metadata = metadata
    }
}
