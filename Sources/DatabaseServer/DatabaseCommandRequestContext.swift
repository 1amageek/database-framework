import DatabaseWire

public struct DatabaseCommandRequestContext: Sendable {
    public let requestID: UInt64
    public let metadata: DatabaseRequestMetadata

    public init(
        requestID: UInt64,
        metadata: DatabaseRequestMetadata
    ) {
        self.requestID = requestID
        self.metadata = metadata
    }
}
