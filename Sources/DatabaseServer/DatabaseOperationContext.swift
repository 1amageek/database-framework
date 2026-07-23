import DatabaseEngine
import DatabaseValue
import DatabaseWire

public struct DatabaseOperationContext: Sendable {
    public let container: DBContainer
    public let requestID: UInt64
    public let metadata: DatabaseRequestMetadata
    public let requestPayload: DatabaseBytes
    public let requestDigest: DatabaseBytes?

    public init(
        container: DBContainer,
        requestID: UInt64,
        metadata: DatabaseRequestMetadata,
        requestPayload: DatabaseBytes,
        requestDigest: DatabaseBytes? = nil
    ) {
        self.container = container
        self.requestID = requestID
        self.metadata = metadata
        self.requestPayload = requestPayload
        self.requestDigest = requestDigest
    }
}
