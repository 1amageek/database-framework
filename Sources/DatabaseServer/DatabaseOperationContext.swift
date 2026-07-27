import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseOperationContext: Sendable {
    public let container: DBContainer
    public let requestID: UInt64
    public let metadata: OperationRequestMetadata
    public let requestPayload: ByteString
    public let requestDigest: ByteString?

    public init(
        container: DBContainer,
        requestID: UInt64,
        metadata: OperationRequestMetadata,
        requestPayload: ByteString,
        requestDigest: ByteString? = nil
    ) {
        self.container = container
        self.requestID = requestID
        self.metadata = metadata
        self.requestPayload = requestPayload
        self.requestDigest = requestDigest
    }
}
