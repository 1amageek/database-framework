import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseSuccessPayload: Sendable, Hashable {
    public let operation: DatabaseOperationIdentifier
    public let bytes: ByteString

    init(
        operation: DatabaseOperationIdentifier,
        bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) {
        try DatabaseWireEncoder(limits: limits).validateSuccessPayload(bytes)
        self.operation = operation
        self.bytes = bytes
    }
}
