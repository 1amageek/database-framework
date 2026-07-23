import DatabaseValue
import DatabaseWire

public struct DatabaseSuccessPayload: Sendable, Hashable {
    public let operation: DatabaseOperationIdentifier
    public let bytes: DatabaseBytes

    init(
        operation: DatabaseOperationIdentifier,
        bytes: DatabaseBytes,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) {
        try DatabaseEnvelopeCodec.validateSuccessResponsePayloadByteCount(
            bytes.count,
            limits: limits
        )
        self.operation = operation
        self.bytes = bytes
    }
}
