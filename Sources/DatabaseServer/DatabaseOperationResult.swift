import DatabaseValue
import DatabaseWire

public struct DatabaseOperationResult: Sendable {
    private enum Body: Sendable {
        case encoder(DatabaseOperationResponseEncoder)
        case frame(requestID: UInt64, bytes: DatabaseBytes)
    }

    public let operation: DatabaseOperationIdentifier
    private let body: Body

    public init(
        operation: DatabaseOperationIdentifier,
        encoder: DatabaseOperationResponseEncoder
    ) {
        self.operation = operation
        self.body = .encoder(encoder)
    }

    init(
        operation: DatabaseOperationIdentifier,
        requestID: UInt64,
        frame: DatabaseBytes
    ) {
        self.operation = operation
        self.body = .frame(requestID: requestID, bytes: frame)
    }

    func encodeResponse(
        requestID: UInt64,
        limits: DatabaseWireLimits
    ) throws -> DatabaseBytes {
        switch body {
        case .encoder(let encoder):
            return try DatabaseEnvelopeCodec.encodeSuccessResponse(
                requestID: requestID,
                operation: operation,
                limits: limits,
                encodePayload: encoder.encode(into:)
            )
        case .frame(let encodedRequestID, let bytes):
            guard encodedRequestID == requestID else {
                throw DatabaseEndpointError.responseRequestIDMismatch(
                    expected: requestID,
                    actual: encodedRequestID
                )
            }
            return bytes
        }
    }
}
