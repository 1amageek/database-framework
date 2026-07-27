import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseOperationResult: Sendable {
    private enum Body: Sendable {
        case response(DatabaseOperationResponseEncoder)
        case frame(requestID: UInt64, bytes: ByteString)
    }

    public let operation: DatabaseOperationIdentifier
    private let body: Body

    public init<Operation: ServerOperationDeclaration>(
        _ operation: Operation.Type,
        response: Operation.Response
    ) {
        let encoder = DatabaseOperationResponseEncoder(
            operation,
            response: response
        )
        self.operation = encoder.operation
        self.body = .response(encoder)
    }

    init(encoder: DatabaseOperationResponseEncoder) {
        self.operation = encoder.operation
        self.body = .response(encoder)
    }

    init(
        operation: DatabaseOperationIdentifier,
        requestID: UInt64,
        frame: ByteString
    ) {
        self.operation = operation
        self.body = .frame(requestID: requestID, bytes: frame)
    }

    func encodeResponse(
        requestID: UInt64,
        limits: DatabaseWireLimits
    ) throws -> ByteString {
        switch body {
        case .response(let encoder):
            return try encoder.encode(
                requestID: requestID,
                limits: limits
            ).frame
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
