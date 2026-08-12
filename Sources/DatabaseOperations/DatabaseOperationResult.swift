import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseOperationResult: Sendable {
    private enum Body: Sendable {
        case response(DatabaseOperationResponseEncoder)
        case frame(requestID: UInt64, bytes: ByteString)
    }

    public let operation: DatabaseOperationIdentifier
    private let body: Body

    public init<Operation: DatabaseOperationDeclaration>(
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

    package func encodeResponse(
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
                throw DatabaseOperationError.responseRequestIDMismatch(
                    expected: requestID,
                    actual: encodedRequestID
                )
            }
            return bytes
        }
    }
}
