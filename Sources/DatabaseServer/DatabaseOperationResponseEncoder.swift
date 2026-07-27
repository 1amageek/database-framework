@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseOperationResponseEncoder: Sendable {
    public let operation: DatabaseOperationIdentifier
    private let encodeResponse: @Sendable (
        UInt64,
        DatabaseWireLimits
    ) throws(DatabaseWireError) -> DatabaseWireEncodedResponse

    public init<Operation: ServerOperationDeclaration>(
        _ operation: Operation.Type,
        response: Operation.Response
    ) {
        self.operation = Operation.operation.identifier
        self.encodeResponse = {
            (
                requestID: UInt64,
                limits: DatabaseWireLimits
            ) throws(DatabaseWireError) -> DatabaseWireEncodedResponse in
            try DatabaseWireEncoder(limits: limits).encodeResponseAndPayload(
                Operation.operation,
                requestID: requestID,
                response: response
            )
        }
    }

    public func encode(
        requestID: UInt64,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) -> DatabaseWireEncodedResponse {
        try encodeResponse(requestID, limits)
    }
}
