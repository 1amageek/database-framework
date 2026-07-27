@_spi(DatabaseServer) import DatabaseWire

public struct DatabasePreparedOperationResponse<
    Operation: ServerOperationDeclaration
>: Sendable {
    public let response: Operation.Response
    let operationResult: DatabaseOperationResult

    init(
        response: Operation.Response,
        operationResult: DatabaseOperationResult
    ) {
        self.response = response
        self.operationResult = operationResult
    }

    public static func encoding(_ response: Operation.Response) -> Self {
        Self(
            response: response,
            operationResult: DatabaseOperationResult(
                Operation.self,
                response: response
            )
        )
    }
}
