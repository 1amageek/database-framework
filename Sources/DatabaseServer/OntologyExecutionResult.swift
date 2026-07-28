@_spi(DatabaseServer) import DatabaseWire

public struct OntologyExecutionResult: Sendable {
    public let response: OntologyExecuteOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        response: OntologyExecuteOperation.Response,
        operationResult: DatabaseOperationResult
    ) {
        self.response = response
        self.operationResult = operationResult
    }

    init(
        coordinated: DatabaseCoordinatedOperationResponse,
        limits: DatabaseWireLimits
    ) throws {
        self.init(
            response: try coordinated.decodeResponse(
                OntologyExecuteOperation.self,
                limits: limits
            ),
            operationResult: coordinated.result
        )
    }

    static func encoding(
        _ response: OntologyExecuteOperation.Response
    ) -> Self {
        Self(
            response: response,
            operationResult: DatabaseOperationResult(
                OntologyExecuteOperation.self,
                response: response
            )
        )
    }
}
