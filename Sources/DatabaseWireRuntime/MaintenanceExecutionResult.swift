@_spi(DatabaseWireRuntime) import DatabaseWire

public struct MaintenanceExecutionResult: Sendable {
    public let response: MaintenanceExecuteOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        response: MaintenanceExecuteOperation.Response,
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
                MaintenanceExecuteOperation.self,
                limits: limits
            ),
            operationResult: coordinated.result
        )
    }

    static func encoding(
        _ response: MaintenanceExecuteOperation.Response
    ) -> Self {
        Self(
            response: response,
            operationResult: DatabaseOperationResult(
                MaintenanceExecuteOperation.self,
                response: response
            )
        )
    }
}
