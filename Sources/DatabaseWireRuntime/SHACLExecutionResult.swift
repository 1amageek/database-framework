#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
@_spi(DatabaseWireRuntime) import DatabaseWire

public struct SHACLExecutionResult: Sendable {
    public let response: SHACLExecuteOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        response: SHACLExecuteOperation.Response,
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
                SHACLExecuteOperation.self,
                limits: limits
            ),
            operationResult: coordinated.result
        )
    }

    static func encoding(_ response: SHACLExecuteOperation.Response) -> Self {
        Self(
            response: response,
            operationResult: DatabaseOperationResult(
                SHACLExecuteOperation.self,
                response: response
            )
        )
    }
}

#endif
