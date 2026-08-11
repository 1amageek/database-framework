@_spi(DatabaseWireRuntime) import DatabaseWire

public struct JobStartExecutionResult: Sendable {
    public let response: JobStartOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        coordinated: DatabaseCoordinatedOperationResponse,
        limits: DatabaseWireLimits
    ) throws {
        self.response = try coordinated.decodeResponse(
            JobStartOperation.self,
            limits: limits
        )
        self.operationResult = coordinated.result
    }
}
