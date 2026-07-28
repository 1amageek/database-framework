@_spi(DatabaseServer) import DatabaseWire

public struct JobCancellationExecutionResult: Sendable {
    public let response: JobCancelOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        coordinated: DatabaseCoordinatedOperationResponse,
        limits: DatabaseWireLimits
    ) throws {
        self.response = try coordinated.decodeResponse(
            JobCancelOperation.self,
            limits: limits
        )
        self.operationResult = coordinated.result
    }
}
