public struct DatabaseCoordinatedOperationResponse: Sendable {
    public let result: DatabaseOperationResult
    let successPayload: DatabaseSuccessPayload

    init(
        result: DatabaseOperationResult,
        successPayload: DatabaseSuccessPayload
    ) {
        self.result = result
        self.successPayload = successPayload
    }
}
