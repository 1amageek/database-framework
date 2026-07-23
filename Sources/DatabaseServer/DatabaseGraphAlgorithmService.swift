import DatabaseWire

public protocol DatabaseGraphAlgorithmService: Sendable {
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response
}
