public protocol DatabaseOperationServiceFactory: AnyObject, Sendable {
    func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices
}
