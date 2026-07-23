public protocol DatabaseServerServiceFactory: AnyObject, Sendable {
    func makeServices(
        context: DatabaseServerServiceContext
    ) async throws -> DatabaseServerServices
}
