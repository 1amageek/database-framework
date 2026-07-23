public protocol DatabaseJobServiceFactory: Sendable {
    func makeJobService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseJobService
}
