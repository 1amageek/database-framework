public protocol DatabaseJobServiceFactory: Sendable {
    func makeJobService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseJobService
}
