public protocol DatabaseMaintenanceServiceFactory: Sendable {
    func makeMaintenanceService(
        context: DatabaseServerServiceContext
    ) async throws -> AnyDatabaseMaintenanceService
}
