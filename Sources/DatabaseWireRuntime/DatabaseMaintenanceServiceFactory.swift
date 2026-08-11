public protocol DatabaseMaintenanceServiceFactory: Sendable {
    func makeMaintenanceService(
        context: DatabaseOperationServiceContext
    ) async throws -> AnyDatabaseMaintenanceService
}
