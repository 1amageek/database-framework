import DatabaseEngine

/// Type-erased owner of one database application composition.
public struct AnyDatabaseApplication: DatabaseApplication,
    Sendable {
    private let createContainerDefinition: @Sendable () async throws
        -> DatabaseContainerDefinition
    private let createRuntimeConfiguration: @Sendable (
        DBContainer
    ) async throws -> DatabaseOperationRuntimeConfiguration

    public init<Application: DatabaseApplication>(
        _ application: Application
    ) {
        self.createContainerDefinition = {
            try await application.makeContainerDefinition()
        }
        self.createRuntimeConfiguration = { container in
            try await application.makeRuntimeConfiguration(for: container)
        }
    }

    public func makeContainerDefinition() async throws
        -> DatabaseContainerDefinition {
        try await createContainerDefinition()
    }

    public func makeRuntimeConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationRuntimeConfiguration {
        try await createRuntimeConfiguration(container)
    }
}
