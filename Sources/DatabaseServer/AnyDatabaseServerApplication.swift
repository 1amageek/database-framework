import DatabaseEngine

/// Type-erased owner of one database server application composition.
public struct AnyDatabaseServerApplication: DatabaseServerApplication,
    Sendable {
    private let createContainerDefinition: @Sendable () async throws
        -> DatabaseContainerDefinition
    private let createRuntimeConfiguration: @Sendable (
        DBContainer
    ) async throws -> DatabaseServerRuntimeConfiguration

    public init<Application: DatabaseServerApplication>(
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
    ) async throws -> DatabaseServerRuntimeConfiguration {
        try await createRuntimeConfiguration(container)
    }
}
