import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine

/// Type-erased owner of one database application composition.
public struct AnyDatabaseOperationApplication: DatabaseOperationApplication,
    Sendable {
    private let createContainerDefinition: @Sendable () async throws
        -> DatabaseContainerDefinition
    private let createOperationConfiguration: @Sendable (
        DBContainer
    ) async throws -> DatabaseOperationConfiguration

    public init<Application: DatabaseOperationApplication>(
        _ application: Application
    ) {
        self.createContainerDefinition = {
            try await application.makeContainerDefinition()
        }
        self.createOperationConfiguration = { container in
            try await application.makeOperationConfiguration(for: container)
        }
    }

    public func makeContainerDefinition() async throws
        -> DatabaseContainerDefinition {
        try await createContainerDefinition()
    }

    public func makeOperationConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationConfiguration {
        try await createOperationConfiguration(container)
    }
}
