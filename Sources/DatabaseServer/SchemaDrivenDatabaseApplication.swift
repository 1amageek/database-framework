import DatabaseEngine

/// Standard application composition for a storage-owned schema catalog.
public struct SchemaDrivenDatabaseApplication: DatabaseServerApplication,
    Sendable {
    private let containerDefinition: DatabaseContainerDefinition
    private let runtimeConfiguration: DatabaseServerRuntimeConfiguration

    public init(
        containerDefinition: DatabaseContainerDefinition,
        runtimeConfiguration: DatabaseServerRuntimeConfiguration
    ) throws(SchemaDrivenDatabaseApplicationError) {
        guard containerDefinition.isSchemaDriven else {
            throw .compiledContainerDefinition
        }
        guard runtimeConfiguration.schemaRuntimeFactory != nil else {
            throw .schemaExecutionUnavailable
        }
        self.containerDefinition = containerDefinition
        self.runtimeConfiguration = runtimeConfiguration
    }

    public func makeContainerDefinition() async throws
        -> DatabaseContainerDefinition {
        containerDefinition
    }

    public func makeRuntimeConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseServerRuntimeConfiguration {
        _ = container
        return runtimeConfiguration
    }
}
