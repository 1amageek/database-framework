import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine

/// Application-owned composition root shared by every database host.
public protocol DatabaseOperationApplication: Sendable {
    /// Describes the container before a host injects its storage engine.
    func makeContainerDefinition() async throws
        -> DatabaseContainerDefinition

    /// Builds the host-independent operation configuration after the container
    /// has opened.
    func makeOperationConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationConfiguration
}
