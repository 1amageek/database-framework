import DatabaseEngine

/// Application-owned composition root shared by every database host.
public protocol DatabaseApplication: Sendable {
    /// Describes the container before a host injects its storage engine.
    func makeContainerDefinition() async throws
        -> DatabaseContainerDefinition

    /// Builds the complete Wire runtime after the container has opened.
    func makeRuntimeConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationRuntimeConfiguration
}
