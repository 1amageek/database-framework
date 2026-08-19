import DatabaseKit
import StorageKit

extension DBContainer {
    @_spi(Testing)
    public func resolveDirectoryForTesting<Model: Persistable>(
        for type: Model.Type,
        path: DirectoryPath<Model> = DirectoryPath()
    ) async throws -> Subspace {
        try await resolveDirectory(for: type, path: path)
    }

    @_spi(Testing)
    public func resolveDirectoryForTesting(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        try await resolveDirectory(for: entity, path: path)
    }

    @_spi(Testing)
    public func installSchemaSnapshotForTesting(
        for version: Schema.Version
    ) async throws {
        try await installSchemaSnapshot(for: version)
    }

    /// Loads the persisted control-domain entity catalog through its canonical
    /// namespace. This SPI exists only for behavioral tests that verify durable
    /// schema publication without exposing storage roots in the public API.
    @_spi(Testing)
    public func persistedControlSchemaEntitiesForTesting()
        async throws -> [Schema.Entity]
    {
        #if DATABASE_MULTI_BASE
        let controlDomain = storageTopology.controlDomain
        let schemaEngine = controlDomain.engine
        let schemaRoot = controlDomain.root
        #else
        let schemaEngine = engine
        let schemaRoot = databaseRoot
        #endif
        return try await SchemaRegistry(
            database: schemaEngine,
            root: schemaRoot,
            clock: monotonicClock
        ).loadAll()
    }
}
