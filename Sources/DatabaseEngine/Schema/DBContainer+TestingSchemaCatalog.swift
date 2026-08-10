import DatabaseKit
import StorageKit

extension DBContainer {
    /// Loads the persisted control-domain entity catalog through its canonical
    /// namespace. This SPI exists only for behavioral tests that verify durable
    /// schema publication without exposing storage roots in the public API.
    @_spi(Testing)
    public func persistedControlSchemaEntitiesForTesting()
        async throws -> [Schema.Entity]
    {
        let controlDomain = storageTopology.controlDomain
        return try await SchemaRegistry(
            database: controlDomain.engine,
            root: controlDomain.root,
            clock: monotonicClock
        ).loadAll()
    }
}
