#if !os(WASI)
#if POSTGRESQL
import DatabaseEngine
import DatabaseKit
import PostgreSQLStorage
import StorageKit

extension PostgreSQLStorageEngine.Configuration: DatabaseContainerConfiguration {
    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration]
    ) async throws -> DBConfiguration {
        let engine = try await PostgreSQLStorageEngine(configuration: self)
        let domainID = try DatabaseStorageDomain.ID("primary")
        let placementID = try Base.Placement.ID("default")
        return DBConfiguration(
            storageTopology: try DatabaseStorageTopology(
                controlDomainID: domainID,
                domains: [
                    try DatabaseStorageDomain(
                        id: domainID,
                        namespacePath: ["database", "main"],
                        storageEngine: engine
                    ),
                ],
                placements: [
                    try DatabaseStoragePlacement(
                        id: placementID,
                        domainID: domainID,
                        path: ["bases"]
                    ),
                ],
                defaultPlacementID: placementID
            ),
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            indexConfigurations: indexConfigurations
        )
    }
}
#endif

#endif
