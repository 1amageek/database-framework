#if !os(WASI)
#if FOUNDATION_DB && !DATABASE_MULTI_BASE
import DatabaseEngine
import FDBStorage
import StorageKit

/// Explicit FoundationDB composition for one ordinary database Directory.
public struct FDBDatabaseConfiguration: DatabaseContainerConfiguration {
    public let storage: FDBStorageEngine.Configuration
    public let directoryPath: [String]

    public init(
        storage: FDBStorageEngine.Configuration,
        directoryPath: [String]
    ) throws(FDBDatabaseConfigurationError) {
        guard !directoryPath.isEmpty else {
            throw .emptyDirectoryPath
        }
        for (index, component) in directoryPath.enumerated()
        where component.isEmpty {
            throw .emptyDirectoryComponent(index: index)
        }
        self.storage = storage
        self.directoryPath = directoryPath
    }

    public func makeDBConfiguration(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock
    ) async throws -> DBConfiguration {
        let engine = try await FDBStorageEngine(configuration: storage)
        do {
            return try DBConfiguration(
                storageEngine: engine,
                databaseRootPath: directoryPath,
                monotonicClock: monotonicClock,
                wallClock: wallClock
            )
        } catch {
            engine.requestShutdown()
            await engine.waitUntilShutdown()
            throw error
        }
    }
}
#endif
#endif
