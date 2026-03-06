import Foundation
import StorageKit

/// Resolves cluster file and opens database connection.
///
/// Auto-discovers `.database/fdb.cluster` by walking up from the current directory.
/// Falls back to the system default if no local cluster is found.
public enum ClusterConnection {

    /// Initializes and opens a StorageEngine, auto-discovering a local `.database/fdb.cluster`.
    ///
    /// - Parameter engineFactory: Factory closure that creates a StorageEngine.
    ///   For FDB, use `FDBStorageEngine.open()`.
    /// - Returns: A tuple of the engine and the resolved cluster file path (nil = system default).
    public static func openDatabase(engineFactory: () async throws -> any StorageEngine) async throws -> (database: any StorageEngine, clusterFile: String?) {
        let clusterFile = LocalCluster.findClusterFile(from: FileManager.default.currentDirectoryPath)
        let database = try await engineFactory()
        return (database, clusterFile)
    }
}
