#if FOUNDATION_DB
import Foundation
import StorageKit
import FDBStorage

/// Resolves cluster file and opens database connection.
///
/// Auto-discovers `.database/fdb.cluster` by walking up from the current directory.
/// Falls back to the system default if no local cluster is found.
public enum ClusterConnection {

    /// Initializes FDB and opens a StorageEngine, auto-discovering a local `.database/fdb.cluster`.
    ///
    /// - Returns: A tuple of the engine and the resolved cluster file path (nil = system default).
    public static func openDatabase() async throws -> (database: any StorageEngine, clusterFile: String?) {
        let clusterFile = LocalCluster.findClusterFile(from: FileManager.default.currentDirectoryPath)
        let database = try await FDBStorageEngine(configuration: .init())
        return (database, clusterFile)
    }
}
#endif
