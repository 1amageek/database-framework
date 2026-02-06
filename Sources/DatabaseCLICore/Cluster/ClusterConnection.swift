import Foundation
import FoundationDB

/// Resolves cluster file and opens database connection.
///
/// Auto-discovers `.database/fdb.cluster` by walking up from the current directory.
/// Falls back to the system default if no local cluster is found.
public enum ClusterConnection {

    /// Initializes FDB and opens a database, auto-discovering a local `.database/fdb.cluster`.
    ///
    /// - Returns: A tuple of the database and the resolved cluster file path (nil = system default).
    public static func openDatabase() async throws -> (database: FDBDatabase, clusterFile: String?) {
        try await FDBClient.initialize()
        let clusterFile = LocalCluster.findClusterFile(from: FileManager.default.currentDirectoryPath)
        let database = try FDBClient.openDatabase(clusterFilePath: clusterFile)
        return (database, clusterFile)
    }
}
