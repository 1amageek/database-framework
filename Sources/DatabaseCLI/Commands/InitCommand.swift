import Foundation

/// Handles `database init` — creates a local .database/ cluster.
public enum InitCommand {

    public static func run(port: UInt16 = LocalCluster.defaultPort) throws {
        let cwd = FileManager.default.currentDirectoryPath
        let dbDir = (cwd as NSString).appendingPathComponent(LocalCluster.directoryName)

        // 1. Check if already initialized
        if FileManager.default.fileExists(atPath: dbDir) {
            throw CLIError.alreadyInitialized(dbDir)
        }

        // 2. Verify fdbserver exists
        guard LocalCluster.findFDBServer() != nil else {
            throw CLIError.serverNotFound(
                "Install FoundationDB: brew install foundationdb (macOS) or see https://apple.github.io/foundationdb/getting-started-linux.html"
            )
        }

        // 3. Check port availability
        guard LocalCluster.checkPortAvailable(port) else {
            throw CLIError.portInUse(port)
        }

        print("Initializing local database in \(dbDir) ...")

        // 4. Create directory structure and cluster file
        let clusterFile = try LocalCluster.create(at: cwd, port: port)

        let dataDir = (dbDir as NSString).appendingPathComponent("data")
        let logDir = (dbDir as NSString).appendingPathComponent("logs")

        // 4. Start fdbserver
        print("Starting fdbserver on port \(port) ...")
        let pid = try LocalCluster.startServer(
            clusterFile: clusterFile,
            dataDir: dataDir,
            logDir: logDir,
            port: port
        )
        print("fdbserver started (PID: \(pid))")

        // 5. Wait for server to be ready
        print("Waiting for server to be ready ...")
        guard LocalCluster.waitForServer(clusterFile: clusterFile) else {
            throw CLIError.initializationFailed(
                "Server did not become ready within timeout. Check \(logDir) for logs."
            )
        }

        // 6. Configure new database
        print("Configuring database ...")
        try LocalCluster.configureDatabase(clusterFile: clusterFile)

        // 7. Success
        print("")
        print("Database initialized successfully!")
        print("  Cluster file: \(clusterFile)")
        print("  Data dir:     \(dataDir)")
        print("  Log dir:      \(logDir)")
        print("")
        print("Run 'database' to start the interactive shell.")
        print("Run 'database stop' to stop the server.")
    }
}
