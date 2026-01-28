import Foundation

/// Handles `database start` — starts fdbserver from an existing .database/ directory.
public enum StartCommand {

    public static func run() throws {
        let cwd = FileManager.default.currentDirectoryPath
        let dbDir = (cwd as NSString).appendingPathComponent(LocalCluster.directoryName)

        // 1. Check .database/ exists
        guard FileManager.default.fileExists(atPath: dbDir) else {
            throw CLIError.initializationFailed(
                "No .database directory found. Run 'database init' first."
            )
        }

        // 2. Find cluster file
        let clusterFile = (dbDir as NSString).appendingPathComponent("fdb.cluster")
        guard FileManager.default.fileExists(atPath: clusterFile) else {
            throw CLIError.initializationFailed(
                "Cluster file not found at \(clusterFile). Directory may be corrupted."
            )
        }

        // 3. Check if server is already running
        if let existingPID = LocalCluster.readPID(fromDBDir: dbDir) {
            if LocalCluster.isProcessAlive(existingPID) {
                print("Server is already running (PID: \(existingPID)).")
                return
            }
            print("Cleaning up stale PID file (process \(existingPID) is not running).")
            LocalCluster.removePIDFile(fromDBDir: dbDir)
        }

        // 4. Parse port from cluster file
        guard let port = LocalCluster.parsePort(fromClusterFile: clusterFile) else {
            throw CLIError.initializationFailed(
                "Could not parse port from cluster file: \(clusterFile)"
            )
        }

        // 5. Check port availability
        guard LocalCluster.checkPortAvailable(port) else {
            throw CLIError.portInUse(port)
        }

        // 6. Verify fdbserver exists
        guard LocalCluster.findFDBServer() != nil else {
            throw CLIError.serverNotFound(
                "Install FoundationDB: brew install foundationdb (macOS) or see https://apple.github.io/foundationdb/getting-started-linux.html"
            )
        }

        let dataDir = (dbDir as NSString).appendingPathComponent("data")
        let logDir = (dbDir as NSString).appendingPathComponent("logs")

        // 7. Start fdbserver
        print("Starting fdbserver on port \(port) ...")
        let pid = try LocalCluster.startServer(
            clusterFile: clusterFile,
            dataDir: dataDir,
            logDir: logDir,
            port: port
        )
        print("fdbserver started (PID: \(pid))")

        // 8. Wait for server to be ready
        print("Waiting for server to be ready ...")
        guard LocalCluster.waitForServer(clusterFile: clusterFile) else {
            throw CLIError.initializationFailed(
                "Server did not become ready within timeout. Check \(logDir) for logs."
            )
        }

        // 9. Success (no configure — already done during init)
        print("")
        print("Server started successfully!")
        print("  Cluster file: \(clusterFile)")
        print("  Port:         \(port)")
        print("")
        print("Run 'database' to start the interactive shell.")
        print("Run 'database stop' to stop the server.")
    }
}
