import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

/// Manages a local FoundationDB cluster under .database/ directory.
public enum LocalCluster {

    public static let directoryName = ".database"
    public static let defaultPort: UInt16 = 4690

    // MARK: - Cluster File Discovery

    /// Walks up from `startPath` looking for `.database/fdb.cluster`.
    /// Returns the path to fdb.cluster if found, nil otherwise.
    public static func findClusterFile(from startPath: String) -> String? {
        var current = URL(fileURLWithPath: startPath).standardized
        let fileManager = FileManager.default
        while true {
            let candidate = current
                .appendingPathComponent(directoryName)
                .appendingPathComponent("fdb.cluster")
            if fileManager.fileExists(atPath: candidate.path) {
                return candidate.path
            }
            let parent = current.deletingLastPathComponent().standardized
            if parent.path == current.path {
                break
            }
            current = parent
        }
        return nil
    }

    // MARK: - Create

    /// Creates the `.database` directory structure and writes `fdb.cluster`.
    /// Returns the path to `fdb.cluster`.
    @discardableResult
    public static func create(at basePath: String, port: UInt16 = defaultPort) throws -> String {
        let fileManager = FileManager.default
        let dbDir = (basePath as NSString).appendingPathComponent(directoryName)

        if fileManager.fileExists(atPath: dbDir) {
            throw CLIError.alreadyInitialized(dbDir)
        }

        let dataDir = (dbDir as NSString).appendingPathComponent("data")
        let logDir = (dbDir as NSString).appendingPathComponent("logs")

        try fileManager.createDirectory(atPath: dataDir, withIntermediateDirectories: true)
        try fileManager.createDirectory(atPath: logDir, withIntermediateDirectories: true)

        let clusterID = generateClusterID()
        let clusterContent = "local:\(clusterID)@127.0.0.1:\(port)"
        let clusterFile = (dbDir as NSString).appendingPathComponent("fdb.cluster")
        try clusterContent.write(toFile: clusterFile, atomically: true, encoding: .utf8)

        return clusterFile
    }

    // MARK: - Start Server

    /// Launches fdbserver in the background. Returns the PID.
    @discardableResult
    public static func startServer(
        clusterFile: String,
        dataDir: String,
        logDir: String,
        port: UInt16 = defaultPort
    ) throws -> Int32 {
        guard let serverPath = findFDBServer() else {
            throw CLIError.serverNotFound(
                "Install FoundationDB: brew install foundationdb (macOS) or see https://apple.github.io/foundationdb/getting-started-linux.html"
            )
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: serverPath)
        process.arguments = [
            "-p", "auto:\(port)",
            "-C", clusterFile,
            "-d", dataDir,
            "-L", logDir,
        ]
        // Detach stdout/stderr to avoid blocking
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice

        try process.run()

        let pid = process.processIdentifier
        // Write PID file
        let dbDir = (clusterFile as NSString).deletingLastPathComponent
        let pidFile = (dbDir as NSString).appendingPathComponent("fdb.pid")
        try "\(pid)".write(toFile: pidFile, atomically: true, encoding: .utf8)

        return pid
    }

    // MARK: - Stop Server

    /// Stops the fdbserver by reading the PID file.
    public static func stopServer(at basePath: String) throws {
        let dbDir = (basePath as NSString).appendingPathComponent(directoryName)

        guard let pid = readPID(fromDBDir: dbDir) else {
            throw CLIError.initializationFailed("No running server found (no PID file in \(dbDir))")
        }

        if isProcessAlive(pid) {
            kill(pid, SIGTERM)
        }

        removePIDFile(fromDBDir: dbDir)
    }

    // MARK: - Port Availability

    /// Checks whether a TCP port is available by attempting to bind a socket to 127.0.0.1:<port>.
    public static func checkPortAvailable(_ port: UInt16) -> Bool {
        let sock = socket(AF_INET, SOCK_STREAM, 0)
        guard sock >= 0 else { return false }
        defer { close(sock) }

        var opt: Int32 = 1
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        addr.sin_addr.s_addr = inet_addr("127.0.0.1")

        let result = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockPtr in
                bind(sock, sockPtr, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        return result == 0
    }

    // MARK: - Process Check

    /// Checks whether a process with the given PID is currently running.
    public static func isProcessAlive(_ pid: Int32) -> Bool {
        kill(pid, 0) == 0
    }

    // MARK: - PID File

    /// Reads the PID from the PID file in the given .database directory.
    ///
    /// Returns `nil` if the file doesn't exist or contains invalid data.
    /// This is intentional as PID file absence indicates no running process.
    public static func readPID(fromDBDir dbDir: String) -> Int32? {
        let pidFile = (dbDir as NSString).appendingPathComponent("fdb.pid")
        let pidString: String
        do {
            pidString = try String(contentsOfFile: pidFile, encoding: .utf8)
                .trimmingCharacters(in: .whitespacesAndNewlines)
        } catch {
            // File doesn't exist or can't be read - no process running
            return nil
        }
        guard let pid = Int32(pidString) else {
            // Invalid PID format - treat as no valid process
            return nil
        }
        return pid
    }

    /// Removes the PID file if present.
    public static func removePIDFile(fromDBDir dbDir: String) {
        let pidFile = (dbDir as NSString).appendingPathComponent("fdb.pid")
        try? FileManager.default.removeItem(atPath: pidFile)
    }

    // MARK: - Cluster File Parsing

    /// Parses the port number from a cluster file.
    /// Expected format: `local:<clusterID>@127.0.0.1:<port>`
    ///
    /// Returns `nil` if the file doesn't exist or has invalid format.
    /// This is intentional as cluster file absence indicates no running cluster.
    public static func parsePort(fromClusterFile path: String) -> UInt16? {
        let content: String
        do {
            content = try String(contentsOfFile: path, encoding: .utf8)
                .trimmingCharacters(in: .whitespacesAndNewlines)
        } catch {
            // File doesn't exist or can't be read - no cluster configured
            return nil
        }
        guard let colonIndex = content.lastIndex(of: ":") else {
            // Invalid format - missing port separator
            return nil
        }
        return UInt16(content[content.index(after: colonIndex)...])
    }

    // MARK: - Configure Database

    /// Runs `fdbcli --exec "configure new single ssd"` to initialize a new database.
    public static func configureDatabase(clusterFile: String) throws {
        guard let cliPath = findFDBCli() else {
            throw CLIError.serverNotFound(
                "fdbcli not found. Install FoundationDB: brew install foundationdb"
            )
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: cliPath)
        process.arguments = [
            "-C", clusterFile,
            "--exec", "configure new single ssd",
        ]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice

        try process.run()
        process.waitUntilExit()

        guard process.terminationStatus == 0 else {
            throw CLIError.initializationFailed(
                "fdbcli configure failed with exit code \(process.terminationStatus)"
            )
        }
    }

    // MARK: - Executable Search

    /// Searches for fdbserver in common installation paths and PATH.
    public static func findFDBServer() -> String? {
        findExecutable(
            name: "fdbserver",
            knownPaths: [
                "/usr/local/libexec/fdbserver",
                "/opt/homebrew/libexec/fdbserver",
                "/usr/sbin/fdbserver",
                "/usr/local/sbin/fdbserver",
            ]
        )
    }

    /// Searches for fdbcli in common installation paths and PATH.
    public static func findFDBCli() -> String? {
        findExecutable(
            name: "fdbcli",
            knownPaths: [
                "/usr/local/bin/fdbcli",
                "/opt/homebrew/bin/fdbcli",
                "/usr/bin/fdbcli",
            ]
        )
    }

    // MARK: - Wait for Server

    /// Waits until fdbserver is accepting connections (polling fdbcli status).
    public static func waitForServer(clusterFile: String, timeoutSeconds: Int = 10) -> Bool {
        guard let cliPath = findFDBCli() else { return false }

        for _ in 0..<timeoutSeconds {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: cliPath)
            process.arguments = ["-C", clusterFile, "--exec", "status minimal"]
            process.standardOutput = FileHandle.nullDevice
            process.standardError = FileHandle.nullDevice

            do {
                try process.run()
                process.waitUntilExit()
                if process.terminationStatus == 0 {
                    return true
                }
            } catch {
                // Ignore and retry
            }

            Thread.sleep(forTimeInterval: 1.0)
        }
        return false
    }

    // MARK: - Private

    private static func generateClusterID() -> String {
        let chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0..<8).map { _ in chars.randomElement()! })
    }

    private static func findExecutable(name: String, knownPaths: [String]) -> String? {
        let fileManager = FileManager.default

        // Check known paths first
        for path in knownPaths {
            if fileManager.isExecutableFile(atPath: path) {
                return path
            }
        }

        // Search PATH
        if let pathEnv = ProcessInfo.processInfo.environment["PATH"] {
            for dir in pathEnv.split(separator: ":") {
                let candidate = "\(dir)/\(name)"
                if fileManager.isExecutableFile(atPath: candidate) {
                    return candidate
                }
            }
        }

        return nil
    }
}
