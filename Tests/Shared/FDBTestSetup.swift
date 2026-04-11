// FDBTestSetup.swift
// Shared FDB initialization and serialization for all test targets

#if FOUNDATION_DB
import Foundation
import FoundationDB
import StorageKit
import FDBStorage
import DatabaseEngine

public enum FDBTestSetupError: Error, LocalizedError {
    case clusterHealthCheckFailed(clusterFile: String?, underlying: Error)

    public var errorDescription: String? {
        switch self {
        case .clusterHealthCheckFailed(let clusterFile, let underlying):
            if let clusterFile {
                return "FoundationDB cluster health check failed for \(clusterFile): \(underlying)"
            }
            return "FoundationDB cluster health check failed: \(underlying)"
        }
    }
}

/// Shared FDB initialization and test serialization singleton
///
/// This actor ensures:
/// 1. FDB client is initialized exactly once (via FDBStorageEngine.init)
/// 2. FDB tests run serially to prevent version conflicts
///
/// **Usage**:
/// ```swift
/// @Test func myFDBTest() async throws {
///     try await FDBTestSetup.shared.withSerializedAccess {
///         // Your FDB test code here
///     }
/// }
/// ```
public actor FDBTestSetup {
    public static let shared = FDBTestSetup()
    private static let transactionTimeoutMs = 10_000
    private static let transactionRetryLimit = 1
    private static let transactionMaxRetryDelayMs = 100
    private static let healthCheckAttemptTimeoutMs = 5_000
    private static let clusterReadyTimeoutMs = 30_000
    private static let clusterReadyPollIntervalNs: UInt64 = 250_000_000

    private enum InitState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized
        case failed(Error)
    }

    private var initState: InitState = .uninitialized
    private var didCleanupTestDirectories: Bool = false

    /// Queue of waiting test continuations for serialization
    private var waitingTests: [CheckedContinuation<Void, Never>] = []

    /// Whether a test is currently running
    private var isTestRunning: Bool = false

    private init() {}

    private func resolvedClusterFilePath() -> String? {
        let fileManager = FileManager.default
        let environment = ProcessInfo.processInfo.environment

        if let configuredPath = environment["FDB_CLUSTER_FILE"],
           fileManager.fileExists(atPath: configuredPath) {
            return configuredPath
        }

        var currentURL = URL(fileURLWithPath: fileManager.currentDirectoryPath, isDirectory: true)
        while true {
            let candidate = currentURL.appendingPathComponent(".database/fdb.cluster").path
            if fileManager.fileExists(atPath: candidate) {
                return candidate
            }

            let parentURL = currentURL.deletingLastPathComponent()
            guard parentURL.path != currentURL.path else { break }
            currentURL = parentURL
        }

        let commonClusterFiles = [
            "/usr/local/etc/foundationdb/fdb.cluster",
            "/opt/homebrew/etc/foundationdb/fdb.cluster",
            "/etc/foundationdb/fdb.cluster",
        ]

        return commonClusterFiles.first(where: fileManager.fileExists(atPath:))
    }

    private func openConfiguredDatabase() throws -> any DatabaseProtocol {
        let database = try FDBClient.openDatabase(clusterFilePath: resolvedClusterFilePath())
        try database.setOption(to: Self.transactionTimeoutMs, forOption: .transactionTimeout)
        try database.setOption(to: Self.transactionRetryLimit, forOption: .transactionRetryLimit)
        try database.setOption(to: Self.transactionMaxRetryDelayMs, forOption: .transactionMaxRetryDelay)
        return database
    }

    private func createConfiguredEngine() async throws -> FDBStorageEngine {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }

        let database = try openConfiguredDatabase()
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func verifyClusterHealth(using engine: FDBStorageEngine) async throws {
        let deadline = Date().addingTimeInterval(TimeInterval(Self.clusterReadyTimeoutMs) / 1_000)
        var lastError: Error?

        while Date() < deadline {
            let transaction = try engine.createTransaction()

            do {
                try transaction.setOption(
                    to: Self.healthCheckAttemptTimeoutMs,
                    forOption: .timeout(milliseconds: Self.healthCheckAttemptTimeoutMs)
                )
                _ = try await transaction.getReadVersion()
                transaction.cancel()
                return
            } catch {
                transaction.cancel()
                lastError = error
                try await Task.sleep(nanoseconds: Self.clusterReadyPollIntervalNs)
            }
        }

        throw FDBTestSetupError.clusterHealthCheckFailed(
            clusterFile: resolvedClusterFilePath(),
            underlying: lastError ?? CancellationError()
        )
    }

    /// Initialize FDB client (called automatically by withSerializedAccess)
    public func initialize() async throws {
        switch initState {
        case .initialized:
            if !didCleanupTestDirectories {
                await cleanupTestDirectoriesBestEffort()
            }
            return

        case .failed(let error):
            throw error

        case .initializing(var continuations):
            return try await withCheckedThrowingContinuation { continuation in
                continuations.append(continuation)
                initState = .initializing(continuations)
            }

        case .uninitialized:
            initState = .initializing([])

            do {
                let engine = try await createConfiguredEngine()
                try await verifyClusterHealth(using: engine)
                await cleanupTestDirectoriesBestEffort()
                if case .initializing(let continuations) = initState {
                    initState = .initialized
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    initState = .initialized
                }
            } catch {
                if case .initializing(let continuations) = initState {
                    initState = .failed(error)
                    for continuation in continuations {
                        continuation.resume(throwing: error)
                    }
                } else {
                    initState = .failed(error)
                }
                throw error
            }
        }
    }

    public func makeEngine() async throws -> FDBStorageEngine {
        try await initialize()
        return try await createConfiguredEngine()
    }

    /// Best-effort cleanup for stale local test data.
    private func cleanupTestDirectoriesBestEffort() async {
        guard !didCleanupTestDirectories else { return }
        didCleanupTestDirectories = true

        do {
            let engine = try await createConfiguredEngine()
            try await engine.directoryService.remove(path: ["test"])
        } catch {
            // Ignore cleanup failures; tests will surface real issues.
        }
    }

    /// Execute a test with serialized FDB access
    ///
    /// This ensures only one FDB test runs at a time across all test suites,
    /// preventing "Version not valid" errors from parallel execution.
    ///
    /// - Parameter operation: The test operation to execute
    /// - Returns: The result of the operation
    public func withSerializedAccess<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        // Ensure FDB is initialized
        try await initialize()

        // Wait for our turn (acquires lock)
        await acquireAccess()

        do {
            let result = try await operation()
            releaseAccess()
            return result
        } catch {
            releaseAccess()
            throw error
        }
    }

    /// Acquire exclusive access for a test
    private func acquireAccess() async {
        while isTestRunning {
            await withCheckedContinuation { continuation in
                waitingTests.append(continuation)
            }
        }
        isTestRunning = true
    }

    /// Release access and wake next waiting test
    private func releaseAccess() {
        isTestRunning = false
        if !waitingTests.isEmpty {
            let next = waitingTests.removeFirst()
            next.resume()
        }
    }
}
#endif
