// FoundationDBScenarioCoordinator.swift
// Coordinates FoundationDB initialization and serialized scenario access.

#if FOUNDATION_DB
import Foundation
import FoundationDB
import StorageKit
import FDBStorage
import DatabaseEngine

public enum FoundationDBScenarioInitializationError: Error, LocalizedError {
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

/// Coordinates FoundationDB initialization and serialized scenario access.
///
/// This actor ensures:
/// 1. FDB client is initialized exactly once (via FDBStorageEngine.init)
/// 2. FoundationDB scenarios run serially to prevent version conflicts
///
/// **Usage**:
/// ```swift
/// @Test func foundationDBOperationIsSerialized() async throws {
///     try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
///         // FoundationDB scenario operations run here.
///     }
/// }
/// ```
public actor FoundationDBScenarioCoordinator {
    public static let shared = FoundationDBScenarioCoordinator()
    private static let transactionTimeoutMs = 30_000
    private static let transactionRetryLimit = 20
    private static let transactionMaxRetryDelayMs = 1_000
    private static let healthCheckAttemptTimeoutMs = 2_000
    private static let clusterReadyTimeoutMs = 10_000
    private static let clusterReadyPollIntervalNs: UInt64 = 250_000_000

    private enum InitializationState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized
        case failed(Error)
    }

    private var initializationState: InitializationState = .uninitialized
    private var selectedClusterFilePath: String?

    /// Access requests waiting for the current scenario to release exclusivity.
    private var waitingAccessRequests: [CheckedContinuation<Void, Never>] = []

    /// Whether one scenario currently holds exclusive database access.
    private var isAccessHeld: Bool = false

    private init() {}

    private func candidateClusterFilePaths() -> [String?] {
        let fileManager = FileManager.default
        let environment = ProcessInfo.processInfo.environment

        if let configuredPath = environment["FDB_CLUSTER_FILE"],
           fileManager.fileExists(atPath: configuredPath) {
            return [configuredPath]
        }

        var candidates: [String?] = []
        func appendCandidate(_ path: String) {
            guard !candidates.contains(where: { $0 == path }) else {
                return
            }
            candidates.append(path)
        }

        var currentURL = URL(fileURLWithPath: fileManager.currentDirectoryPath, isDirectory: true)
        while true {
            let candidate = currentURL.appendingPathComponent(".database/fdb.cluster").path
            if fileManager.fileExists(atPath: candidate) {
                appendCandidate(candidate)
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

        for path in commonClusterFiles where fileManager.fileExists(atPath: path) {
            appendCandidate(path)
        }

        if candidates.isEmpty {
            candidates.append(nil)
        }
        return candidates
    }

    private func resolvedClusterFilePath() -> String? {
        if let selectedClusterFilePath {
            return selectedClusterFilePath
        }
        let candidates = candidateClusterFilePaths()
        guard let first = candidates.first else {
            return nil
        }
        return first
    }

    private func openConfiguredDatabase(clusterFilePath: String? = nil) throws -> any DatabaseProtocol {
        let database = try FDBClient.openDatabase(
            clusterFilePath: clusterFilePath ?? selectedClusterFilePath ?? resolvedClusterFilePath()
        )
        try database.setOption(to: Self.transactionTimeoutMs, forOption: .transactionTimeout)
        try database.setOption(to: Self.transactionRetryLimit, forOption: .transactionRetryLimit)
        try database.setOption(to: Self.transactionMaxRetryDelayMs, forOption: .transactionMaxRetryDelay)
        return database
    }

    private func createConfiguredEngine(
        systemPriority: Bool = false,
        clusterFilePath: String? = nil
    ) async throws -> FDBStorageEngine {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }

        let baseDatabase = try openConfiguredDatabase(clusterFilePath: clusterFilePath)
        let database: any DatabaseProtocol
        if systemPriority {
            database = FDBSystemPriorityDatabase(wrapping: baseDatabase)
        } else {
            database = baseDatabase
        }
        return try await FDBStorageEngine(configuration: .init(database: database))
    }

    private func verifyClusterHealth(
        using engine: FDBStorageEngine,
        clusterFilePath: String?
    ) async throws {
        let deadline = Date().addingTimeInterval(TimeInterval(Self.clusterReadyTimeoutMs) / 1_000)
        var lastError: Error?

        while Date() < deadline {
            let transaction = try engine.createTransaction()
            let operationError: (any Error)?
            do {
                try transaction.setOption(forOption: .prioritySystemImmediate)
                try transaction.setOption(forOption: .readPriorityHigh)
                try transaction.setOption(
                    to: Self.healthCheckAttemptTimeoutMs,
                    forOption: .timeout(milliseconds: Self.healthCheckAttemptTimeoutMs)
                )
                _ = try await transaction.getReadVersion()
                operationError = nil
            } catch {
                operationError = error
            }

            do {
                try await transaction.cancel()
            } catch {
                if let operationError {
                    lastError = StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                } else {
                    lastError = error
                }
                try await Task.sleep(nanoseconds: Self.clusterReadyPollIntervalNs)
                continue
            }
            if let operationError {
                lastError = operationError
                try await Task.sleep(nanoseconds: Self.clusterReadyPollIntervalNs)
            } else {
                return
            }
        }

        throw FoundationDBScenarioInitializationError.clusterHealthCheckFailed(
            clusterFile: clusterFilePath,
            underlying: lastError ?? CancellationError()
        )
    }

    private func createHealthyEngine() async throws -> FDBStorageEngine {
        let candidates = candidateClusterFilePaths()
        var lastError: Error?

        for candidate in candidates {
            do {
                let engine = try await createConfiguredEngine(
                    systemPriority: true,
                    clusterFilePath: candidate
                )
                try await verifyClusterHealth(using: engine, clusterFilePath: candidate)
                selectedClusterFilePath = candidate
                return engine
            } catch {
                lastError = error
            }
        }

        throw FoundationDBScenarioInitializationError.clusterHealthCheckFailed(
            clusterFile: candidates.compactMap { $0 }.last,
            underlying: lastError ?? CancellationError()
        )
    }

    /// Initialize FDB client (called automatically by withSerializedAccess)
    public func initialize() async throws {
        switch initializationState {
        case .initialized:
            return

        case .failed(let error):
            throw error

        case .initializing(var continuations):
            return try await withCheckedThrowingContinuation { continuation in
                continuations.append(continuation)
                initializationState = .initializing(continuations)
            }

        case .uninitialized:
            initializationState = .initializing([])

            do {
                _ = try await createHealthyEngine()
                try await resetDatabaseConsistencyDomain()
                if case .initializing(let continuations) = initializationState {
                    initializationState = .initialized
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    initializationState = .initialized
                }
            } catch {
                if case .initializing(let continuations) = initializationState {
                    initializationState = .failed(error)
                    for continuation in continuations {
                        continuation.resume(throwing: error)
                    }
                } else {
                    initializationState = .failed(error)
                }
                throw error
            }
        }
    }

    public func makeEngine() async throws -> FDBStorageEngine {
        try await initialize()
        return try await createConfiguredEngine()
    }

    /// Clears the complete user keyspace owned by the isolated test cluster.
    ///
    /// Directory metadata, records, indexes, the schema catalog, and the
    /// database format descriptor form one consistency domain. Clearing only
    /// selected directories can leave a non-empty database without its format
    /// descriptor, so initialization must reset that domain atomically and
    /// surface any failure before tests run.
    private func resetDatabaseConsistencyDomain() async throws {
        let engine = try await createConfiguredEngine(systemPriority: true)
        try await engine.withTransaction { transaction in
            try transaction.clearRange(
                beginKey: Bytes(),
                endKey: Bytes([0xFF])
            )
        }
    }

    /// Execute a test with serialized FDB access
    ///
    /// This ensures only one FoundationDB scenario runs at a time,
    /// preventing "Version not valid" errors from parallel execution.
    ///
    /// - Parameter operation: The database operation to execute
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
        while isAccessHeld {
            await withCheckedContinuation { continuation in
                waitingAccessRequests.append(continuation)
            }
        }
        isAccessHeld = true
    }

    /// Release access and resume the next waiting request.
    private func releaseAccess() {
        isAccessHeld = false
        if !waitingAccessRequests.isEmpty {
            let nextRequest = waitingAccessRequests.removeFirst()
            nextRequest.resume()
        }
    }
}
#endif
