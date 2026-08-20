import DatabaseTypes
import FDBStorage
import Foundation
import FoundationDB
import StorageKit

enum FoundationDBBenchmarkEnvironmentError: Error, LocalizedError {
    case clusterHealthCheckFailed(clusterFile: String?, underlying: Error)

    var errorDescription: String? {
        switch self {
        case .clusterHealthCheckFailed(let clusterFile, let underlying):
            if let clusterFile {
                return "FoundationDB benchmark cluster health check failed for \(clusterFile): \(underlying)"
            }
            return "FoundationDB benchmark cluster health check failed: \(underlying)"
        }
    }
}

actor FoundationDBBenchmarkEnvironment {
    static let shared = FoundationDBBenchmarkEnvironment()

    @TaskLocal private static var holdsExclusiveAccess = false

    private static let transactionTimeoutMilliseconds = 30_000
    private static let transactionRetryLimit = 20
    private static let transactionMaximumRetryDelayMilliseconds = 1_000
    private static let healthCheckAttemptTimeoutMilliseconds = 2_000
    private static let readinessTimeoutMilliseconds = 10_000
    private static let readinessPollIntervalNanoseconds: UInt64 = 250_000_000

    private enum InitializationState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized
        case failed(Error)
    }

    private var initializationState: InitializationState = .uninitialized
    private var selectedClusterFilePath: String?
    private let exclusiveAccess = SerializedBenchmarkAccessGate()

    private init() {}

    func initialize() async throws {
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
        }

        do {
            try await verifyHealthyCluster()
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

    func makeEngine() async throws -> FDBStorageEngine {
        try await initialize()
        return try await createConfiguredEngine()
    }

    func withExclusiveAccess<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await initialize()
        if Self.holdsExclusiveAccess {
            return try await operation()
        }
        return try await exclusiveAccess.withAccess {
            try await self.resetDatabaseConsistencyDomain()
            return try await Self.$holdsExclusiveAccess.withValue(true) {
                try await operation()
            }
        }
    }

    private func candidateClusterFilePaths() -> [String?] {
        let fileManager = FileManager.default
        let environment = ProcessInfo.processInfo.environment

        if let configuredPath = environment["FDB_CLUSTER_FILE"],
           fileManager.fileExists(atPath: configuredPath) {
            return [configuredPath]
        }

        var candidates: [String?] = []
        func appendCandidate(_ path: String) {
            guard !candidates.contains(where: { $0 == path }) else { return }
            candidates.append(path)
        }

        var currentURL = URL(
            fileURLWithPath: fileManager.currentDirectoryPath,
            isDirectory: true
        )
        while true {
            let candidate = currentURL
                .appendingPathComponent(".database/fdb.cluster")
                .path
            if fileManager.fileExists(atPath: candidate) {
                appendCandidate(candidate)
            }
            let parentURL = currentURL.deletingLastPathComponent()
            guard parentURL.path != currentURL.path else { break }
            currentURL = parentURL
        }

        for path in [
            "/usr/local/etc/foundationdb/fdb.cluster",
            "/opt/homebrew/etc/foundationdb/fdb.cluster",
            "/etc/foundationdb/fdb.cluster",
        ] where fileManager.fileExists(atPath: path) {
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
        guard let firstCandidate = candidateClusterFilePaths().first else {
            return nil
        }
        return firstCandidate
    }

    private func openConfiguredDatabase(
        clusterFilePath: String? = nil
    ) throws -> any DatabaseProtocol {
        let database = try FDBClient.openDatabase(
            clusterFilePath: clusterFilePath
                ?? selectedClusterFilePath
                ?? resolvedClusterFilePath()
        )
        try database.setOption(
            to: Self.transactionTimeoutMilliseconds,
            forOption: .transactionTimeout
        )
        try database.setOption(
            to: Self.transactionRetryLimit,
            forOption: .transactionRetryLimit
        )
        try database.setOption(
            to: Self.transactionMaximumRetryDelayMilliseconds,
            forOption: .transactionMaxRetryDelay
        )
        return database
    }

    private func createConfiguredEngine(
        systemPriority: Bool = false,
        clusterFilePath: String? = nil
    ) async throws -> FDBStorageEngine {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }

        let baseDatabase = try openConfiguredDatabase(
            clusterFilePath: clusterFilePath
        )
        let database: any DatabaseProtocol
        if systemPriority {
            database = FDBSystemPriorityDatabase(wrapping: baseDatabase)
        } else {
            database = baseDatabase
        }
        return try await FDBStorageEngine(
            configuration: .init(database: database)
        )
    }

    private func verifyClusterHealth(
        using engine: FDBStorageEngine,
        clusterFilePath: String?
    ) async throws {
        let deadline = Date().addingTimeInterval(
            TimeInterval(Self.readinessTimeoutMilliseconds) / 1_000
        )
        var lastError: Error?

        while Date() < deadline {
            let transaction = try engine.createTransaction()
            let operationError: (any Error)?
            do {
                try transaction.setOption(forOption: .prioritySystemImmediate)
                try transaction.setOption(forOption: .readPriorityHigh)
                try transaction.setOption(
                    to: Self.healthCheckAttemptTimeoutMilliseconds,
                    forOption: .timeout(
                        milliseconds: Self.healthCheckAttemptTimeoutMilliseconds
                    )
                )
                _ = try await transaction.getReadVersion()
                operationError = nil
            } catch {
                operationError = error
            }

            do {
                try await transaction.cancel()
            } catch {
                lastError = operationError.map {
                    StorageTransactionCleanupError(
                        operationError: $0,
                        cancellationError: error
                    )
                } ?? error
                try await Task.sleep(
                    nanoseconds: Self.readinessPollIntervalNanoseconds
                )
                continue
            }

            if let operationError {
                lastError = operationError
                try await Task.sleep(
                    nanoseconds: Self.readinessPollIntervalNanoseconds
                )
            } else {
                return
            }
        }

        throw FoundationDBBenchmarkEnvironmentError
            .clusterHealthCheckFailed(
                clusterFile: clusterFilePath,
                underlying: lastError ?? CancellationError()
            )
    }

    private func verifyHealthyCluster() async throws {
        let candidates = candidateClusterFilePaths()
        var lastError: Error?

        for candidate in candidates {
            let engine: FDBStorageEngine
            do {
                engine = try await createConfiguredEngine(
                    systemPriority: true,
                    clusterFilePath: candidate
                )
            } catch {
                lastError = error
                continue
            }

            do {
                try await verifyClusterHealth(
                    using: engine,
                    clusterFilePath: candidate
                )
                await engine.waitUntilShutdown()
                selectedClusterFilePath = candidate
                return
            } catch {
                await engine.waitUntilShutdown()
                lastError = error
            }
        }

        throw FoundationDBBenchmarkEnvironmentError
            .clusterHealthCheckFailed(
                clusterFile: candidates.compactMap { $0 }.last,
                underlying: lastError ?? CancellationError()
            )
    }

    private func resetDatabaseConsistencyDomain() async throws {
        let engine = try await createConfiguredEngine(systemPriority: true)
        do {
            try await engine.withTransaction { transaction in
                try transaction.clearRange(
                    beginKey: ByteString(),
                    endKey: ByteString([0xFF])
                )
            }
            await engine.waitUntilShutdown()
        } catch {
            await engine.waitUntilShutdown()
            throw error
        }
    }
}
