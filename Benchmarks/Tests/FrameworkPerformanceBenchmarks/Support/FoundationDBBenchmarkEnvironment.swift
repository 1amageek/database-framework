import DatabaseTypes
import Darwin
import FDBStorage
import Foundation
import FoundationDB
import StorageKit

enum FoundationDBBenchmarkEnvironmentError: Error, LocalizedError {
    case missingClusterFile
    case clusterFileDoesNotExist(path: String)
    case harnessIdentityEnvironmentMissing
    case clusterOwnershipMarkerMissing(path: String)
    case clusterOwnershipMarkerInvalid(path: String)
    case clusterOwnershipMarkerMismatch(
        clusterFile: String,
        markerClusterFile: String?
    )
    case clusterEndpointIsNotIsolated(clusterFile: String)
    case clusterHealthCheckFailed(clusterFile: String?, underlying: Error)

    var errorDescription: String? {
        switch self {
        case .missingClusterFile:
            return "FDB_CLUSTER_FILE must identify a cluster owned by "
                + "scripts/apple-container-test-harness foundationdb-run"
        case .clusterFileDoesNotExist(let path):
            return "FoundationDB benchmark cluster file does not exist: \(path)"
        case .harnessIdentityEnvironmentMissing:
            return "FoundationDB benchmark requires the Apple Container "
                + "harness identity path and token"
        case .clusterOwnershipMarkerMissing(let path):
            return "FoundationDB benchmark ownership marker is missing: \(path)"
        case .clusterOwnershipMarkerInvalid(let path):
            return "FoundationDB benchmark ownership marker is invalid: \(path)"
        case .clusterOwnershipMarkerMismatch(
            let clusterFile,
            let markerClusterFile
        ):
            return "FoundationDB benchmark cluster \(clusterFile) does not "
                + "match its ownership marker "
                + (markerClusterFile ?? "<missing>")
        case .clusterEndpointIsNotIsolated(let clusterFile):
            return "FoundationDB benchmark cluster must be the single "
                + "loopback cluster created by the Apple Container harness: "
                + clusterFile
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

    private func requiredOwnedClusterFilePath() throws -> String {
        let fileManager = FileManager.default
        let environment = ProcessInfo.processInfo.environment
        guard let configuredPath = environment["FDB_CLUSTER_FILE"],
              !configuredPath.isEmpty else {
            throw FoundationDBBenchmarkEnvironmentError.missingClusterFile
        }

        let clusterFileURL = URL(fileURLWithPath: configuredPath)
            .standardizedFileURL
            .resolvingSymlinksInPath()
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(
            atPath: clusterFileURL.path,
            isDirectory: &isDirectory
        ), !isDirectory.boolValue else {
            throw FoundationDBBenchmarkEnvironmentError
                .clusterFileDoesNotExist(path: clusterFileURL.path)
        }

        guard let identityPath = environment[
            "DATABASE_FRAMEWORK_FDB_HARNESS_IDENTITY"
        ], !identityPath.isEmpty,
        let expectedToken = environment[
            "DATABASE_FRAMEWORK_FDB_HARNESS_TOKEN"
        ], !expectedToken.isEmpty else {
            throw FoundationDBBenchmarkEnvironmentError
                .harnessIdentityEnvironmentMissing
        }
        let identityURL = URL(fileURLWithPath: identityPath)
            .standardizedFileURL
            .resolvingSymlinksInPath()
        guard fileManager.fileExists(atPath: identityURL.path) else {
            throw FoundationDBBenchmarkEnvironmentError
                .clusterOwnershipMarkerMissing(path: identityURL.path)
        }

        let identity = try String(contentsOf: identityURL, encoding: .utf8)
        var fields: [Substring: Substring] = [:]
        for line in identity.split(whereSeparator: \.isNewline) {
            let components = line.split(
                separator: "=",
                maxSplits: 1,
                omittingEmptySubsequences: false
            )
            guard components.count == 2,
                  !components[0].isEmpty,
                  !components[1].isEmpty,
                  fields.updateValue(
                    components[1],
                    forKey: components[0]
                  ) == nil else {
                throw FoundationDBBenchmarkEnvironmentError
                    .clusterOwnershipMarkerInvalid(path: identityURL.path)
            }
        }
        guard fields["format"]
                == "database-framework-apple-container-foundationdb-v1",
              fields["token"] == Substring(expectedToken),
              let markerClusterFile = fields["cluster_file"],
              let pidText = fields["forwarder_pid"],
              let pid = Int32(pidText),
              pid > 0,
              Darwin.kill(pid, 0) == 0,
              let markerEndpoint = fields["endpoint"],
              markerEndpoint.starts(with: "127.0.0.1:") else {
            throw FoundationDBBenchmarkEnvironmentError
                .clusterOwnershipMarkerInvalid(path: identityURL.path)
        }
        let markerClusterFileURL = URL(
            fileURLWithPath: String(markerClusterFile)
        )
            .standardizedFileURL
            .resolvingSymlinksInPath()
        guard markerClusterFileURL.path == clusterFileURL.path else {
            throw FoundationDBBenchmarkEnvironmentError
                .clusterOwnershipMarkerMismatch(
                    clusterFile: clusterFileURL.path,
                    markerClusterFile: markerClusterFileURL.path
                )
        }

        let clusterDescription = try String(
            contentsOf: clusterFileURL,
            encoding: .utf8
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let coordinators = clusterDescription.split(
            separator: "@",
            maxSplits: 1,
            omittingEmptySubsequences: false
        )
        guard coordinators.count == 2,
              coordinators[1] == markerEndpoint else {
            throw FoundationDBBenchmarkEnvironmentError
                .clusterEndpointIsNotIsolated(
                    clusterFile: clusterFileURL.path
                )
        }
        return clusterFileURL.path
    }

    private func openConfiguredDatabase(
        clusterFilePath: String? = nil
    ) throws -> any DatabaseProtocol {
        let database = try FDBClient.openDatabase(
            clusterFilePath: clusterFilePath
                ?? selectedClusterFilePath
                ?? requiredOwnedClusterFilePath()
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
        clusterFilePath: String? = nil
    ) async throws -> FDBStorageEngine {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }

        let database = try openConfiguredDatabase(
            clusterFilePath: clusterFilePath
        )
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
        let clusterFilePath = try requiredOwnedClusterFilePath()
        let engine = try await createConfiguredEngine(
            clusterFilePath: clusterFilePath
        )
        do {
            try await verifyClusterHealth(
                using: engine,
                clusterFilePath: clusterFilePath
            )
            await engine.waitUntilShutdown()
            selectedClusterFilePath = clusterFilePath
        } catch {
            await engine.waitUntilShutdown()
            throw FoundationDBBenchmarkEnvironmentError
                .clusterHealthCheckFailed(
                    clusterFile: clusterFilePath,
                    underlying: error
                )
        }
    }

    private func resetDatabaseConsistencyDomain() async throws {
        let engine = try await createConfiguredEngine()
        do {
            try await engine.withTransaction { transaction in
                try transaction.setOption(forOption: .prioritySystemImmediate)
                try transaction.setOption(forOption: .readPriorityHigh)
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
