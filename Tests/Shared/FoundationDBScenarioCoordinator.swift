// FoundationDBScenarioCoordinator.swift
// Coordinates FoundationDB initialization and serialized scenario access.
// TEST_TIME_SEMANTICS: correctness - bounds real service readiness checks.

#if FOUNDATION_DB
import DatabaseTypes
import Foundation
import StorageKit
import FDBStorage

public enum FoundationDBScenarioInitializationError: Error, LocalizedError {
    case missingClusterFile
    case clusterFileDoesNotExist(path: String)
    case harnessIdentityEnvironmentMissing
    case clusterOwnershipMarkerMissing(path: String)
    case clusterOwnershipMarkerInvalid(path: String)
    case clusterOwnershipMarkerMismatch(clusterFile: String, markerClusterFile: String?)
    case clusterEndpointIsNotIsolated(clusterFile: String)
    case clusterHealthCheckFailed(clusterFile: String?, underlying: Error)

    public var errorDescription: String? {
        switch self {
        case .missingClusterFile:
            return "FDB_CLUSTER_FILE must identify a cluster owned by "
                + "scripts/docker-test-harness foundationdb"
        case .clusterFileDoesNotExist(let path):
            return "FoundationDB test cluster file does not exist: \(path)"
        case .harnessIdentityEnvironmentMissing:
            return "FoundationDB tests require the Docker harness identity path and token"
        case .clusterOwnershipMarkerMissing(let path):
            return "FoundationDB test ownership marker is missing: \(path)"
        case .clusterOwnershipMarkerInvalid(let path):
            return "FoundationDB test ownership marker is invalid: \(path)"
        case .clusterOwnershipMarkerMismatch(let clusterFile, let markerClusterFile):
            return "FoundationDB test cluster \(clusterFile) does not match its ownership marker "
                + (markerClusterFile ?? "<missing>")
        case .clusterEndpointIsNotIsolated(let clusterFile):
            return "FoundationDB tests require the private cluster created by the Docker harness: "
                + clusterFile
        case .clusterHealthCheckFailed(let clusterFile, let underlying):
            if let clusterFile {
                return "FoundationDB cluster health check failed for \(clusterFile): \(underlying)"
            }
            return "FoundationDB cluster health check failed: \(underlying)"
        }
    }
}

public enum FoundationDBScenarioAccessError: Error, LocalizedError, Equatable {
    case engineRequestedOutsideScenario
    case scenarioEndedDuringEngineCreation

    public var errorDescription: String? {
        switch self {
        case .engineRequestedOutsideScenario:
            return "FoundationDB test engines are available only inside withSerializedAccess."
        case .scenarioEndedDuringEngineCreation:
            return "The FoundationDB scenario ended while creating an engine."
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
    @TaskLocal private static var holdsSerializedAccess = false
    @TaskLocal private static var scenarioResourceOwner: ScenarioResourceOwner?
    private static let transactionTimeoutMs = 30_000
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
    private let serializedAccess = SerializedScenarioAccessGate()

    private init() {}

    private func requiredOwnedClusterFilePath() throws -> String {
        let fileManager = FileManager.default
        let environment = ProcessInfo.processInfo.environment
        guard let configuredPath = environment["FDB_CLUSTER_FILE"],
              !configuredPath.isEmpty else {
            throw FoundationDBScenarioInitializationError.missingClusterFile
        }
        let clusterFileURL = URL(fileURLWithPath: configuredPath)
            .standardizedFileURL
            .resolvingSymlinksInPath()
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(
            atPath: clusterFileURL.path,
            isDirectory: &isDirectory
        ), !isDirectory.boolValue else {
            throw FoundationDBScenarioInitializationError
                .clusterFileDoesNotExist(path: clusterFileURL.path)
        }
        guard let identityPath = environment["DATABASE_FRAMEWORK_FDB_HARNESS_IDENTITY"],
              !identityPath.isEmpty,
              let expectedToken = environment["DATABASE_FRAMEWORK_FDB_HARNESS_TOKEN"],
              !expectedToken.isEmpty else {
            throw FoundationDBScenarioInitializationError
                .harnessIdentityEnvironmentMissing
        }
        let identityURL = URL(fileURLWithPath: identityPath)
            .standardizedFileURL
            .resolvingSymlinksInPath()
        guard fileManager.fileExists(atPath: identityURL.path) else {
            throw FoundationDBScenarioInitializationError
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
                  fields.updateValue(components[1], forKey: components[0]) == nil else {
                throw FoundationDBScenarioInitializationError
                    .clusterOwnershipMarkerInvalid(path: identityURL.path)
            }
        }
        guard fields["format"] == "database-framework-docker-foundationdb-v1",
              fields["token"] == Substring(expectedToken),
              let markerClusterFile = fields["cluster_file"],
              let markerEndpoint = fields["endpoint"],
              markerEndpoint.hasSuffix(":4500"),
              !markerEndpoint.starts(with: "127."),
              !markerEndpoint.starts(with: "localhost:"),
              fields["network"] != nil,
              fields["service_container"] != nil,
              fields["server_version"] != nil else {
            throw FoundationDBScenarioInitializationError
                .clusterOwnershipMarkerInvalid(path: identityURL.path)
        }
        let markerClusterFileURL = URL(fileURLWithPath: String(markerClusterFile))
            .standardizedFileURL
            .resolvingSymlinksInPath()
        guard markerClusterFileURL.path == clusterFileURL.path else {
            throw FoundationDBScenarioInitializationError
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
            throw FoundationDBScenarioInitializationError
                .clusterEndpointIsNotIsolated(clusterFile: clusterFileURL.path)
        }
        return clusterFileURL.path
    }

    private func createConfiguredEngine(
        systemPriority: Bool = false,
        clusterFilePath: String? = nil
    ) async throws -> FDBStorageEngine {
        let selectedPath: String
        if let clusterFilePath {
            selectedPath = clusterFilePath
        } else if let selectedClusterFilePath {
            selectedPath = selectedClusterFilePath
        } else {
            selectedPath = try requiredOwnedClusterFilePath()
        }
        var transactionOptions: [TransactionOption] = [
            .timeout(milliseconds: Self.transactionTimeoutMs)
        ]
        if systemPriority {
            transactionOptions.append(.prioritySystemImmediate)
            transactionOptions.append(.readPriorityHigh)
        }
        return try await FDBStorageEngine(
            configuration: .init(
                clusterFilePath: selectedPath,
                transactionOptions: transactionOptions
            )
        )
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

    private func verifyHealthyCluster() async throws {
        let clusterFilePath = try requiredOwnedClusterFilePath()
        let engine = try await createConfiguredEngine(
            systemPriority: true,
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
            throw FoundationDBScenarioInitializationError.clusterHealthCheckFailed(
                clusterFile: clusterFilePath,
                underlying: error
            )
        }
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
    }

    public func makeEngine() async throws -> FDBStorageEngine {
        try await makeScenarioEngine(systemPriority: false)
    }

    public func makeSystemPriorityEngine() async throws -> FDBStorageEngine {
        try await makeScenarioEngine(systemPriority: true)
    }

    private func makeScenarioEngine(
        systemPriority: Bool
    ) async throws -> FDBStorageEngine {
        guard let owner = Self.scenarioResourceOwner else {
            throw FoundationDBScenarioAccessError.engineRequestedOutsideScenario
        }
        try await initialize()
        let engine = try await createConfiguredEngine(systemPriority: systemPriority)
        guard await owner.register(
            engine,
            shutdown: { engine in await engine.waitUntilShutdown() }
        ) else {
            await engine.waitUntilShutdown()
            throw FoundationDBScenarioAccessError.scenarioEndedDuringEngineCreation
        }
        return engine
    }

    /// Clears the complete user keyspace owned by the isolated test cluster.
    ///
    /// Directory metadata, entities, indexes, the schema catalog, and the
    /// database format descriptor form one consistency domain. Clearing only
    /// selected directories can leave a non-empty database without its format
    /// descriptor, so initialization must reset that domain atomically and
    /// surface any failure before tests run.
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
        try await initialize()
        if Self.holdsSerializedAccess {
            return try await operation()
        }
        return try await serializedAccess.withAccess {
            try await self.resetDatabaseConsistencyDomain()
            let resourceOwner = ScenarioResourceOwner()
            do {
                let result = try await Self.$holdsSerializedAccess.withValue(true) {
                    try await Self.$scenarioResourceOwner.withValue(resourceOwner) {
                        try await operation()
                    }
                }
                await resourceOwner.shutdownAll()
                return result
            } catch {
                await resourceOwner.shutdownAll()
                throw error
            }
        }
    }
}
#endif
