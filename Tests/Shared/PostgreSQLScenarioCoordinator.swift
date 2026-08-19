#if POSTGRESQL
// PostgreSQLScenarioCoordinator.swift
// Coordinates PostgreSQL initialization and isolated scenario access.
//
// Requires a running PostgreSQL instance. Set these environment variables:
// - `POSTGRES_TEST_UNIX_SOCKET` (preferred for isolated local instances)
// - `POSTGRES_TEST_HOST` (required when no Unix socket is configured)
// - `POSTGRES_TEST_PORT` (optional, default: 5432)
// - `POSTGRES_TEST_USER` (optional, default: "postgres")
// - `POSTGRES_TEST_PASSWORD` (optional, default: "test")
// - `POSTGRES_TEST_DB` (optional, default: "database_framework_test")
//
import DatabaseTypes
import Foundation
import StorageKit
import PostgreSQLStorage
import DatabaseEngine
import DatabaseRuntime
import DatabaseKit

/// Coordinates PostgreSQL initialization and isolated scenario access.
///
/// This actor ensures:
/// 1. PostgreSQL configuration and connectivity are validated exactly once
/// 2. PostgreSQL scenarios run serially
/// 3. Every scenario starts from an empty logical database
/// 4. Every container receives an exclusively owned storage engine
///
/// **Usage**:
/// ```swift
/// @Test func postgreSQLScenarioIsIsolated() async throws {
///     try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
///         let engine = try await PostgreSQLScenarioCoordinator.shared.engine
///         // PostgreSQL scenario operations run in an isolated database state.
///     }
/// }
/// ```
public actor PostgreSQLScenarioCoordinator {
    public static let shared = PostgreSQLScenarioCoordinator()

    public nonisolated static var isConfigured: Bool {
        let environment = ProcessInfo.processInfo.environment
        return environment["POSTGRES_TEST_UNIX_SOCKET"] != nil
            || environment["POSTGRES_TEST_HOST"] != nil
    }

    private enum InitializationState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized(PostgreSQLConfiguration)
        case unavailable(String)
        case failed(Error)
    }

    private var initializationState: InitializationState = .uninitialized
    private var scenarioIsActive = false
    private var scenarioEngines: [PostgreSQLStorageEngine] = []
    private let serializedAccess = SerializedScenarioAccessGate()

    private init() {}

    /// Whether PostgreSQL is available for testing
    public var isAvailable: Bool {
        if case .initialized = initializationState { return true }
        return false
    }

    /// Creates an engine owned by the active isolated scenario.
    ///
    /// Every caller receives a distinct engine because `DBContainer` assumes
    /// exclusive ownership of its injected storage engine and shuts it down
    /// when the container's lifecycle ends.
    public var engine: PostgreSQLStorageEngine {
        get async throws {
            try await makeScenarioEngine()
        }
    }

    /// Validates PostgreSQL configuration and connectivity.
    ///
    /// Called automatically by `withIsolatedScenario`.
    public func initialize() async throws {
        switch initializationState {
        case .initialized:
            return

        case .unavailable(let reason):
            throw PostgreSQLScenarioAvailabilityError.unavailable(reason)

        case .failed(let error):
            throw error

        case .initializing(var continuations):
            return try await withCheckedThrowingContinuation { continuation in
                continuations.append(continuation)
                initializationState = .initializing(continuations)
            }

        case .uninitialized:
            initializationState = .initializing([])

            let environment = ProcessInfo.processInfo.environment
            let socketPath = environment["POSTGRES_TEST_UNIX_SOCKET"]
            let host = environment["POSTGRES_TEST_HOST"]
            guard socketPath != nil || host != nil else {
                let reason = "POSTGRES_TEST_UNIX_SOCKET or POSTGRES_TEST_HOST is required."
                let waiters: [CheckedContinuation<Void, Error>]
                if case .initializing(let continuations) = initializationState {
                    waiters = continuations
                } else {
                    waiters = []
                }
                initializationState = .unavailable(reason)
                let error = PostgreSQLScenarioAvailabilityError.unavailable(reason)
                for continuation in waiters {
                    continuation.resume(throwing: error)
                }
                throw error
            }

            do {
                let port: Int
                if let portValue = environment["POSTGRES_TEST_PORT"] {
                    guard let parsedPort = Int(portValue), (1...65_535).contains(parsedPort) else {
                        throw PostgreSQLScenarioAvailabilityError.unavailable(
                            "POSTGRES_TEST_PORT must be an integer from 1 through 65535."
                        )
                    }
                    port = parsedPort
                } else {
                    port = 5_432
                }
                let username = environment["POSTGRES_TEST_USER"] ?? "postgres"
                let password = environment["POSTGRES_TEST_PASSWORD"] ?? "test"
                let database = environment["POSTGRES_TEST_DB"] ?? "database_framework_test"

                let config: PostgreSQLConfiguration
                if let socketPath {
                    config = PostgreSQLConfiguration(
                        unixSocketPath: socketPath,
                        username: username,
                        password: password,
                        database: database
                    )
                } else if let host {
                    config = PostgreSQLConfiguration(
                        host: host,
                        port: port,
                        username: username,
                        password: password,
                        database: database
                    )
                } else {
                    throw PostgreSQLScenarioAvailabilityError.unavailable(
                        "PostgreSQL endpoint is not configured."
                    )
                }
                try await verifyConnectionAndClear(configuration: config)

                if case .initializing(let continuations) = initializationState {
                    initializationState = .initialized(config)
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    initializationState = .initialized(config)
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

    /// Execute one isolated PostgreSQL test scenario.
    ///
    /// The gate remains held while the database is cleared and throughout the
    /// operation, so no other scenario can observe or replace its schema state.
    ///
    /// - Parameter operation: The database operation to execute
    /// - Returns: The result of the operation
    public func withIsolatedScenario<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        try await initialize()
        return try await serializedAccess.withAccess { [self] in
            try await beginScenario()
            do {
                let result = try await operation()
                await finishScenario()
                return result
            } catch {
                await finishScenario()
                throw error
            }
        }
    }

    /// Create a DBContainer using the PostgreSQL engine
    public func makeContainer(
        schema: Schema,
        entityRuntimes: [EntityRuntimeRegistration]
    ) async throws -> DBContainer {
        let pgEngine = try await makeScenarioEngine()
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: pgEngine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: entityRuntimes
            ),
            security: .testingDisabled
        )
    }

    private func beginScenario() async throws {
        guard !scenarioIsActive, scenarioEngines.isEmpty else {
            throw PostgreSQLScenarioAvailabilityError.unavailable(
                "A PostgreSQL scenario is already active."
            )
        }
        try await verifyConnectionAndClear(configuration: try configuration())
        scenarioIsActive = true
    }

    private func finishScenario() async {
        scenarioIsActive = false
        let engines = scenarioEngines
        scenarioEngines.removeAll(keepingCapacity: false)
        for engine in engines {
            await engine.shutdown()
        }
    }

    private func makeScenarioEngine() async throws -> PostgreSQLStorageEngine {
        guard scenarioIsActive else {
            throw PostgreSQLScenarioAvailabilityError.unavailable(
                "Engine access is only valid within withIsolatedScenario."
            )
        }
        let engine = try await PostgreSQLStorageEngine(
            configuration: try configuration()
        )
        guard scenarioIsActive else {
            await engine.shutdown()
            throw PostgreSQLScenarioAvailabilityError.unavailable(
                "The PostgreSQL scenario ended while creating an engine."
            )
        }
        scenarioEngines.append(engine)
        return engine
    }

    private func configuration() throws -> PostgreSQLConfiguration {
        switch initializationState {
        case .initialized(let configuration):
            return configuration
        case .unavailable(let reason):
            throw PostgreSQLScenarioAvailabilityError.unavailable(reason)
        case .failed(let error):
            throw error
        case .uninitialized, .initializing:
            throw PostgreSQLScenarioAvailabilityError.unavailable(
                "PostgreSQL has not finished initializing."
            )
        }
    }

    private func verifyConnectionAndClear(
        configuration: PostgreSQLConfiguration
    ) async throws {
        let engine = try await PostgreSQLStorageEngine(
            configuration: configuration
        )
        do {
            try await engine.withTransaction { transaction in
                try transaction.clearRange(
                    beginKey: ByteString(),
                    endKey: ByteString([0xFF])
                )
            }
            await engine.shutdown()
        } catch {
            await engine.shutdown()
            throw error
        }
    }

}

// MARK: - Error

public enum PostgreSQLScenarioAvailabilityError: Error, CustomStringConvertible {
    case unavailable(String)

    public var description: String {
        switch self {
        case .unavailable(let reason):
            return "PostgreSQL scenario unavailable: \(reason)"
        }
    }
}
#endif
