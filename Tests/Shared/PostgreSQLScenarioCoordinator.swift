#if POSTGRESQL
// PostgreSQLScenarioCoordinator.swift
// Coordinates PostgreSQL initialization and serialized scenario access.
//
// Requires a running PostgreSQL instance. Set these environment variables:
// - `POSTGRES_TEST_HOST` (required, e.g. "localhost")
// - `POSTGRES_TEST_PORT` (optional, default: 5432)
// - `POSTGRES_TEST_PASSWORD` (optional, default: "test")
// - `POSTGRES_TEST_DB` (optional, default: "database_framework_test")
//
// Quick start with Docker:
// ```
// docker run --rm -d -p 5432:5432 \
//   -e POSTGRES_PASSWORD=test \
//   -e POSTGRES_DB=database_framework_test \
//   postgres:16
// ```

import Foundation
import StorageKit
import PostgreSQLStorage
import DatabaseEngine
import DatabaseRuntime
import DatabaseKit

/// Coordinates PostgreSQL initialization and serialized scenario access.
///
/// This actor ensures:
/// 1. PostgreSQL engine is initialized exactly once
/// 2. PostgreSQL scenarios run serially to prevent conflicts
///
/// **Usage**:
/// ```swift
/// @Test func postgreSQLOperationIsSerialized() async throws {
///     try await PostgreSQLScenarioCoordinator.shared.withSerializedAccess {
///         let engine = PostgreSQLScenarioCoordinator.shared.engine
///         // PostgreSQL scenario operations run here.
///     }
/// }
/// ```
public actor PostgreSQLScenarioCoordinator {
    public static let shared = PostgreSQLScenarioCoordinator()

    public nonisolated static var isConfigured: Bool {
        ProcessInfo.processInfo.environment["POSTGRES_TEST_HOST"] != nil
    }

    private enum InitializationState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized(PostgreSQLStorageEngine)
        case unavailable(String)
        case failed(Error)
    }

    private var initializationState: InitializationState = .uninitialized
    private let serializedAccess = SerializedScenarioAccessGate()

    private init() {}

    /// Whether PostgreSQL is available for testing
    public var isAvailable: Bool {
        if case .initialized = initializationState { return true }
        return false
    }

    /// Get the initialized engine (only valid after initialize())
    public var engine: PostgreSQLStorageEngine {
        get throws {
            switch initializationState {
            case .initialized(let engine):
                return engine
            case .unavailable(let reason):
                throw PostgreSQLScenarioAvailabilityError.unavailable(reason)
            case .failed(let error):
                throw error
            default:
                throw PostgreSQLScenarioAvailabilityError.unavailable("Not initialized")
            }
        }
    }

    /// Initialize PostgreSQL engine (called automatically by withSerializedAccess)
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

            // Check if PostgreSQL is configured
            guard let host = ProcessInfo.processInfo.environment["POSTGRES_TEST_HOST"] else {
                let reason = "POSTGRES_TEST_HOST is not configured."
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

            let port = Int(ProcessInfo.processInfo.environment["POSTGRES_TEST_PORT"] ?? "5432") ?? 5432
            let password = ProcessInfo.processInfo.environment["POSTGRES_TEST_PASSWORD"] ?? "test"
            let database = ProcessInfo.processInfo.environment["POSTGRES_TEST_DB"] ?? "database_framework_test"

            do {
                let config = PostgreSQLConfiguration(
                    host: host,
                    port: port,
                    username: "postgres",
                    password: password,
                    database: database
                )
                let engine = try await PostgreSQLStorageEngine(configuration: config)

                // Clean all data on startup
                try await engine.withTransaction { tx in
                    try tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
                }

                if case .initializing(let continuations) = initializationState {
                    initializationState = .initialized(engine)
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    initializationState = .initialized(engine)
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

    /// Execute a test with serialized PostgreSQL access
    ///
    /// This ensures only one PostgreSQL scenario runs at a time.
    ///
    /// - Parameter operation: The database operation to execute
    /// - Returns: The result of the operation
    public func withSerializedAccess<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        try await initialize()
        return try await serializedAccess.withAccess(operation)
    }

    /// Create a DBContainer using the PostgreSQL engine
    public func makeContainer(
        schema: Schema,
        persistableTypes: [any Persistable.Type]
    ) async throws -> DBContainer {
        let pgEngine = try engine
        return try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(pgEngine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: persistableTypes
            ),
            security: .disabled
        )
    }

    /// Clean all data in the PostgreSQL database
    public func clearScenarioData() async throws {
        let pgEngine = try engine
        try await pgEngine.withTransaction { tx in
            try tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
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
