#if POSTGRESQL
// PostgreSQLTestSetup.swift
// Shared PostgreSQL initialization and serialization for all test targets
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
import Core

/// Shared PostgreSQL initialization and test serialization singleton
///
/// This actor ensures:
/// 1. PostgreSQL engine is initialized exactly once
/// 2. PostgreSQL tests run serially to prevent conflicts
///
/// **Usage**:
/// ```swift
/// @Test func myPostgreSQLTest() async throws {
///     try await PostgreSQLTestSetup.shared.withSerializedAccess {
///         let engine = PostgreSQLTestSetup.shared.engine
///         // Your test code here
///     }
/// }
/// ```
public actor PostgreSQLTestSetup {
    public static let shared = PostgreSQLTestSetup()

    private enum InitState {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized(PostgreSQLStorageEngine)
        case unavailable(String)
        case failed(Error)
    }

    private var initState: InitState = .uninitialized

    /// Queue of waiting test continuations for serialization
    private var waitingTests: [CheckedContinuation<Void, Never>] = []

    /// Whether a test is currently running
    private var isTestRunning: Bool = false

    private init() {}

    /// Whether PostgreSQL is available for testing
    public var isAvailable: Bool {
        if case .initialized = initState { return true }
        return false
    }

    /// Get the initialized engine (only valid after initialize())
    public var engine: PostgreSQLStorageEngine {
        get throws {
            switch initState {
            case .initialized(let engine):
                return engine
            case .unavailable(let reason):
                throw PostgreSQLTestError.unavailable(reason)
            case .failed(let error):
                throw error
            default:
                throw PostgreSQLTestError.unavailable("Not initialized")
            }
        }
    }

    /// Initialize PostgreSQL engine (called automatically by withSerializedAccess)
    public func initialize() async throws {
        switch initState {
        case .initialized:
            return

        case .unavailable(let reason):
            throw PostgreSQLTestError.unavailable(reason)

        case .failed(let error):
            throw error

        case .initializing(var continuations):
            return try await withCheckedThrowingContinuation { continuation in
                continuations.append(continuation)
                initState = .initializing(continuations)
            }

        case .uninitialized:
            initState = .initializing([])

            // Check if PostgreSQL is configured
            guard let host = ProcessInfo.processInfo.environment["POSTGRES_TEST_HOST"] else {
                let reason = "POSTGRES_TEST_HOST not set. Skipping PostgreSQL tests."
                let waiters: [CheckedContinuation<Void, Error>]
                if case .initializing(let continuations) = initState {
                    waiters = continuations
                } else {
                    waiters = []
                }
                initState = .unavailable(reason)
                let error = PostgreSQLTestError.unavailable(reason)
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
                    tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
                }

                if case .initializing(let continuations) = initState {
                    initState = .initialized(engine)
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    initState = .initialized(engine)
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

    /// Execute a test with serialized PostgreSQL access
    ///
    /// This ensures only one PostgreSQL test runs at a time across all test suites.
    ///
    /// - Parameter operation: The test operation to execute
    /// - Returns: The result of the operation
    public func withSerializedAccess<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        // Ensure PostgreSQL is initialized
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

    /// Create a DBContainer using the PostgreSQL engine
    public func makeContainer(schema: Schema) async throws -> DBContainer {
        let pgEngine = try engine
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(pgEngine)),
            security: .disabled
        )
    }

    /// Clean all data in the PostgreSQL database
    public func cleanAllData() async throws {
        let pgEngine = try engine
        try await pgEngine.withTransaction { tx in
            tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
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

// MARK: - Error

public enum PostgreSQLTestError: Error, CustomStringConvertible {
    case unavailable(String)

    public var description: String {
        switch self {
        case .unavailable(let reason):
            return "PostgreSQL test unavailable: \(reason)"
        }
    }
}
#endif
