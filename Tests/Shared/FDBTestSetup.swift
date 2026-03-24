// FDBTestSetup.swift
// Shared FDB initialization and serialization for all test targets

#if FOUNDATION_DB
import Foundation
import StorageKit
import FDBStorage
import DatabaseEngine

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
                // FDBStorageEngine.init(configuration:) handles FDBClient.initialize() internally
                _ = try await FDBStorageEngine(configuration: .init())
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

    /// Best-effort cleanup for stale local test data.
    private func cleanupTestDirectoriesBestEffort() async {
        guard !didCleanupTestDirectories else { return }
        didCleanupTestDirectories = true

        do {
            let engine = try await FDBStorageEngine(configuration: .init())
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
