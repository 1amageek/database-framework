// FDBTestSetup.swift
// Shared FDB initialization and serialization for all test targets

import Foundation
import FoundationDB
@testable import DatabaseEngine

/// Shared FDB initialization and test serialization singleton
///
/// This actor ensures:
/// 1. FDBClient.initialize() is called only once across all test suites
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

    /// Queue of waiting test continuations for serialization
    private var waitingTests: [CheckedContinuation<Void, Never>] = []

    /// Whether a test is currently running
    private var isTestRunning: Bool = false

    private init() {}

    /// Initialize FDB client (called automatically by withSerializedAccess)
    public func initialize() async throws {
        switch initState {
        case .initialized:
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
                try await FDBClient.initialize()
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
        // If a test is already running, wait in queue
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
