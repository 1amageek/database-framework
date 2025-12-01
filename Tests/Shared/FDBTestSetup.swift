// FDBTestSetup.swift
// Shared FDB initialization for all test targets

import Foundation
import FoundationDB

/// Shared FDB initialization singleton for all tests
///
/// This actor ensures FDBClient.initialize() is called only once
/// across all test suites, preventing "API version may be set only once" errors.
///
/// The initialization uses a continuation to ensure only one call to
/// FDBClient.initialize() is made even with concurrent callers.
public actor FDBTestSetup {
    public static let shared = FDBTestSetup()

    private enum State {
        case uninitialized
        case initializing([CheckedContinuation<Void, Error>])
        case initialized
        case failed(Error)
    }

    private var state: State = .uninitialized

    private init() {}

    public func initialize() async throws {
        switch state {
        case .initialized:
            // Already initialized, nothing to do
            return

        case .failed(let error):
            // Previous initialization failed, rethrow the error
            throw error

        case .initializing(var continuations):
            // Another task is initializing, wait for it
            return try await withCheckedThrowingContinuation { continuation in
                continuations.append(continuation)
                state = .initializing(continuations)
            }

        case .uninitialized:
            // We are the first, start initialization
            state = .initializing([])

            do {
                try await FDBClient.initialize()
                // Resume all waiting continuations
                if case .initializing(let continuations) = state {
                    state = .initialized
                    for continuation in continuations {
                        continuation.resume(returning: ())
                    }
                } else {
                    state = .initialized
                }
            } catch {
                // Resume all waiting continuations with error
                if case .initializing(let continuations) = state {
                    state = .failed(error)
                    for continuation in continuations {
                        continuation.resume(throwing: error)
                    }
                } else {
                    state = .failed(error)
                }
                throw error
            }
        }
    }
}
