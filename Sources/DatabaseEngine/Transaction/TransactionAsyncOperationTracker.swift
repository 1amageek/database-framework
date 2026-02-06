// TransactionAsyncOperationTracker.swift
// DatabaseEngine - Tracks async operations (AsyncSequence iterators) during transaction
//
// Purpose: Prevent "Operation issued while a commit was outstanding" errors
// by ensuring all async iterators are deallocated before commit().
//
// Reference: FoundationDB Record Layer transaction lifecycle pattern
//
// Problem:
//   AsyncKVSequence.AsyncIterator creates a background preFetchTask.
//   If commit() is called while the iterator is still alive, FDB returns an error.
//   The iterator's deinit cancels the preFetchTask, but deinit may occur after commit().
//
// Solution:
//   Track all iterators created during the transaction.
//   Before commit(), wait for all iterators to be deallocated (their deinit cancels tasks).

import Foundation
import Synchronization

/// Tracks async operations (AsyncSequence iterators) created during a transaction
///
/// **Thread Safety**: Uses `Mutex` for state management (no I/O, high frequency access)
///
/// **Usage**:
/// ```swift
/// let tracker = TransactionAsyncOperationTracker()
///
/// // When creating an iterator
/// let iterator = sequence.makeAsyncIterator()
/// tracker.register(iterator)
///
/// // Before commit
/// try await tracker.waitForCompletion()
/// ```
internal final class TransactionAsyncOperationTracker: Sendable {

    // MARK: - State

    private struct State: Sendable {
        /// Active iterator identifiers (ObjectIdentifier for lightweight tracking)
        var activeIterators: Set<ObjectIdentifier> = []

        /// Total iterators registered (for debugging)
        var totalRegistered: Int = 0
    }

    private let state: Mutex<State>

    // MARK: - Initialization

    init() {
        self.state = Mutex(State())
    }

    // MARK: - Registration

    /// Register an async iterator for tracking
    ///
    /// Call this immediately after creating an iterator from getRange().
    /// The iterator will be automatically unregistered when it is deallocated.
    ///
    /// - Parameter iterator: The iterator object to track (uses ObjectIdentifier)
    func register(_ iterator: AnyObject) {
        state.withLock { state in
            state.activeIterators.insert(ObjectIdentifier(iterator))
            state.totalRegistered += 1
        }
    }

    /// Unregister an async iterator
    ///
    /// Called from the iterator's deinit or when iteration completes.
    ///
    /// - Parameter iterator: The iterator object to unregister
    func unregister(_ iterator: AnyObject) {
        state.withLock { state in
            state.activeIterators.remove(ObjectIdentifier(iterator))
        }
    }

    // MARK: - Completion Fence

    /// Wait for all async iterators to complete or be deallocated
    ///
    /// This method polls until all registered iterators are unregistered.
    /// Iterators are unregistered either:
    /// 1. Explicitly via `unregister()` when iteration completes
    /// 2. Implicitly when the iterator is deallocated (its deinit cancels background tasks)
    ///
    /// - Parameter timeout: Maximum time to wait (default: 5 seconds)
    /// - Throws: Never throws, but logs a warning if timeout is exceeded
    func waitForCompletion(timeout: Duration = .seconds(5)) async {
        // Fast path: no iterators registered
        if state.withLock({ $0.activeIterators.isEmpty }) {
            return
        }

        let deadline = ContinuousClock.now + timeout

        while true {
            // Check if all iterators are deallocated
            if state.withLock({ $0.activeIterators.isEmpty }) {
                return
            }

            // Check timeout
            if ContinuousClock.now > deadline {
                let count = state.withLock { $0.activeIterators.count }
                // Log warning but proceed with commit
                // Better to fail with FDB error than to hang indefinitely
                print("WARNING: TransactionAsyncOperationTracker timeout - \(count) iterators still active after \(timeout)")
                return
            }

            // Yield to allow ARC to deallocate iterators
            // Deallocation typically happens within microseconds
            await Task.yield()
        }
    }

    // MARK: - Status

    /// Check if there are any active iterators
    var hasActiveIterators: Bool {
        state.withLock { !$0.activeIterators.isEmpty }
    }

    /// Number of currently active iterators
    var activeCount: Int {
        state.withLock { $0.activeIterators.count }
    }

    /// Total number of iterators registered during this transaction's lifetime
    var totalRegisteredCount: Int {
        state.withLock { $0.totalRegistered }
    }
}
