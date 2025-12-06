// AdaptiveThrottler.swift
// DatabaseEngine - Adaptive throttling for index building and batch operations
//
// Reference: FDB Record Layer IndexingThrottle.java
// Dynamically adjusts batch size and delay based on operation success/failure.

import Foundation
import Synchronization
import FoundationDB

// MARK: - ThrottleConfiguration

/// Configuration for adaptive throttling
public struct ThrottleConfiguration: Sendable, Equatable {
    /// Initial batch size
    public let initialBatchSize: Int

    /// Minimum batch size (won't go below this)
    public let minBatchSize: Int

    /// Maximum batch size (won't exceed this)
    public let maxBatchSize: Int

    /// Batch size increase factor on success (e.g., 1.5 = 50% increase)
    public let increaseRatio: Double

    /// Batch size decrease factor on failure (e.g., 0.5 = 50% decrease)
    public let decreaseRatio: Double

    /// Minimum delay between batches (milliseconds)
    public let minDelayMs: Int

    /// Maximum delay between batches (milliseconds)
    public let maxDelayMs: Int

    /// Initial delay (milliseconds)
    public let initialDelayMs: Int

    /// Delay increase factor on failure
    public let delayIncreaseRatio: Double

    /// Delay decrease factor on success
    public let delayDecreaseRatio: Double

    /// Number of consecutive successes before increasing batch size
    public let successesBeforeIncrease: Int

    /// Default configuration
    public static let `default` = ThrottleConfiguration(
        initialBatchSize: 100,
        minBatchSize: 10,
        maxBatchSize: 1000,
        increaseRatio: 1.5,
        decreaseRatio: 0.5,
        minDelayMs: 0,
        maxDelayMs: 1000,
        initialDelayMs: 0,
        delayIncreaseRatio: 2.0,
        delayDecreaseRatio: 0.9,
        successesBeforeIncrease: 3
    )

    /// Conservative configuration (smaller batches, more delay)
    public static let conservative = ThrottleConfiguration(
        initialBatchSize: 50,
        minBatchSize: 10,
        maxBatchSize: 200,
        increaseRatio: 1.2,
        decreaseRatio: 0.3,
        minDelayMs: 10,
        maxDelayMs: 5000,
        initialDelayMs: 50,
        delayIncreaseRatio: 2.0,
        delayDecreaseRatio: 0.95,
        successesBeforeIncrease: 5
    )

    /// Aggressive configuration (larger batches, less delay)
    public static let aggressive = ThrottleConfiguration(
        initialBatchSize: 200,
        minBatchSize: 50,
        maxBatchSize: 2000,
        increaseRatio: 2.0,
        decreaseRatio: 0.7,
        minDelayMs: 0,
        maxDelayMs: 500,
        initialDelayMs: 0,
        delayIncreaseRatio: 1.5,
        delayDecreaseRatio: 0.8,
        successesBeforeIncrease: 2
    )

    public init(
        initialBatchSize: Int = 100,
        minBatchSize: Int = 10,
        maxBatchSize: Int = 1000,
        increaseRatio: Double = 1.5,
        decreaseRatio: Double = 0.5,
        minDelayMs: Int = 0,
        maxDelayMs: Int = 1000,
        initialDelayMs: Int = 0,
        delayIncreaseRatio: Double = 2.0,
        delayDecreaseRatio: Double = 0.9,
        successesBeforeIncrease: Int = 3
    ) {
        precondition(minBatchSize > 0, "minBatchSize must be positive")
        precondition(maxBatchSize >= minBatchSize, "maxBatchSize must be >= minBatchSize")
        precondition(initialBatchSize >= minBatchSize && initialBatchSize <= maxBatchSize,
                     "initialBatchSize must be between minBatchSize and maxBatchSize")
        precondition(increaseRatio > 1.0, "increaseRatio must be > 1.0")
        precondition(decreaseRatio > 0 && decreaseRatio < 1.0, "decreaseRatio must be between 0 and 1")

        self.initialBatchSize = initialBatchSize
        self.minBatchSize = minBatchSize
        self.maxBatchSize = maxBatchSize
        self.increaseRatio = increaseRatio
        self.decreaseRatio = decreaseRatio
        self.minDelayMs = minDelayMs
        self.maxDelayMs = maxDelayMs
        self.initialDelayMs = initialDelayMs
        self.delayIncreaseRatio = delayIncreaseRatio
        self.delayDecreaseRatio = delayDecreaseRatio
        self.successesBeforeIncrease = successesBeforeIncrease
    }
}

// MARK: - AdaptiveThrottler

/// Adaptive throttler for batch operations
///
/// Dynamically adjusts batch size and inter-batch delay based on operation
/// success/failure rates. On success, gradually increases batch size and
/// decreases delay. On failure, immediately decreases batch size and
/// increases delay.
///
/// **Thread Safety**: This class is thread-safe and can be used from
/// multiple concurrent tasks.
///
/// **Usage**:
/// ```swift
/// let throttler = AdaptiveThrottler(configuration: .default)
///
/// while !isComplete {
///     let batchSize = throttler.currentBatchSize
///
///     do {
///         let items = try await processBatch(size: batchSize)
///         throttler.recordSuccess(itemCount: items.count, durationNs: elapsed)
///     } catch {
///         throttler.recordFailure(error: error)
///         if !throttler.isRetryable(error) { throw error }
///     }
///
///     try await throttler.waitBeforeNextBatch()
/// }
/// ```
public final class AdaptiveThrottler: Sendable {
    // MARK: - State

    private struct State: Sendable {
        var currentBatchSize: Int
        var currentDelayMs: Int
        var consecutiveSuccesses: Int = 0
        var consecutiveFailures: Int = 0
        var totalSuccesses: Int = 0
        var totalFailures: Int = 0
        var totalItemsProcessed: Int = 0
        var totalDurationNs: UInt64 = 0
    }

    private let configuration: ThrottleConfiguration
    private let state: Mutex<State>

    // MARK: - Initialization

    public init(configuration: ThrottleConfiguration = .default) {
        self.configuration = configuration
        self.state = Mutex(State(
            currentBatchSize: configuration.initialBatchSize,
            currentDelayMs: configuration.initialDelayMs
        ))
    }

    // MARK: - Current State

    /// Current batch size
    public var currentBatchSize: Int {
        state.withLock { $0.currentBatchSize }
    }

    /// Current delay in milliseconds
    public var currentDelayMs: Int {
        state.withLock { $0.currentDelayMs }
    }

    /// Get current throttler statistics
    public var statistics: ThrottlerStatistics {
        state.withLock { state in
            ThrottlerStatistics(
                currentBatchSize: state.currentBatchSize,
                currentDelayMs: state.currentDelayMs,
                consecutiveSuccesses: state.consecutiveSuccesses,
                consecutiveFailures: state.consecutiveFailures,
                totalSuccesses: state.totalSuccesses,
                totalFailures: state.totalFailures,
                totalItemsProcessed: state.totalItemsProcessed,
                totalDurationNs: state.totalDurationNs
            )
        }
    }

    // MARK: - Recording Results

    /// Record a successful batch operation
    ///
    /// - Parameters:
    ///   - itemCount: Number of items processed in the batch
    ///   - durationNs: Duration of the batch operation in nanoseconds
    public func recordSuccess(itemCount: Int, durationNs: UInt64) {
        state.withLock { state in
            state.consecutiveSuccesses += 1
            state.consecutiveFailures = 0
            state.totalSuccesses += 1
            state.totalItemsProcessed += itemCount
            state.totalDurationNs += durationNs

            // Decrease delay on success
            let newDelay = Double(state.currentDelayMs) * configuration.delayDecreaseRatio
            state.currentDelayMs = max(configuration.minDelayMs, Int(newDelay))

            // Increase batch size after enough consecutive successes
            if state.consecutiveSuccesses >= configuration.successesBeforeIncrease {
                let newSize = Double(state.currentBatchSize) * configuration.increaseRatio
                state.currentBatchSize = min(configuration.maxBatchSize, Int(newSize))
                state.consecutiveSuccesses = 0  // Reset counter
            }
        }
    }

    /// Record a failed batch operation
    ///
    /// - Parameter error: The error that occurred
    public func recordFailure(error: Error) {
        state.withLock { state in
            state.consecutiveFailures += 1
            state.consecutiveSuccesses = 0
            state.totalFailures += 1

            // Decrease batch size on failure
            let newSize = Double(state.currentBatchSize) * configuration.decreaseRatio
            state.currentBatchSize = max(configuration.minBatchSize, Int(newSize))

            // Increase delay on failure
            let newDelay = Double(max(state.currentDelayMs, 1)) * configuration.delayIncreaseRatio
            state.currentDelayMs = min(configuration.maxDelayMs, Int(newDelay))
        }
    }

    /// Check if an error is retryable
    ///
    /// - Parameter error: The error to check
    /// - Returns: True if the operation should be retried
    public func isRetryable(_ error: Error) -> Bool {
        // Check for common retryable patterns in error description
        let description = String(describing: error).lowercased()
        return description.contains("retry") ||
               description.contains("timeout") ||
               description.contains("conflict") ||
               description.contains("too old") ||
               description.contains("transaction_too_old") ||
               description.contains("future_version") ||
               description.contains("not_committed")
    }

    /// Wait before the next batch
    ///
    /// Call this between batches to apply throttling delay.
    public func waitBeforeNextBatch() async throws {
        let delayMs = state.withLock { $0.currentDelayMs }
        if delayMs > 0 {
            try await Task.sleep(nanoseconds: UInt64(delayMs) * 1_000_000)
        }
    }

    /// Reset the throttler to initial state
    public func reset() {
        state.withLock { state in
            state.currentBatchSize = configuration.initialBatchSize
            state.currentDelayMs = configuration.initialDelayMs
            state.consecutiveSuccesses = 0
            state.consecutiveFailures = 0
        }
    }

    /// Reset statistics while keeping current throttle settings
    public func resetStatistics() {
        state.withLock { state in
            state.totalSuccesses = 0
            state.totalFailures = 0
            state.totalItemsProcessed = 0
            state.totalDurationNs = 0
        }
    }
}

// MARK: - ThrottlerStatistics

/// Statistics about throttler performance
public struct ThrottlerStatistics: Sendable {
    /// Current batch size
    public let currentBatchSize: Int

    /// Current delay in milliseconds
    public let currentDelayMs: Int

    /// Number of consecutive successes
    public let consecutiveSuccesses: Int

    /// Number of consecutive failures
    public let consecutiveFailures: Int

    /// Total number of successful batches
    public let totalSuccesses: Int

    /// Total number of failed batches
    public let totalFailures: Int

    /// Total number of items processed
    public let totalItemsProcessed: Int

    /// Total duration in nanoseconds
    public let totalDurationNs: UInt64

    /// Success rate (0.0 - 1.0)
    public var successRate: Double {
        let total = totalSuccesses + totalFailures
        guard total > 0 else { return 0.0 }
        return Double(totalSuccesses) / Double(total)
    }

    /// Average items per successful batch
    public var averageItemsPerBatch: Double {
        guard totalSuccesses > 0 else { return 0.0 }
        return Double(totalItemsProcessed) / Double(totalSuccesses)
    }

    /// Average duration per batch in milliseconds
    public var averageDurationMs: Double {
        let totalBatches = totalSuccesses + totalFailures
        guard totalBatches > 0 else { return 0.0 }
        return Double(totalDurationNs) / Double(totalBatches) / 1_000_000
    }

    /// Throughput in items per second
    public var throughputPerSecond: Double {
        guard totalDurationNs > 0 else { return 0.0 }
        return Double(totalItemsProcessed) / (Double(totalDurationNs) / 1_000_000_000)
    }
}

// MARK: - ThrottledOperation

/// A convenience wrapper for running throttled operations
public struct ThrottledOperation<T: Sendable>: Sendable {
    private let throttler: AdaptiveThrottler
    private let operation: @Sendable (Int) async throws -> (result: T, itemCount: Int)

    public init(
        throttler: AdaptiveThrottler,
        operation: @escaping @Sendable (Int) async throws -> (result: T, itemCount: Int)
    ) {
        self.throttler = throttler
        self.operation = operation
    }

    /// Execute the operation with automatic throttling and retry
    ///
    /// - Parameter maxRetries: Maximum number of retries for retryable errors
    /// - Returns: The result of the operation
    public func execute(maxRetries: Int = 3) async throws -> T {
        var lastError: Error?

        for attempt in 0..<(maxRetries + 1) {
            let batchSize = throttler.currentBatchSize
            let startTime = DispatchTime.now()

            do {
                let (result, itemCount) = try await operation(batchSize)
                let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                throttler.recordSuccess(itemCount: itemCount, durationNs: duration)
                return result
            } catch {
                throttler.recordFailure(error: error)
                lastError = error

                if !throttler.isRetryable(error) || attempt >= maxRetries {
                    throw error
                }

                // Wait before retry
                try await throttler.waitBeforeNextBatch()
            }
        }

        throw lastError ?? ThrottleError.exhaustedRetries
    }
}

// MARK: - ThrottleError

/// Errors from throttled operations
public enum ThrottleError: Error, Sendable {
    case exhaustedRetries
}
