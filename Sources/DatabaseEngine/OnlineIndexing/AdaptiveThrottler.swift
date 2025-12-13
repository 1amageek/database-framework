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
///
/// Groups batch and delay settings for cleaner initialization.
///
/// **Usage**:
/// ```swift
/// // Use presets
/// let config = ThrottleConfiguration.default
///
/// // Custom batch settings only
/// let config = ThrottleConfiguration(
///     batch: .init(initial: 200, min: 50, max: 500)
/// )
///
/// // Custom both
/// let config = ThrottleConfiguration(
///     batch: .init(initial: 100, increaseRatio: 2.0),
///     delay: .init(max: 2000)
/// )
/// ```
public struct ThrottleConfiguration: Sendable, Equatable {
    /// Batch size settings
    public let batch: BatchSettings

    /// Delay settings
    public let delay: DelaySettings

    /// Number of consecutive successes before increasing batch size
    public let successesBeforeIncrease: Int

    /// Default configuration
    public static let `default` = ThrottleConfiguration()

    /// Conservative configuration (smaller batches, more delay)
    public static let conservative = ThrottleConfiguration(
        batch: .init(initial: 50, min: 10, max: 200, increaseRatio: 1.2, decreaseRatio: 0.3),
        delay: .init(min: 10, max: 5000, initial: 50, increaseRatio: 2.0, decreaseRatio: 0.95),
        successesBeforeIncrease: 5
    )

    /// Aggressive configuration (larger batches, less delay)
    public static let aggressive = ThrottleConfiguration(
        batch: .init(initial: 200, min: 50, max: 2000, increaseRatio: 2.0, decreaseRatio: 0.7),
        delay: .init(min: 0, max: 500, initial: 0, increaseRatio: 1.5, decreaseRatio: 0.8),
        successesBeforeIncrease: 2
    )

    public init(
        batch: BatchSettings = .default,
        delay: DelaySettings = .default,
        successesBeforeIncrease: Int = 3
    ) {
        self.batch = batch
        self.delay = delay
        self.successesBeforeIncrease = successesBeforeIncrease
    }
}

// MARK: - BatchSettings

extension ThrottleConfiguration {
    /// Batch size configuration
    ///
    /// Controls how batch sizes are adjusted based on success/failure.
    public struct BatchSettings: Sendable, Equatable {
        /// Initial batch size
        public let initial: Int

        /// Minimum batch size (won't go below this)
        public let min: Int

        /// Maximum batch size (won't exceed this)
        public let max: Int

        /// Batch size increase factor on success (e.g., 1.5 = 50% increase)
        public let increaseRatio: Double

        /// Batch size decrease factor on failure (e.g., 0.5 = 50% decrease)
        public let decreaseRatio: Double

        /// Default batch settings
        public static let `default` = BatchSettings()

        public init(
            initial: Int = 100,
            min: Int = 10,
            max: Int = 1000,
            increaseRatio: Double = 1.5,
            decreaseRatio: Double = 0.5
        ) {
            precondition(min > 0, "min must be positive")
            precondition(max >= min, "max must be >= min")
            precondition(initial >= min && initial <= max,
                         "initial must be between min and max")
            precondition(increaseRatio > 1.0, "increaseRatio must be > 1.0")
            precondition(decreaseRatio > 0 && decreaseRatio < 1.0, "decreaseRatio must be between 0 and 1")

            self.initial = initial
            self.min = min
            self.max = max
            self.increaseRatio = increaseRatio
            self.decreaseRatio = decreaseRatio
        }
    }
}

// MARK: - DelaySettings

extension ThrottleConfiguration {
    /// Delay configuration
    ///
    /// Controls inter-batch delay adjustments based on success/failure.
    public struct DelaySettings: Sendable, Equatable {
        /// Minimum delay between batches (milliseconds)
        public let min: Int

        /// Maximum delay between batches (milliseconds)
        public let max: Int

        /// Initial delay (milliseconds)
        public let initial: Int

        /// Delay increase factor on failure
        public let increaseRatio: Double

        /// Delay decrease factor on success
        public let decreaseRatio: Double

        /// Default delay settings (no delay)
        public static let `default` = DelaySettings()

        public init(
            min: Int = 0,
            max: Int = 1000,
            initial: Int = 0,
            increaseRatio: Double = 2.0,
            decreaseRatio: Double = 0.9
        ) {
            self.min = min
            self.max = max
            self.initial = initial
            self.increaseRatio = increaseRatio
            self.decreaseRatio = decreaseRatio
        }
    }
}

// MARK: - Convenience Accessors

extension ThrottleConfiguration {
    /// Initial batch size (convenience accessor)
    public var initialBatchSize: Int { batch.initial }

    /// Minimum batch size (convenience accessor)
    public var minBatchSize: Int { batch.min }

    /// Maximum batch size (convenience accessor)
    public var maxBatchSize: Int { batch.max }

    /// Batch size increase ratio (convenience accessor)
    public var increaseRatio: Double { batch.increaseRatio }

    /// Batch size decrease ratio (convenience accessor)
    public var decreaseRatio: Double { batch.decreaseRatio }

    /// Minimum delay in ms (convenience accessor)
    public var minDelayMs: Int { delay.min }

    /// Maximum delay in ms (convenience accessor)
    public var maxDelayMs: Int { delay.max }

    /// Initial delay in ms (convenience accessor)
    public var initialDelayMs: Int { delay.initial }

    /// Delay increase ratio (convenience accessor)
    public var delayIncreaseRatio: Double { delay.increaseRatio }

    /// Delay decrease ratio (convenience accessor)
    public var delayDecreaseRatio: Double { delay.decreaseRatio }
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

    /// Get statistics
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
                avgItemsPerSecond: state.totalDurationNs > 0
                    ? Double(state.totalItemsProcessed) / (Double(state.totalDurationNs) / 1_000_000_000)
                    : 0
            )
        }
    }

    // MARK: - Recording Results

    /// Record a successful operation
    ///
    /// - Parameters:
    ///   - itemCount: Number of items processed
    ///   - durationNs: Duration in nanoseconds
    public func recordSuccess(itemCount: Int, durationNs: UInt64) {
        state.withLock { state in
            state.consecutiveSuccesses += 1
            state.consecutiveFailures = 0
            state.totalSuccesses += 1
            state.totalItemsProcessed += itemCount
            state.totalDurationNs += durationNs

            // Increase batch size after consecutive successes
            if state.consecutiveSuccesses >= configuration.successesBeforeIncrease {
                let newSize = Int(Double(state.currentBatchSize) * configuration.increaseRatio)
                state.currentBatchSize = min(newSize, configuration.maxBatchSize)
                state.consecutiveSuccesses = 0
            }

            // Decrease delay on success
            let newDelay = Int(Double(state.currentDelayMs) * configuration.delayDecreaseRatio)
            state.currentDelayMs = max(newDelay, configuration.minDelayMs)
        }
    }

    /// Record a failed operation
    ///
    /// - Parameter error: The error that occurred
    public func recordFailure(error: Error) {
        state.withLock { state in
            state.consecutiveFailures += 1
            state.consecutiveSuccesses = 0
            state.totalFailures += 1

            // Immediately decrease batch size
            let newSize = Int(Double(state.currentBatchSize) * configuration.decreaseRatio)
            state.currentBatchSize = max(newSize, configuration.minBatchSize)

            // Increase delay on failure
            let newDelay = Int(Double(max(state.currentDelayMs, 10)) * configuration.delayIncreaseRatio)
            state.currentDelayMs = min(newDelay, configuration.maxDelayMs)
        }
    }

    /// Check if an error is retryable
    ///
    /// - Parameter error: The error to check
    /// - Returns: true if the operation should be retried
    public func isRetryable(_ error: Error) -> Bool {
        // FDB errors have a built-in isRetryable property
        if let fdbError = error as? FDBError {
            return fdbError.isRetryable
        }

        // Generic timeout errors
        if (error as NSError).domain == NSURLErrorDomain {
            return true
        }

        return false
    }

    /// Wait before next batch
    ///
    /// Waits for the current delay duration.
    public func waitBeforeNextBatch() async throws {
        let delayMs = currentDelayMs
        if delayMs > 0 {
            try await Task.sleep(nanoseconds: UInt64(delayMs) * 1_000_000)
        }
    }

    /// Reset to initial state
    public func reset() {
        state.withLock { state in
            state.currentBatchSize = configuration.initialBatchSize
            state.currentDelayMs = configuration.initialDelayMs
            state.consecutiveSuccesses = 0
            state.consecutiveFailures = 0
            state.totalSuccesses = 0
            state.totalFailures = 0
            state.totalItemsProcessed = 0
            state.totalDurationNs = 0
        }
    }
}

// MARK: - Statistics

/// Throttler statistics
public struct ThrottlerStatistics: Sendable {
    public let currentBatchSize: Int
    public let currentDelayMs: Int
    public let consecutiveSuccesses: Int
    public let consecutiveFailures: Int
    public let totalSuccesses: Int
    public let totalFailures: Int
    public let totalItemsProcessed: Int
    public let avgItemsPerSecond: Double

    public var successRate: Double {
        let total = totalSuccesses + totalFailures
        return total > 0 ? Double(totalSuccesses) / Double(total) : 0
    }
}

// MARK: - ThrottledBatchExecutor

/// Executor for throttled batch operations
///
/// Wraps an operation with automatic throttling and retry logic.
///
/// **Usage**:
/// ```swift
/// let throttler = AdaptiveThrottler()
/// let executor = ThrottledBatchExecutor(throttler: throttler) { batchSize in
///     let items = try await fetchItems(limit: batchSize)
///     return (result: items, itemCount: items.count)
/// }
///
/// while !isDone {
///     let items = try await executor.execute()
///     // Process items
/// }
/// ```
public struct ThrottledBatchExecutor<T: Sendable>: Sendable {
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
                let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
                throttler.recordSuccess(itemCount: itemCount, durationNs: elapsed)

                // Wait before next batch
                try await throttler.waitBeforeNextBatch()

                return result
            } catch {
                throttler.recordFailure(error: error)
                lastError = error

                if !throttler.isRetryable(error) || attempt == maxRetries {
                    throw error
                }

                // Wait before retry (exponential backoff built into delay)
                try await throttler.waitBeforeNextBatch()
            }
        }

        throw lastError ?? ThrottlerError.maxRetriesExceeded
    }
}

// MARK: - ThrottlerError

/// Errors from throttling operations
public enum ThrottlerError: Error {
    case maxRetriesExceeded
}

// MARK: - CustomStringConvertible

extension ThrottleConfiguration: CustomStringConvertible {
    public var description: String {
        if self == .default {
            return "ThrottleConfiguration.default"
        }

        var parts: [String] = []

        if batch != .default {
            parts.append("batch: \(batch)")
        }
        if delay != .default {
            parts.append("delay: \(delay)")
        }
        if successesBeforeIncrease != 3 {
            parts.append("successesBeforeIncrease: \(successesBeforeIncrease)")
        }

        if parts.isEmpty {
            return "ThrottleConfiguration.default"
        }

        return "ThrottleConfiguration(\(parts.joined(separator: ", ")))"
    }
}

extension ThrottleConfiguration.BatchSettings: CustomStringConvertible {
    public var description: String {
        "BatchSettings(initial: \(initial), min: \(min), max: \(max))"
    }
}

extension ThrottleConfiguration.DelaySettings: CustomStringConvertible {
    public var description: String {
        "DelaySettings(min: \(min), max: \(max), initial: \(initial))"
    }
}
