// DatabaseConfiguration.swift
// DatabaseEngine - Global configuration for database operations
//
// Reference: apple/swift-configuration
// https://github.com/apple/swift-configuration

import Foundation
import Configuration

// MARK: - DatabaseConfiguration

/// Global configuration for database operations
///
/// DatabaseConfiguration provides a centralized place to configure database behavior
/// such as transaction retry limits, backoff delays, and other operational parameters.
///
/// **Configuration Sources** (in priority order):
/// 1. Environment variables (highest priority)
/// 2. Programmatic defaults (lowest priority)
///
/// **Environment Variables**:
/// - `DATABASE_TRANSACTION_RETRY_LIMIT`: Max retry attempts (default: 5)
/// - `DATABASE_TRANSACTION_MAX_RETRY_DELAY`: Max delay between retries in ms (default: 1000)
/// - `DATABASE_TRANSACTION_INITIAL_DELAY`: Initial backoff delay in ms (default: 300)
/// - `DATABASE_TRANSACTION_TIMEOUT`: Default timeout in ms (default: nil = FDB default ~5s)
///
/// **Usage**:
/// ```swift
/// // Access global configuration
/// let retryLimit = DatabaseConfiguration.shared.transactionRetryLimit
///
/// // Or create custom configuration
/// let config = DatabaseConfiguration(transactionRetryLimit: 10)
/// ```
///
/// **Reference**: apple/swift-configuration provider hierarchy
public struct DatabaseConfiguration: Sendable {
    // MARK: - Properties

    /// Maximum number of retry attempts for transactions
    ///
    /// After this many retries, the transaction will fail with an error.
    /// Default: 5
    public let transactionRetryLimit: Int

    /// Maximum delay between retries in milliseconds
    ///
    /// Caps the exponential backoff delay.
    /// Default: 1000ms
    public let transactionMaxRetryDelay: Int

    /// Initial backoff delay in milliseconds
    ///
    /// The base delay before the first retry, which doubles with each subsequent attempt.
    /// Default: 300ms
    public let transactionInitialDelay: Int

    /// Default transaction timeout in milliseconds
    ///
    /// When elapsed, the transaction is automatically cancelled.
    /// - `nil`: Use FDB default (typically 5 seconds)
    /// - `0`: Disable timeout
    /// Default: nil
    public let transactionTimeout: Int?

    // MARK: - Initialization

    /// Create a custom database configuration
    ///
    /// - Parameters:
    ///   - transactionRetryLimit: Max retry attempts (default: 5)
    ///   - transactionMaxRetryDelay: Max delay between retries in ms (default: 1000)
    ///   - transactionInitialDelay: Initial backoff delay in ms (default: 300)
    ///   - transactionTimeout: Default timeout in ms (default: nil = FDB default)
    public init(
        transactionRetryLimit: Int = 5,
        transactionMaxRetryDelay: Int = 1000,
        transactionInitialDelay: Int = 300,
        transactionTimeout: Int? = nil
    ) {
        self.transactionRetryLimit = transactionRetryLimit
        self.transactionMaxRetryDelay = transactionMaxRetryDelay
        self.transactionInitialDelay = transactionInitialDelay
        self.transactionTimeout = transactionTimeout
    }

    // MARK: - Shared Instance

    /// Shared global configuration instance
    ///
    /// This instance reads configuration from environment variables with
    /// fallback to programmatic defaults.
    ///
    /// **Environment Variables**:
    /// - `DATABASE_TRANSACTION_RETRY_LIMIT`: Max retry attempts
    /// - `DATABASE_TRANSACTION_MAX_RETRY_DELAY`: Max delay in ms
    /// - `DATABASE_TRANSACTION_INITIAL_DELAY`: Initial delay in ms
    /// - `DATABASE_TRANSACTION_TIMEOUT`: Default timeout in ms
    public static let shared: DatabaseConfiguration = {
        let configReader = ConfigReader(providers: [
            // Environment variables take priority
            EnvironmentVariablesProvider(),
            // Fallback to programmatic defaults
            InMemoryProvider(
                name: "database-defaults",
                values: [
                    "database.transaction.retry_limit": 5,
                    "database.transaction.max_retry_delay": 1000,
                    "database.transaction.initial_delay": 300,
                ]
            )
        ])

        let dbConfig = configReader.scoped(to: "database.transaction")

        return DatabaseConfiguration(
            transactionRetryLimit: dbConfig.int(forKey: "retry_limit", default: 5),
            transactionMaxRetryDelay: dbConfig.int(forKey: "max_retry_delay", default: 1000),
            transactionInitialDelay: dbConfig.int(forKey: "initial_delay", default: 300),
            transactionTimeout: dbConfig.int(forKey: "timeout")
        )
    }()
}

// MARK: - CustomStringConvertible

extension DatabaseConfiguration: CustomStringConvertible {
    public var description: String {
        var parts: [String] = [
            "retryLimit: \(transactionRetryLimit)",
            "maxRetryDelay: \(transactionMaxRetryDelay)ms",
            "initialDelay: \(transactionInitialDelay)ms"
        ]

        if let timeout = transactionTimeout {
            parts.append("timeout: \(timeout)ms")
        }

        return "DatabaseConfiguration(\(parts.joined(separator: ", ")))"
    }
}
