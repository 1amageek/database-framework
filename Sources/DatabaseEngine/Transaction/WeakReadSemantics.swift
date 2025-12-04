// WeakReadSemantics.swift
// DatabaseEngine - Configuration for weak read consistency
//
// Reference: FDB Record Layer FDBRecordContextConfig.java
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBRecordContextConfig.java

import Foundation

// MARK: - WeakReadSemantics

/// Configuration for weak read semantics
///
/// Weak read semantics allow transactions to reuse cached read versions,
/// reducing the number of `getReadVersion()` calls (network round-trips).
///
/// **Trade-off**: Slightly stale data vs. reduced latency and load.
///
/// **Reference**: FDB Record Layer `WeakReadSemantics` class
///
/// **Usage**:
/// ```swift
/// // Use relaxed consistency for analytics queries
/// let config = TransactionConfiguration(
///     priority: .batch,
///     weakReadSemantics: .relaxed  // Up to 30 second staleness
/// )
///
/// try await container.withTransaction(configuration: config) { tx in
///     // May read slightly stale data, but with lower latency
/// }
/// ```
public struct WeakReadSemantics: Sendable, Hashable {
    // MARK: - Properties

    /// Minimum acceptable read version
    ///
    /// Cached versions older than this will not be used.
    /// Use `0` to accept any cached version (subject to staleness limit).
    ///
    /// **Use Case**: Ensure reads see at least version X (e.g., after a known write).
    public let minVersion: Int64

    /// Maximum staleness in milliseconds
    ///
    /// Cached versions older than this will not be used.
    /// - `0`: No staleness allowed (equivalent to strict consistency)
    /// - `5000`: Up to 5 seconds old (default)
    /// - `30000`: Up to 30 seconds old (relaxed)
    ///
    /// **Reference**: FDB typically uses 5-10 seconds for most use cases.
    public let maxStalenessMillis: Int64

    /// Whether to use cached read version
    ///
    /// If `false`, always get a fresh read version from the cluster.
    public let useCachedVersion: Bool

    // MARK: - Initialization

    /// Create a custom weak read semantics configuration
    ///
    /// - Parameters:
    ///   - minVersion: Minimum acceptable read version (default: 0 = any)
    ///   - maxStalenessMillis: Maximum staleness in ms (default: 5000 = 5 seconds)
    ///   - useCachedVersion: Whether to use cached version (default: true)
    public init(
        minVersion: Int64 = 0,
        maxStalenessMillis: Int64 = 5000,
        useCachedVersion: Bool = true
    ) {
        self.minVersion = minVersion
        self.maxStalenessMillis = maxStalenessMillis
        self.useCachedVersion = useCachedVersion
    }

    // MARK: - Presets

    /// Default: 5 second staleness, use cache
    ///
    /// Good balance for most read-heavy workloads.
    public static let `default` = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 5000,
        useCachedVersion: true
    )

    /// Strict consistency: never use cache
    ///
    /// Always get fresh read version. Use for:
    /// - Read-after-write consistency requirements
    /// - Critical business logic
    /// - When staleness is not acceptable
    public static let strict = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 0,
        useCachedVersion: false
    )

    /// Relaxed: up to 30 second staleness
    ///
    /// Use for:
    /// - Analytics queries
    /// - Background batch processing
    /// - Dashboard/reporting reads
    /// - Scenarios where slightly stale data is acceptable
    public static let relaxed = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 30_000,
        useCachedVersion: true
    )

    /// Very relaxed: up to 60 second staleness
    ///
    /// Use for:
    /// - Long-running batch jobs
    /// - Historical data analysis
    /// - Scenarios where data freshness is not critical
    public static let veryRelaxed = WeakReadSemantics(
        minVersion: 0,
        maxStalenessMillis: 60_000,
        useCachedVersion: true
    )

    // MARK: - Factory Methods

    /// Create semantics requiring a minimum version
    ///
    /// Useful for read-after-write scenarios where you know
    /// the version of a recent write.
    ///
    /// - Parameter version: Minimum acceptable read version
    /// - Returns: Semantics that require at least the specified version
    public static func atLeast(version: Int64) -> WeakReadSemantics {
        WeakReadSemantics(
            minVersion: version,
            maxStalenessMillis: 5000,
            useCachedVersion: true
        )
    }

    /// Create semantics with custom staleness tolerance
    ///
    /// - Parameter seconds: Maximum staleness in seconds
    /// - Returns: Semantics with the specified staleness limit
    public static func maxStaleness(seconds: Int) -> WeakReadSemantics {
        WeakReadSemantics(
            minVersion: 0,
            maxStalenessMillis: Int64(seconds) * 1000,
            useCachedVersion: true
        )
    }
}

// MARK: - CustomStringConvertible

extension WeakReadSemantics: CustomStringConvertible {
    public var description: String {
        if !useCachedVersion || maxStalenessMillis == 0 {
            return "WeakReadSemantics.strict"
        }

        var parts: [String] = []

        if minVersion > 0 {
            parts.append("minVersion: \(minVersion)")
        }

        if maxStalenessMillis != 5000 {
            if maxStalenessMillis >= 1000 {
                parts.append("maxStaleness: \(maxStalenessMillis / 1000)s")
            } else {
                parts.append("maxStaleness: \(maxStalenessMillis)ms")
            }
        }

        if parts.isEmpty {
            return "WeakReadSemantics.default"
        }

        return "WeakReadSemantics(\(parts.joined(separator: ", ")))"
    }
}

// MARK: - Codable

extension WeakReadSemantics: Codable {}
