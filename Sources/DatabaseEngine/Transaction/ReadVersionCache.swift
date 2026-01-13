// ReadVersionCache.swift
// DatabaseEngine - Cache for read versions to support CachePolicy
//
// Reference: FDB Record Layer FDBDatabase.java - lastSeenVersion caching
// https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/FDBDatabase.java

import Foundation
import Synchronization
import Dispatch

// MARK: - ReadVersionCache

/// Caches read versions from successful transactions
///
/// Used to implement cache policies by allowing transactions
/// to reuse recently observed read versions, reducing the number of
/// `getReadVersion()` calls to the cluster.
///
/// **Thread Safety**: Uses `Mutex` for thread-safe access.
///
/// **Monotonic Time**: Uses `DispatchTime.now().uptimeNanoseconds` for
/// monotonic timestamps that don't jump during clock adjustments.
///
/// **Reference**: FDB Record Layer caches `lastSeenVersion` and
/// `lastSeenVersionTime` in `FDBDatabase`.
///
/// **Usage**:
/// ```swift
/// let cache = ReadVersionCache()
///
/// // After successful commit
/// cache.updateFromCommit(version: committedVersion)
///
/// // Before starting transaction
/// if let cachedVersion = cache.getCachedVersion(policy: .cached) {
///     transaction.setReadVersion(cachedVersion)
/// }
/// ```
public final class ReadVersionCache: Sendable {
    // MARK: - Cached Version

    /// A cached version with its observation timestamp
    private struct CachedVersion: Sendable {
        /// The FDB read version
        let version: Int64

        /// Monotonic timestamp when this version was observed (nanoseconds)
        let timestamp: UInt64
    }

    // MARK: - Properties

    /// The cached version (thread-safe via Mutex)
    private let cache: Mutex<CachedVersion?>

    // MARK: - Initialization

    /// Create an empty read version cache
    public init() {
        self.cache = Mutex(nil)
    }

    // MARK: - Update Methods

    /// Update cache after successful commit
    ///
    /// The committed version represents the point in time when the
    /// transaction's writes became visible. This is the most accurate
    /// version to cache.
    ///
    /// - Parameter version: The committed version from the transaction
    public func updateFromCommit(version: Int64) {
        let now = DispatchTime.now().uptimeNanoseconds
        cache.withLock { $0 = CachedVersion(version: version, timestamp: now) }
    }

    /// Update cache after reading version
    ///
    /// Less accurate than commit version, but still useful for
    /// read-only transactions.
    ///
    /// Only updates if the new version is greater than the cached version
    /// to ensure monotonicity.
    ///
    /// - Parameter version: The read version from the transaction
    public func updateFromRead(version: Int64) {
        let now = DispatchTime.now().uptimeNanoseconds
        cache.withLock { cached in
            // Only update if newer (or no cached value)
            if cached == nil || version > cached!.version {
                cached = CachedVersion(version: version, timestamp: now)
            }
        }
    }

    // MARK: - Query Methods

    /// Get cached version if valid according to cache policy
    ///
    /// Returns the cached version based on policy:
    /// - `.server`: Always returns `nil` (no cache)
    /// - `.cached`: Returns cached version regardless of age
    /// - `.stale(N)`: Returns cached version only if age ≤ N seconds
    ///
    /// - Parameter policy: The cache policy
    /// - Returns: The cached version, or `nil` if not valid
    public func getCachedVersion(policy: CachePolicy) -> Int64? {
        switch policy {
        case .server:
            // Always fetch from server
            return nil

        case .cached:
            // Use cache if available (no age check)
            return cache.withLock { cached in
                cached?.version
            }

        case .stale(let seconds):
            // Use cache only if fresh enough
            return cache.withLock { cached in
                guard let cached = cached else { return nil }

                let now = DispatchTime.now().uptimeNanoseconds
                let ageNanos = now - cached.timestamp
                let ageSeconds = Double(ageNanos) / 1_000_000_000

                guard ageSeconds <= seconds else { return nil }

                return cached.version
            }
        }
    }

    /// Get current cached version info (for debugging/metrics)
    ///
    /// - Returns: Tuple of (version, ageMillis), or `nil` if no cached value
    public func currentCacheInfo() -> (version: Int64, ageMillis: Int64)? {
        cache.withLock { cached in
            guard let cached = cached else { return nil }

            let now = DispatchTime.now().uptimeNanoseconds
            let ageNanos = now - cached.timestamp
            let ageMillis = Int64(ageNanos / 1_000_000)

            return (cached.version, ageMillis)
        }
    }

    /// Check if cache has a valid entry for given policy
    ///
    /// - Parameter policy: The cache policy
    /// - Returns: `true` if cache has a valid entry
    public func hasValidCache(for policy: CachePolicy) -> Bool {
        getCachedVersion(policy: policy) != nil
    }

    // MARK: - Management

    /// Clear the cache
    ///
    /// Use after:
    /// - Schema changes that affect read compatibility
    /// - Testing scenarios requiring fresh reads
    /// - Recovery from errors
    public func clear() {
        cache.withLock { $0 = nil }
    }
}

// MARK: - ReadVersionCacheMetrics

/// Metrics for read version cache performance
///
/// Use to monitor cache effectiveness and tune staleness settings.
public struct ReadVersionCacheMetrics: Sendable {
    /// Total number of cache lookups
    public let lookups: Int64

    /// Number of cache hits
    public let hits: Int64

    /// Number of cache misses
    public let misses: Int64

    /// Cache hit rate (0.0 - 1.0)
    public var hitRate: Double {
        guard lookups > 0 else { return 0 }
        return Double(hits) / Double(lookups)
    }
}

// MARK: - MetricsCollectingReadVersionCache

/// Read version cache with metrics collection
///
/// Wraps `ReadVersionCache` to collect hit/miss statistics.
///
/// **Usage**:
/// ```swift
/// let cache = MetricsCollectingReadVersionCache()
///
/// // Use normally...
/// if let version = cache.getCachedVersion(policy: .cached) { ... }
///
/// // Check metrics
/// let metrics = cache.metrics
/// print("Cache hit rate: \(metrics.hitRate * 100)%")
/// ```
public final class MetricsCollectingReadVersionCache: Sendable {
    // MARK: - Properties

    private let inner: ReadVersionCache

    private struct Counters: Sendable {
        var lookups: Int64 = 0
        var hits: Int64 = 0
    }

    private let counters: Mutex<Counters>

    // MARK: - Initialization

    public init() {
        self.inner = ReadVersionCache()
        self.counters = Mutex(Counters())
    }

    // MARK: - Delegated Methods

    public func updateFromCommit(version: Int64) {
        inner.updateFromCommit(version: version)
    }

    public func updateFromRead(version: Int64) {
        inner.updateFromRead(version: version)
    }

    public func getCachedVersion(policy: CachePolicy) -> Int64? {
        let result = inner.getCachedVersion(policy: policy)

        counters.withLock { counters in
            counters.lookups += 1
            if result != nil {
                counters.hits += 1
            }
        }

        return result
    }

    public func clear() {
        inner.clear()
    }

    // MARK: - Metrics

    /// Get current metrics
    public var metrics: ReadVersionCacheMetrics {
        counters.withLock { counters in
            ReadVersionCacheMetrics(
                lookups: counters.lookups,
                hits: counters.hits,
                misses: counters.lookups - counters.hits
            )
        }
    }

    /// Reset metrics counters
    public func resetMetrics() {
        counters.withLock { counters in
            counters.lookups = 0
            counters.hits = 0
        }
    }
}
