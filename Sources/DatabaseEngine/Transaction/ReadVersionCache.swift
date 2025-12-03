// ReadVersionCache.swift
// DatabaseEngine - Caching for read versions to support weak read semantics
//
// Reference: FDB Record Layer WeakReadSemantics.java
// Allows reusing cached read versions for improved performance when
// strict consistency is not required.

import Foundation
import Synchronization

// MARK: - WeakReadSemantics

/// Configuration for weak read semantics
///
/// Weak read semantics allows transactions to use a cached read version
/// instead of getting a fresh one from the database. This improves performance
/// by avoiding a round-trip to the database, but may return slightly stale data.
///
/// **Usage**:
/// ```swift
/// // Use cached version within 5 seconds
/// let semantics = WeakReadSemantics.bounded(seconds: 5.0)
///
/// // Ensure we see data at least as new as a specific version
/// let semantics = WeakReadSemantics(minReadVersion: knownVersion)
/// ```
public struct WeakReadSemantics: Sendable, Equatable {
    /// Maximum staleness allowed for cached read version (seconds)
    public let maxStalenessSeconds: Double

    /// Minimum read version that must be used (if known)
    public let minReadVersion: Int64?

    /// Whether to use cached read version
    public let useCachedReadVersion: Bool

    /// Strict read semantics - always get fresh read version
    public static let strict = WeakReadSemantics(
        maxStalenessSeconds: 0,
        minReadVersion: nil,
        useCachedReadVersion: false
    )

    /// Bounded staleness - use cached version within specified time
    public static func bounded(seconds: Double) -> WeakReadSemantics {
        WeakReadSemantics(
            maxStalenessSeconds: seconds,
            minReadVersion: nil,
            useCachedReadVersion: true
        )
    }

    /// Minimum version - ensure we see at least this version
    public static func atLeast(version: Int64) -> WeakReadSemantics {
        WeakReadSemantics(
            maxStalenessSeconds: .infinity,
            minReadVersion: version,
            useCachedReadVersion: true
        )
    }

    public init(
        maxStalenessSeconds: Double = 0,
        minReadVersion: Int64? = nil,
        useCachedReadVersion: Bool = false
    ) {
        self.maxStalenessSeconds = maxStalenessSeconds
        self.minReadVersion = minReadVersion
        self.useCachedReadVersion = useCachedReadVersion
    }
}

// MARK: - CachedVersion

/// A cached read version with its timestamp
struct CachedVersion: Sendable {
    /// The cached read version
    let version: Int64

    /// When the version was cached
    let timestamp: Date

    /// Whether this version is still valid for the given semantics
    func isValid(for semantics: WeakReadSemantics, now: Date = Date()) -> Bool {
        guard semantics.useCachedReadVersion else {
            return false
        }

        // Check staleness bound
        let age = now.timeIntervalSince(timestamp)
        if age > semantics.maxStalenessSeconds {
            return false
        }

        // Check minimum version requirement
        if let minVersion = semantics.minReadVersion, version < minVersion {
            return false
        }

        return true
    }
}

// MARK: - ReadVersionCache

/// Cache for read versions to support weak read semantics
///
/// This cache stores the most recently seen read version and commit version,
/// allowing subsequent transactions to reuse these versions when strict
/// consistency is not required.
///
/// **Thread Safety**: This class is thread-safe and can be shared across
/// multiple transactions.
///
/// **Usage**:
/// ```swift
/// let cache = ReadVersionCache()
///
/// // When opening a transaction with weak read semantics
/// if let cachedVersion = cache.getCachedVersion(semantics: .bounded(seconds: 5.0)) {
///     transaction.setReadVersion(cachedVersion)
/// }
///
/// // After getting a read version, update the cache
/// cache.updateReadVersion(readVersion, timestamp: Date())
///
/// // After a successful commit, record the commit version
/// cache.recordCommitVersion(commitVersion)
/// ```
public final class ReadVersionCache: Sendable {
    // MARK: - State

    private struct State: Sendable {
        /// Last seen read version
        var lastReadVersion: CachedVersion?

        /// Last seen commit version (always at least as recent as read version)
        var lastCommitVersion: Int64?

        /// Number of cache hits
        var hitCount: Int = 0

        /// Number of cache misses
        var missCount: Int = 0
    }

    private let state: Mutex<State>

    // MARK: - Initialization

    public init() {
        self.state = Mutex(State())
    }

    // MARK: - Public API

    /// Get a cached read version if available and valid for the given semantics
    ///
    /// - Parameter semantics: The weak read semantics to use
    /// - Returns: A cached read version, or nil if none is available/valid
    public func getCachedVersion(semantics: WeakReadSemantics) -> Int64? {
        state.withLock { state in
            guard let cached = state.lastReadVersion,
                  cached.isValid(for: semantics) else {
                state.missCount += 1
                return nil
            }

            // Also check against commit version if we have one
            if let commitVersion = state.lastCommitVersion,
               let minVersion = semantics.minReadVersion,
               commitVersion < minVersion {
                state.missCount += 1
                return nil
            }

            state.hitCount += 1
            return cached.version
        }
    }

    /// Update the cached read version
    ///
    /// - Parameters:
    ///   - version: The read version to cache
    ///   - timestamp: When this version was obtained
    public func updateReadVersion(_ version: Int64, timestamp: Date = Date()) {
        state.withLock { state in
            // Only update if this is a newer version
            if let current = state.lastReadVersion, current.version >= version {
                return
            }
            state.lastReadVersion = CachedVersion(version: version, timestamp: timestamp)
        }
    }

    /// Record a commit version
    ///
    /// Commit versions are always at least as recent as read versions,
    /// so they can be used to update the cache as well.
    ///
    /// - Parameter version: The commit version
    public func recordCommitVersion(_ version: Int64) {
        state.withLock { state in
            // Update commit version
            if let current = state.lastCommitVersion, current >= version {
                return
            }
            state.lastCommitVersion = version

            // Also update read version if this is newer
            if state.lastReadVersion == nil || state.lastReadVersion!.version < version {
                state.lastReadVersion = CachedVersion(version: version, timestamp: Date())
            }
        }
    }

    /// Invalidate the cache
    ///
    /// This forces the next transaction to get a fresh read version.
    public func invalidate() {
        state.withLock { state in
            state.lastReadVersion = nil
            state.lastCommitVersion = nil
        }
    }

    /// Get cache statistics
    public var statistics: ReadVersionCacheStatistics {
        state.withLock { state in
            ReadVersionCacheStatistics(
                hitCount: state.hitCount,
                missCount: state.missCount,
                lastReadVersion: state.lastReadVersion?.version,
                lastCommitVersion: state.lastCommitVersion
            )
        }
    }

    /// Reset statistics counters
    public func resetStatistics() {
        state.withLock { state in
            state.hitCount = 0
            state.missCount = 0
        }
    }
}

// MARK: - ReadVersionCacheStatistics

/// Statistics about the read version cache
public struct ReadVersionCacheStatistics: Sendable {
    /// Number of cache hits
    public let hitCount: Int

    /// Number of cache misses
    public let missCount: Int

    /// Last cached read version
    public let lastReadVersion: Int64?

    /// Last recorded commit version
    public let lastCommitVersion: Int64?

    /// Cache hit ratio (0.0 - 1.0)
    public var hitRatio: Double {
        let total = hitCount + missCount
        guard total > 0 else { return 0.0 }
        return Double(hitCount) / Double(total)
    }
}

// MARK: - ReadVersionCacheConfiguration

/// Configuration for read version caching
public struct ReadVersionCacheConfiguration: Sendable {
    /// Whether caching is enabled
    public let enabled: Bool

    /// Default staleness for cached reads (seconds)
    public let defaultStalenessSeconds: Double

    /// Whether to track last seen version even when not using weak semantics
    public let trackLastSeenVersion: Bool

    /// Default configuration
    public static let `default` = ReadVersionCacheConfiguration(
        enabled: true,
        defaultStalenessSeconds: 5.0,
        trackLastSeenVersion: true
    )

    /// Disabled caching
    public static let disabled = ReadVersionCacheConfiguration(
        enabled: false,
        defaultStalenessSeconds: 0,
        trackLastSeenVersion: false
    )

    public init(
        enabled: Bool = true,
        defaultStalenessSeconds: Double = 5.0,
        trackLastSeenVersion: Bool = true
    ) {
        self.enabled = enabled
        self.defaultStalenessSeconds = defaultStalenessSeconds
        self.trackLastSeenVersion = trackLastSeenVersion
    }
}
