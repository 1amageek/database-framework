import Foundation
import Synchronization
import Core

/// Thread-safe cache for Schema.Entity entries with TTL
///
/// **Purpose**: Reduce FDB reads by caching schema metadata in memory.
///
/// **Cache Strategy**:
/// - TTL-based: Entries expire after a configurable duration (default: 5 minutes)
/// - Write-through: Schema changes immediately invalidate the cache
/// - Read-through: Cache misses fetch from FDB and populate the cache
///
/// **Thread-safety**: Uses `Mutex` for concurrent access
public final class SchemaCatalogCache: Sendable {
    // MARK: - Internal State

    private struct CachedEntities: Sendable {
        let entities: [Schema.Entity]
        let timestamp: UInt64  // Unix timestamp in milliseconds
    }

    private struct State: Sendable {
        var cached: CachedEntities?
    }

    private let state: Mutex<State>
    private let ttlMilliseconds: UInt64

    // MARK: - Initialization

    /// Initialize the cache
    ///
    /// - Parameter ttlSeconds: Time-to-live in seconds (default: 300 = 5 minutes)
    public init(ttlSeconds: Int = 300) {
        self.state = Mutex(State())
        self.ttlMilliseconds = UInt64(ttlSeconds) * 1000
    }

    // MARK: - Public API

    /// Get cached entities if not expired
    ///
    /// - Returns: Cached entities, or nil if cache miss or expired
    public func get() -> [Schema.Entity]? {
        state.withLock { state in
            guard let cached = state.cached else {
                return nil
            }

            let now = currentTimestamp()
            let age = now - cached.timestamp

            if age > ttlMilliseconds {
                // Expired - clear cache
                state.cached = nil
                return nil
            }

            return cached.entities
        }
    }

    /// Set cached entities
    ///
    /// - Parameter entities: Entities to cache
    public func set(_ entities: [Schema.Entity]) {
        state.withLock { state in
            state.cached = CachedEntities(
                entities: entities,
                timestamp: currentTimestamp()
            )
        }
    }

    /// Clear all cached entities
    ///
    /// **When to call**:
    /// - After schema changes (persist/delete)
    /// - On explicit cache invalidation
    public func clear() {
        state.withLock { state in
            state.cached = nil
        }
    }

    // MARK: - Helper Methods

    /// Get current timestamp in milliseconds
    private func currentTimestamp() -> UInt64 {
        UInt64(Date().timeIntervalSince1970 * 1000)
    }
}
