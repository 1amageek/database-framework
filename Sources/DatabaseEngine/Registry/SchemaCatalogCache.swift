import Synchronization
import DatabaseKit
import StorageKit

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
        let insertedAt: StorageInstant
    }

    private struct State: Sendable {
        var cached: CachedEntities?
    }

    private let state: Mutex<State>
    private let timeToLive: Duration
    private let monotonicClock: any StorageMonotonicClock

    // MARK: - Initialization

    /// Initialize the cache
    ///
    /// - Parameter ttlSeconds: Time-to-live in seconds (default: 300 = 5 minutes)
    public init(
        timeToLive: Duration = .seconds(300),
        monotonicClock: any StorageMonotonicClock
    ) {
        self.state = Mutex(State())
        self.timeToLive = timeToLive
        self.monotonicClock = monotonicClock
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

            let age = cached.insertedAt.duration(to: monotonicClock.now)

            if age > timeToLive {
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
                insertedAt: monotonicClock.now
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
}
