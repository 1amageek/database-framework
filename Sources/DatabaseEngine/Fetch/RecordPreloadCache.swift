// RecordPreloadCache.swift
// DatabaseEngine - In-memory cache for preloaded records
//
// Reference: FDB Record Layer ScanProperties.ExecuteState and preload behavior
// Caches frequently accessed records to reduce database reads.

import Foundation
import Synchronization
import Core

// MARK: - CacheConfiguration

/// Configuration for record preload cache
public struct CacheConfiguration: Sendable, Equatable {
    /// Maximum number of entries in the cache
    public let maxEntries: Int

    /// Maximum memory usage in bytes (approximate)
    public let maxMemoryBytes: Int

    /// Time-to-live for cache entries in seconds
    public let ttlSeconds: Double

    /// Whether to track cache statistics
    public let enableStatistics: Bool

    /// Eviction policy
    public let evictionPolicy: CacheEvictionPolicy

    /// Default configuration
    public static let `default` = CacheConfiguration(
        maxEntries: 10_000,
        maxMemoryBytes: 100 * 1024 * 1024, // 100MB
        ttlSeconds: 300, // 5 minutes
        enableStatistics: true,
        evictionPolicy: .lru
    )

    /// Small cache configuration
    public static let small = CacheConfiguration(
        maxEntries: 1_000,
        maxMemoryBytes: 10 * 1024 * 1024, // 10MB
        ttlSeconds: 60, // 1 minute
        enableStatistics: true,
        evictionPolicy: .lru
    )

    /// Large cache configuration
    public static let large = CacheConfiguration(
        maxEntries: 100_000,
        maxMemoryBytes: 1024 * 1024 * 1024, // 1GB
        ttlSeconds: 600, // 10 minutes
        enableStatistics: true,
        evictionPolicy: .lru
    )

    public init(
        maxEntries: Int = 10_000,
        maxMemoryBytes: Int = 100 * 1024 * 1024,
        ttlSeconds: Double = 300,
        enableStatistics: Bool = true,
        evictionPolicy: CacheEvictionPolicy = .lru
    ) {
        precondition(maxEntries > 0, "maxEntries must be positive")
        precondition(maxMemoryBytes > 0, "maxMemoryBytes must be positive")
        precondition(ttlSeconds > 0, "ttlSeconds must be positive")

        self.maxEntries = maxEntries
        self.maxMemoryBytes = maxMemoryBytes
        self.ttlSeconds = ttlSeconds
        self.enableStatistics = enableStatistics
        self.evictionPolicy = evictionPolicy
    }
}

// MARK: - CacheEvictionPolicy

/// Eviction policy for cache entries
public enum CacheEvictionPolicy: String, Sendable, CaseIterable {
    /// Least Recently Used - evict entries not accessed recently
    case lru

    /// Least Frequently Used - evict entries accessed less often
    case lfu

    /// First In First Out - evict oldest entries
    case fifo

    /// Time-based only - evict only expired entries
    case ttlOnly
}

// MARK: - RecordPreloadCache

/// In-memory cache for preloaded records
///
/// Provides fast access to frequently used records without database reads.
/// Supports multiple eviction policies and statistics tracking.
///
/// **Thread Safety**: This class is thread-safe using Mutex synchronization.
///
/// **Usage**:
/// ```swift
/// let cache = RecordPreloadCache<User>(configuration: .default)
///
/// // Preload records
/// cache.preload(users)
///
/// // Get from cache (returns nil if not cached)
/// if let user = cache.get(key: userId) {
///     // Cache hit
/// }
///
/// // Get or fetch
/// let user = try await cache.getOrFetch(key: userId) {
///     try await fetchUserFromDatabase(userId)
/// }
/// ```
public final class RecordPreloadCache<Item: Persistable>: Sendable {
    // MARK: - Cache Entry

    private struct CacheEntry: Sendable {
        let item: Item
        let insertedAt: Date
        var lastAccessedAt: Date
        var accessCount: Int
        let approximateSize: Int

        var isExpired: Bool {
            false // Checked against TTL externally
        }

        func isExpired(ttl: TimeInterval) -> Bool {
            Date().timeIntervalSince(insertedAt) > ttl
        }
    }

    // MARK: - LRU Node (Doubly Linked List)

    /// Node for doubly linked list used in LRU/FIFO tracking
    /// Reference: Standard LRU cache implementation pattern
    private final class LRUNode: @unchecked Sendable {
        let key: String
        var prev: LRUNode?
        var next: LRUNode?

        init(key: String) {
            self.key = key
        }
    }

    // MARK: - State

    private struct State: @unchecked Sendable {
        var entries: [String: CacheEntry] = [:]

        // LRU doubly-linked list for O(1) access order management
        // head = least recently used, tail = most recently used
        var lruHead: LRUNode?
        var lruTail: LRUNode?
        var lruNodes: [String: LRUNode] = [:]  // O(1) node lookup

        // FIFO doubly-linked list
        var fifoHead: LRUNode?
        var fifoTail: LRUNode?
        var fifoNodes: [String: LRUNode] = [:]

        var totalSize: Int = 0

        // Statistics
        var hits: Int = 0
        var misses: Int = 0
        var evictions: Int = 0
        var expirations: Int = 0
    }

    private let state: Mutex<State>
    public let configuration: CacheConfiguration

    // MARK: - Initialization

    public init(configuration: CacheConfiguration = .default) {
        self.configuration = configuration
        self.state = Mutex(State())
    }

    // MARK: - Basic Operations

    /// Get an item from cache
    ///
    /// - Parameter key: The cache key
    /// - Returns: The cached item, or nil if not found/expired
    public func get(key: String) -> Item? {
        state.withLock { state in
            guard var entry = state.entries[key] else {
                if configuration.enableStatistics {
                    state.misses += 1
                }
                return nil
            }

            // Check expiration
            if entry.isExpired(ttl: configuration.ttlSeconds) {
                state.entries.removeValue(forKey: key)
                state.totalSize -= entry.approximateSize
                removeFromLRU(key: key, state: &state)
                removeFromFIFO(key: key, state: &state)
                if configuration.enableStatistics {
                    state.expirations += 1
                    state.misses += 1
                }
                return nil
            }

            // Update access tracking
            entry.lastAccessedAt = Date()
            entry.accessCount += 1
            state.entries[key] = entry

            // Update LRU order - O(1) with doubly linked list
            if configuration.evictionPolicy == .lru {
                moveToLRUTail(key: key, state: &state)
            }

            if configuration.enableStatistics {
                state.hits += 1
            }

            return entry.item
        }
    }

    /// Put an item into cache
    ///
    /// - Parameters:
    ///   - item: The item to cache
    ///   - key: The cache key
    public func put(item: Item, key: String) {
        let size = estimateSize(item)

        state.withLock { state in
            // Remove existing entry if present
            if let existing = state.entries[key] {
                state.totalSize -= existing.approximateSize
                removeFromLRU(key: key, state: &state)
                removeFromFIFO(key: key, state: &state)
            }

            // Evict if necessary
            while state.entries.count >= configuration.maxEntries ||
                  state.totalSize + size > configuration.maxMemoryBytes {
                evictOne(state: &state)
            }

            // Add new entry
            let entry = CacheEntry(
                item: item,
                insertedAt: Date(),
                lastAccessedAt: Date(),
                accessCount: 1,
                approximateSize: size
            )

            state.entries[key] = entry
            state.totalSize += size

            // Add to LRU list (at tail = most recently used)
            addToLRUTail(key: key, state: &state)
            // Add to FIFO list (at tail = newest)
            addToFIFOTail(key: key, state: &state)
        }
    }

    /// Remove an item from cache
    ///
    /// - Parameter key: The cache key
    /// - Returns: The removed item, or nil if not found
    @discardableResult
    public func remove(key: String) -> Item? {
        state.withLock { state in
            guard let entry = state.entries.removeValue(forKey: key) else {
                return nil
            }
            state.totalSize -= entry.approximateSize
            removeFromLRU(key: key, state: &state)
            removeFromFIFO(key: key, state: &state)
            return entry.item
        }
    }

    /// Check if cache contains key
    ///
    /// - Parameter key: The cache key
    /// - Returns: True if key exists and is not expired
    public func contains(key: String) -> Bool {
        state.withLock { state in
            guard let entry = state.entries[key] else { return false }
            return !entry.isExpired(ttl: configuration.ttlSeconds)
        }
    }

    /// Clear all entries
    public func clear() {
        state.withLock { state in
            state.entries.removeAll()
            // Clear LRU list
            state.lruHead = nil
            state.lruTail = nil
            state.lruNodes.removeAll()
            // Clear FIFO list
            state.fifoHead = nil
            state.fifoTail = nil
            state.fifoNodes.removeAll()
            state.totalSize = 0
        }
    }

    // MARK: - Bulk Operations

    /// Preload multiple items
    ///
    /// - Parameter items: Items to preload with their keys
    public func preload(_ items: [(key: String, item: Item)]) {
        for (key, item) in items {
            put(item: item, key: key)
        }
    }

    /// Preload items using ID as key
    ///
    /// - Parameter items: Items to preload
    public func preload(_ items: [Item]) {
        for item in items {
            let key = cacheKey(for: item)
            put(item: item, key: key)
        }
    }

    /// Get multiple items from cache
    ///
    /// - Parameter keys: Keys to retrieve
    /// - Returns: Dictionary of found items
    public func getMultiple(keys: [String]) -> [String: Item] {
        var result: [String: Item] = [:]
        for key in keys {
            if let item = get(key: key) {
                result[key] = item
            }
        }
        return result
    }

    // MARK: - Get or Fetch

    /// Get from cache or fetch if not present
    ///
    /// - Parameters:
    ///   - key: The cache key
    ///   - fetch: Closure to fetch the item if not cached
    /// - Returns: The item (from cache or freshly fetched)
    public func getOrFetch(
        key: String,
        fetch: () async throws -> Item
    ) async throws -> Item {
        // Check cache first
        if let cached = get(key: key) {
            return cached
        }

        // Fetch and cache
        let item = try await fetch()
        put(item: item, key: key)
        return item
    }

    /// Get from cache or fetch if not present (optional result)
    ///
    /// - Parameters:
    ///   - key: The cache key
    ///   - fetch: Closure to fetch the item if not cached
    /// - Returns: The item or nil if not found in cache or fetch
    public func getOrFetchOptional(
        key: String,
        fetch: () async throws -> Item?
    ) async throws -> Item? {
        // Check cache first
        if let cached = get(key: key) {
            return cached
        }

        // Fetch and cache
        guard let item = try await fetch() else {
            return nil
        }
        put(item: item, key: key)
        return item
    }

    // MARK: - Statistics

    /// Get cache statistics
    public var statistics: PreloadCacheStatistics {
        state.withLock { state in
            PreloadCacheStatistics(
                entryCount: state.entries.count,
                totalSizeBytes: state.totalSize,
                hits: state.hits,
                misses: state.misses,
                evictions: state.evictions,
                expirations: state.expirations
            )
        }
    }

    /// Reset statistics
    public func resetStatistics() {
        state.withLock { state in
            state.hits = 0
            state.misses = 0
            state.evictions = 0
            state.expirations = 0
        }
    }

    // MARK: - Eviction

    private func evictOne(state: inout State) {
        guard !state.entries.isEmpty else { return }

        let keyToEvict: String?

        switch configuration.evictionPolicy {
        case .lru:
            // Evict least recently accessed - O(1) from head of LRU list
            keyToEvict = state.lruHead?.key

        case .lfu:
            // Evict least frequently accessed - O(n) scan still needed
            // Note: Could be optimized with priority queue if needed
            keyToEvict = state.entries.min(by: { $0.value.accessCount < $1.value.accessCount })?.key

        case .fifo:
            // Evict oldest - O(1) from head of FIFO list
            keyToEvict = state.fifoHead?.key

        case .ttlOnly:
            // Only evict expired entries - O(n) scan
            keyToEvict = state.entries.first(where: { $0.value.isExpired(ttl: configuration.ttlSeconds) })?.key
        }

        if let key = keyToEvict, let entry = state.entries.removeValue(forKey: key) {
            state.totalSize -= entry.approximateSize
            removeFromLRU(key: key, state: &state)
            removeFromFIFO(key: key, state: &state)
            if configuration.enableStatistics {
                state.evictions += 1
            }
        }
    }

    // MARK: - LRU Doubly Linked List Operations (O(1))

    /// Add a key to the tail of the LRU list (most recently used)
    private func addToLRUTail(key: String, state: inout State) {
        let node = LRUNode(key: key)
        state.lruNodes[key] = node

        if let tail = state.lruTail {
            tail.next = node
            node.prev = tail
            state.lruTail = node
        } else {
            state.lruHead = node
            state.lruTail = node
        }
    }

    /// Remove a key from the LRU list
    private func removeFromLRU(key: String, state: inout State) {
        guard let node = state.lruNodes.removeValue(forKey: key) else { return }

        // Update prev/next pointers
        if let prev = node.prev {
            prev.next = node.next
        } else {
            state.lruHead = node.next
        }

        if let next = node.next {
            next.prev = node.prev
        } else {
            state.lruTail = node.prev
        }

        node.prev = nil
        node.next = nil
    }

    /// Move a key to the tail of the LRU list (most recently used)
    private func moveToLRUTail(key: String, state: inout State) {
        guard state.lruNodes[key] != nil else { return }
        removeFromLRU(key: key, state: &state)
        addToLRUTail(key: key, state: &state)
    }

    // MARK: - FIFO Doubly Linked List Operations (O(1))

    /// Add a key to the tail of the FIFO list (newest)
    private func addToFIFOTail(key: String, state: inout State) {
        let node = LRUNode(key: key)
        state.fifoNodes[key] = node

        if let tail = state.fifoTail {
            tail.next = node
            node.prev = tail
            state.fifoTail = node
        } else {
            state.fifoHead = node
            state.fifoTail = node
        }
    }

    /// Remove a key from the FIFO list
    private func removeFromFIFO(key: String, state: inout State) {
        guard let node = state.fifoNodes.removeValue(forKey: key) else { return }

        // Update prev/next pointers
        if let prev = node.prev {
            prev.next = node.next
        } else {
            state.fifoHead = node.next
        }

        if let next = node.next {
            next.prev = node.prev
        } else {
            state.fifoTail = node.prev
        }

        node.prev = nil
        node.next = nil
    }

    // MARK: - Helpers

    private func cacheKey(for item: Item) -> String {
        // Use the item's ID as cache key - Persistable guarantees id property
        return "\(item.id)"
    }

    private func estimateSize(_ item: Item) -> Int {
        // Estimate size using JSON encoding as proxy
        do {
            let data = try JSONEncoder().encode(item)
            return data.count + 64 // Add overhead for cache entry
        } catch {
            return 256 // Default estimate
        }
    }
}

// MARK: - CacheStatistics

/// Statistics about preload cache performance
public struct PreloadCacheStatistics: Sendable {
    /// Current number of entries
    public let entryCount: Int

    /// Current total size in bytes
    public let totalSizeBytes: Int

    /// Total cache hits
    public let hits: Int

    /// Total cache misses
    public let misses: Int

    /// Total evictions
    public let evictions: Int

    /// Total expirations
    public let expirations: Int

    /// Hit rate (0.0 - 1.0)
    public var hitRate: Double {
        let total = hits + misses
        guard total > 0 else { return 0 }
        return Double(hits) / Double(total)
    }

    /// Miss rate (0.0 - 1.0)
    public var missRate: Double {
        1.0 - hitRate
    }

    /// Total requests
    public var totalRequests: Int {
        hits + misses
    }
}

// MARK: - ScopedCache

/// Cache scoped to a specific record type
///
/// Provides type-safe caching with automatic key generation.
public struct ScopedCache<Item: Persistable>: Sendable {
    private let cache: RecordPreloadCache<Item>
    private let keyPrefix: String

    public init(cache: RecordPreloadCache<Item>, keyPrefix: String = "") {
        self.cache = cache
        self.keyPrefix = keyPrefix
    }

    /// Generate a scoped key
    public func scopedKey(_ key: String) -> String {
        keyPrefix.isEmpty ? key : "\(keyPrefix):\(key)"
    }

    /// Get item by key
    public func get(key: String) -> Item? {
        cache.get(key: scopedKey(key))
    }

    /// Put item with key
    public func put(item: Item, key: String) {
        cache.put(item: item, key: scopedKey(key))
    }

    /// Remove item by key
    @discardableResult
    public func remove(key: String) -> Item? {
        cache.remove(key: scopedKey(key))
    }

    /// Get or fetch
    public func getOrFetch(
        key: String,
        fetch: () async throws -> Item
    ) async throws -> Item {
        try await cache.getOrFetch(key: scopedKey(key), fetch: fetch)
    }
}

// MARK: - CacheWarmer

/// Utility for warming caches
///
/// Pre-populates caches with frequently accessed data.
public struct CacheWarmer<Item: Persistable>: Sendable {
    private let cache: RecordPreloadCache<Item>

    public init(cache: RecordPreloadCache<Item>) {
        self.cache = cache
    }

    /// Warm cache with items from a source
    ///
    /// - Parameters:
    ///   - source: Async sequence of items to preload
    ///   - keyExtractor: Function to extract cache key from item
    ///   - limit: Maximum items to preload (nil for unlimited)
    /// - Returns: Number of items preloaded
    @discardableResult
    public func warm<S: AsyncSequence>(
        from source: S,
        keyExtractor: (Item) -> String,
        limit: Int? = nil
    ) async throws -> Int where S.Element == Item {
        var count = 0

        for try await item in source {
            let key = keyExtractor(item)
            cache.put(item: item, key: key)
            count += 1

            if let limit = limit, count >= limit {
                break
            }
        }

        return count
    }

    /// Warm cache with items array
    ///
    /// - Parameters:
    ///   - items: Items to preload
    ///   - keyExtractor: Function to extract cache key from item
    @discardableResult
    public func warm(
        items: [Item],
        keyExtractor: (Item) -> String
    ) -> Int {
        for item in items {
            let key = keyExtractor(item)
            cache.put(item: item, key: key)
        }
        return items.count
    }
}
