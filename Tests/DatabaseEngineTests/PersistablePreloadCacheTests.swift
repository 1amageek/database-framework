#if !os(WASI)
// PersistablePreloadCacheTests.swift
// DatabaseEngine Tests - Entity preload cache tests

import Testing
import TestHeartbeat
import Foundation
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Model

@Persistable
struct PreloadCacheItem: Equatable {
    var id: String = UUID().uuidString
    var name: String
    var value: Int64
}

// MARK: - CacheConfiguration Tests

@Suite("CacheConfiguration Tests", .heartbeat)
struct CacheConfigurationTests {

    @Test("Default configuration values")
    func defaultConfiguration() throws {
        let config = CacheConfiguration.default

        #expect(config.maxEntries == 10_000)
        #expect(config.maxMemoryBytes == 100 * 1024 * 1024)
        #expect(config.timeToLive == .seconds(300))
        #expect(config.enableStatistics == true)
        #expect(config.evictionPolicy == .lru)
    }

    @Test("Small configuration")
    func smallConfiguration() throws {
        let config = CacheConfiguration.small

        #expect(config.maxEntries == 1_000)
        #expect(config.timeToLive == .seconds(60))
    }

    @Test("Large configuration")
    func largeConfiguration() throws {
        let config = CacheConfiguration.large

        #expect(config.maxEntries == 100_000)
        #expect(config.maxMemoryBytes == 1024 * 1024 * 1024)
    }

    @Test("Configuration equality")
    func configurationEquality() throws {
        let config1 = CacheConfiguration.default
        let config2 = CacheConfiguration.default
        let config3 = CacheConfiguration.small

        #expect(config1 == config2)
        #expect(config1 != config3)
    }
}

// MARK: - PersistablePreloadCache Tests

@Suite("PersistablePreloadCache Tests", .heartbeat)
struct PersistablePreloadCacheTests {

    @Test("Put and get item")
    func putAndGet() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "test", value: 42)

        try cache.put(item: item, key: "key1")
        let retrieved = cache.get(key: "key1")

        #expect(retrieved == item)
    }

    @Test("Get missing item returns nil")
    func getMissingItem() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        let result = cache.get(key: "nonexistent")
        #expect(result == nil)
    }

    @Test("Contains check")
    func containsCheck() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "test", value: 42)

        #expect(cache.contains(key: "key1") == false)

        try cache.put(item: item, key: "key1")
        #expect(cache.contains(key: "key1") == true)
    }

    @Test("Remove item")
    func removeItem() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "test", value: 42)

        try cache.put(item: item, key: "key1")
        let removed = cache.remove(key: "key1")

        #expect(removed == item)
        #expect(cache.get(key: "key1") == nil)
    }

    @Test("Clear cache")
    func clearCache() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "key1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "key2")

        cache.clear()

        #expect(cache.get(key: "key1") == nil)
        #expect(cache.get(key: "key2") == nil)
        #expect(cache.statistics.entryCount == 0)
    }

    @Test("Update existing key")
    func updateExistingKey() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        let item1 = PreloadCacheItem(name: "original", value: 1)
        let item2 = PreloadCacheItem(name: "updated", value: 2)

        try cache.put(item: item1, key: "key1")
        try cache.put(item: item2, key: "key1")

        let retrieved = cache.get(key: "key1")
        #expect(retrieved?.name == "updated")
        #expect(retrieved?.value == 2)
    }
}

// MARK: - Preload Cache Statistics Tests

@Suite("Preload Cache Statistics Tests", .heartbeat)
struct PreloadCacheStatisticsTests {

    @Test("Hit and miss tracking")
    func hitMissTracking() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "test", value: 42)

        try cache.put(item: item, key: "key1")

        // Generate some hits and misses
        _ = cache.get(key: "key1") // hit
        _ = cache.get(key: "key1") // hit
        _ = cache.get(key: "missing") // miss

        let stats = cache.statistics
        #expect(stats.hits == 2)
        #expect(stats.misses == 1)
        #expect(abs(stats.hitRate - 0.666) < 0.01)
    }

    @Test("Reset statistics")
    func resetStatistics() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "test", value: 42)

        try cache.put(item: item, key: "key1")
        _ = cache.get(key: "key1")
        _ = cache.get(key: "missing")

        cache.resetStatistics()

        let stats = cache.statistics
        #expect(stats.hits == 0)
        #expect(stats.misses == 0)
    }

    @Test("Entry count tracking")
    func entryCountTracking() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "key1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "key2")
        try cache.put(item: PreloadCacheItem(name: "3", value: 3), key: "key3")

        #expect(cache.statistics.entryCount == 3)
    }
}

// MARK: - Cache Eviction Tests

@Suite("Cache Eviction Tests", .heartbeat)
struct CacheEvictionTests {

    @Test("LRU eviction")
    func lruEviction() throws {
        let config = CacheConfiguration(
            maxEntries: 3,
            evictionPolicy: .lru
        )
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: config, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "key1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "key2")
        try cache.put(item: PreloadCacheItem(name: "3", value: 3), key: "key3")

        // Access key1 to make it recently used
        _ = cache.get(key: "key1")

        // Add key4 - should evict key2 (least recently used)
        try cache.put(item: PreloadCacheItem(name: "4", value: 4), key: "key4")

        #expect(cache.get(key: "key1") != nil)
        #expect(cache.get(key: "key2") == nil) // Evicted
        #expect(cache.get(key: "key3") != nil)
        #expect(cache.get(key: "key4") != nil)
    }

    @Test("FIFO eviction")
    func fifoEviction() throws {
        let config = CacheConfiguration(
            maxEntries: 3,
            evictionPolicy: .fifo
        )
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: config, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "key1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "key2")
        try cache.put(item: PreloadCacheItem(name: "3", value: 3), key: "key3")

        // Access key1 (shouldn't matter for FIFO)
        _ = cache.get(key: "key1")

        // Add key4 - should evict key1 (oldest)
        try cache.put(item: PreloadCacheItem(name: "4", value: 4), key: "key4")

        #expect(cache.get(key: "key1") == nil) // Evicted (first in)
        #expect(cache.get(key: "key2") != nil)
        #expect(cache.get(key: "key3") != nil)
        #expect(cache.get(key: "key4") != nil)
    }

    @Test("Eviction counter tracking")
    func evictionCounterTracking() throws {
        let config = CacheConfiguration(
            maxEntries: 2,
            evictionPolicy: .lru
        )
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: config, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "key1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "key2")
        try cache.put(item: PreloadCacheItem(name: "3", value: 3), key: "key3") // Causes eviction

        #expect(cache.statistics.evictions >= 1)
    }
}

// MARK: - Bulk Operations Tests

@Suite("Cache Bulk Operations Tests", .heartbeat)
struct CacheBulkOperationsTests {

    @Test("Preload multiple items")
    func preloadMultipleItems() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        let items = [
            (key: "k1", item: PreloadCacheItem(name: "1", value: 1)),
            (key: "k2", item: PreloadCacheItem(name: "2", value: 2)),
            (key: "k3", item: PreloadCacheItem(name: "3", value: 3))
        ]

        try cache.preload(items)

        #expect(cache.get(key: "k1")?.value == 1)
        #expect(cache.get(key: "k2")?.value == 2)
        #expect(cache.get(key: "k3")?.value == 3)
    }

    @Test("Get multiple items")
    func getMultipleItems() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        try cache.put(item: PreloadCacheItem(name: "1", value: 1), key: "k1")
        try cache.put(item: PreloadCacheItem(name: "2", value: 2), key: "k2")

        let results = cache.getMultiple(keys: ["k1", "k2", "k3"])

        #expect(results.count == 2)
        #expect(results["k1"]?.value == 1)
        #expect(results["k2"]?.value == 2)
        #expect(results["k3"] == nil)
    }
}

// MARK: - GetOrFetch Tests

@Suite("Cache GetOrFetch Tests", .heartbeat)
struct CacheGetOrFetchTests {

    @Test("GetOrFetch returns cached item")
    func getOrFetchReturnsCached() async throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let item = PreloadCacheItem(name: "cached", value: 100)

        try cache.put(item: item, key: "key1")

        var fetchCalled = false
        let result = try await cache.getOrFetch(key: "key1") {
            fetchCalled = true
            return PreloadCacheItem(name: "fetched", value: 999)
        }

        #expect(fetchCalled == false)
        #expect(result.value == 100)
    }

    @Test("GetOrFetch calls fetch on miss")
    func getOrFetchCallsFetch() async throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        var fetchCalled = false
        let result = try await cache.getOrFetch(key: "key1") {
            fetchCalled = true
            return PreloadCacheItem(name: "fetched", value: 42)
        }

        #expect(fetchCalled == true)
        #expect(result.value == 42)
    }

    @Test("GetOrFetch caches fetched item")
    func getOrFetchCachesFetched() async throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())

        _ = try await cache.getOrFetch(key: "key1") {
            PreloadCacheItem(name: "fetched", value: 42)
        }

        let cached = cache.get(key: "key1")
        #expect(cached?.value == 42)
    }
}

// MARK: - ScopedCache Tests

@Suite("ScopedCache Tests", .heartbeat)
struct ScopedCacheTests {

    @Test("Scoped key generation")
    func scopedKeyGeneration() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let scoped = ScopedCache(cache: cache, keyPrefix: "users")

        #expect(scoped.scopedKey("123") == "users:123")
    }

    @Test("Scoped operations use prefix")
    func scopedOperationsUsePrefix() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let scoped = ScopedCache(cache: cache, keyPrefix: "users")

        let item = PreloadCacheItem(name: "test", value: 42)
        try scoped.put(item: item, key: "123")

        // Should be accessible via scoped cache
        #expect(scoped.get(key: "123") != nil)

        // Should be accessible via full key on base cache
        #expect(cache.get(key: "users:123") != nil)
    }
}

// MARK: - CacheWarmer Tests

@Suite("CacheWarmer Tests", .heartbeat)
struct CacheWarmerTests {

    @Test("Warm from array")
    func warmFromArray() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let warmer = CacheWarmer(cache: cache)

        let items = [
            PreloadCacheItem(name: "1", value: 1),
            PreloadCacheItem(name: "2", value: 2),
            PreloadCacheItem(name: "3", value: 3)
        ]

        let count = try warmer.warm(items: items) { $0.id }

        #expect(count == 3)
        #expect(cache.statistics.entryCount == 3)
    }

    @Test("Warm with custom key")
    func warmWithCustomKey() throws {
        let cache = PersistablePreloadCache<PreloadCacheItem>(configuration: .small, monotonicClock: TestProcessMonotonicClock())
        let warmer = CacheWarmer(cache: cache)

        let items = [
            PreloadCacheItem(name: "item1", value: 1),
            PreloadCacheItem(name: "item2", value: 2)
        ]

        _ = try warmer.warm(items: items) { $0.name }

        #expect(cache.get(key: "item1") != nil)
        #expect(cache.get(key: "item2") != nil)
    }
}
#endif
