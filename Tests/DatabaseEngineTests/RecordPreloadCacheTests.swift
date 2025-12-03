// RecordPreloadCacheTests.swift
// DatabaseEngine Tests - Record preload cache tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine

// MARK: - Test Model

@Persistable
struct CacheTestItem: Equatable {
    var id: String = UUID().uuidString
    var name: String
    var value: Int
}

// MARK: - CacheConfiguration Tests

@Suite("CacheConfiguration Tests")
struct CacheConfigurationTests {

    @Test("Default configuration values")
    func defaultConfiguration() {
        let config = CacheConfiguration.default

        #expect(config.maxEntries == 10_000)
        #expect(config.maxMemoryBytes == 100 * 1024 * 1024)
        #expect(config.ttlSeconds == 300)
        #expect(config.enableStatistics == true)
        #expect(config.evictionPolicy == .lru)
    }

    @Test("Small configuration")
    func smallConfiguration() {
        let config = CacheConfiguration.small

        #expect(config.maxEntries == 1_000)
        #expect(config.ttlSeconds == 60)
    }

    @Test("Large configuration")
    func largeConfiguration() {
        let config = CacheConfiguration.large

        #expect(config.maxEntries == 100_000)
        #expect(config.maxMemoryBytes == 1024 * 1024 * 1024)
    }

    @Test("Configuration equality")
    func configurationEquality() {
        let config1 = CacheConfiguration.default
        let config2 = CacheConfiguration.default
        let config3 = CacheConfiguration.small

        #expect(config1 == config2)
        #expect(config1 != config3)
    }
}

// MARK: - RecordPreloadCache Tests

@Suite("RecordPreloadCache Tests")
struct RecordPreloadCacheTests {

    @Test("Put and get item")
    func putAndGet() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "test", value: 42)

        cache.put(item: item, key: "key1")
        let retrieved = cache.get(key: "key1")

        #expect(retrieved == item)
    }

    @Test("Get missing item returns nil")
    func getMissingItem() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        let result = cache.get(key: "nonexistent")
        #expect(result == nil)
    }

    @Test("Contains check")
    func containsCheck() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "test", value: 42)

        #expect(cache.contains(key: "key1") == false)

        cache.put(item: item, key: "key1")
        #expect(cache.contains(key: "key1") == true)
    }

    @Test("Remove item")
    func removeItem() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "test", value: 42)

        cache.put(item: item, key: "key1")
        let removed = cache.remove(key: "key1")

        #expect(removed == item)
        #expect(cache.get(key: "key1") == nil)
    }

    @Test("Clear cache")
    func clearCache() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "key1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "key2")

        cache.clear()

        #expect(cache.get(key: "key1") == nil)
        #expect(cache.get(key: "key2") == nil)
        #expect(cache.statistics.entryCount == 0)
    }

    @Test("Update existing key")
    func updateExistingKey() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        let item1 = CacheTestItem(name: "original", value: 1)
        let item2 = CacheTestItem(name: "updated", value: 2)

        cache.put(item: item1, key: "key1")
        cache.put(item: item2, key: "key1")

        let retrieved = cache.get(key: "key1")
        #expect(retrieved?.name == "updated")
        #expect(retrieved?.value == 2)
    }
}

// MARK: - Preload Cache Statistics Tests

@Suite("Preload Cache Statistics Tests")
struct PreloadCacheStatisticsTests {

    @Test("Hit and miss tracking")
    func hitMissTracking() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "test", value: 42)

        cache.put(item: item, key: "key1")

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
    func resetStatistics() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "test", value: 42)

        cache.put(item: item, key: "key1")
        _ = cache.get(key: "key1")
        _ = cache.get(key: "missing")

        cache.resetStatistics()

        let stats = cache.statistics
        #expect(stats.hits == 0)
        #expect(stats.misses == 0)
    }

    @Test("Entry count tracking")
    func entryCountTracking() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "key1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "key2")
        cache.put(item: CacheTestItem(name: "3", value: 3), key: "key3")

        #expect(cache.statistics.entryCount == 3)
    }
}

// MARK: - Cache Eviction Tests

@Suite("Cache Eviction Tests")
struct CacheEvictionTests {

    @Test("LRU eviction")
    func lruEviction() {
        let config = CacheConfiguration(
            maxEntries: 3,
            evictionPolicy: .lru
        )
        let cache = RecordPreloadCache<CacheTestItem>(configuration: config)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "key1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "key2")
        cache.put(item: CacheTestItem(name: "3", value: 3), key: "key3")

        // Access key1 to make it recently used
        _ = cache.get(key: "key1")

        // Add key4 - should evict key2 (least recently used)
        cache.put(item: CacheTestItem(name: "4", value: 4), key: "key4")

        #expect(cache.get(key: "key1") != nil)
        #expect(cache.get(key: "key2") == nil) // Evicted
        #expect(cache.get(key: "key3") != nil)
        #expect(cache.get(key: "key4") != nil)
    }

    @Test("FIFO eviction")
    func fifoEviction() {
        let config = CacheConfiguration(
            maxEntries: 3,
            evictionPolicy: .fifo
        )
        let cache = RecordPreloadCache<CacheTestItem>(configuration: config)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "key1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "key2")
        cache.put(item: CacheTestItem(name: "3", value: 3), key: "key3")

        // Access key1 (shouldn't matter for FIFO)
        _ = cache.get(key: "key1")

        // Add key4 - should evict key1 (oldest)
        cache.put(item: CacheTestItem(name: "4", value: 4), key: "key4")

        #expect(cache.get(key: "key1") == nil) // Evicted (first in)
        #expect(cache.get(key: "key2") != nil)
        #expect(cache.get(key: "key3") != nil)
        #expect(cache.get(key: "key4") != nil)
    }

    @Test("Eviction counter tracking")
    func evictionCounterTracking() {
        let config = CacheConfiguration(
            maxEntries: 2,
            evictionPolicy: .lru
        )
        let cache = RecordPreloadCache<CacheTestItem>(configuration: config)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "key1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "key2")
        cache.put(item: CacheTestItem(name: "3", value: 3), key: "key3") // Causes eviction

        #expect(cache.statistics.evictions >= 1)
    }
}

// MARK: - Bulk Operations Tests

@Suite("Cache Bulk Operations Tests")
struct CacheBulkOperationsTests {

    @Test("Preload multiple items")
    func preloadMultipleItems() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        let items = [
            (key: "k1", item: CacheTestItem(name: "1", value: 1)),
            (key: "k2", item: CacheTestItem(name: "2", value: 2)),
            (key: "k3", item: CacheTestItem(name: "3", value: 3))
        ]

        cache.preload(items)

        #expect(cache.get(key: "k1")?.value == 1)
        #expect(cache.get(key: "k2")?.value == 2)
        #expect(cache.get(key: "k3")?.value == 3)
    }

    @Test("Get multiple items")
    func getMultipleItems() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        cache.put(item: CacheTestItem(name: "1", value: 1), key: "k1")
        cache.put(item: CacheTestItem(name: "2", value: 2), key: "k2")

        let results = cache.getMultiple(keys: ["k1", "k2", "k3"])

        #expect(results.count == 2)
        #expect(results["k1"]?.value == 1)
        #expect(results["k2"]?.value == 2)
        #expect(results["k3"] == nil)
    }
}

// MARK: - GetOrFetch Tests

@Suite("Cache GetOrFetch Tests")
struct CacheGetOrFetchTests {

    @Test("GetOrFetch returns cached item")
    func getOrFetchReturnsCached() async throws {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let item = CacheTestItem(name: "cached", value: 100)

        cache.put(item: item, key: "key1")

        var fetchCalled = false
        let result = try await cache.getOrFetch(key: "key1") {
            fetchCalled = true
            return CacheTestItem(name: "fetched", value: 999)
        }

        #expect(fetchCalled == false)
        #expect(result.value == 100)
    }

    @Test("GetOrFetch calls fetch on miss")
    func getOrFetchCallsFetch() async throws {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        var fetchCalled = false
        let result = try await cache.getOrFetch(key: "key1") {
            fetchCalled = true
            return CacheTestItem(name: "fetched", value: 42)
        }

        #expect(fetchCalled == true)
        #expect(result.value == 42)
    }

    @Test("GetOrFetch caches fetched item")
    func getOrFetchCachesFetched() async throws {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)

        _ = try await cache.getOrFetch(key: "key1") {
            CacheTestItem(name: "fetched", value: 42)
        }

        let cached = cache.get(key: "key1")
        #expect(cached?.value == 42)
    }
}

// MARK: - ScopedCache Tests

@Suite("ScopedCache Tests")
struct ScopedCacheTests {

    @Test("Scoped key generation")
    func scopedKeyGeneration() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let scoped = ScopedCache(cache: cache, keyPrefix: "users")

        #expect(scoped.scopedKey("123") == "users:123")
    }

    @Test("Scoped operations use prefix")
    func scopedOperationsUsePrefix() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let scoped = ScopedCache(cache: cache, keyPrefix: "users")

        let item = CacheTestItem(name: "test", value: 42)
        scoped.put(item: item, key: "123")

        // Should be accessible via scoped cache
        #expect(scoped.get(key: "123") != nil)

        // Should be accessible via full key on base cache
        #expect(cache.get(key: "users:123") != nil)
    }
}

// MARK: - CacheWarmer Tests

@Suite("CacheWarmer Tests")
struct CacheWarmerTests {

    @Test("Warm from array")
    func warmFromArray() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let warmer = CacheWarmer(cache: cache)

        let items = [
            CacheTestItem(name: "1", value: 1),
            CacheTestItem(name: "2", value: 2),
            CacheTestItem(name: "3", value: 3)
        ]

        let count = warmer.warm(items: items) { $0.id }

        #expect(count == 3)
        #expect(cache.statistics.entryCount == 3)
    }

    @Test("Warm with custom key")
    func warmWithCustomKey() {
        let cache = RecordPreloadCache<CacheTestItem>(configuration: .small)
        let warmer = CacheWarmer(cache: cache)

        let items = [
            CacheTestItem(name: "item1", value: 1),
            CacheTestItem(name: "item2", value: 2)
        ]

        _ = warmer.warm(items: items) { $0.name }

        #expect(cache.get(key: "item1") != nil)
        #expect(cache.get(key: "item2") != nil)
    }
}
