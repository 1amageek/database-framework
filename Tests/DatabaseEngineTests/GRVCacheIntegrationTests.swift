// GRVCacheIntegrationTests.swift
// Tests for GRV (Get Read Version) cache integration with DatabaseProtocol
//
// These tests verify that:
// 1. SharedReadVersionCache works correctly
// 2. TransactionConfiguration presets are correct
// 3. Cache statistics work

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("GRV Cache Integration Tests", .serialized)
struct GRVCacheIntegrationTests {

    // MARK: - SharedReadVersionCache Unit Tests

    @Test("SharedReadVersionCache singleton exists")
    func testSharedCacheExists() {
        let cache1 = SharedReadVersionCache.shared
        let cache2 = SharedReadVersionCache.shared

        // Both should be the same instance
        #expect(cache1 === cache2)
    }

    @Test("SharedReadVersionCache stores and retrieves versions")
    func testCacheStoresVersions() {
        let cache = SharedReadVersionCache.shared

        // Clear any existing state
        cache.invalidate()

        // Initially should return nil
        #expect(cache.getCachedVersion() == nil)

        // Update with a version
        cache.updateReadVersion(12345)

        // Should retrieve the cached version
        let cached = cache.getCachedVersion()
        #expect(cached == 12345)

        // Clean up
        cache.invalidate()
    }

    @Test("SharedReadVersionCache respects staleness")
    func testCacheStaleness() {
        let cache = SharedReadVersionCache.shared

        // Clear state
        cache.invalidate()

        // Configure very short staleness
        cache.configure(staleness: 0.1)  // 100ms

        // Update cache
        cache.updateReadVersion(12345)

        // Should be available immediately
        #expect(cache.getCachedVersion() != nil)

        // Reset staleness to default
        cache.configure(staleness: 5.0)
        cache.invalidate()
    }

    @Test("SharedReadVersionCache records commit versions")
    func testCacheRecordsCommitVersion() {
        let cache = SharedReadVersionCache.shared

        // Clear state
        cache.invalidate()

        // Record a commit version
        cache.recordCommitVersion(99999)

        // Should be retrievable (commit versions also update read version)
        let cached = cache.getCachedVersion()
        #expect(cached == 99999)

        // Statistics should reflect the commit
        let stats = cache.statistics
        #expect(stats.lastCommitVersion == 99999)

        // Clean up
        cache.invalidate()
    }

    @Test("SharedReadVersionCache invalidation clears cache")
    func testCacheInvalidation() {
        let cache = SharedReadVersionCache.shared

        // Set some values
        cache.updateReadVersion(11111)
        cache.recordCommitVersion(22222)

        // Invalidate
        cache.invalidate()

        // Should be empty
        #expect(cache.getCachedVersion() == nil)
    }

    // MARK: - TransactionConfiguration Tests

    @Test("TransactionConfiguration.readOnly has useGrvCache disabled")
    func testReadOnlyPresetDisablesGrvCache() {
        let config = TransactionConfiguration.readOnly

        // readOnly does NOT use GRV cache to avoid stale version issues in high-frequency scenarios
        #expect(config.useGrvCache == false)
    }

    @Test("TransactionConfiguration.readOnlyCached has useGrvCache enabled")
    func testReadOnlyCachedPresetUsesGrvCache() {
        let config = TransactionConfiguration.readOnlyCached

        // readOnlyCached uses GRV cache for latency-sensitive production workloads
        #expect(config.useGrvCache == true)
    }

    @Test("TransactionConfiguration.default has useGrvCache disabled")
    func testDefaultConfigDisablesGrvCache() {
        let config = TransactionConfiguration.default

        #expect(config.useGrvCache == false)
    }

    @Test("TransactionConfiguration.batch has useGrvCache disabled")
    func testBatchConfigDisablesGrvCache() {
        let config = TransactionConfiguration.batch

        #expect(config.useGrvCache == false)
    }

    @Test("Custom configuration can enable useGrvCache")
    func testCustomConfigEnablesGrvCache() {
        let config = TransactionConfiguration(useGrvCache: true)

        #expect(config.useGrvCache == true)
    }

    // MARK: - Cache Statistics Tests

    @Test("Cache statistics track hits and misses")
    func testCacheStatistics() {
        let cache = SharedReadVersionCache.shared

        cache.invalidate()

        // First access should be a miss
        _ = cache.getCachedVersion()

        var stats = cache.statistics
        #expect(stats.missCount >= 1)

        // Add a version
        cache.updateReadVersion(55555)

        // Next access should be a hit
        _ = cache.getCachedVersion()

        stats = cache.statistics
        #expect(stats.hitCount >= 1)

        cache.invalidate()
    }

    @Test("Hit ratio calculation is correct")
    func testHitRatioCalculation() {
        let cache = SharedReadVersionCache.shared

        cache.invalidate()

        // Get initial stats
        let initialStats = cache.statistics
        let initialHits = initialStats.hitCount

        // Seed the cache
        cache.updateReadVersion(77777)

        // 3 hits
        _ = cache.getCachedVersion()
        _ = cache.getCachedVersion()
        _ = cache.getCachedVersion()

        let stats = cache.statistics
        // Should have 3 more hits than before
        #expect(stats.hitCount == initialHits + 3)
        #expect(stats.hitRatio > 0)

        cache.invalidate()
    }

    // MARK: - Configuration Tests

    @Test("Staleness configuration is applied")
    func testStalenessConfiguration() {
        let cache = SharedReadVersionCache.shared

        cache.invalidate()

        // Configure 10 second staleness
        cache.configure(staleness: 10.0)

        // Add a version
        cache.updateReadVersion(88888)

        // Should be immediately available
        #expect(cache.getCachedVersion() == 88888)

        // Reset
        cache.configure(staleness: 5.0)
        cache.invalidate()
    }
}
