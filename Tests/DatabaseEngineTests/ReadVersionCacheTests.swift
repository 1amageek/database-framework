// ReadVersionCacheTests.swift
// Tests for ReadVersionCache and WeakReadSemantics

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("ReadVersionCache Tests")
struct ReadVersionCacheTests {

    // MARK: - WeakReadSemantics Tests

    @Test func strictSemanticsDoesNotUseCachedVersion() {
        let semantics = WeakReadSemantics.strict

        #expect(!semantics.useCachedReadVersion)
        #expect(semantics.maxStalenessSeconds == 0)
        #expect(semantics.minReadVersion == nil)
    }

    @Test func boundedStalenessConfiguration() {
        let semantics = WeakReadSemantics.bounded(seconds: 5.0)

        #expect(semantics.useCachedReadVersion)
        #expect(semantics.maxStalenessSeconds == 5.0)
        #expect(semantics.minReadVersion == nil)
    }

    @Test func atLeastVersionConfiguration() {
        let semantics = WeakReadSemantics.atLeast(version: 12345)

        #expect(semantics.useCachedReadVersion)
        #expect(semantics.maxStalenessSeconds == .infinity)
        #expect(semantics.minReadVersion == 12345)
    }

    // MARK: - Cache Initial State Tests

    @Test func cacheInitiallyEmpty() {
        let cache = ReadVersionCache()

        let cachedVersion = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        #expect(cachedVersion == nil)

        let stats = cache.statistics
        #expect(stats.hitCount == 0)
        #expect(stats.missCount == 1)
        #expect(stats.lastReadVersion == nil)
        #expect(stats.lastCommitVersion == nil)
    }

    // MARK: - Update and Retrieve Tests

    @Test func updateAndRetrieveReadVersion() {
        let cache = ReadVersionCache()
        let version: Int64 = 12345

        cache.updateReadVersion(version)

        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        #expect(cached == version)
    }

    @Test func strictSemanticsIgnoresCache() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345)

        let cached = cache.getCachedVersion(semantics: .strict)
        #expect(cached == nil)
    }

    @Test func staleCacheIsRejected() async throws {
        let cache = ReadVersionCache()

        // Update with a timestamp in the past
        let oldTimestamp = Date().addingTimeInterval(-10) // 10 seconds ago
        cache.updateReadVersion(12345, timestamp: oldTimestamp)

        // Request with 5 second staleness limit
        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 5.0))
        #expect(cached == nil)
    }

    @Test func freshCacheIsAccepted() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345) // Current timestamp

        // Request with 5 second staleness limit
        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 5.0))
        #expect(cached == 12345)
    }

    // MARK: - Minimum Version Tests

    @Test func cacheRejectedWhenBelowMinVersion() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(100)

        // Request at least version 200
        let cached = cache.getCachedVersion(semantics: .atLeast(version: 200))
        #expect(cached == nil)
    }

    @Test func cacheAcceptedWhenAboveMinVersion() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(300)

        // Request at least version 200
        let cached = cache.getCachedVersion(semantics: .atLeast(version: 200))
        #expect(cached == 300)
    }

    @Test func cacheAcceptedWhenEqualToMinVersion() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(200)

        // Request at least version 200
        let cached = cache.getCachedVersion(semantics: .atLeast(version: 200))
        #expect(cached == 200)
    }

    // MARK: - Commit Version Tests

    @Test func recordCommitVersionUpdatesCache() {
        let cache = ReadVersionCache()

        cache.recordCommitVersion(12345)

        let stats = cache.statistics
        #expect(stats.lastCommitVersion == 12345)
        #expect(stats.lastReadVersion == 12345)
    }

    @Test func commitVersionAlsoUpdatesReadVersion() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(100)
        cache.recordCommitVersion(200)

        let stats = cache.statistics
        #expect(stats.lastReadVersion == 200)
        #expect(stats.lastCommitVersion == 200)
    }

    @Test func olderCommitVersionDoesNotUpdate() {
        let cache = ReadVersionCache()

        cache.recordCommitVersion(200)
        cache.recordCommitVersion(100) // Older version

        let stats = cache.statistics
        #expect(stats.lastCommitVersion == 200)
    }

    // MARK: - Version Monotonicity Tests

    @Test func olderReadVersionDoesNotOverwrite() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(200)
        cache.updateReadVersion(100) // Older version

        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        #expect(cached == 200)
    }

    @Test func newerReadVersionOverwrites() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(100)
        cache.updateReadVersion(200) // Newer version

        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        #expect(cached == 200)
    }

    // MARK: - Invalidation Tests

    @Test func invalidateClearsCache() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345)
        cache.recordCommitVersion(12346)

        cache.invalidate()

        let stats = cache.statistics
        #expect(stats.lastReadVersion == nil)
        #expect(stats.lastCommitVersion == nil)

        let cached = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        #expect(cached == nil)
    }

    // MARK: - Statistics Tests

    @Test func hitCountIncrementsOnCacheHit() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345)

        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))

        let stats = cache.statistics
        #expect(stats.hitCount == 2)
        #expect(stats.missCount == 0)
    }

    @Test func missCountIncrementsOnCacheMiss() {
        let cache = ReadVersionCache()

        // Miss due to empty cache
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))

        // Miss due to strict semantics
        cache.updateReadVersion(12345)
        _ = cache.getCachedVersion(semantics: .strict)

        let stats = cache.statistics
        #expect(stats.missCount == 2)
    }

    @Test func hitRatioCalculation() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345)

        // 3 hits
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))

        // 1 miss
        _ = cache.getCachedVersion(semantics: .strict)

        let stats = cache.statistics
        #expect(stats.hitRatio == 0.75)
    }

    @Test func resetStatisticsKeepsVersions() {
        let cache = ReadVersionCache()

        cache.updateReadVersion(12345)
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))
        _ = cache.getCachedVersion(semantics: .bounded(seconds: 10.0))

        cache.resetStatistics()

        let stats = cache.statistics
        #expect(stats.hitCount == 0)
        #expect(stats.missCount == 0)
        #expect(stats.lastReadVersion == 12345) // Version preserved
    }

    // MARK: - Configuration Tests

    @Test func defaultConfiguration() {
        let config = ReadVersionCacheConfiguration.default

        #expect(config.enabled)
        #expect(config.defaultStalenessSeconds == 5.0)
        #expect(config.trackLastSeenVersion)
    }

    @Test func disabledConfiguration() {
        let config = ReadVersionCacheConfiguration.disabled

        #expect(!config.enabled)
        #expect(config.defaultStalenessSeconds == 0)
        #expect(!config.trackLastSeenVersion)
    }
}
