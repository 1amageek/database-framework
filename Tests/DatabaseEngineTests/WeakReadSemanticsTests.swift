import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for Weak Read Semantics
///
/// **Coverage**:
/// - WeakReadSemantics configuration
/// - ReadVersionCache behavior
/// - TransactionRunner integration
/// - Cache invalidation on retry
@Suite("Weak Read Semantics Tests", .serialized)
struct WeakReadSemanticsTests {

    // MARK: - WeakReadSemantics Configuration Tests

    @Test("WeakReadSemantics.default has expected values")
    func defaultSemanticsValues() {
        let semantics = WeakReadSemantics.default
        #expect(semantics.minVersion == 0)
        #expect(semantics.maxStalenessMillis == 5000)
        #expect(semantics.useCachedVersion == true)
    }

    @Test("WeakReadSemantics.strict disables caching")
    func strictSemanticsValues() {
        let semantics = WeakReadSemantics.strict
        #expect(semantics.minVersion == 0)
        #expect(semantics.maxStalenessMillis == 0)
        #expect(semantics.useCachedVersion == false)
    }

    @Test("WeakReadSemantics.relaxed has 30 second staleness")
    func relaxedSemanticsValues() {
        let semantics = WeakReadSemantics.relaxed
        #expect(semantics.minVersion == 0)
        #expect(semantics.maxStalenessMillis == 30_000)
        #expect(semantics.useCachedVersion == true)
    }

    @Test("WeakReadSemantics.veryRelaxed has 60 second staleness")
    func veryRelaxedSemanticsValues() {
        let semantics = WeakReadSemantics.veryRelaxed
        #expect(semantics.minVersion == 0)
        #expect(semantics.maxStalenessMillis == 60_000)
        #expect(semantics.useCachedVersion == true)
    }

    @Test("WeakReadSemantics.atLeast requires minimum version")
    func atLeastSemanticsValues() {
        let semantics = WeakReadSemantics.atLeast(version: 12345)
        #expect(semantics.minVersion == 12345)
        #expect(semantics.maxStalenessMillis == 5000)
        #expect(semantics.useCachedVersion == true)
    }

    @Test("WeakReadSemantics.maxStaleness creates custom staleness")
    func maxStalenessSemanticsValues() {
        let semantics = WeakReadSemantics.maxStaleness(seconds: 10)
        #expect(semantics.minVersion == 0)
        #expect(semantics.maxStalenessMillis == 10_000)
        #expect(semantics.useCachedVersion == true)
    }

    // MARK: - ReadVersionCache Tests

    @Test("ReadVersionCache starts empty")
    func cacheStartsEmpty() {
        let cache = ReadVersionCache()
        let result = cache.getCachedVersion(semantics: .default)
        #expect(result == nil)
    }

    @Test("ReadVersionCache.updateFromCommit stores version")
    func updateFromCommitStoresVersion() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        let result = cache.getCachedVersion(semantics: .relaxed)
        #expect(result == 12345)
    }

    @Test("ReadVersionCache.updateFromRead stores version")
    func updateFromReadStoresVersion() {
        let cache = ReadVersionCache()
        cache.updateFromRead(version: 67890)

        let result = cache.getCachedVersion(semantics: .relaxed)
        #expect(result == 67890)
    }

    @Test("ReadVersionCache.updateFromRead only updates if newer")
    func updateFromReadOnlyIfNewer() {
        let cache = ReadVersionCache()

        // First update
        cache.updateFromRead(version: 200)
        #expect(cache.getCachedVersion(semantics: .relaxed) == 200)

        // Older version - should not update
        cache.updateFromRead(version: 100)
        #expect(cache.getCachedVersion(semantics: .relaxed) == 200)

        // Newer version - should update
        cache.updateFromRead(version: 300)
        #expect(cache.getCachedVersion(semantics: .relaxed) == 300)
    }

    @Test("ReadVersionCache respects minVersion")
    func cacheRespectsMinVersion() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 100)

        // Version meets minimum
        let semantics1 = WeakReadSemantics.atLeast(version: 50)
        #expect(cache.getCachedVersion(semantics: semantics1) == 100)

        // Version below minimum
        let semantics2 = WeakReadSemantics.atLeast(version: 150)
        #expect(cache.getCachedVersion(semantics: semantics2) == nil)
    }

    @Test("ReadVersionCache respects useCachedVersion=false")
    func cacheRespectsUseCachedVersionFalse() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // strict has useCachedVersion=false
        let result = cache.getCachedVersion(semantics: .strict)
        #expect(result == nil)
    }

    @Test("ReadVersionCache.clear removes cached version")
    func clearRemovesCachedVersion() {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)
        #expect(cache.getCachedVersion(semantics: .relaxed) == 12345)

        cache.clear()
        #expect(cache.getCachedVersion(semantics: .relaxed) == nil)
    }

    @Test("ReadVersionCache expires based on staleness")
    func cacheExpiresBasedOnStaleness() async throws {
        let cache = ReadVersionCache()
        cache.updateFromCommit(version: 12345)

        // With 100ms staleness, should be valid immediately
        let shortStaleness = WeakReadSemantics(maxStalenessMillis: 100)
        #expect(cache.getCachedVersion(semantics: shortStaleness) == 12345)

        // Wait for expiration
        try await Task.sleep(nanoseconds: 150_000_000)  // 150ms

        // Should now be expired
        #expect(cache.getCachedVersion(semantics: shortStaleness) == nil)

        // But relaxed (30s) should still be valid
        #expect(cache.getCachedVersion(semantics: .relaxed) == 12345)
    }

    @Test("ReadVersionCache.currentCacheInfo returns version and age")
    func currentCacheInfoReturnsVersionAndAge() async throws {
        let cache = ReadVersionCache()

        // Empty cache
        #expect(cache.currentCacheInfo() == nil)

        // After update
        cache.updateFromCommit(version: 12345)
        try await Task.sleep(nanoseconds: 10_000_000)  // 10ms

        let info = cache.currentCacheInfo()
        #expect(info != nil)
        #expect(info?.version == 12345)
        #expect(info!.ageMillis >= 10)
        #expect(info!.ageMillis < 1000)  // Should be less than 1 second
    }

    // MARK: - MetricsCollectingReadVersionCache Tests

    @Test("MetricsCollectingReadVersionCache tracks hits and misses")
    func metricsTrackingWorks() {
        let cache = MetricsCollectingReadVersionCache()

        // Initial metrics
        var metrics = cache.metrics
        #expect(metrics.lookups == 0)
        #expect(metrics.hits == 0)
        #expect(metrics.misses == 0)

        // Miss (empty cache)
        _ = cache.getCachedVersion(semantics: .relaxed)
        metrics = cache.metrics
        #expect(metrics.lookups == 1)
        #expect(metrics.hits == 0)
        #expect(metrics.misses == 1)

        // Update cache
        cache.updateFromCommit(version: 12345)

        // Hit
        _ = cache.getCachedVersion(semantics: .relaxed)
        metrics = cache.metrics
        #expect(metrics.lookups == 2)
        #expect(metrics.hits == 1)
        #expect(metrics.misses == 1)
        #expect(metrics.hitRate == 0.5)
    }

    @Test("MetricsCollectingReadVersionCache.resetMetrics clears counters")
    func resetMetricsWorks() {
        let cache = MetricsCollectingReadVersionCache()
        cache.updateFromCommit(version: 12345)
        _ = cache.getCachedVersion(semantics: .relaxed)
        _ = cache.getCachedVersion(semantics: .relaxed)

        #expect(cache.metrics.lookups == 2)

        cache.resetMetrics()

        #expect(cache.metrics.lookups == 0)
        #expect(cache.metrics.hits == 0)
    }

    // MARK: - TransactionConfiguration Integration Tests

    @Test("TransactionConfiguration.batch includes relaxed weak read semantics")
    func batchConfigIncludesWeakReadSemantics() {
        let config = TransactionConfiguration.batch
        #expect(config.weakReadSemantics != nil)
        #expect(config.weakReadSemantics?.maxStalenessMillis == 30_000)
    }

    @Test("TransactionConfiguration.longRunning includes very relaxed semantics")
    func longRunningConfigIncludesWeakReadSemantics() {
        let config = TransactionConfiguration.longRunning
        #expect(config.weakReadSemantics != nil)
        #expect(config.weakReadSemantics?.maxStalenessMillis == 60_000)
    }

    @Test("TransactionConfiguration.default has no weak read semantics")
    func defaultConfigHasNoWeakReadSemantics() {
        let config = TransactionConfiguration.default
        #expect(config.weakReadSemantics == nil)
    }

    @Test("TransactionConfiguration.interactive has no weak read semantics")
    func interactiveConfigHasNoWeakReadSemantics() {
        let config = TransactionConfiguration.interactive
        #expect(config.weakReadSemantics == nil)
    }

    @Test("Custom TransactionConfiguration with weak read semantics")
    func customConfigWithWeakReadSemantics() {
        let config = TransactionConfiguration(
            timeout: 5000,
            weakReadSemantics: .maxStaleness(seconds: 15)
        )
        #expect(config.weakReadSemantics != nil)
        #expect(config.weakReadSemantics?.maxStalenessMillis == 15_000)
    }

    // MARK: - WeakReadSemantics Description Tests

    @Test("WeakReadSemantics.description shows preset names")
    func semanticsDescriptionShowsPresets() {
        #expect(WeakReadSemantics.default.description == "WeakReadSemantics.default")
        #expect(WeakReadSemantics.strict.description == "WeakReadSemantics.strict")
    }

    @Test("WeakReadSemantics.description shows custom values")
    func semanticsDescriptionShowsCustomValues() {
        let custom = WeakReadSemantics(minVersion: 100, maxStalenessMillis: 10_000)
        #expect(custom.description.contains("minVersion: 100"))
        #expect(custom.description.contains("maxStaleness: 10s"))
    }

    // MARK: - FDBContainer Integration Tests

    @Test("FDBContainer has readVersionCache")
    func containerHasReadVersionCache() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [WeakReadTestModel.self],
            version: Schema.Version(1, 0, 0)
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        // FDBDatabase should have a ReadVersionCache
        #expect(container.fdbDatabase.readVersionCache.getCachedVersion(semantics: .relaxed) == nil)

        // After a transaction, cache should be populated
        let context = container.newContext()
        try await context.withTransaction(configuration: .batch) { _ in
            // Just start and commit a transaction
        }

        // Cache may or may not be populated depending on transaction type
        // This just verifies the integration exists
    }

    // MARK: - Test Model

    @Persistable
    struct WeakReadTestModel {
        #Directory<WeakReadTestModel>("test", "weakread")
        var id: String = ULID().ulidString
        var value: Int
    }
}
