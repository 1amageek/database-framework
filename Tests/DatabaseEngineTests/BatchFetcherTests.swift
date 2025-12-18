// BatchFetcherTests.swift
// DatabaseEngine Tests - BatchFetcher configuration and result types

import Testing
import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

// MARK: - BatchFetchConfiguration Tests

@Suite("BatchFetchConfiguration Tests")
struct BatchFetchConfigurationTests {

    @Test("Default configuration values")
    func defaultConfiguration() {
        let config = BatchFetchConfiguration.default

        #expect(config.batchSize == 100)
        #expect(config.prefetchEnabled == true)
        #expect(config.prefetchCount == 1)
        #expect(config.batchTimeoutSeconds == 5.0)
        #expect(config.continueOnError == false)
    }

    @Test("Interactive configuration has small batches")
    func interactiveConfiguration() {
        let config = BatchFetchConfiguration.interactive

        #expect(config.batchSize == 20)
        #expect(config.prefetchEnabled == false)
        #expect(config.prefetchCount == 0)
        #expect(config.batchTimeoutSeconds == 1.0)
        #expect(config.continueOnError == false)
    }

    @Test("Bulk configuration has large batches")
    func bulkConfiguration() {
        let config = BatchFetchConfiguration.bulk

        #expect(config.batchSize == 500)
        #expect(config.prefetchEnabled == true)
        #expect(config.prefetchCount == 2)
        #expect(config.batchTimeoutSeconds == 30.0)
        #expect(config.continueOnError == true)
    }

    @Test("Custom configuration")
    func customConfiguration() {
        let config = BatchFetchConfiguration(
            batchSize: 50,
            prefetchEnabled: false,
            prefetchCount: 0,
            batchTimeoutSeconds: 10.0,
            continueOnError: true
        )

        #expect(config.batchSize == 50)
        #expect(config.prefetchEnabled == false)
        #expect(config.prefetchCount == 0)
        #expect(config.batchTimeoutSeconds == 10.0)
        #expect(config.continueOnError == true)
    }

    @Test("Equal configurations are equal")
    func equalConfigurations() {
        let config1 = BatchFetchConfiguration.default
        let config2 = BatchFetchConfiguration.default

        #expect(config1 == config2)
    }

    @Test("Different configurations are not equal")
    func differentConfigurations() {
        let config1 = BatchFetchConfiguration.default
        let config2 = BatchFetchConfiguration.bulk

        #expect(config1 != config2)
    }
}

// MARK: - BatchFetchResult Tests

@Suite("BatchFetchResult Tests")
struct BatchFetchResultTests {

    // Test model
    @Persistable
    struct TestItem {
        var id: String = UUID().uuidString
        var name: String
    }

    @Test("Empty result")
    func emptyResult() {
        let result = BatchFetchResult<TestItem>(
            items: [],
            notFound: [],
            failed: []
        )

        #expect(result.items.isEmpty)
        #expect(result.notFound.isEmpty)
        #expect(result.failed.isEmpty)
        #expect(result.totalRequested == 0)
        #expect(result.successRate == 0)
    }

    @Test("All items found")
    func allItemsFound() {
        let items = [
            TestItem(name: "Item 1"),
            TestItem(name: "Item 2"),
            TestItem(name: "Item 3")
        ]

        let result = BatchFetchResult<TestItem>(
            items: items,
            notFound: [],
            failed: []
        )

        #expect(result.items.count == 3)
        #expect(result.totalRequested == 3)
        #expect(result.successRate == 1.0)
    }

    @Test("Partial success")
    func partialSuccess() {
        let items = [TestItem(name: "Found")]
        let notFound = [Tuple("key1"), Tuple("key2")]

        let result = BatchFetchResult<TestItem>(
            items: items,
            notFound: notFound,
            failed: []
        )

        #expect(result.items.count == 1)
        #expect(result.notFound.count == 2)
        #expect(result.totalRequested == 3)
        #expect(abs(result.successRate - (1.0/3.0)) < 0.001)
    }

    @Test("Mixed results with failures")
    func mixedResultsWithFailures() {
        let items = [TestItem(name: "Success")]
        let notFound = [Tuple("missing")]
        let failed: [(key: Tuple, error: Error)] = [
            (key: Tuple("error1"), error: NSError(domain: "test", code: 1)),
            (key: Tuple("error2"), error: NSError(domain: "test", code: 2))
        ]

        let result = BatchFetchResult<TestItem>(
            items: items,
            notFound: notFound,
            failed: failed
        )

        #expect(result.items.count == 1)
        #expect(result.notFound.count == 1)
        #expect(result.failed.count == 2)
        #expect(result.totalRequested == 4)
        #expect(result.successRate == 0.25)
    }
}

// MARK: - BatchFetchStatistics Tests

@Suite("BatchFetchStatistics Tests")
struct BatchFetchStatisticsTests {

    @Test("Initial statistics are zero")
    func initialStatistics() {
        let stats = BatchFetchStatistics()

        #expect(stats.totalFetched == 0)
        #expect(stats.batchCount == 0)
        #expect(stats.notFoundCount == 0)
        #expect(stats.errorCount == 0)
        #expect(stats.totalDurationSeconds == 0)
        #expect(stats.averageItemsPerBatch == 0)
        #expect(stats.throughputPerSecond == 0)
    }

    @Test("Recording single batch")
    func recordSingleBatch() {
        var stats = BatchFetchStatistics()

        stats.recordBatch(items: 100, notFound: 5, errors: 2, durationSeconds: 0.5)

        #expect(stats.totalFetched == 100)
        #expect(stats.batchCount == 1)
        #expect(stats.notFoundCount == 5)
        #expect(stats.errorCount == 2)
        #expect(stats.totalDurationSeconds == 0.5)
        #expect(stats.averageItemsPerBatch == 100)
        #expect(stats.throughputPerSecond == 200) // 100 items / 0.5 seconds
    }

    @Test("Recording multiple batches")
    func recordMultipleBatches() {
        var stats = BatchFetchStatistics()

        stats.recordBatch(items: 100, notFound: 0, errors: 0, durationSeconds: 1.0)
        stats.recordBatch(items: 50, notFound: 10, errors: 5, durationSeconds: 0.5)
        stats.recordBatch(items: 150, notFound: 0, errors: 0, durationSeconds: 1.5)

        #expect(stats.totalFetched == 300)
        #expect(stats.batchCount == 3)
        #expect(stats.notFoundCount == 10)
        #expect(stats.errorCount == 5)
        #expect(stats.totalDurationSeconds == 3.0)
        #expect(stats.averageItemsPerBatch == 100) // 300 / 3
        #expect(stats.throughputPerSecond == 100) // 300 / 3.0
    }

    @Test("Throughput with no duration")
    func throughputWithNoDuration() {
        let stats = BatchFetchStatistics()

        // Should not crash, just return 0
        #expect(stats.throughputPerSecond == 0)
    }

    @Test("Average items per batch with no batches")
    func averageItemsNoBatches() {
        let stats = BatchFetchStatistics()

        // Should not crash, just return 0
        #expect(stats.averageItemsPerBatch == 0)
    }
}

// MARK: - BatchFetcher Integration Tests

@Suite("BatchFetcher Integration Tests", .serialized)
struct BatchFetcherIntegrationTests {

    @Persistable
    struct TestUser {
        #Directory<TestUser>("test", "batchfetcher", "users")
        var id: String = UUID().uuidString
        var name: String
        var email: String
    }

    private func setupDatabase() async throws -> any DatabaseProtocol {
        try await FDBTestEnvironment.shared.ensureInitialized()
        return try FDBClient.openDatabase()
    }

    private func testSubspace() -> Subspace {
        Subspace(prefix: Tuple("test", "batchfetcher", UUID().uuidString).pack())
    }

    private func blobsSubspace(from base: Subspace) -> Subspace {
        base.subspace("blobs")
    }

    @Test("Fetch empty list returns empty")
    func fetchEmptyList() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()

        let fetcher = BatchFetcher<TestUser>(
            itemSubspace: subspace,
            blobsSubspace: blobsSubspace(from: subspace),
            itemType: "TestUser",
            configuration: .default
        )

        let results = try await database.withTransaction { transaction in
            try await fetcher.fetch(primaryKeys: [], transaction: transaction)
        }

        #expect(results.isEmpty)
    }

    @Test("Fetch non-existent keys returns empty")
    func fetchNonExistentKeys() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()

        let fetcher = BatchFetcher<TestUser>(
            itemSubspace: subspace,
            blobsSubspace: blobsSubspace(from: subspace),
            itemType: "TestUser",
            configuration: .default
        )

        let nonExistentKeys = [
            Tuple("nonexistent1"),
            Tuple("nonexistent2")
        ]

        let results = try await database.withTransaction { transaction in
            try await fetcher.fetch(primaryKeys: nonExistentKeys, transaction: transaction)
        }

        #expect(results.isEmpty)
    }

    @Test("FetchWithResults returns detailed results")
    func fetchWithResults() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()

        let fetcher = BatchFetcher<TestUser>(
            itemSubspace: subspace,
            blobsSubspace: blobsSubspace(from: subspace),
            itemType: "TestUser",
            configuration: .default
        )

        let keys = [Tuple("key1"), Tuple("key2")]

        let result = try await database.withTransaction { transaction in
            await fetcher.fetchWithResults(primaryKeys: keys, transaction: transaction)
        }

        // All keys should be not found (nothing saved)
        #expect(result.items.isEmpty)
        #expect(result.notFound.count == 2)
        #expect(result.failed.isEmpty)
    }
}
