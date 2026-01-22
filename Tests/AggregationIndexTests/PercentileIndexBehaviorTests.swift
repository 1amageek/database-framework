// PercentileIndexBehaviorTests.swift
// Integration tests for PercentileIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct PercentileTestRequest: Persistable {
    typealias ID = String

    var id: String
    var endpoint: String
    var latencyMs: Double
    var timestamp: Date

    init(id: String = UUID().uuidString, endpoint: String, latencyMs: Double, timestamp: Date = Date()) {
        self.id = id
        self.endpoint = endpoint
        self.latencyMs = latencyMs
        self.timestamp = timestamp
    }

    static var persistableType: String { "PercentileTestRequest" }
    static var allFields: [String] { ["id", "endpoint", "latencyMs", "timestamp"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "endpoint": return endpoint
        case "latencyMs": return latencyMs
        case "timestamp": return timestamp
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PercentileTestRequest, Value>) -> String {
        switch keyPath {
        case \PercentileTestRequest.id: return "id"
        case \PercentileTestRequest.endpoint: return "endpoint"
        case \PercentileTestRequest.latencyMs: return "latencyMs"
        case \PercentileTestRequest.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PercentileTestRequest>) -> String {
        switch keyPath {
        case \PercentileTestRequest.id: return "id"
        case \PercentileTestRequest.endpoint: return "endpoint"
        case \PercentileTestRequest.latencyMs: return "latencyMs"
        case \PercentileTestRequest.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PercentileTestRequest> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: PercentileIndexMaintainer<PercentileTestRequest>

    init(indexName: String = "PercentileTestRequest_endpoint_latencyMs") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "percentile", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: endpoint + latencyMs (grouping + percentile value)
        let index = Index(
            name: indexName,
            kind: PercentileIndexKind<PercentileTestRequest, Double>(
                groupBy: [\.endpoint],
                value: \.latencyMs
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "endpoint"),
                FieldKeyExpression(fieldName: "latencyMs")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["PercentileTestRequest"])
        )

        self.maintainer = PercentileIndexMaintainer<PercentileTestRequest>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            compression: 100
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getPercentile(percentile: Double, for endpoint: String) async throws -> Double? {
        try await database.withTransaction { transaction in
            try await maintainer.getPercentile(
                percentile: percentile,
                groupingValues: [endpoint],
                transaction: transaction
            )
        }
    }

    func getPercentiles(percentiles: [Double], for endpoint: String) async throws -> [Double: Double] {
        try await database.withTransaction { transaction in
            try await maintainer.getPercentiles(
                percentiles: percentiles,
                groupingValues: [endpoint],
                transaction: transaction
            )
        }
    }

    func getStatistics(for endpoint: String) async throws -> (count: Int64, min: Double, max: Double, median: Double)? {
        try await database.withTransaction { transaction in
            try await maintainer.getStatistics(
                groupingValues: [endpoint],
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("PercentileIndex Behavior Tests", .tags(.fdb), .serialized)
struct PercentileIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds value to TDigest")
    func testInsertAddsValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: 100.0)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as PercentileTestRequest?,
                newItem: request,
                transaction: transaction
            )
        }

        let p50 = try await ctx.getPercentile(percentile: 0.5, for: "/api/users")
        #expect(p50 != nil, "Should have data after insert")
        #expect(abs(p50! - 100.0) < 1.0, "P50 should be close to 100.0 for single value")

        try await ctx.cleanup()
    }

    @Test("Multiple values produce expected percentiles")
    func testMultipleValuesPercentiles() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert latency values: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        let latencies = stride(from: 10.0, through: 100.0, by: 10.0)

        try await ctx.database.withTransaction { transaction in
            for latency in latencies {
                let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        let stats = try await ctx.getStatistics(for: "/api/users")
        #expect(stats != nil)
        #expect(stats!.count == 10, "Should have 10 values")
        #expect(abs(stats!.min - 10.0) < 1.0, "Min should be 10.0")
        #expect(abs(stats!.max - 100.0) < 1.0, "Max should be 100.0")

        // P50 should be around 50-55
        let p50 = try await ctx.getPercentile(percentile: 0.5, for: "/api/users")
        #expect(p50 != nil)
        #expect(p50! >= 45 && p50! <= 65, "P50 should be around 55 (actual: \(p50!))")

        try await ctx.cleanup()
    }

    @Test("Different groups have independent percentiles")
    func testDifferentGroupsIndependent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Fast endpoint: 10-50ms
        try await ctx.database.withTransaction { transaction in
            for latency in stride(from: 10.0, through: 50.0, by: 10.0) {
                let request = PercentileTestRequest(endpoint: "/api/fast", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        // Slow endpoint: 100-500ms
        try await ctx.database.withTransaction { transaction in
            for latency in stride(from: 100.0, through: 500.0, by: 100.0) {
                let request = PercentileTestRequest(endpoint: "/api/slow", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        let fastStats = try await ctx.getStatistics(for: "/api/fast")
        let slowStats = try await ctx.getStatistics(for: "/api/slow")

        #expect(fastStats != nil)
        #expect(slowStats != nil)

        // Fast endpoint should have max ~50ms
        #expect(fastStats!.max <= 60, "Fast endpoint max should be <=60")

        // Slow endpoint should have min ~100ms
        #expect(slowStats!.min >= 90, "Slow endpoint min should be >=90")

        try await ctx.cleanup()
    }

    // MARK: - Add-Only Behavior Tests

    @Test("Delete does NOT change percentiles (add-only)")
    func testDeleteDoesNotChangePercentiles() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: 100.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as PercentileTestRequest?,
                newItem: request,
                transaction: transaction
            )
        }

        let statsBefore = try await ctx.getStatistics(for: "/api/users")
        #expect(statsBefore!.count == 1)

        // Delete - TDigest is add-only, percentiles should NOT change
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: request,
                newItem: nil as PercentileTestRequest?,
                transaction: transaction
            )
        }

        let statsAfter = try await ctx.getStatistics(for: "/api/users")
        // TDigest is add-only: delete does NOT remove value
        #expect(statsAfter!.count == 1, "Count should remain 1 after delete (add-only)")

        try await ctx.cleanup()
    }

    @Test("Update adds new value (old value remains in TDigest)")
    func testUpdateAddsNewValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let request = PercentileTestRequest(id: "req1", endpoint: "/api/users", latencyMs: 100.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as PercentileTestRequest?,
                newItem: request,
                transaction: transaction
            )
        }

        // Update latency
        let updatedRequest = PercentileTestRequest(id: "req1", endpoint: "/api/users", latencyMs: 200.0)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: request,
                newItem: updatedRequest,
                transaction: transaction
            )
        }

        let stats = try await ctx.getStatistics(for: "/api/users")
        // Both 100.0 (old) and 200.0 (new) should be in TDigest (add-only)
        #expect(stats!.count == 2, "Count should be 2 after update (both old and new values)")
        #expect(abs(stats!.min - 100.0) < 1.0, "Min should still be 100.0")
        #expect(abs(stats!.max - 200.0) < 1.0, "Max should be 200.0")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("GetPercentiles returns multiple percentiles efficiently")
    func testGetMultiplePercentiles() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 100 values (1 to 100)
        try await ctx.database.withTransaction { transaction in
            for i in 1...100 {
                let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: Double(i))
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        let percentiles = try await ctx.getPercentiles(
            percentiles: [0.5, 0.90, 0.95, 0.99],
            for: "/api/users"
        )

        #expect(percentiles.count == 4, "Should return 4 percentiles")

        // Verify approximate values
        #expect(percentiles[0.5] != nil)
        #expect(percentiles[0.5]! >= 45 && percentiles[0.5]! <= 55, "P50 should be around 50")

        #expect(percentiles[0.90] != nil)
        #expect(percentiles[0.90]! >= 85 && percentiles[0.90]! <= 95, "P90 should be around 90")

        #expect(percentiles[0.99] != nil)
        #expect(percentiles[0.99]! >= 95 && percentiles[0.99]! <= 100, "P99 should be around 99")

        try await ctx.cleanup()
    }

    @Test("GetPercentile for non-existent group returns nil")
    func testGetPercentileNonExistentReturnsNil() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let p50 = try await ctx.getPercentile(percentile: 0.5, for: "nonexistent")
        #expect(p50 == nil, "Percentile for non-existent group should be nil")

        try await ctx.cleanup()
    }

    @Test("GetStatistics for non-existent group returns nil")
    func testGetStatisticsNonExistentReturnsNil() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let stats = try await ctx.getStatistics(for: "nonexistent")
        #expect(stats == nil, "Statistics for non-existent group should be nil")

        try await ctx.cleanup()
    }

    // MARK: - CDF Tests

    @Test("GetCDF returns correct cumulative distribution")
    func testGetCDF() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert values: 10, 20, 30, 40, 50
        try await ctx.database.withTransaction { transaction in
            for latency in [10.0, 20.0, 30.0, 40.0, 50.0] {
                let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        // CDF(30) should be ~0.5 (50% of values are <= 30)
        let cdf30 = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getCDF(
                value: 30.0,
                groupingValues: ["/api/users"],
                transaction: transaction
            )
        }

        #expect(cdf30 != nil)
        #expect(cdf30! >= 0.4 && cdf30! <= 0.6, "CDF(30) should be around 0.5 (actual: \(cdf30!))")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to TDigest")
    func testScanItemAddsToTDigest() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let requests = [
            PercentileTestRequest(endpoint: "/api/users", latencyMs: 100.0),
            PercentileTestRequest(endpoint: "/api/users", latencyMs: 200.0),
            PercentileTestRequest(endpoint: "/api/users", latencyMs: 300.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for request in requests {
                try await ctx.maintainer.scanItem(
                    request,
                    id: Tuple(request.id),
                    transaction: transaction
                )
            }
        }

        let stats = try await ctx.getStatistics(for: "/api/users")
        #expect(stats != nil)
        #expect(stats!.count == 3, "Should have 3 values after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Extreme Percentile Tests

    @Test("TDigest accuracy at extreme percentiles (p99, p99.9)")
    func testExtremePercentileAccuracy() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Insert 1000 values with heavy tail distribution
        // Most values between 50-100, a few outliers at 500-1000
        try await ctx.database.withTransaction { transaction in
            // 950 fast requests (50-100ms)
            for i in 0..<950 {
                let latency = 50.0 + Double(i % 50)
                let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }

            // 50 slow requests (500-1000ms)
            for i in 0..<50 {
                let latency = 500.0 + Double(i * 10)
                let request = PercentileTestRequest(endpoint: "/api/users", latencyMs: latency)
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as PercentileTestRequest?,
                    newItem: request,
                    transaction: transaction
                )
            }
        }

        let stats = try await ctx.getStatistics(for: "/api/users")
        #expect(stats!.count == 1000)

        let percentiles = try await ctx.getPercentiles(
            percentiles: [0.50, 0.90, 0.95, 0.99],
            for: "/api/users"
        )

        // P50 should be in the fast range (50-100)
        #expect(percentiles[0.50]! >= 50 && percentiles[0.50]! <= 100, "P50 should be in fast range")

        // P99 should capture the slow requests (above 100)
        #expect(percentiles[0.99]! >= 100, "P99 should capture slow requests")

        try await ctx.cleanup()
    }
}
