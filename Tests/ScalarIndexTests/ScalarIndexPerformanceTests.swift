// ScalarIndexPerformanceTests.swift
// Performance benchmarks for ScalarIndex

import Testing
import Foundation
import Core
import FoundationDB
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// MARK: - Test Models

@Persistable
struct BenchmarkUser {
    #Directory<BenchmarkUser>("benchmark", "users")

    var id: String = ULID().ulidString
    var email: String = ""
    var age: Int64 = 0
    var country: String = ""
    var createdAt: Date = Date()

    #Index<BenchmarkUser>(ScalarIndexKind(fields: [\.email]))
    #Index<BenchmarkUser>(ScalarIndexKind(fields: [\.age]))
    #Index<BenchmarkUser>(ScalarIndexKind(fields: [\.country, \.age]))
    #Index<BenchmarkUser>(ScalarIndexKind(fields: [\.createdAt]))
}

@Suite("ScalarIndex Performance Tests", .serialized)
struct ScalarIndexPerformanceTests {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let schema = Schema(entities: [
            Schema.Entity(BenchmarkUser.self)
        ])
        return try await FDBContainer(for: schema)
    }

    // MARK: - Point Lookup Performance

    @Test("Point lookup performance - single field equality")
    func testPointLookupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Insert test data
        let testPrefix = uniqueID("email")
        let itemCount = 100

        for i in 0..<itemCount {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"
            user.age = Int64(20 + (i % 50))
            user.country = ["US", "JP", "UK", "DE", "FR"][i % 5]
            context.insert(user)
        }
        try await context.save()

        // Benchmark: Point lookups
        let lookupCount = 50
        let startTime = DispatchTime.now()

        for i in 0..<lookupCount {
            let targetEmail = "\(testPrefix)-\(i % itemCount)@example.com"
            let results = try await context.fetch(BenchmarkUser.self)
                .where(\.email == targetEmail)
                .execute()
            #expect(results.count == 1)
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(lookupCount) / 1_000_000

        print("ScalarIndex Point Lookup:")
        print("  - Total lookups: \(lookupCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(lookupCount) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion: should be under 50ms average
        #expect(avgMs < 50, "Point lookup should be under 50ms average")
    }

    // MARK: - Range Scan Performance

    @Test("Range scan performance - numeric range")
    func testRangeScanPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Insert test data
        let testPrefix = uniqueID("range")
        let itemCount = 500

        for i in 0..<itemCount {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"
            user.age = Int64(i % 100)  // Ages 0-99
            user.country = ["US", "JP", "UK", "DE", "FR"][i % 5]
            context.insert(user)
        }
        try await context.save()

        // Benchmark: Range scans
        let scanCount = 20
        let startTime = DispatchTime.now()

        for i in 0..<scanCount {
            let minAge = Int64((i * 5) % 50)
            let maxAge = minAge + 10
            let results = try await context.fetch(BenchmarkUser.self)
                .where(\.age >= minAge)
                .where(\.age <= maxAge)
                .execute()
            #expect(results.count > 0)
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(scanCount) / 1_000_000

        print("ScalarIndex Range Scan:")
        print("  - Total scans: \(scanCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion: should be under 100ms average
        #expect(avgMs < 100, "Range scan should be under 100ms average")
    }

    // MARK: - Composite Index Performance

    @Test("Composite index performance - multi-field query")
    func testCompositeIndexPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Insert test data
        let testPrefix = uniqueID("composite")
        let itemCount = 500
        let countries = ["US", "JP", "UK", "DE", "FR"]

        for i in 0..<itemCount {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"
            user.age = Int64(20 + (i % 50))
            user.country = countries[i % 5]
            context.insert(user)
        }
        try await context.save()

        // Benchmark: Composite index queries
        let queryCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<queryCount {
            let country = countries[i % 5]
            let minAge = Int64(25 + (i % 10))
            let results = try await context.fetch(BenchmarkUser.self)
                .where(\.country == country)
                .where(\.age >= minAge)
                .execute()
            // Results may vary, just ensure query executes
            _ = results.count
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(queryCount) / 1_000_000

        print("ScalarIndex Composite Query:")
        print("  - Total queries: \(queryCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion: should be under 100ms average
        #expect(avgMs < 100, "Composite query should be under 100ms average")
    }

    // MARK: - Bulk Insert Performance

    @Test("Bulk insert performance")
    func testBulkInsertPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let testPrefix = uniqueID("bulk")
        let batchSize = 100

        // Benchmark: Bulk insert
        let startTime = DispatchTime.now()

        for i in 0..<batchSize {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"
            user.age = Int64(20 + (i % 50))
            user.country = ["US", "JP", "UK", "DE", "FR"][i % 5]
            context.insert(user)
        }
        try await context.save()

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let totalMs = Double(totalNs) / 1_000_000

        print("ScalarIndex Bulk Insert:")
        print("  - Items inserted: \(batchSize)")
        print("  - Total time: \(String(format: "%.2f", totalMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(batchSize) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion: should complete in reasonable time
        #expect(totalMs < 10000, "Bulk insert of \(batchSize) items should complete in under 10s")
    }

    // MARK: - Pagination Performance

    @Test("Cursor-based pagination performance")
    func testPaginationPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Insert test data
        let testPrefix = uniqueID("page")
        let itemCount = 200

        for i in 0..<itemCount {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"
            user.age = Int64(i)
            user.createdAt = Date().addingTimeInterval(Double(i))
            context.insert(user)
        }
        try await context.save()

        // Benchmark: Paginated queries
        let pageSize = 20
        let pageCount = 5
        let startTime = DispatchTime.now()

        var lastDate: Date? = nil
        for _ in 0..<pageCount {
            var query = context.fetch(BenchmarkUser.self)
                .orderBy(\.createdAt, .ascending)
                .limit(pageSize)

            if let cursor = lastDate {
                query = query.where(\.createdAt > cursor)
            }

            let results = try await query.execute()
            if let last = results.last {
                lastDate = last.createdAt
            }
            #expect(results.count <= pageSize)
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(pageCount) / 1_000_000

        print("ScalarIndex Pagination:")
        print("  - Pages fetched: \(pageCount)")
        print("  - Page size: \(pageSize)")
        print("  - Average page latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 100, "Pagination should be under 100ms per page")
    }

    // MARK: - Index Selectivity Test

    @Test("Index selectivity comparison")
    func testIndexSelectivity() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Setup: Insert test data with varying cardinality
        let testPrefix = uniqueID("select")
        let itemCount = 300

        for i in 0..<itemCount {
            var user = BenchmarkUser()
            user.email = "\(testPrefix)-\(i)@example.com"  // High cardinality (unique)
            user.age = Int64(20 + (i % 10))  // Low cardinality (10 values)
            user.country = ["US", "JP"][i % 2]  // Very low cardinality (2 values)
            context.insert(user)
        }
        try await context.save()

        // High selectivity query (email - unique)
        let highSelectivityStart = DispatchTime.now()
        let highSelectivityResults = try await context.fetch(BenchmarkUser.self)
            .where(\.email == "\(testPrefix)-50@example.com")
            .execute()
        let highSelectivityNs = DispatchTime.now().uptimeNanoseconds - highSelectivityStart.uptimeNanoseconds

        // Low selectivity query (country - 2 values)
        let lowSelectivityStart = DispatchTime.now()
        let lowSelectivityResults = try await context.fetch(BenchmarkUser.self)
            .where(\.country == "US")
            .execute()
        let lowSelectivityNs = DispatchTime.now().uptimeNanoseconds - lowSelectivityStart.uptimeNanoseconds

        print("Index Selectivity Comparison:")
        print("  - High selectivity (email): \(highSelectivityResults.count) results, \(String(format: "%.2f", Double(highSelectivityNs) / 1_000_000))ms")
        print("  - Low selectivity (country): \(lowSelectivityResults.count) results, \(String(format: "%.2f", Double(lowSelectivityNs) / 1_000_000))ms")

        #expect(highSelectivityResults.count == 1)
        #expect(lowSelectivityResults.count == itemCount / 2)
    }
}
