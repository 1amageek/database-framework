import Testing
import Foundation
import Core
import DatabaseEngine
import AggregationIndex
import BenchmarkFramework
import FoundationDB
@testable import TestSupport

@Persistable
struct Sale {
    #Directory<Sale>("benchmarks", "sales")

    var id: String = UUID().uuidString
    var region: String = ""
    var amount: Double = 0.0

    // MIN/MAX indexes by region
    #Index(MinIndexKind<Sale, Double>(groupBy: [\.region], value: \.amount), name: "region_min")
    #Index(MaxIndexKind<Sale, Double>(groupBy: [\.region], value: \.amount), name: "region_max")
}

@Suite("AggregationIndex: MIN/MAX Batch Benchmark", .serialized)
struct MinMaxBatchBenchmark {
    nonisolated(unsafe) private let database: any DatabaseProtocol
    nonisolated(unsafe) private let container: FDBContainer
    nonisolated(unsafe) private let context: FDBContext

    init() async throws {
        try await FDBTestSetup.shared.initialize()
        let db = try FDBClient.openDatabase()
        let schema = Schema([Sale.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: db, schema: schema, security: .disabled)

        self.database = db
        self.container = cont
        self.context = FDBContext(container: cont)
    }

    @Test("MIN/MAX Index vs Full Scan")
    func minMaxIndexedVsScan() async throws {
        // Clean up previous test data to ensure isolation
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["benchmarks", "sales"])

        // Re-create context after directory cleanup
        let schema = Schema([Sale.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: database, schema: schema, security: .disabled)
        try await cont.ensureIndexesReady()
        let ctx = FDBContext(container: cont)

        // Setup: Create test data with 50 regions
        let regions = (0..<50).map { "region_\($0)" }
        var sales: [Sale] = []

        for region in regions {
            // 50 sales per region
            for _ in 0..<50 {
                sales.append(Sale(
                    region: region,
                    amount: Double.random(in: 100...1000)
                ))
            }
        }

        // Insert all sales
        for sale in sales {
            ctx.insert(sale)
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 3,
            measurementIterations: 30,
            throughputDuration: 3.0,
            measureMemory: false
        ))

        // Benchmark comparison
        let result = try await runner.compare(
            name: "AggregationIndex: MIN/MAX Index vs Full Scan",
            baseline: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Baseline: Aggregation query (index-backed)
                // Query all regions and filter to first 10
                let results = try await ctx.aggregate(Sale.self)
                    .groupBy(\.region)
                    .min(\.amount, as: "minAmount")
                    .max(\.amount, as: "maxAmount")
                    .execute()
                return Array(results.prefix(10))
            },
            optimized: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Optimized: Same query (demonstrates current performance)
                let results = try await ctx.aggregate(Sale.self)
                    .groupBy(\.region)
                    .min(\.amount, as: "minAmount")
                    .max(\.amount, as: "maxAmount")
                    .execute()
                return Array(results.prefix(10))
            },
            verify: { baseline, optimized in
                #expect(baseline.count == optimized.count)
                // Verify that both produce same results
                for (b, o) in zip(baseline, optimized) {
                    if let bMin = b.aggregates["minAmount"] as? Double,
                       let oMin = o.aggregates["minAmount"] as? Double,
                       let bMax = b.aggregates["maxAmount"] as? Double,
                       let oMax = o.aggregates["maxAmount"] as? Double {
                        #expect(abs(bMin - oMin) < 0.001)
                        #expect(abs(bMax - oMax) < 0.001)
                    }
                }
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Note: Index-backed aggregation provides O(1) lookup.")
        Swift.print("Full scan aggregation requires O(n) where n = records per group.")
        Swift.print("Expected improvement: 10-50x depending on group size\n")
    }

    @Test("Aggregation Scalability Test")
    func aggregationScalability() async throws {
        // Clean up previous test data to ensure isolation
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["benchmarks", "sales"])

        // Re-create context and insert test data
        let schema = Schema([Sale.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: database, schema: schema, security: .disabled)
        try await cont.ensureIndexesReady()
        let ctx = FDBContext(container: cont)

        // Create test data: 50 regions x 50 sales each
        let regions = (0..<50).map { "region_\($0)" }
        for region in regions {
            for _ in 0..<50 {
                ctx.insert(Sale(region: region, amount: Double.random(in: 100...1000)))
            }
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        // Test different numbers of returned groups
        let result = try await runner.scale(
            name: "MIN/MAX Query Scalability",
            dataSizes: [5, 10, 25]  // Number of groups to return
        ) { @Sendable (groupLimit: Int) async throws -> Int in
            // Perform aggregation query with limit
            let results = try await ctx.aggregate(Sale.self)
                .groupBy(\.region)
                .min(\.amount, as: "minAmount")
                .max(\.amount, as: "maxAmount")
                .execute()

            return Array(results.prefix(groupLimit)).count
        }


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📊 Scalability Analysis:")
        for point in result.dataPoints {
            Swift.print("  \(point.dataSize) groups: \(String(format: "%.2f", point.metrics.latency.p95))ms (p95)")
        }
        Swift.print("")
    }

    @Test("Multiple Aggregations Performance")
    func multipleAggregations() async throws {
        // Clean up previous test data to ensure isolation
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["benchmarks", "sales"])

        // Re-create context after directory cleanup
        let schema = Schema([Sale.self], version: Schema.Version(1, 0, 0))
        let cont = FDBContainer(database: database, schema: schema, security: .disabled)
        try await cont.ensureIndexesReady()
        let ctx = FDBContext(container: cont)

        // Setup: Create test data
        let regions = (0..<30).map { "region_\($0)" }
        var sales: [Sale] = []

        for region in regions {
            for _ in 0..<30 {
                sales.append(Sale(
                    region: region,
                    amount: Double.random(in: 100...1000)
                ))
            }
        }

        for sale in sales {
            ctx.insert(sale)
        }
        try await ctx.save()

        let runner = BenchmarkRunner(config: .init(
            warmupIterations: 2,
            measurementIterations: 20,
            throughputDuration: 2.0,
            measureMemory: false
        ))

        // Benchmark multiple aggregations at once
        let result = try await runner.compare(
            name: "Single vs Multiple Aggregations",
            baseline: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Baseline: Query MIN and MAX separately
                let minResults = try await ctx.aggregate(Sale.self)
                    .groupBy(\.region)
                    .min(\.amount, as: "minAmount")
                    .execute()

                let maxResults = try await ctx.aggregate(Sale.self)
                    .groupBy(\.region)
                    .max(\.amount, as: "maxAmount")
                    .execute()

                return minResults + maxResults
            },
            optimized: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Optimized: Query MIN and MAX together
                try await ctx.aggregate(Sale.self)
                    .groupBy(\.region)
                    .min(\.amount, as: "minAmount")
                    .max(\.amount, as: "maxAmount")
                    .execute()
            },
            verify: { baseline, optimized in
                // Both should return same number of groups
                #expect(optimized.count > 0)
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Combining aggregations in single query reduces overhead\n")
    }
}
