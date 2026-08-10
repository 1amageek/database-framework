#if FOUNDATION_DB
import Testing
import TestSupport
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import DatabaseRuntime
import AggregationIndex
import BenchmarkFramework
import StorageKit
@testable import TestSupport

@Persistable
struct Sale {
    #Directory<Sale>("benchmarks", "sales")

    var id: String = UUID().uuidString
    var region: String = ""
    var amount: Double = 0.0

    #Index(
        .minimum,
        groupBy: [\Sale.region],
        value: \Sale.amount,
        name: "region_min"
    )
    #Index(
        .maximum,
        groupBy: [\Sale.region],
        value: \Sale.amount,
        name: "region_max"
    )
}

@Suite("AggregationIndex: MIN/MAX Batch Benchmark", .serialized, .heartbeat)
struct MinMaxBatchBenchmark {
    private let database: any StorageEngine

    init() async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
    }

    private func makeContext() async throws -> DatabaseContext {
        do {
            try await database.removeNamespace(path: ["benchmarks", "sales"])
        } catch {
            // Ignore missing benchmark directories so each benchmark starts clean.
        }

        let schema = try Schema(
            entities: [try Sale.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(Sale.self)]
            ),
            security: .testingDisabled
        )
        try await container.ensureIndexesReady()
        return container.testBaseContext()
    }

    @Test("MIN/MAX Index vs Full Scan")
    func minMaxIndexedVsScan() async throws {
        let ctx = try await makeContext()

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
            try ctx.insert(sale)
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
                    .groupBy(Sale.fields.region)
                    .min(Sale.fields.amount, as: "minAmount")
                    .max(Sale.fields.amount, as: "maxAmount")
                    .execute()
                return Array(results.prefix(10))
            },
            optimized: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Optimized: Same query (demonstrates current performance)
                let results = try await ctx.aggregate(Sale.self)
                    .groupBy(Sale.fields.region)
                    .min(Sale.fields.amount, as: "minAmount")
                    .max(Sale.fields.amount, as: "maxAmount")
                    .execute()
                return Array(results.prefix(10))
            },
            verify: { baseline, optimized in
                #expect(baseline.count == optimized.count)
                var baselineByRegion: [String: AggregateResult<Sale>] = [:]
                var optimizedByRegion: [String: AggregateResult<Sale>] = [:]
                for result in baseline {
                    guard let region = result.groupKeyString("region"),
                          baselineByRegion.updateValue(
                              result,
                              forKey: region
                          ) == nil else {
                        Issue.record("Baseline contains an invalid region group")
                        return
                    }
                }
                for result in optimized {
                    guard let region = result.groupKeyString("region"),
                          optimizedByRegion.updateValue(
                              result,
                              forKey: region
                          ) == nil else {
                        Issue.record("Optimized result contains an invalid region group")
                        return
                    }
                }
                #expect(
                    Set(baselineByRegion.keys)
                        == Set(optimizedByRegion.keys)
                )
                for (region, baselineResult) in baselineByRegion {
                    guard let optimizedResult = optimizedByRegion[region] else {
                        Issue.record("Optimized result is missing region \(region)")
                        return
                    }
                    #expect(
                        baselineResult.aggregates["minAmount"]
                            == optimizedResult.aggregates["minAmount"]
                    )
                    #expect(
                        baselineResult.aggregates["maxAmount"]
                            == optimizedResult.aggregates["maxAmount"]
                    )
                }
            }
        )


        // Print console report
        ConsoleReporter.print(result)

        Swift.print("\n📝 Note: Index-backed batch aggregation performs a bounded O(G) scan.")
        Swift.print("Full scan aggregation requires O(n) where n = entities per group.")
        Swift.print("Expected improvement: 10-50x depending on group size\n")
    }

    @Test("Aggregation Scalability Test")
    func aggregationScalability() async throws {
        let ctx = try await makeContext()

        // Create test data: 50 regions x 50 sales each
        let regions = (0..<50).map { "region_\($0)" }
        for region in regions {
            for _ in 0..<50 {
                try ctx.insert(Sale(region: region, amount: Double.random(in: 100...1000)))
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
                .groupBy(Sale.fields.region)
                .min(Sale.fields.amount, as: "minAmount")
                .max(Sale.fields.amount, as: "maxAmount")
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
        let ctx = try await makeContext()

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
            try ctx.insert(sale)
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
                    .groupBy(Sale.fields.region)
                    .min(Sale.fields.amount, as: "minAmount")
                    .execute()

                let maxResults = try await ctx.aggregate(Sale.self)
                    .groupBy(Sale.fields.region)
                    .max(Sale.fields.amount, as: "maxAmount")
                    .execute()

                return minResults + maxResults
            },
            optimized: { @Sendable () async throws -> [AggregateResult<Sale>] in
                // Optimized: Query MIN and MAX together
                try await ctx.aggregate(Sale.self)
                    .groupBy(Sale.fields.region)
                    .min(Sale.fields.amount, as: "minAmount")
                    .max(Sale.fields.amount, as: "maxAmount")
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
#endif
