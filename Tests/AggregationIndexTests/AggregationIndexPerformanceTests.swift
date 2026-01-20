// AggregationIndexPerformanceTests.swift
// Performance tests for AggregationIndex maintainers

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Models

struct PerfTestSale: Persistable {
    typealias ID = String

    var id: String
    var region: String
    var category: String
    var amount: Double
    var quantity: Int64

    init(id: String = UUID().uuidString, region: String, category: String, amount: Double, quantity: Int64 = 1) {
        self.id = id
        self.region = region
        self.category = category
        self.amount = amount
        self.quantity = quantity
    }

    static var persistableType: String { "PerfTestSale" }
    static var allFields: [String] { ["id", "region", "category", "amount", "quantity"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "region": return region
        case "category": return category
        case "amount": return amount
        case "quantity": return quantity
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PerfTestSale, Value>) -> String {
        switch keyPath {
        case \PerfTestSale.id: return "id"
        case \PerfTestSale.region: return "region"
        case \PerfTestSale.category: return "category"
        case \PerfTestSale.amount: return "amount"
        case \PerfTestSale.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PerfTestSale>) -> String {
        switch keyPath {
        case \PerfTestSale.id: return "id"
        case \PerfTestSale.region: return "region"
        case \PerfTestSale.category: return "category"
        case \PerfTestSale.amount: return "amount"
        case \PerfTestSale.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PerfTestSale> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Benchmark Helper

private struct BenchmarkResult {
    let operation: String
    let itemCount: Int
    let durationMs: Double
    let throughput: Double

    var description: String {
        String(format: "%@ - %d items in %.2fms (%.0f items/sec)",
               operation, itemCount, durationMs, throughput)
    }
}

private func benchmark<T>(
    _ operation: String,
    itemCount: Int,
    _ block: () async throws -> T
) async throws -> (result: T, benchmark: BenchmarkResult) {
    let start = DispatchTime.now()
    let result = try await block()
    let end = DispatchTime.now()

    let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
    let ms = Double(nanos) / 1_000_000
    let throughput = Double(itemCount) / (ms / 1000)

    return (result, BenchmarkResult(
        operation: operation,
        itemCount: itemCount,
        durationMs: ms,
        throughput: throughput
    ))
}

// MARK: - Performance Tests

@Suite("AggregationIndex Performance Tests", .tags(.fdb, .performance), .serialized)
struct AggregationIndexPerformanceTests {

    // MARK: - COUNT Index Performance

    @Test("COUNT index bulk insert performance")
    func testCountBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "count", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("count_region")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"]
        let categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]

        let index = Index(
            name: "count_region",
            kind: CountIndexKind<PerfTestSale>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "count_region",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = CountIndexMaintainer<PerfTestSale>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        let sales = (0..<itemCount).map { i in
            PerfTestSale(
                id: "sale-\(i)",
                region: regions[i % regions.count],
                category: categories[i % categories.count],
                amount: Double.random(in: 100...10000),
                quantity: Int64.random(in: 1...10)
            )
        }

        // Benchmark bulk insert
        let (_, insertBenchmark) = try await benchmark("COUNT bulk insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 500, "COUNT insert throughput should be > 500/s")

        // Verify counts
        let allCounts = try await database.withTransaction { transaction in
            try await maintainer.getAllCounts(transaction: transaction)
        }
        #expect(allCounts.count == regions.count, "Should have \(regions.count) groups")

        let totalCount = allCounts.reduce(0) { $0 + $1.count }
        #expect(totalCount == Int64(itemCount), "Total count should be \(itemCount)")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("COUNT index query performance")
    func testCountQueryPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "count", "query", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("count_region")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo",
                       "Kobe", "Kyoto", "Sendai", "Hiroshima", "Yokohama"]

        let index = Index(
            name: "count_region",
            kind: CountIndexKind<PerfTestSale>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "count_region",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = CountIndexMaintainer<PerfTestSale>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert test data
        let itemCount = 500
        try await database.withTransaction { transaction in
            for i in 0..<itemCount {
                let sale = PerfTestSale(
                    id: "sale-\(i)",
                    region: regions[i % regions.count],
                    category: "Category",
                    amount: 100.0
                )
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        // Benchmark single group query
        let queryCount = 100
        let (_, singleQueryBenchmark) = try await benchmark("COUNT single query", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await database.withTransaction { transaction in
                    try await maintainer.getCount(
                        groupingValues: [regions[i % regions.count]],
                        transaction: transaction
                    )
                }
            }
        }

        print(singleQueryBenchmark.description)
        #expect(singleQueryBenchmark.throughput > 50, "COUNT single query throughput should be > 50/s")

        // Benchmark getAllCounts
        let (_, allCountsBenchmark) = try await benchmark("COUNT getAllCounts", itemCount: regions.count) {
            try await database.withTransaction { transaction in
                try await maintainer.getAllCounts(transaction: transaction)
            }
        }

        print(allCountsBenchmark.description)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - SUM Index Performance

    @Test("SUM index bulk insert performance")
    func testSumBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "sum", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("sum_region_amount")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"]

        let index = Index(
            name: "sum_region_amount",
            kind: SumIndexKind<PerfTestSale, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "sum_region_amount",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = SumIndexMaintainer<PerfTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        var expectedSums: [String: Double] = [:]
        let sales = (0..<itemCount).map { i in
            let region = regions[i % regions.count]
            let amount = Double(i % 100) * 10.0 + Double.random(in: 0.01...0.99)
            expectedSums[region, default: 0] += amount
            return PerfTestSale(
                id: "sale-\(i)",
                region: region,
                category: "Category",
                amount: amount
            )
        }

        // Benchmark bulk insert
        let (_, insertBenchmark) = try await benchmark("SUM bulk insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 400, "SUM insert throughput should be > 400/s")

        // Verify sums
        let allSums = try await database.withTransaction { transaction in
            try await maintainer.getAllSums(transaction: transaction)
        }
        #expect(allSums.count == regions.count, "Should have \(regions.count) groups")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("SUM index update performance (same group)")
    func testSumUpdateSameGroupPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "sum", "update", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("sum_region_amount")

        let index = Index(
            name: "sum_region_amount",
            kind: SumIndexKind<PerfTestSale, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "sum_region_amount",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = SumIndexMaintainer<PerfTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert initial data
        let itemCount = 100
        var sales: [PerfTestSale] = []
        for i in 0..<itemCount {
            let sale = PerfTestSale(
                id: "sale-\(i)",
                region: "Tokyo",
                category: "Category",
                amount: 100.0
            )
            sales.append(sale)
        }

        try await database.withTransaction { transaction in
            for sale in sales {
                try await maintainer.updateIndex(oldItem: nil, newItem: sale, transaction: transaction)
            }
        }

        // Benchmark updates (same group, only amount changes)
        let updateCount = 500
        let (_, updateBenchmark) = try await benchmark("SUM update same group", itemCount: updateCount) {
            for i in 0..<updateCount {
                let saleIndex = i % itemCount
                let oldSale = sales[saleIndex]
                let newSale = PerfTestSale(
                    id: oldSale.id,
                    region: oldSale.region,
                    category: oldSale.category,
                    amount: oldSale.amount + 10.0
                )

                try await database.withTransaction { transaction in
                    try await maintainer.updateIndex(
                        oldItem: oldSale,
                        newItem: newSale,
                        transaction: transaction
                    )
                }

                sales[saleIndex] = newSale
            }
        }

        print(updateBenchmark.description)
        #expect(updateBenchmark.throughput > 100, "SUM update throughput should be > 100/s")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - MIN/MAX Index Performance

    @Test("MIN index bulk insert performance")
    func testMinBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "min", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("min_region_amount")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"]

        let index = Index(
            name: "min_region_amount",
            kind: MinIndexKind<PerfTestSale, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "min_region_amount",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = MinIndexMaintainer<PerfTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        var minPerRegion: [String: Double] = [:]
        let sales = (0..<itemCount).map { i in
            let region = regions[i % regions.count]
            let amount = Double.random(in: 10...10000)
            if minPerRegion[region] == nil || amount < minPerRegion[region]! {
                minPerRegion[region] = amount
            }
            return PerfTestSale(
                id: "sale-\(i)",
                region: region,
                category: "Category",
                amount: amount
            )
        }

        // Benchmark bulk insert
        let (_, insertBenchmark) = try await benchmark("MIN bulk insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 300, "MIN insert throughput should be > 300/s")

        // Benchmark MIN query
        let queryCount = 50
        let (_, queryBenchmark) = try await benchmark("MIN query", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await database.withTransaction { transaction in
                    try await maintainer.getMin(
                        groupingValues: [regions[i % regions.count]],
                        transaction: transaction
                    )
                }
            }
        }

        print(queryBenchmark.description)
        #expect(queryBenchmark.throughput > 30, "MIN query throughput should be > 30/s")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("MAX index bulk insert performance")
    func testMaxBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "max", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("max_region_amount")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"]

        let index = Index(
            name: "max_region_amount",
            kind: MaxIndexKind<PerfTestSale, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "max_region_amount",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = MaxIndexMaintainer<PerfTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        let sales = (0..<itemCount).map { i in
            PerfTestSale(
                id: "sale-\(i)",
                region: regions[i % regions.count],
                category: "Category",
                amount: Double.random(in: 10...10000)
            )
        }

        // Benchmark bulk insert
        let (_, insertBenchmark) = try await benchmark("MAX bulk insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 300, "MAX insert throughput should be > 300/s")

        // Benchmark MAX query
        let queryCount = 50
        let (_, queryBenchmark) = try await benchmark("MAX query", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await database.withTransaction { transaction in
                    try await maintainer.getMax(
                        groupingValues: [regions[i % regions.count]],
                        transaction: transaction
                    )
                }
            }
        }

        print(queryBenchmark.description)
        #expect(queryBenchmark.throughput > 30, "MAX query throughput should be > 30/s")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - AVERAGE Index Performance

    @Test("AVERAGE index bulk insert performance")
    func testAverageBulkInsertPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "avg", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("avg_region_amount")

        let regions = ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"]

        let index = Index(
            name: "avg_region_amount",
            kind: AverageIndexKind<PerfTestSale, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "avg_region_amount",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = AverageIndexMaintainer<PerfTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        var sumPerRegion: [String: Double] = [:]
        var countPerRegion: [String: Int] = [:]
        let sales = (0..<itemCount).map { i in
            let region = regions[i % regions.count]
            let amount = Double(100 + (i % 100))
            sumPerRegion[region, default: 0] += amount
            countPerRegion[region, default: 0] += 1
            return PerfTestSale(
                id: "sale-\(i)",
                region: region,
                category: "Category",
                amount: amount
            )
        }

        // Benchmark bulk insert
        let (_, insertBenchmark) = try await benchmark("AVERAGE bulk insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 300, "AVERAGE insert throughput should be > 300/s")

        // Benchmark AVERAGE query
        let queryCount = 50
        let (_, queryBenchmark) = try await benchmark("AVERAGE query", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await database.withTransaction { transaction in
                    try await maintainer.getAverage(
                        groupingValues: [regions[i % regions.count]],
                        transaction: transaction
                    )
                }
            }
        }

        print(queryBenchmark.description)
        #expect(queryBenchmark.throughput > 25, "AVERAGE query throughput should be > 25/s")

        // Verify averages
        for region in regions {
            let result = try await database.withTransaction { transaction in
                try await maintainer.getAverage(
                    groupingValues: [region],
                    transaction: transaction
                )
            }

            let expectedAvg = sumPerRegion[region]! / Double(countPerRegion[region]!)
            #expect(abs(result.average - expectedAvg) < 0.01, "Average for \(region) should be ~\(expectedAvg)")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Composite Grouping Performance

    @Test("Composite grouping performance")
    func testCompositeGroupingPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("count_region_category")

        let regions = ["Tokyo", "Osaka", "Nagoya"]
        let categories = ["Electronics", "Clothing", "Food", "Books"]

        let index = Index(
            name: "count_region_category",
            kind: CountIndexKind<PerfTestSale>(groupBy: [\.region, \.category]),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category")
            ]),
            subspaceKey: "count_region_category",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = CountIndexMaintainer<PerfTestSale>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Generate test data
        let itemCount = 1000
        let sales = (0..<itemCount).map { i in
            PerfTestSale(
                id: "sale-\(i)",
                region: regions[i % regions.count],
                category: categories[i % categories.count],
                amount: Double.random(in: 100...1000)
            )
        }

        // Benchmark bulk insert with composite key
        let (_, insertBenchmark) = try await benchmark("Composite grouping insert", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 400, "Composite insert throughput should be > 400/s")

        // Verify group count
        let allCounts = try await database.withTransaction { transaction in
            try await maintainer.getAllCounts(transaction: transaction)
        }
        let expectedGroups = regions.count * categories.count
        #expect(allCounts.count == expectedGroups, "Should have \(expectedGroups) composite groups")

        // Benchmark composite query
        let queryCount = 50
        let (_, queryBenchmark) = try await benchmark("Composite grouping query", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await database.withTransaction { transaction in
                    try await maintainer.getCount(
                        groupingValues: [
                            regions[i % regions.count],
                            categories[i % categories.count]
                        ],
                        transaction: transaction
                    )
                }
            }
        }

        print(queryBenchmark.description)
        #expect(queryBenchmark.throughput > 40, "Composite query throughput should be > 40/s")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Scale Tests

    @Test("Large scale COUNT performance")
    func testLargeScaleCountPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "scale", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("count_scale")

        // Use many groups to test scaling
        let groupCount = 100
        let itemCount = 5000

        let index = Index(
            name: "count_scale",
            kind: CountIndexKind<PerfTestSale>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "count_scale",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = CountIndexMaintainer<PerfTestSale>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert in batches
        let batchSize = 500
        var totalInsertTime: Double = 0

        for batch in 0..<(itemCount / batchSize) {
            let start = DispatchTime.now()

            try await database.withTransaction { transaction in
                for i in 0..<batchSize {
                    let idx = batch * batchSize + i
                    let sale = PerfTestSale(
                        id: "sale-\(idx)",
                        region: "region-\(idx % groupCount)",
                        category: "Category",
                        amount: 100.0
                    )
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: sale,
                        transaction: transaction
                    )
                }
            }

            let end = DispatchTime.now()
            totalInsertTime += Double(end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
        }

        let insertThroughput = Double(itemCount) / (totalInsertTime / 1000)
        print("Large scale COUNT insert: \(itemCount) items in \(totalInsertTime)ms (\(Int(insertThroughput)) items/sec)")
        #expect(insertThroughput > 1000, "Large scale COUNT insert throughput should be > 1000/s")

        // Verify all groups exist
        let allCounts = try await database.withTransaction { transaction in
            try await maintainer.getAllCounts(transaction: transaction)
        }
        #expect(allCounts.count == groupCount, "Should have \(groupCount) groups")

        let totalCount = allCounts.reduce(0) { $0 + $1.count }
        #expect(totalCount == Int64(itemCount), "Total count should be \(itemCount)")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Delete Performance

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "perf", "delete", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("count_delete")

        let regions = ["Tokyo", "Osaka", "Nagoya"]

        let index = Index(
            name: "count_delete",
            kind: CountIndexKind<PerfTestSale>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "count_delete",
            itemTypes: Set(["PerfTestSale"])
        )

        let maintainer = CountIndexMaintainer<PerfTestSale>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Insert test data
        let itemCount = 300
        var sales: [PerfTestSale] = []
        for i in 0..<itemCount {
            let sale = PerfTestSale(
                id: "sale-\(i)",
                region: regions[i % regions.count],
                category: "Category",
                amount: 100.0
            )
            sales.append(sale)
        }

        try await database.withTransaction { transaction in
            for sale in sales {
                try await maintainer.updateIndex(oldItem: nil, newItem: sale, transaction: transaction)
            }
        }

        // Verify initial counts
        let initialTotal = try await database.withTransaction { transaction in
            let counts = try await maintainer.getAllCounts(transaction: transaction)
            return counts.reduce(0) { $0 + $1.count }
        }
        #expect(initialTotal == Int64(itemCount))

        // Benchmark deletes
        let (_, deleteBenchmark) = try await benchmark("COUNT delete", itemCount: itemCount) {
            try await database.withTransaction { transaction in
                for sale in sales {
                    try await maintainer.updateIndex(
                        oldItem: sale,
                        newItem: nil,
                        transaction: transaction
                    )
                }
            }
        }

        print(deleteBenchmark.description)
        #expect(deleteBenchmark.throughput > 500, "COUNT delete throughput should be > 500/s")

        // Verify all counts are zero
        let finalTotal = try await database.withTransaction { transaction in
            let counts = try await maintainer.getAllCounts(transaction: transaction)
            return counts.reduce(0) { $0 + $1.count }
        }
        #expect(finalTotal == 0, "All counts should be zero after deletes")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
