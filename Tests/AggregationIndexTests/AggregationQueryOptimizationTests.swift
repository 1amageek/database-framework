// AggregationQueryOptimizationTests.swift
// Integration tests for AggregationQuery index-backed execution

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Models

/// Test model with COUNT index for testing index-backed execution
struct AggQueryTestOrder: Persistable {
    typealias ID = String

    var id: String
    var region: String
    var amount: Int64
    var quantity: Int64

    init(id: String = UUID().uuidString, region: String, amount: Int64, quantity: Int64 = 1) {
        self.id = id
        self.region = region
        self.amount = amount
        self.quantity = quantity
    }

    static var persistableType: String { "AggQueryTestOrder" }
    static var allFields: [String] { ["id", "region", "amount", "quantity"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "region": return region
        case "amount": return amount
        case "quantity": return quantity
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<AggQueryTestOrder, Value>) -> String {
        switch keyPath {
        case \AggQueryTestOrder.id: return "id"
        case \AggQueryTestOrder.region: return "region"
        case \AggQueryTestOrder.amount: return "amount"
        case \AggQueryTestOrder.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<AggQueryTestOrder>) -> String {
        switch keyPath {
        case \AggQueryTestOrder.id: return "id"
        case \AggQueryTestOrder.region: return "region"
        case \AggQueryTestOrder.amount: return "amount"
        case \AggQueryTestOrder.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<AggQueryTestOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Entity Helper

/// Create a Schema.Entity with runtime indexDescriptors for testing
private func makeTestEntity(
    name: String,
    allFields: [String],
    indexDescriptors: [IndexDescriptor]
) -> Schema.Entity {
    let fields = allFields.enumerated().map { (i, f) in
        FieldSchema(name: f, fieldNumber: i + 1, type: .string)
    }
    var entity = Schema.Entity(name: name, fields: fields)
    entity.indexDescriptors = indexDescriptors
    return entity
}

// MARK: - Test Helper

private struct OptTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let testId: String

    // Maintainers
    let countMaintainer: CountIndexMaintainer<AggQueryTestOrder>
    let sumMaintainer: SumIndexMaintainer<AggQueryTestOrder, Int64>
    let avgMaintainer: AverageIndexMaintainer<AggQueryTestOrder, Int64>

    init() throws {
        self.database = try FDBClient.openDatabase()
        self.testId = String(UUID().uuidString.prefix(8))
        self.subspace = Subspace(prefix: Tuple("test", "aggquery", testId).pack())
        self.indexSubspace = subspace.subspace("I")

        // COUNT index: group by region
        let countIndex = Index(
            name: "AggQueryTestOrder_count_region",
            kind: CountIndexKind<AggQueryTestOrder>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "AggQueryTestOrder_count_region",
            itemTypes: Set(["AggQueryTestOrder"])
        )
        self.countMaintainer = CountIndexMaintainer<AggQueryTestOrder>(
            index: countIndex,
            subspace: indexSubspace.subspace("AggQueryTestOrder_count_region"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // SUM index: group by region, sum amount
        let sumIndex = Index(
            name: "AggQueryTestOrder_sum_region_amount",
            kind: SumIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "AggQueryTestOrder_sum_region_amount",
            itemTypes: Set(["AggQueryTestOrder"])
        )
        self.sumMaintainer = SumIndexMaintainer<AggQueryTestOrder, Int64>(
            index: sumIndex,
            subspace: indexSubspace.subspace("AggQueryTestOrder_sum_region_amount"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // AVG index: group by region, avg amount
        let avgIndex = Index(
            name: "AggQueryTestOrder_avg_region_amount",
            kind: AverageIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "AggQueryTestOrder_avg_region_amount",
            itemTypes: Set(["AggQueryTestOrder"])
        )
        self.avgMaintainer = AverageIndexMaintainer<AggQueryTestOrder, Int64>(
            index: avgIndex,
            subspace: indexSubspace.subspace("AggQueryTestOrder_avg_region_amount"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Insert orders and update all indexes
    func insertOrders(_ orders: [AggQueryTestOrder]) async throws {
        try await database.withTransaction { transaction in
            for order in orders {
                try await countMaintainer.updateIndex(oldItem: nil, newItem: order, transaction: transaction)
                try await sumMaintainer.updateIndex(oldItem: nil, newItem: order, transaction: transaction)
                try await avgMaintainer.updateIndex(oldItem: nil, newItem: order, transaction: transaction)
            }
        }
    }

    /// Get all counts from COUNT index
    func getAllCounts() async throws -> [(grouping: [any TupleElement], count: Int64)] {
        try await database.withTransaction { transaction in
            try await countMaintainer.getAllCounts(transaction: transaction)
        }
    }

    /// Get all sums from SUM index (returns Double)
    func getAllSums() async throws -> [(grouping: [any TupleElement], sum: Double)] {
        try await database.withTransaction { transaction in
            try await sumMaintainer.getAllSums(transaction: transaction)
        }
    }

    /// Get all averages from AVG index
    func getAllAverages() async throws -> [(grouping: [any TupleElement], average: Double)] {
        try await database.withTransaction { transaction in
            let results = try await avgMaintainer.getAllAverages(transaction: transaction)
            return results.map { ($0.grouping, $0.average) }
        }
    }
}

// MARK: - Behavior Tests

@Suite("AggregationQuery Optimization Tests", .tags(.fdb), .serialized)
struct AggregationQueryOptimizationTests {

    // MARK: - Index Maintainer Direct Tests

    @Test("COUNT index maintains correct counts")
    func testCountIndexMaintainsCorrectCounts() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try OptTestContext()

        let orders = [
            AggQueryTestOrder(region: "Tokyo", amount: 100),
            AggQueryTestOrder(region: "Tokyo", amount: 200),
            AggQueryTestOrder(region: "Osaka", amount: 150),
            AggQueryTestOrder(region: "Kyoto", amount: 300)
        ]

        try await ctx.insertOrders(orders)

        let counts = try await ctx.getAllCounts()
        #expect(counts.count == 3, "Should have 3 regions")

        let countByRegion = Dictionary(uniqueKeysWithValues: counts.map { (grouping, count) in
            (grouping.first as! String, count)
        })
        #expect(countByRegion["Tokyo"] == 2, "Tokyo should have 2 orders")
        #expect(countByRegion["Osaka"] == 1, "Osaka should have 1 order")
        #expect(countByRegion["Kyoto"] == 1, "Kyoto should have 1 order")

        try await ctx.cleanup()
    }

    @Test("SUM index maintains correct sums")
    func testSumIndexMaintainsCorrectSums() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try OptTestContext()

        let orders = [
            AggQueryTestOrder(region: "Tokyo", amount: 100),
            AggQueryTestOrder(region: "Tokyo", amount: 200),
            AggQueryTestOrder(region: "Osaka", amount: 150)
        ]

        try await ctx.insertOrders(orders)

        let sums = try await ctx.getAllSums()
        #expect(sums.count == 2, "Should have 2 regions")

        let sumByRegion = Dictionary(uniqueKeysWithValues: sums.map { (grouping, sum) in
            (grouping.first as! String, sum)
        })
        #expect(sumByRegion["Tokyo"] == 300.0, "Tokyo sum should be 300")
        #expect(sumByRegion["Osaka"] == 150.0, "Osaka sum should be 150")

        try await ctx.cleanup()
    }

    @Test("AVG index maintains correct averages")
    func testAvgIndexMaintainsCorrectAverages() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try OptTestContext()

        let orders = [
            AggQueryTestOrder(region: "Tokyo", amount: 100),
            AggQueryTestOrder(region: "Tokyo", amount: 200),
            AggQueryTestOrder(region: "Osaka", amount: 150)
        ]

        try await ctx.insertOrders(orders)

        let averages = try await ctx.getAllAverages()
        #expect(averages.count == 2, "Should have 2 regions")

        let avgByRegion = Dictionary(uniqueKeysWithValues: averages.map { (grouping, avg) in
            (grouping.first as! String, avg)
        })
        #expect(avgByRegion["Tokyo"] == 150.0, "Tokyo avg should be 150.0")
        #expect(avgByRegion["Osaka"] == 150.0, "Osaka avg should be 150.0")

        try await ctx.cleanup()
    }

    // MARK: - Index Matching Tests

    @Test("MIN aggregation uses index when available")
    func testMinAggregationUsesIndex() async throws {
        try await FDBTestSetup.shared.initialize()

        // Create a mock IndexQueryContext
        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "min", testId).pack())

        // Create schema with MinIndexKind
        let minIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_min_region_amount",
            keyPaths: [\AggQueryTestOrder.region, \AggQueryTestOrder.amount],
            kind: MinIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [minIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query with MIN aggregation
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.region)
            .min(\AggQueryTestOrder.amount, as: "minAmount")

        // Check that determineExecutionStrategies returns useIndex for MIN (Phase 1 implementation)
        let strategies = builder.determineExecutionStrategies()
        guard let minStrategy = strategies["minAmount"] else {
            Issue.record("minAmount strategy should exist")
            return
        }

        switch minStrategy {
        case .useIndex:
            // Expected: MIN should use index-backed execution (Phase 1)
            break
        case .inMemory:
            Issue.record("MIN aggregation should use index when available")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("MAX aggregation uses index when available")
    func testMaxAggregationUsesIndex() async throws {
        try await FDBTestSetup.shared.initialize()

        // Create a mock IndexQueryContext
        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "max", testId).pack())

        // Create schema with MaxIndexKind
        let maxIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_max_region_amount",
            keyPaths: [\AggQueryTestOrder.region, \AggQueryTestOrder.amount],
            kind: MaxIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [maxIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query with MAX aggregation
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.region)
            .max(\AggQueryTestOrder.amount, as: "maxAmount")

        // Check that determineExecutionStrategies returns useIndex for MAX (Phase 1 implementation)
        let strategies = builder.determineExecutionStrategies()
        guard let maxStrategy = strategies["maxAmount"] else {
            Issue.record("maxAmount strategy should exist")
            return
        }

        switch maxStrategy {
        case .useIndex:
            // Expected: MAX should use index-backed execution (Phase 1)
            break
        case .inMemory:
            Issue.record("MAX aggregation should use index when available")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("COUNT aggregation matches CountIndexKind")
    func testCountAggregationMatchesIndex() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "count_match", testId).pack())

        // Create schema with CountIndexKind
        let countIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_count_region",
            keyPaths: [\AggQueryTestOrder.region],
            kind: CountIndexKind<AggQueryTestOrder>(groupBy: [\.region])
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query with COUNT aggregation matching the index
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.region)
            .count(as: "orderCount")

        // Check that determineExecutionStrategies returns useIndex for COUNT
        let strategies = builder.determineExecutionStrategies()
        guard let countStrategy = strategies["orderCount"] else {
            Issue.record("orderCount strategy should exist")
            return
        }

        switch countStrategy {
        case .useIndex(let descriptor):
            #expect(descriptor.name == "AggQueryTestOrder_count_region", "Should match the count index")
        case .inMemory:
            Issue.record("COUNT aggregation with matching index should use index-backed execution")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("SUM aggregation matches SumIndexKind")
    func testSumAggregationMatchesIndex() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "sum_match", testId).pack())

        // Create schema with SumIndexKind
        let sumIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_sum_region_amount",
            keyPaths: [\AggQueryTestOrder.region, \AggQueryTestOrder.amount],
            kind: SumIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [sumIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query with SUM aggregation matching the index
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.region)
            .sum(\AggQueryTestOrder.amount, as: "totalAmount")

        // Check that determineExecutionStrategies returns useIndex for SUM
        let strategies = builder.determineExecutionStrategies()
        guard let sumStrategy = strategies["totalAmount"] else {
            Issue.record("totalAmount strategy should exist")
            return
        }

        switch sumStrategy {
        case .useIndex(let descriptor):
            #expect(descriptor.name == "AggQueryTestOrder_sum_region_amount", "Should match the sum index")
        case .inMemory:
            Issue.record("SUM aggregation with matching index should use index-backed execution")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Mixed Aggregation Tests

    @Test("Mixed aggregations with COUNT and MIN both use indexes")
    func testMixedAggregationsWithCountAndMinUseIndexes() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "mixed", testId).pack())

        // Create schema with COUNT and MIN indexes
        let countIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_count_region",
            keyPaths: [\AggQueryTestOrder.region],
            kind: CountIndexKind<AggQueryTestOrder>(groupBy: [\.region])
        )
        let minIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_min_region_amount",
            keyPaths: [\AggQueryTestOrder.region, \AggQueryTestOrder.amount],
            kind: MinIndexKind<AggQueryTestOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor, minIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query with both COUNT and MIN (both have indexes)
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.region)
            .count(as: "orderCount")
            .min(\AggQueryTestOrder.amount, as: "minAmount")

        // Check strategies
        let strategies = builder.determineExecutionStrategies()

        // COUNT should find index
        if case .useIndex = strategies["orderCount"] {
            // Good
        } else {
            Issue.record("COUNT should use index")
        }

        // MIN should also use index (Phase 1 implementation)
        if case .useIndex = strategies["minAmount"] {
            // Good
        } else {
            Issue.record("MIN should use index")
        }

        // Overall execution: both are index-backed, so execute() should use index path

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Aggregation without matching groupBy uses in-memory")
    func testAggregationWithoutMatchingGroupByUsesInMemory() async throws {
        try await FDBTestSetup.shared.initialize()

        let database = try FDBClient.openDatabase()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "no_match", testId).pack())

        // Create schema with COUNT index grouped by 'region'
        let countIndexDescriptor = IndexDescriptor(
            name: "AggQueryTestOrder_count_region",
            keyPaths: [\AggQueryTestOrder.region],
            kind: CountIndexKind<AggQueryTestOrder>(groupBy: [\.region])
        )

        let schema = Schema(
            entities: [
                makeTestEntity(
                    name: "AggQueryTestOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor]
                )
            ]
        )

        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        let context = container.newContext()

        // Build query grouping by DIFFERENT field (amount instead of region)
        // This should NOT match the index
        let builder = context.aggregate(AggQueryTestOrder.self)
            .groupBy(\AggQueryTestOrder.amount)  // Different field than index
            .count(as: "orderCount")

        // Check that it falls back to in-memory
        let strategies = builder.determineExecutionStrategies()
        guard let countStrategy = strategies["orderCount"] else {
            Issue.record("orderCount strategy should exist")
            return
        }

        switch countStrategy {
        case .inMemory:
            // Expected: groupBy doesn't match index
            break
        case .useIndex:
            Issue.record("COUNT with non-matching groupBy should use in-memory")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
