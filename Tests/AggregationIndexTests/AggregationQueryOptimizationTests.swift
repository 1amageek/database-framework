#if FOUNDATION_DB
// AggregationQueryOptimizationTests.swift
// Integration tests for AggregationQuery index-backed execution

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import DatabaseValue
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex
import DatabaseRuntime

// MARK: - Test Models

/// Test model with COUNT index for testing index-backed execution
struct AggregationOrder: Persistable {
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

    static var persistableType: String { "AggregationOrder" }
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

    static func fieldName<Value>(for keyPath: KeyPath<AggregationOrder, Value>) -> String {
        switch keyPath {
        case \AggregationOrder.id: return "id"
        case \AggregationOrder.region: return "region"
        case \AggregationOrder.amount: return "amount"
        case \AggregationOrder.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<AggregationOrder>) -> String {
        switch keyPath {
        case \AggregationOrder.id: return "id"
        case \AggregationOrder.region: return "region"
        case \AggregationOrder.amount: return "amount"
        case \AggregationOrder.quantity: return "quantity"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<AggregationOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Schema Entity Construction

/// Create a Schema.Entity with runtime indexDescriptors for testing
private func makeAggregationOrderEntity(
    name: String,
    allFields: [String],
    indexDescriptors: [IndexDescriptor]
) -> Schema.Entity {
    precondition(name == AggregationOrder.persistableType)
    precondition(allFields == AggregationOrder.allFields)
    var entity = Schema.Entity(from: AggregationOrder.self)
    entity.indexDescriptors = indexDescriptors
    return entity
}

// MARK: - Aggregation Query Context

private struct AggregationQueryContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let testId: String

    // Maintainers
    let countMaintainer: CountIndexMaintainer<AggregationOrder>
    let sumMaintainer: SumIndexMaintainer<AggregationOrder, Int64>
    let avgMaintainer: AverageIndexMaintainer<AggregationOrder, Int64>

    init() async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        self.testId = String(UUID().uuidString.prefix(8))
        self.subspace = Subspace(prefix: Tuple("test", "aggquery", testId).pack())
        self.indexSubspace = subspace.subspace("I")

        // COUNT index: group by region
        let countIndex = Index(
            name: "AggregationOrder_count_region",
            kind: CountIndexKind<AggregationOrder>(groupBy: [\.region]),
            rootExpression: FieldKeyExpression(fieldName: "region"),
            subspaceKey: "AggregationOrder_count_region",
            itemTypes: Set(["AggregationOrder"])
        )
        self.countMaintainer = CountIndexMaintainer<AggregationOrder>(
            index: countIndex,
            subspace: indexSubspace.subspace("AggregationOrder_count_region"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // SUM index: group by region, sum amount
        let sumIndex = Index(
            name: "AggregationOrder_sum_region_amount",
            kind: SumIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "AggregationOrder_sum_region_amount",
            itemTypes: Set(["AggregationOrder"])
        )
        self.sumMaintainer = SumIndexMaintainer<AggregationOrder, Int64>(
            index: sumIndex,
            subspace: indexSubspace.subspace("AggregationOrder_sum_region_amount"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // AVG index: group by region, avg amount
        let avgIndex = Index(
            name: "AggregationOrder_avg_region_amount",
            kind: AverageIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "AggregationOrder_avg_region_amount",
            itemTypes: Set(["AggregationOrder"])
        )
        self.avgMaintainer = AverageIndexMaintainer<AggregationOrder, Int64>(
            index: avgIndex,
            subspace: indexSubspace.subspace("AggregationOrder_avg_region_amount"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    /// Insert orders and update all indexes
    func insertOrders(_ orders: [AggregationOrder]) async throws {
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
            try await sumMaintainer.getAllSumsAsDouble(transaction: transaction)
        }
    }

    /// Get all averages from AVG index
    func getAllAverages() async throws -> [(grouping: [any TupleElement], average: Double)] {
        try await database.withTransaction { transaction in
            let results = try await avgMaintainer.getAllAveragesAsDouble(transaction: transaction)
            return results.map { ($0.grouping, $0.average) }
        }
    }
}

// MARK: - Behavior Tests

@Suite("AggregationQuery Optimization Tests", .tags(.fdb), .serialized, .heartbeat)
struct AggregationQueryOptimizationTests {

    // MARK: - Index Maintainer Direct Tests

    @Test("COUNT index maintains correct counts")
    func testCountIndexMaintainsCorrectCounts() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await AggregationQueryContext()

        let orders = [
            AggregationOrder(region: "Tokyo", amount: 100),
            AggregationOrder(region: "Tokyo", amount: 200),
            AggregationOrder(region: "Osaka", amount: 150),
            AggregationOrder(region: "Kyoto", amount: 300)
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await AggregationQueryContext()

        let orders = [
            AggregationOrder(region: "Tokyo", amount: 100),
            AggregationOrder(region: "Tokyo", amount: 200),
            AggregationOrder(region: "Osaka", amount: 150)
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await AggregationQueryContext()

        let orders = [
            AggregationOrder(region: "Tokyo", amount: 100),
            AggregationOrder(region: "Tokyo", amount: 200),
            AggregationOrder(region: "Osaka", amount: 150)
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
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "min", testId).pack())

        // Create schema with MinIndexKind
        let minIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_min_region_amount",
            keyPaths: [\AggregationOrder.region, \AggregationOrder.amount],
            kind: MinIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [minIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query with MIN aggregation
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.region)
            .min(\AggregationOrder.amount, as: "minAmount")

        // Check that determineExecutionStrategies returns useIndex for MIN (Phase 1 implementation)
        let strategies = try builder.determineExecutionStrategies()
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
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("MAX aggregation uses index when available")
    func testMaxAggregationUsesIndex() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "max", testId).pack())

        // Create schema with MaxIndexKind
        let maxIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_max_region_amount",
            keyPaths: [\AggregationOrder.region, \AggregationOrder.amount],
            kind: MaxIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [maxIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query with MAX aggregation
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.region)
            .max(\AggregationOrder.amount, as: "maxAmount")

        // Check that determineExecutionStrategies returns useIndex for MAX (Phase 1 implementation)
        let strategies = try builder.determineExecutionStrategies()
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
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("COUNT aggregation matches CountIndexKind")
    func testCountAggregationMatchesIndex() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "count_match", testId).pack())

        // Create schema with CountIndexKind
        let countIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_count_region",
            keyPaths: [\AggregationOrder.region],
            kind: CountIndexKind<AggregationOrder>(groupBy: [\.region])
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query with COUNT aggregation matching the index
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.region)
            .count(as: "orderCount")

        // Check that determineExecutionStrategies returns useIndex for COUNT
        let strategies = try builder.determineExecutionStrategies()
        guard let countStrategy = strategies["orderCount"] else {
            Issue.record("orderCount strategy should exist")
            return
        }

        switch countStrategy {
        case .useIndex(let descriptor):
            #expect(descriptor.name == "AggregationOrder_count_region", "Should match the count index")
        case .inMemory:
            Issue.record("COUNT aggregation with matching index should use index-backed execution")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("SUM aggregation matches SumIndexKind")
    func testSumAggregationMatchesIndex() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "sum_match", testId).pack())

        // Create schema with SumIndexKind
        let sumIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_sum_region_amount",
            keyPaths: [\AggregationOrder.region, \AggregationOrder.amount],
            kind: SumIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [sumIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query with SUM aggregation matching the index
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.region)
            .sum(\AggregationOrder.amount, as: "totalAmount")

        // Check that determineExecutionStrategies returns useIndex for SUM
        let strategies = try builder.determineExecutionStrategies()
        guard let sumStrategy = strategies["totalAmount"] else {
            Issue.record("totalAmount strategy should exist")
            return
        }

        switch sumStrategy {
        case .useIndex(let descriptor):
            #expect(descriptor.name == "AggregationOrder_sum_region_amount", "Should match the sum index")
        case .inMemory:
            Issue.record("SUM aggregation with matching index should use index-backed execution")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Mixed Aggregation Tests

    @Test("Mixed aggregations with COUNT and MIN both use indexes")
    func testMixedAggregationsWithCountAndMinUseIndexes() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "mixed", testId).pack())

        // Create schema with COUNT and MIN indexes
        let countIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_count_region",
            keyPaths: [\AggregationOrder.region],
            kind: CountIndexKind<AggregationOrder>(groupBy: [\.region])
        )
        let minIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_min_region_amount",
            keyPaths: [\AggregationOrder.region, \AggregationOrder.amount],
            kind: MinIndexKind<AggregationOrder, Int64>(groupBy: [\.region], value: \.amount)
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor, minIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query with both COUNT and MIN (both have indexes)
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.region)
            .count(as: "orderCount")
            .min(\AggregationOrder.amount, as: "minAmount")

        // Check strategies
        let strategies = try builder.determineExecutionStrategies()

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
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Aggregation without matching groupBy rejects an incompatible index")
    func aggregationWithoutMatchingGroupByRejectsIncompatibleIndex() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let subspace = Subspace(prefix: Tuple("test", "aggquery", "no_match", testId).pack())

        // Create schema with COUNT index grouped by 'region'
        let countIndexDescriptor = IndexDescriptor(
            name: "AggregationOrder_count_region",
            keyPaths: [\AggregationOrder.region],
            kind: CountIndexKind<AggregationOrder>(groupBy: [\.region])
        )

        let schema = Schema(
            entities: [
                makeAggregationOrderEntity(
                    name: "AggregationOrder",
                    allFields: ["id", "region", "amount", "quantity"],
                    indexDescriptors: [countIndexDescriptor]
                )
            ]
        )

        let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AggregationOrder.self]), security: .disabled)
        let context = container.newContext()

        // Build query grouping by DIFFERENT field (amount instead of region)
        // This should NOT match the index
        let builder = context.aggregate(AggregationOrder.self)
            .groupBy(\AggregationOrder.amount)  // Different field than index
            .count(as: "orderCount")

        // Verify that an index with incompatible grouping is not selected.
        let strategies = try builder.determineExecutionStrategies()
        guard let countStrategy = strategies["orderCount"] else {
            Issue.record("orderCount strategy should exist")
            return
        }

        switch countStrategy {
        case .inMemory:
            // Expected: the incompatible grouping excludes the index.
            break
        case .useIndex:
            Issue.record("COUNT with non-matching groupBy should not select the index")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
#endif
