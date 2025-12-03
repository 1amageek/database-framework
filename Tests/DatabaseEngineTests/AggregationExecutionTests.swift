// AggregationExecutionTests.swift
// Tests for AggregationResult and aggregation execution

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

@Suite("Aggregation Execution Tests")
struct AggregationExecutionTests {

    // MARK: - AggregationResult Tests

    @Test func aggregationResultCreation() {
        let result = AggregationResult(
            aggregationType: .count,
            value: Int64(100),
            groupKey: [:],
            recordCount: 100
        )

        #expect(result.intValue == 100)
        #expect(result.recordCount == 100)
        #expect(result.groupKey.isEmpty)
    }

    @Test func aggregationResultWithGroupKey() {
        let result = AggregationResult(
            aggregationType: .sum(field: "amount"),
            value: 1250.50,
            groupKey: ["region": "West", "year": 2024],
            recordCount: 50
        )

        #expect(result.groupKey.count == 2)
        #expect(result.groupKey["region"] as? String == "West")
        #expect(result.groupKey["year"] as? Int == 2024)
        #expect(result.doubleValue == 1250.50)
    }

    @Test func aggregationResultIntValue() {
        // Test Int64 direct
        let result1 = AggregationResult(
            aggregationType: .count,
            value: Int64(42)
        )
        #expect(result1.intValue == 42)

        // Test Int conversion
        let result2 = AggregationResult(
            aggregationType: .count,
            value: 42
        )
        #expect(result2.intValue == 42)

        // Test Double (should return nil for intValue)
        let result3 = AggregationResult(
            aggregationType: .avg(field: "x"),
            value: 3.14
        )
        #expect(result3.intValue == nil)
    }

    @Test func aggregationResultDoubleValue() {
        // Test Double direct
        let result1 = AggregationResult(
            aggregationType: .avg(field: "amount"),
            value: 99.5
        )
        #expect(result1.doubleValue == 99.5)

        // Test Float conversion
        let result2 = AggregationResult(
            aggregationType: .avg(field: "amount"),
            value: Float(3.14)
        )
        #expect(result2.doubleValue != nil)
        #expect(abs(result2.doubleValue! - 3.14) < 0.01)

        // Test Int conversion
        let result3 = AggregationResult(
            aggregationType: .sum(field: "count"),
            value: 100
        )
        #expect(result3.doubleValue == 100.0)
    }

    @Test func aggregationResultStringValue() {
        let result = AggregationResult(
            aggregationType: .min(field: "name"),
            value: "Aaron"
        )
        #expect(result.stringValue == "Aaron")

        let numericResult = AggregationResult(
            aggregationType: .count,
            value: 100
        )
        #expect(numericResult.stringValue == nil)
    }

    @Test func aggregationResultDescription() {
        let result = AggregationResult(
            aggregationType: .sum(field: "amount"),
            value: 1500.0,
            groupKey: ["region": "East"],
            recordCount: 25
        )

        let description = result.description
        #expect(description.contains("AggregationResult"))
        #expect(description.contains("sum"))
        #expect(description.contains("region=East"))
        #expect(description.contains("1500"))
        #expect(description.contains("count=25"))
    }

    // MARK: - AggregationType Tests

    @Test func aggregationTypeEquality() {
        #expect(AggregationType.count == AggregationType.count)
        #expect(AggregationType.sum(field: "a") == AggregationType.sum(field: "a"))
        #expect(AggregationType.sum(field: "a") != AggregationType.sum(field: "b"))
        #expect(AggregationType.min(field: "x") != AggregationType.max(field: "x"))
    }

    @Test func aggregationTypeHashable() {
        var set = Set<AggregationType>()
        set.insert(.count)
        set.insert(.sum(field: "amount"))
        set.insert(.avg(field: "price"))
        set.insert(.sum(field: "amount")) // Duplicate

        #expect(set.count == 3) // No duplicate
    }

    // MARK: - PredicateEvaluator Tests

    @Test func predicateEvaluatorTrue() {
        let evaluator = PredicateEvaluator<AggTestOrder>()
        let order = AggTestOrder(id: "1", customerId: "c1", amount: 100.0, status: "pending", region: "East")

        #expect(evaluator.evaluate(.true, on: order) == true)
    }

    @Test func predicateEvaluatorFalse() {
        let evaluator = PredicateEvaluator<AggTestOrder>()
        let order = AggTestOrder(id: "1", customerId: "c1", amount: 100.0, status: "pending", region: "East")

        #expect(evaluator.evaluate(.false, on: order) == false)
    }

    @Test func predicateEvaluatorAnd() {
        let evaluator = PredicateEvaluator<AggTestOrder>()
        let order = AggTestOrder(id: "1", customerId: "c1", amount: 100.0, status: "pending", region: "East")

        #expect(evaluator.evaluate(.and([.true, .true]), on: order) == true)
        #expect(evaluator.evaluate(.and([.true, .false]), on: order) == false)
        #expect(evaluator.evaluate(.and([.false, .true]), on: order) == false)
    }

    @Test func predicateEvaluatorOr() {
        let evaluator = PredicateEvaluator<AggTestOrder>()
        let order = AggTestOrder(id: "1", customerId: "c1", amount: 100.0, status: "pending", region: "East")

        #expect(evaluator.evaluate(.or([.true, .false]), on: order) == true)
        #expect(evaluator.evaluate(.or([.false, .true]), on: order) == true)
        #expect(evaluator.evaluate(.or([.false, .false]), on: order) == false)
    }

    @Test func predicateEvaluatorNot() {
        let evaluator = PredicateEvaluator<AggTestOrder>()
        let order = AggTestOrder(id: "1", customerId: "c1", amount: 100.0, status: "pending", region: "East")

        #expect(evaluator.evaluate(.not(.true), on: order) == false)
        #expect(evaluator.evaluate(.not(.false), on: order) == true)
    }

    // MARK: - Grouped Result Tests

    @Test func aggregationResultGroupKeyAccess() {
        let result = AggregationResult(
            aggregationType: .count,
            value: Int64(50),
            groupKey: [
                "region": "North",
                "category": "Electronics",
                "year": 2024
            ]
        )

        #expect(result.groupKey["region"] as? String == "North")
        #expect(result.groupKey["category"] as? String == "Electronics")
        #expect(result.groupKey["year"] as? Int == 2024)
        #expect(result.groupKey["nonexistent"] == nil)
    }

    // MARK: - Edge Cases

    @Test func aggregationResultWithZeroValue() {
        let result = AggregationResult(
            aggregationType: .count,
            value: Int64(0),
            recordCount: 0
        )

        #expect(result.intValue == 0)
        #expect(result.recordCount == 0)
    }

    @Test func aggregationResultWithNegativeValue() {
        let result = AggregationResult(
            aggregationType: .sum(field: "balance"),
            value: -500.0
        )

        #expect(result.doubleValue == -500.0)
    }

    @Test func aggregationResultWithLargeValue() {
        let largeValue = Int64.max
        let result = AggregationResult(
            aggregationType: .count,
            value: largeValue
        )

        #expect(result.intValue == largeValue)
    }

    @Test func aggregationResultWithDecimalPrecision() {
        let result = AggregationResult(
            aggregationType: .avg(field: "price"),
            value: 123.456789
        )

        #expect(result.doubleValue == 123.456789)
    }

    // MARK: - Multiple Aggregation Types

    @Test func allAggregationTypesSupported() {
        let types: [AggregationType] = [
            .count,
            .sum(field: "amount"),
            .avg(field: "price"),
            .min(field: "date"),
            .max(field: "quantity")
        ]

        for type in types {
            let result = AggregationResult(
                aggregationType: type,
                value: 0
            )
            #expect(result.aggregationType == type)
        }
    }
}

// MARK: - Test Model

/// Test model for aggregation tests
struct AggTestOrder: Persistable {
    typealias ID = String

    var id: String
    var customerId: String
    var amount: Double
    var status: String
    var region: String

    init(id: String = UUID().uuidString, customerId: String = "", amount: Double = 0, status: String = "", region: String = "") {
        self.id = id
        self.customerId = customerId
        self.amount = amount
        self.status = status
        self.region = region
    }

    static var persistableType: String { "AggTestOrder" }

    static var allFields: [String] { ["id", "customerId", "amount", "status", "region"] }

    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "customerId": return customerId
        case "amount": return amount
        case "status": return status
        case "region": return region
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<AggTestOrder, Value>) -> String {
        switch keyPath {
        case \AggTestOrder.id: return "id"
        case \AggTestOrder.customerId: return "customerId"
        case \AggTestOrder.amount: return "amount"
        case \AggTestOrder.status: return "status"
        case \AggTestOrder.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<AggTestOrder>) -> String {
        switch keyPath {
        case \AggTestOrder.id: return "id"
        case \AggTestOrder.customerId: return "customerId"
        case \AggTestOrder.amount: return "amount"
        case \AggTestOrder.status: return "status"
        case \AggTestOrder.region: return "region"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<AggTestOrder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
