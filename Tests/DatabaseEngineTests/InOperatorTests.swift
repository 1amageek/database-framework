// InOperatorTests.swift
// Tests for IN-Union and IN-Join plan operators

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core
@testable import ScalarIndex

@Suite("IN Operator Tests")
struct InOperatorTests {

    // MARK: - Helper

    private func makeIndex(name: String) -> IndexDescriptor {
        let kind = ScalarIndexKind<InOpTestUser>(fields: [\.status])
        return IndexDescriptor(
            name: name,
            keyPaths: [\InOpTestUser.status],
            kind: kind
        )
    }

    // MARK: - InUnionOperator Tests

    @Test func inUnionOperatorCreation() {
        let index = makeIndex(name: "idx_status")

        // Type-safe values using String
        let values: [String] = ["active", "pending", "verified"]

        let op = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "status",
            values: values,
            additionalFilter: nil,
            estimatedResultsPerValue: 100,
            deduplicate: true
        )

        #expect(op.index.name == "idx_status")
        #expect(op.fieldPath == "status")
        #expect(op.values.count == 3)
        #expect(op.estimatedResultsPerValue == 100)
        #expect(op.deduplicate == true)
        #expect(op.estimatedTotalResults == 300)
    }

    @Test func inUnionOperatorEstimation() {
        let index = makeIndex(name: "idx_category")
        // Type-safe values using Int
        let values: [Int] = Array(0..<10)

        let op = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "category",
            values: values,
            estimatedResultsPerValue: 50
        )

        #expect(op.estimatedTotalResults == 500) // 10 * 50
    }

    @Test func inUnionOperatorWithFilter() {
        let index = makeIndex(name: "idx_status")
        let values: [String] = ["active"]

        // Create a filter predicate
        let filter: DatabaseEngine.Predicate<InOpTestUser> = .true

        let op = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "status",
            values: values,
            additionalFilter: filter
        )

        #expect(op.additionalFilter != nil)
    }

    // MARK: - InJoinOperator Tests

    @Test func inJoinOperatorCreation() {
        let index = makeIndex(name: "idx_customer_id")
        let values: [String] = (0..<100).map { "customer_\($0)" }

        let op = InJoinOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "customerId",
            values: values,
            batchSize: 50,
            additionalFilter: nil,
            estimatedSelectivity: 0.01
        )

        #expect(op.index.name == "idx_customer_id")
        #expect(op.fieldPath == "customerId")
        #expect(op.values.count == 100)
        #expect(op.batchSize == 50)
        #expect(op.estimatedSelectivity == 0.01)
    }

    @Test func inJoinOperatorDefaults() {
        let index = makeIndex(name: "idx_region")
        let values: [String] = ["US", "EU"]

        let op = InJoinOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "region",
            values: values
        )

        #expect(op.batchSize == 100) // Default batch size
        #expect(op.estimatedSelectivity > 0)
    }

    // MARK: - PlanOperator Integration Tests

    @Test func inUnionPlanOperatorDescription() {
        let index = makeIndex(name: "idx_status")
        let values: [String] = ["A", "B"]

        let inUnionOp = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "status",
            values: values
        )
        let planOp: PlanOperator<InOpTestUser> = .inUnion(inUnionOp)

        let description = planOp.description
        #expect(description.contains("InUnion"))
        #expect(description.contains("idx_status"))
    }

    @Test func inJoinPlanOperatorDescription() {
        let index = makeIndex(name: "idx_region")
        let values: [Int] = Array(0..<50)

        let inJoinOp = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "region",
            values: values
        )
        let planOp: PlanOperator<InOpTestUser> = .inJoin(inJoinOp)

        let description = planOp.description
        #expect(description.contains("InJoin"))
        #expect(description.contains("idx_region"))
    }

    // MARK: - InPredicateOptimizer Configuration Tests

    @Test func inPredicateOptimizerDefaultConfiguration() {
        let config = InPredicateOptimizer<InOpTestUser>.Configuration.default

        #expect(config.unionThreshold == 10)
        #expect(config.joinThreshold == 1000)
        #expect(config.minSelectivityImprovement == 0.1)
    }

    @Test func inPredicateOptimizerCustomConfiguration() {
        let config = InPredicateOptimizer<InOpTestUser>.Configuration(
            unionThreshold: 5,
            joinThreshold: 500,
            minSelectivityImprovement: 0.2
        )

        #expect(config.unionThreshold == 5)
        #expect(config.joinThreshold == 500)
        #expect(config.minSelectivityImprovement == 0.2)
    }

    // MARK: - Operator Behavior Tests

    @Test func inUnionSmallValueList() {
        let index = makeIndex(name: "idx_status")

        // Small list should use union strategy
        let values: [Int] = Array(0..<5)

        let op = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "status",
            values: values,
            estimatedResultsPerValue: 10
        )

        #expect(op.values.count == 5)
        #expect(op.estimatedTotalResults == 50)
    }

    @Test func inUnionMediumValueList() {
        let index = makeIndex(name: "idx_category")

        // Medium list
        let values: [Int] = Array(0..<15)

        let op = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "category",
            values: values,
            estimatedResultsPerValue: 100
        )

        #expect(op.estimatedTotalResults == 1500)
    }

    @Test func inJoinLargeValueList() {
        let index = makeIndex(name: "idx_customer_id")

        // Large list should use join strategy
        let values: [Int] = Array(0..<500)

        let op = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "customerId",
            values: values,
            estimatedSelectivity: 0.001
        )

        #expect(op.values.count == 500)
    }

    @Test func inUnionDeduplication() {
        let index = makeIndex(name: "idx_tags")
        let values: [String] = ["a", "b", "c"]

        // With deduplication
        let opWithDedup = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "tags",
            values: values,
            deduplicate: true
        )
        #expect(opWithDedup.deduplicate == true)

        // Without deduplication
        let opNoDedup = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "tags",
            values: values,
            deduplicate: false
        )
        #expect(opNoDedup.deduplicate == false)
    }

    @Test func inJoinBatchSizeConfiguration() {
        let index = makeIndex(name: "idx_id")
        let values: [Int] = Array(0..<200)

        // Small batch
        let opSmallBatch = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "id",
            values: values,
            batchSize: 10
        )
        #expect(opSmallBatch.batchSize == 10)

        // Large batch
        let opLargeBatch = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "id",
            values: values,
            batchSize: 500
        )
        #expect(opLargeBatch.batchSize == 500)
    }

    @Test func inUnionEstimatedResults() {
        let index = makeIndex(name: "idx_field")

        // Edge case: empty values
        let emptyOp = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "field",
            values: [],
            estimatedResultsPerValue: 100
        )
        #expect(emptyOp.estimatedTotalResults == 0)

        // Single value
        let singleOp = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "field",
            values: [1],
            estimatedResultsPerValue: 100
        )
        #expect(singleOp.estimatedTotalResults == 100)
    }

    @Test func inJoinSelectivityCalculation() {
        let index = makeIndex(name: "idx_region")
        let values: [Int] = Array(0..<1000)

        // Very selective (few matches)
        let selectiveOp = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "region",
            values: values,
            estimatedSelectivity: 0.001
        )
        #expect(selectiveOp.estimatedSelectivity == 0.001)
        #expect(selectiveOp.estimatedResults(indexSize: 1_000_000) == 1000)
    }

    // MARK: - InOperatorExecutable Protocol Tests

    @Test func inOperatorExecutableProtocolConformance() {
        let index = makeIndex(name: "idx_status")
        let values: [String] = ["active", "pending"]

        let op = InUnionOperator<InOpTestUser, String>(
            index: index,
            fieldPath: "status",
            values: values
        )

        // Test protocol methods
        #expect(op.valueCount == 2)
        #expect(op.containsValue("active") == true)
        #expect(op.containsValue("unknown") == false)
        #expect(op.containsValue(123) == false) // Wrong type

        // Test valuesAsTupleElements
        let elements = op.valuesAsTupleElements()
        #expect(elements.count == 2)

        // Test valueRange
        let range = op.valueRange()
        #expect(range != nil)
    }

    @Test func inJoinBloomFilterCreation() {
        let index = makeIndex(name: "idx_large")
        // Create operator with > 50 values to trigger Bloom filter creation
        let values: [Int] = Array(0..<100)

        let op = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "largeField",
            values: values,
            useBloomFilter: true
        )

        #expect(op.bloomFilter != nil)

        // Test Bloom filter membership
        #expect(op.containsValue(50) == true)
        #expect(op.containsValue(99) == true)
        // Note: Bloom filter may have false positives but no false negatives
    }

    @Test func inJoinWithoutBloomFilter() {
        let index = makeIndex(name: "idx_small")
        // Create operator with < 50 values (no Bloom filter)
        let values: [Int] = Array(0..<30)

        let op = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "smallField",
            values: values,
            useBloomFilter: true
        )

        #expect(op.bloomFilter == nil) // Too few values

        // containsValue still works via Set lookup
        #expect(op.containsValue(15) == true)
        #expect(op.containsValue(100) == false)
    }

    // MARK: - InJoinStrategySelector Tests

    @Test func strategySelectionForSmallValueSet() {
        let index = makeIndex(name: "idx_test")
        let values: [Int] = Array(0..<5)

        let op = InUnionOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "field",
            values: values
        )

        let selector = InJoinStrategySelector()
        let strategy = selector.selectStrategy(for: op, estimatedIndexSize: 10000)

        // Small value count should suggest converting to union
        #expect(strategy == .convertToUnion)
    }

    @Test func strategySelectionForLargeValueSet() {
        let index = makeIndex(name: "idx_test")
        let values: [Int] = Array(0..<1000)

        let op = InJoinOperator<InOpTestUser, Int>(
            index: index,
            fieldPath: "field",
            values: values
        )

        let selector = InJoinStrategySelector()
        let strategy = selector.selectStrategy(for: op, estimatedIndexSize: 100000)

        // Large value count with range should use bounded scan or full scan
        #expect(strategy == .boundedRangeScan || strategy == .fullScan)
    }
}

// MARK: - Test Model

/// Test model for IN operator tests
struct InOpTestUser: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var status: String
    var age: Int
    var country: String

    init(id: String = UUID().uuidString, name: String = "", status: String = "", age: Int = 0, country: String = "") {
        self.id = id
        self.name = name
        self.status = status
        self.age = age
        self.country = country
    }

    static var persistableType: String { "InOpTestUser" }

    static var allFields: [String] { ["id", "name", "status", "age", "country"] }

    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "status": return status
        case "age": return age
        case "country": return country
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<InOpTestUser, Value>) -> String {
        switch keyPath {
        case \InOpTestUser.id: return "id"
        case \InOpTestUser.name: return "name"
        case \InOpTestUser.status: return "status"
        case \InOpTestUser.age: return "age"
        case \InOpTestUser.country: return "country"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<InOpTestUser>) -> String {
        switch keyPath {
        case \InOpTestUser.id: return "id"
        case \InOpTestUser.name: return "name"
        case \InOpTestUser.status: return "status"
        case \InOpTestUser.age: return "age"
        case \InOpTestUser.country: return "country"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<InOpTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
