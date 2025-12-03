// InPredicateOptimizerTests.swift
// DatabaseEngine Tests - InPredicateOptimizer tests

import Testing
import Foundation
import Core
import FoundationDB
@testable import DatabaseEngine

// MARK: - Test Model

@Persistable
struct InOptTestModel {
    var id: String = UUID().uuidString
    var name: String
    var email: String
    var age: Int64
    var status: String
}

// MARK: - Configuration Tests

@Suite("InPredicateOptimizer Configuration Tests")
struct InPredicateOptimizerConfigurationTests {

    @Test("Default configuration values")
    func defaultConfiguration() {
        let config = InPredicateOptimizer<InOptTestModel>.Configuration.default

        #expect(config.unionThreshold == 10)
        #expect(config.joinThreshold == 1000)
        #expect(config.minSelectivityImprovement == 0.1)
    }

    @Test("Custom configuration")
    func customConfiguration() {
        let config = InPredicateOptimizer<InOptTestModel>.Configuration(
            unionThreshold: 5,
            joinThreshold: 500,
            minSelectivityImprovement: 0.2
        )

        #expect(config.unionThreshold == 5)
        #expect(config.joinThreshold == 500)
        #expect(config.minSelectivityImprovement == 0.2)
    }
}

// MARK: - QueryCondition Extension Tests

@Suite("QueryCondition IN Predicate Tests")
struct QueryConditionInPredicateTests {

    @Test("containsInPredicate returns false for simple equals")
    func containsInPredicateFalseForEquals() {
        let field = FieldReference<InOptTestModel>(\InOptTestModel.name)
        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.equals(field: field, value: "Alice")
        )

        #expect(condition.containsInPredicate == false)
        #expect(condition.inPredicateCount == 0)
    }

    @Test("containsInPredicate returns true for IN constraint")
    func containsInPredicateTrueForIn() {
        let field = FieldReference<InOptTestModel>(\InOptTestModel.status)
        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: field, values: ["active", "pending"])
        )

        #expect(condition.containsInPredicate == true)
        #expect(condition.inPredicateCount == 1)
    }

    @Test("inPredicateCount counts nested IN predicates")
    func inPredicateCountNested() {
        let statusField = FieldReference<InOptTestModel>(\InOptTestModel.status)
        let nameField = FieldReference<InOptTestModel>(\InOptTestModel.name)
        let emailField = FieldReference<InOptTestModel>(\InOptTestModel.email)

        let inCondition1: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: statusField, values: ["a", "b"])
        )

        let inCondition2: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: nameField, values: ["x", "y", "z"])
        )

        let equalsCondition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.equals(field: emailField, value: "test@example.com")
        )

        let conjunction: QueryCondition<InOptTestModel> = .conjunction([
            inCondition1,
            inCondition2,
            equalsCondition
        ])

        #expect(conjunction.containsInPredicate == true)
        #expect(conjunction.inPredicateCount == 2)
    }

    @Test("alwaysTrue has no IN predicates")
    func alwaysTrueNoInPredicates() {
        let condition: QueryCondition<InOptTestModel> = .alwaysTrue

        #expect(condition.containsInPredicate == false)
        #expect(condition.inPredicateCount == 0)
    }

    @Test("alwaysFalse has no IN predicates")
    func alwaysFalseNoInPredicates() {
        let condition: QueryCondition<InOptTestModel> = .alwaysFalse

        #expect(condition.containsInPredicate == false)
        #expect(condition.inPredicateCount == 0)
    }
}

// MARK: - Extract IN Predicates Tests

@Suite("InPredicateOptimizer Extract Tests")
struct InPredicateOptimizerExtractTests {

    @Test("Extract IN predicate from simple condition")
    func extractSimpleInPredicate() {
        let optimizer = InPredicateOptimizer<InOptTestModel>()
        let field = FieldReference<InOptTestModel>(\InOptTestModel.status)

        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: field, values: ["active", "pending"])
        )

        let predicates = optimizer.extractInPredicates(from: condition)

        #expect(predicates.count == 1)
        #expect(predicates[0].fieldPath == "status")
        #expect(predicates[0].values.count == 2)
    }

    @Test("Extract no predicates from equals condition")
    func extractNoPredicatesFromEquals() {
        let optimizer = InPredicateOptimizer<InOptTestModel>()
        let field = FieldReference<InOptTestModel>(\InOptTestModel.name)

        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.equals(field: field, value: "Alice")
        )

        let predicates = optimizer.extractInPredicates(from: condition)

        #expect(predicates.isEmpty)
    }

    @Test("Extract multiple IN predicates from conjunction")
    func extractMultipleInPredicates() {
        let optimizer = InPredicateOptimizer<InOptTestModel>()
        let statusField = FieldReference<InOptTestModel>(\InOptTestModel.status)
        let nameField = FieldReference<InOptTestModel>(\InOptTestModel.name)
        let emailField = FieldReference<InOptTestModel>(\InOptTestModel.email)

        let condition: QueryCondition<InOptTestModel> = .conjunction([
            .field(ScalarFieldCondition.in(field: statusField, values: ["a", "b"])),
            .field(ScalarFieldCondition.in(field: nameField, values: ["x", "y"])),
            .field(ScalarFieldCondition.equals(field: emailField, value: "test@example.com"))
        ])

        let predicates = optimizer.extractInPredicates(from: condition)

        #expect(predicates.count == 2)
    }
}

// MARK: - Optimization Strategy Tests

@Suite("InPredicateOptimizer Strategy Tests")
struct InPredicateOptimizerStrategyTests {

    @Test("No optimization for condition without IN")
    func noOptimizationWithoutIn() {
        let optimizer = InPredicateOptimizer<InOptTestModel>()
        let field = FieldReference<InOptTestModel>(\InOptTestModel.name)

        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.equals(field: field, value: "Alice")
        )

        let (_, strategy) = optimizer.optimize(
            condition: condition,
            availableIndexes: []
        )

        if case .noOptimization = strategy {
            // Expected
        } else {
            Issue.record("Expected noOptimization strategy")
        }
    }

    @Test("Index union strategy with index and small value count")
    func indexUnionStrategy() {
        let optimizer = InPredicateOptimizer<InOptTestModel>(
            configuration: .init(unionThreshold: 10)
        )
        let field = FieldReference<InOptTestModel>(\InOptTestModel.status)

        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: field, values: ["a", "b", "c"])
        )

        // Create index on status field
        let index = IndexDescriptor(
            name: "status_idx",
            keyPaths: [\InOptTestModel.status],
            kind: ScalarIndexKind<InOptTestModel>(fields: [\InOptTestModel.status])
        )

        let (_, strategy) = optimizer.optimize(
            condition: condition,
            availableIndexes: [index]
        )

        if case .indexUnion(let field, let values) = strategy {
            #expect(field == "status")
            #expect(values.count == 3)
        } else {
            Issue.record("Expected indexUnion strategy, got \(strategy)")
        }
    }

    @Test("In-join strategy for larger value count without index")
    func inJoinStrategy() {
        let optimizer = InPredicateOptimizer<InOptTestModel>(
            configuration: .init(unionThreshold: 5, joinThreshold: 100)
        )
        let field = FieldReference<InOptTestModel>(\InOptTestModel.status)

        // Create 20 values (> unionThreshold, <= joinThreshold)
        let values: [any TupleElement] = (0..<20).map { "value\($0)" }

        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: field, values: values)
        )

        let (_, strategy) = optimizer.optimize(
            condition: condition,
            availableIndexes: []
        )

        if case .inJoin(let field, let vals) = strategy {
            #expect(field == "status")
            #expect(vals.count == 20)
        } else {
            Issue.record("Expected inJoin strategy, got \(strategy)")
        }
    }

    @Test("OR expansion strategy for small value count without index")
    func orExpansionStrategy() {
        let optimizer = InPredicateOptimizer<InOptTestModel>(
            configuration: .init(unionThreshold: 10, joinThreshold: 3)
        )
        let field = FieldReference<InOptTestModel>(\InOptTestModel.status)

        // 4 values: > joinThreshold (3), <= 5 for OR expansion
        let condition: QueryCondition<InOptTestModel> = .field(
            ScalarFieldCondition.in(field: field, values: ["a", "b", "c", "d"])
        )

        let (_, strategy) = optimizer.optimize(
            condition: condition,
            availableIndexes: []
        )

        if case .orExpansion = strategy {
            // Expected
        } else {
            Issue.record("Expected orExpansion strategy, got \(strategy)")
        }
    }
}
