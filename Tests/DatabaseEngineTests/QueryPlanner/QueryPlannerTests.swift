// QueryPlannerTests.swift
// Tests for QueryPlanner core functionality

import Testing
import Foundation
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// MARK: - Test Model for QueryPlanner

/// User model with various field types for comprehensive query testing
struct QPTestUser: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var email: String
    var age: Int
    var score: Double
    var isActive: Bool
    var department: String?
    var createdAt: Date

    init(
        id: String = UUID().uuidString,
        name: String,
        email: String,
        age: Int,
        score: Double = 0.0,
        isActive: Bool = true,
        department: String? = nil,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.name = name
        self.email = email
        self.age = age
        self.score = score
        self.isActive = isActive
        self.department = department
        self.createdAt = createdAt
    }

    static var persistableType: String { "QPTestUser" }

    static var allFields: [String] {
        ["id", "name", "email", "age", "score", "isActive", "department", "createdAt"]
    }

    static var descriptors: [any Descriptor] {
        return [
            IndexDescriptor(name: "idx_email", keyPaths: [\QPTestUser.email], kind: ScalarIndexKind<QPTestUser>(fields: [\.email])),
            IndexDescriptor(name: "idx_age", keyPaths: [\QPTestUser.age], kind: ScalarIndexKind<QPTestUser>(fields: [\.age])),
            IndexDescriptor(name: "idx_name_age", keyPaths: [\QPTestUser.name, \QPTestUser.age], kind: ScalarIndexKind<QPTestUser>(fields: [\.name, \.age])),
            IndexDescriptor(name: "idx_department", keyPaths: [\QPTestUser.department], kind: ScalarIndexKind<QPTestUser>(fields: [\.department])),
            IndexDescriptor(name: "idx_isActive", keyPaths: [\QPTestUser.isActive], kind: ScalarIndexKind<QPTestUser>(fields: [\.isActive]))
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "email": return email
        case "age": return age
        case "score": return score
        case "isActive": return isActive
        case "department": return department
        case "createdAt": return createdAt
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<QPTestUser, Value>) -> String {
        switch keyPath {
        case \QPTestUser.id: return "id"
        case \QPTestUser.name: return "name"
        case \QPTestUser.email: return "email"
        case \QPTestUser.age: return "age"
        case \QPTestUser.score: return "score"
        case \QPTestUser.isActive: return "isActive"
        case \QPTestUser.department: return "department"
        case \QPTestUser.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<QPTestUser>) -> String {
        switch keyPath {
        case \QPTestUser.id: return "id"
        case \QPTestUser.name: return "name"
        case \QPTestUser.email: return "email"
        case \QPTestUser.age: return "age"
        case \QPTestUser.score: return "score"
        case \QPTestUser.isActive: return "isActive"
        case \QPTestUser.department: return "department"
        case \QPTestUser.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<QPTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - QueryPlanner Tests

@Suite("QueryPlanner Tests")
struct QueryPlannerTests {

    @Test("Plan simple equality query uses index")
    func testSimpleEqualityUsesIndex() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.contains { $0.name == "idx_email" })
    }

    @Test("Plan range query uses index scan")
    func testRangeQueryUsesIndexScan() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.contains { $0.name == "idx_age" })
    }

    @Test("Plan query without matching index falls back to table scan")
    func testNoMatchingIndexUsesTableScan() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: [])

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.isEmpty)
    }

    @Test("Force table scan hint")
    func testForceTableScanHint() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "test@example.com")

        let hints = QueryHints(forceTableScan: true)
        let plan = try planner.plan(query: query, hints: hints)

        #expect(plan.usedIndexes.isEmpty)
    }

    @Test("Explain produces human-readable output")
    func testExplainOutput() throws {
        let planner = QueryPlanner<QPTestUser>(indexes: QPTestUser.indexDescriptors)

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)
        query = query.orderBy(\QPTestUser.age)
        query = query.limit(10)

        let explanation = try planner.explain(query: query)

        #expect(explanation.description.contains("Cost"))
        #expect(!explanation.description.isEmpty)
    }
}

// MARK: - PredicateNormalizer Tests

@Suite("PredicateNormalizer Tests")
struct PredicateNormalizerTests {

    @Test("Combine predicates with AND")
    func testCombinePredicates() {
        let normalizer = PredicateNormalizer<QPTestUser>()

        let predicates: [DatabaseEngine.Predicate<QPTestUser>] = [
            \QPTestUser.age > 18,
            \QPTestUser.isActive == true
        ]

        let combined = normalizer.combinePredicates(predicates)

        if case .and(let terms) = combined {
            #expect(terms.count == 2)
        }
    }
}

// MARK: - QueryAnalyzer Tests

@Suite("QueryAnalyzer Tests")
struct QueryAnalyzerTests {

    @Test("Analyze extracts field conditions")
    func testAnalyzeExtractsFieldConditions() throws {
        let analyzer = QueryAnalyzer<QPTestUser>()

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.age > 18)
        query = query.where(\QPTestUser.isActive == true)

        let analysis = try analyzer.analyze(query)

        #expect(analysis.fieldConditions.count == 2)
        #expect(analysis.referencedFields.contains("age"))
        #expect(analysis.referencedFields.contains("isActive"))
    }

    @Test("Analyze extracts sort requirements")
    func testAnalyzeExtractsSortRequirements() throws {
        let analyzer = QueryAnalyzer<QPTestUser>()

        var query = Query<QPTestUser>()
        query = query.orderBy(\QPTestUser.name)
        query = query.orderBy(\QPTestUser.age, .descending)

        let analysis = try analyzer.analyze(query)

        #expect(analysis.sortRequirements.count == 2)
        #expect(analysis.sortRequirements[0].fieldName == "name")
        #expect(analysis.sortRequirements[1].fieldName == "age")
        #expect(analysis.sortRequirements[1].order == .descending)
    }
}
