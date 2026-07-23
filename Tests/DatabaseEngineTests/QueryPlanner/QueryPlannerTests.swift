#if !os(WASI)
#if FOUNDATION_DB
// QueryPlannerTests.swift
// Tests for QueryPlanner core functionality

import Testing
import TestHeartbeat
import Foundation
@testable import DatabaseEngine
import DatabaseValue
@testable import ScalarIndex
@testable import Core

// MARK: - Test Model for QueryPlanner

/// User model with various field types for comprehensive query testing
struct QueryPlannerUser: Persistable {
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

    static var persistableType: String { "QueryPlannerUser" }

    static var allFields: [String] {
        ["id", "name", "email", "age", "score", "isActive", "department", "createdAt"]
    }

    static var descriptors: [any Descriptor] {
        return [
            IndexDescriptor(name: "idx_email", keyPaths: [\QueryPlannerUser.email], kind: ScalarIndexKind<QueryPlannerUser>(fields: [\.email])),
            IndexDescriptor(name: "idx_age", keyPaths: [\QueryPlannerUser.age], kind: ScalarIndexKind<QueryPlannerUser>(fields: [\.age])),
            IndexDescriptor(name: "idx_name_age", keyPaths: [\QueryPlannerUser.name, \QueryPlannerUser.age], kind: ScalarIndexKind<QueryPlannerUser>(fields: [\.name, \.age])),
            IndexDescriptor(name: "idx_department", keyPaths: [\QueryPlannerUser.department], kind: ScalarIndexKind<QueryPlannerUser>(fields: [\.department])),
            IndexDescriptor(name: "idx_isActive", keyPaths: [\QueryPlannerUser.isActive], kind: ScalarIndexKind<QueryPlannerUser>(fields: [\.isActive]))
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

    static func fieldName<Value>(for keyPath: KeyPath<QueryPlannerUser, Value>) -> String {
        switch keyPath {
        case \QueryPlannerUser.id: return "id"
        case \QueryPlannerUser.name: return "name"
        case \QueryPlannerUser.email: return "email"
        case \QueryPlannerUser.age: return "age"
        case \QueryPlannerUser.score: return "score"
        case \QueryPlannerUser.isActive: return "isActive"
        case \QueryPlannerUser.department: return "department"
        case \QueryPlannerUser.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<QueryPlannerUser>) -> String {
        switch keyPath {
        case \QueryPlannerUser.id: return "id"
        case \QueryPlannerUser.name: return "name"
        case \QueryPlannerUser.email: return "email"
        case \QueryPlannerUser.age: return "age"
        case \QueryPlannerUser.score: return "score"
        case \QueryPlannerUser.isActive: return "isActive"
        case \QueryPlannerUser.department: return "department"
        case \QueryPlannerUser.createdAt: return "createdAt"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<QueryPlannerUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - QueryPlanner Tests

@Suite("QueryPlanner Tests", .heartbeat)
struct QueryPlannerTests {

    @Test("Plan simple equality query uses index")
    func testSimpleEqualityUsesIndex() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.contains { $0.name == "idx_email" })
    }

    @Test("Plan range query uses index scan")
    func testRangeQueryUsesIndexScan() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.contains { $0.name == "idx_age" })
    }

    @Test("Plan query without matching index falls back to table scan")
    func testNoMatchingIndexUsesTableScan() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: [])

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")

        let plan = try planner.plan(query: query)

        #expect(plan.usedIndexes.isEmpty)
    }

    @Test("Force table scan hint")
    func testForceTableScanHint() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.email == "test@example.com")

        let hints = QueryHints(forceTableScan: true)
        let plan = try planner.plan(query: query, hints: hints)

        #expect(plan.usedIndexes.isEmpty)
    }

    @Test("Explain produces human-readable output")
    func testExplainOutput() throws {
        let planner = QueryPlanner<QueryPlannerUser>(indexes: QueryPlannerUser.indexDescriptors)

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)
        query = query.orderBy(\QueryPlannerUser.age)
        query = query.limit(10)

        let explanation = try planner.explain(query: query)

        #expect(explanation.description.contains("Cost"))
        #expect(!explanation.description.isEmpty)
    }
}

// MARK: - PredicateNormalizer Tests

@Suite("PredicateNormalizer Tests", .heartbeat)
struct PredicateNormalizerTests {

    @Test("Combine predicates with AND")
    func testCombinePredicates() {
        let normalizer = PredicateNormalizer<QueryPlannerUser>()

        let predicates: [DatabaseEngine.Predicate<QueryPlannerUser>] = [
            \QueryPlannerUser.age > 18,
            \QueryPlannerUser.isActive == true
        ]

        let combined = normalizer.combinePredicates(predicates)

        if case .and(let terms) = combined {
            #expect(terms.count == 2)
        }
    }
}

// MARK: - QueryAnalyzer Tests

@Suite("QueryAnalyzer Tests", .heartbeat)
struct QueryAnalyzerTests {

    @Test("Analyze extracts field conditions")
    func testAnalyzeExtractsFieldConditions() throws {
        let analyzer = QueryAnalyzer<QueryPlannerUser>()

        var query = Query<QueryPlannerUser>()
        query = query.where(\QueryPlannerUser.age > 18)
        query = query.where(\QueryPlannerUser.isActive == true)

        let analysis = try analyzer.analyze(query)

        #expect(analysis.fieldConditions.count == 2)
        #expect(analysis.referencedFields.contains("age"))
        #expect(analysis.referencedFields.contains("isActive"))
    }

    @Test("Analyze extracts sort requirements")
    func testAnalyzeExtractsSortRequirements() throws {
        let analyzer = QueryAnalyzer<QueryPlannerUser>()

        var query = Query<QueryPlannerUser>()
        query = query.orderBy(\QueryPlannerUser.name)
        query = query.orderBy(\QueryPlannerUser.age, .descending)

        let analysis = try analyzer.analyze(query)

        #expect(analysis.sortRequirements.count == 2)
        #expect(analysis.sortRequirements[0].fieldName == "name")
        #expect(analysis.sortRequirements[1].fieldName == "age")
        #expect(analysis.sortRequirements[1].order == .descending)
    }
}
#endif

#endif
