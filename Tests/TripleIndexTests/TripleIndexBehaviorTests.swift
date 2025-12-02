// TripleIndexBehaviorTests.swift
// Integration tests for TripleIndex (SPO/POS/OSP) behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import Triple
import TestSupport
@testable import DatabaseEngine
@testable import TripleIndex

// MARK: - Test Model

struct TestStatement: Persistable {
    typealias ID = String

    var id: String
    var subject: String
    var predicate: String
    var object: String

    init(id: String = UUID().uuidString, subject: String, predicate: String, object: String) {
        self.id = id
        self.subject = subject
        self.predicate = predicate
        self.object = object
    }

    static var persistableType: String { "TestStatement" }
    static var allFields: [String] { ["id", "subject", "predicate", "object"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "subject": return subject
        case "predicate": return predicate
        case "object": return object
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestStatement, Value>) -> String {
        switch keyPath {
        case \TestStatement.id: return "id"
        case \TestStatement.subject: return "subject"
        case \TestStatement.predicate: return "predicate"
        case \TestStatement.object: return "object"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestStatement>) -> String {
        switch keyPath {
        case \TestStatement.id: return "id"
        case \TestStatement.subject: return "subject"
        case \TestStatement.predicate: return "predicate"
        case \TestStatement.object: return "object"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestStatement> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: TripleIndexMaintainer<TestStatement>
    let kind: TripleIndexKind<TestStatement>

    init(indexName: String = "TestStatement_triple") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "triple", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = TripleIndexKind<TestStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["TestStatement"])
        )

        self.maintainer = TripleIndexMaintainer<TestStatement>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            subjectField: kind.subjectField,
            predicateField: kind.predicateField,
            objectField: kind.objectField
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countSPOEntries() async throws -> Int {
        let spoSubspace = indexSubspace.subspace("spo")
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = spoSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func countPOSEntries() async throws -> Int {
        let posSubspace = indexSubspace.subspace("pos")
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = posSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func countOSPEntries() async throws -> Int {
        let ospSubspace = indexSubspace.subspace("osp")
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = ospSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func countByPredicate(_ predicate: String) async throws -> Int {
        let posSubspace = indexSubspace.subspace("pos").subspace(predicate)
        return try await database.withTransaction { transaction -> Int in
            let (begin, end) = posSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }
}

// MARK: - Test Suite

@Suite struct TripleIndexBehaviorTests {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func testTripleIndexMaintainerCreation() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        // Maintainer created successfully - configuration is stored internally
        #expect(context.kind.subjectField == "subject")
        #expect(context.kind.predicateField == "predicate")
        #expect(context.kind.objectField == "object")
    }

    @Test func testTripleIndexKeyGeneration() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        let statement = TestStatement(
            id: "stmt1",
            subject: "Engineer",
            predicate: "rdfs:subClassOf",
            object: "Employee"
        )

        let id = Tuple(["stmt1"])
        let keys = try await context.maintainer.computeIndexKeys(for: statement, id: id)

        // Should generate 3 keys (SPO, POS, OSP)
        #expect(keys.count == 3)
    }

    @Test func testTripleIndexInsert() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        // Insert statements
        let statements = [
            TestStatement(id: "s1", subject: "Engineer", predicate: "rdfs:subClassOf", object: "Employee"),
            TestStatement(id: "s2", subject: "Manager", predicate: "rdfs:subClassOf", object: "Employee"),
            TestStatement(id: "s3", subject: "Employee", predicate: "rdfs:subClassOf", object: "Person"),
        ]

        try await context.database.withTransaction { transaction in
            for statement in statements {
                try await context.maintainer.updateIndex(oldItem: nil, newItem: statement, transaction: transaction)
            }
        }

        // Verify all 3 index orderings have entries
        let spoCount = try await context.countSPOEntries()
        let posCount = try await context.countPOSEntries()
        let ospCount = try await context.countOSPEntries()

        #expect(spoCount == 3)
        #expect(posCount == 3)
        #expect(ospCount == 3)
    }

    @Test func testTripleIndexPredicateQuery() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        // Insert ontology triples
        let statements = [
            TestStatement(id: "s1", subject: "Engineer", predicate: "rdfs:subClassOf", object: "Employee"),
            TestStatement(id: "s2", subject: "Manager", predicate: "rdfs:subClassOf", object: "Employee"),
            TestStatement(id: "s3", subject: "Employee", predicate: "rdfs:subClassOf", object: "Person"),
            TestStatement(id: "s4", subject: "name", predicate: "rdfs:domain", object: "Person"),
            TestStatement(id: "s5", subject: "email", predicate: "rdfs:domain", object: "Person"),
        ]

        try await context.database.withTransaction { transaction in
            for statement in statements {
                try await context.maintainer.updateIndex(oldItem: nil, newItem: statement, transaction: transaction)
            }
        }

        // Query by predicate using POS index
        let subClassOfCount = try await context.countByPredicate("rdfs:subClassOf")
        let domainCount = try await context.countByPredicate("rdfs:domain")

        #expect(subClassOfCount == 3) // Engineer, Manager, Employee
        #expect(domainCount == 2)     // name, email
    }

    @Test func testTripleIndexDelete() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        let statement = TestStatement(id: "del1", subject: "Test", predicate: "test:pred", object: "Value")

        // Insert
        try await context.database.withTransaction { transaction in
            try await context.maintainer.updateIndex(oldItem: nil, newItem: statement, transaction: transaction)
        }

        // Verify inserted
        let spoCountBefore = try await context.countSPOEntries()
        #expect(spoCountBefore == 1)

        // Delete
        try await context.database.withTransaction { transaction in
            try await context.maintainer.updateIndex(oldItem: statement, newItem: nil, transaction: transaction)
        }

        // Verify deleted (all 3 indexes should be empty)
        let spoCountAfter = try await context.countSPOEntries()
        let posCountAfter = try await context.countPOSEntries()
        let ospCountAfter = try await context.countOSPEntries()

        #expect(spoCountAfter == 0)
        #expect(posCountAfter == 0)
        #expect(ospCountAfter == 0)
    }

    @Test func testTripleIndexUpdate() async throws {
        let context = try TestContext()
        defer { Task { try? await context.cleanup() } }

        let oldStatement = TestStatement(id: "upd1", subject: "OldSubject", predicate: "test:pred", object: "OldObject")
        let newStatement = TestStatement(id: "upd1", subject: "NewSubject", predicate: "test:pred", object: "NewObject")

        // Insert original
        try await context.database.withTransaction { transaction in
            try await context.maintainer.updateIndex(oldItem: nil, newItem: oldStatement, transaction: transaction)
        }

        // Update
        try await context.database.withTransaction { transaction in
            try await context.maintainer.updateIndex(oldItem: oldStatement, newItem: newStatement, transaction: transaction)
        }

        // Verify: should still have exactly 1 entry in each index
        let spoCount = try await context.countSPOEntries()
        let posCount = try await context.countPOSEntries()
        let ospCount = try await context.countOSPEntries()

        #expect(spoCount == 1)
        #expect(posCount == 1)
        #expect(ospCount == 1)
    }
}
