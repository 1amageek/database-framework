#if FOUNDATION_DB
/// SPARQLPropertyFilterEdgeCaseTests.swift
/// Edge case and complex scenario tests for SPARQL property filtering

import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseRuntime
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
fileprivate struct EdgeCaseConnection {
    #Directory<EdgeCaseConnection>("sparql_property_filter_edge_case_tests")
    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var target: RDFTerm = .iri(.xsdString)
    var relation: RDFTerm = .iri(.xsdString)
    var score: Int64 = 0
    var note: String = ""

    #Index(
        .rdfDataset,
        from: \EdgeCaseConnection.from,
        edge: \EdgeCaseConnection.relation,
        to: \EdgeCaseConnection.target,
        storedFields: [
            \EdgeCaseConnection.score,
            \EdgeCaseConnection.note,
        ],
        name: "edge_case_graph"
    )
}

@Suite("SPARQL Property Filter Edge Case Tests", .serialized, .foundationDBScenario, .heartbeat)
struct SPARQLPropertyFilterEdgeCaseTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeConnection(
        from: String,
        target: String,
        relation: String,
        score: Int64,
        note: String
    ) throws -> EdgeCaseConnection {
        var connection = EdgeCaseConnection()
        connection.from = try Self.resource(from)
        connection.target = try Self.resource(target)
        connection.relation = try Self.predicate(relation)
        connection.score = score
        connection.note = note
        return connection
    }

    private static func resource(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "did:database-framework:test-resource:\(identifier)"
        )
    }

    private static func predicate(_ identifier: String) throws -> RDFTerm {
        try .iri(
            validating: "did:database-framework:test-predicate:\(identifier)"
        )
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try EdgeCaseConnection.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [EdgeCaseConnection.self]),
            security: .disabled,
        )

        if try await database.namespaceExists(path: ["sparql_property_filter_edge_case_tests"]) {
            try await database.removeNamespace(path: ["sparql_property_filter_edge_case_tests"])
        }
        try await container.ensureIndexesReady()

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: EdgeCaseConnection.self)
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)

        for descriptor in try EdgeCaseConnection.indexDescriptors {
            let currentState = try await indexLifecycleStore.state(of: descriptor.name)
            if currentState == .disabled {
                try await indexLifecycleStore.enable(descriptor.name)
                try await indexLifecycleStore.makeReadable(descriptor.name)
            } else if currentState == .writeOnly {
                try await indexLifecycleStore.makeReadable(descriptor.name)
            }
        }

        return container
    }

    // MARK: - Complex Filter Tests (post-scan evaluation)

    @Test("Complex filter: OR expression (post-scan)")
    func testOrExpressionPostScan() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: "friend"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 50, note: "colleague"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 90, note: "family"))
        try await context.save()

        // OR: score < 20 OR score > 80 (both sides are post-scan)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .or(
                .lessThan("?score", .int64(20)),
                .greaterThan("?score", .int64(80))
            )
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 2)  // Bob (10) and Dave (90)
        let scores = result.bindings.compactMap { $0.int64("?score") }.sorted()
        #expect(scores == [10, 90])
    }

    @Test("Complex filter: NOT expression (post-scan)")
    func testNotExpressionPostScan() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 50, note: "active"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 60, note: "inactive"))
        try await context.save()

        // NOT(note = "inactive")
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .not(.equals("?note", .string("inactive")))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        #expect(binding["?note"] == .string("active"))
    }

    @Test("Complex filter: regex (post-scan)")
    func testRegexPostScan() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: "active-premium"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 20, note: "inactive"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 30, note: "active"))
        try await context.save()

        // note =~ /^active/
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .regex("?note", "^active")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 2)  // "active-premium" and "active"
    }

    // MARK: - Property Variable Projection Tests

    @Test("Explicit SELECT of property variable only")
    func testSelectPropertyVariableOnly() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 100, note: "test"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 200, note: "test"))
        try await context.save()

        // SELECT ?score WHERE { ... } FILTER(?score > 150)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .greaterThan("?score", .int64(150))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self,
            projection: ["?score"]  // Property variable only
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        #expect(binding["?score"] == .int64(200))
        #expect(binding["?target"] == nil)  // Structure variable excluded
        #expect(binding["?note"] == nil)    // Other property excluded
    }

    @Test("SELECT mix of structure and property variables")
    func testSelectMixedVariables() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        try context.insert(try makeConnection(from: alice, target: bob, relation: "knows", score: 100, note: "friend"))
        try await context.save()

        // SELECT ?target ?score WHERE { ... }
        let result = try await context.executeSPARQLPattern(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            on: EdgeCaseConnection.self,
            projection: ["?target", "?score"]
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        let expectedTarget = FieldValue.rdfTerm(try Self.resource(bob))
        #expect(binding["?target"] == expectedTarget)
        #expect(binding["?score"] == .int64(100))
        #expect(binding["?note"] == nil)  // Not in projection
    }

    // MARK: - Comparison Operator Coverage

    @Test("Comparison operators: lessThanOrEqual")
    func testLessThanOrEqual() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        for score: Int64 in [10, 20, 30, 40, 50] {
            try context.insert(try makeConnection(from: alice, target: uniqueID("user\(score)"), relation: "knows", score: score, note: ""))
        }
        try await context.save()

        // score <= 30
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .lessThanOrEqual("?score", .int64(30))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 3)  // 10, 20, 30
        let scores = result.bindings.compactMap { $0.int64("?score") }.sorted()
        #expect(scores == [10, 20, 30])
    }

    @Test("Comparison operators: greaterThan")
    func testGreaterThan() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        for score: Int64 in [10, 20, 30, 40, 50] {
            try context.insert(try makeConnection(from: alice, target: uniqueID("user\(score)"), relation: "knows", score: score, note: ""))
        }
        try await context.save()

        // score > 30
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .greaterThan("?score", .int64(30))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 2)  // 40, 50
        let scores = result.bindings.compactMap { $0.int64("?score") }.sorted()
        #expect(scores == [40, 50])
    }

    @Test("Comparison operators: notEquals")
    func testNotEquals() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 50, note: "test"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 100, note: "test"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 50, note: "test"))
        try await context.save()

        // score != 50
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .notEquals("?score", .int64(50))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 1)
        let binding = try #require(result.bindings.first)
        #expect(binding["?score"] == .int64(100))
    }

    @Test("String operators: hasPrefix")
    func testHasPrefix() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 0, note: "active-user"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 0, note: "inactive-user"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 0, note: "active-admin"))
        try await context.save()

        // note STARTS WITH "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .startsWith("?note", "active")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 2)
    }

    @Test("String operators: hasSuffix")
    func testHasSuffix() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 0, note: "user-active"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 0, note: "user-disabled"))
        try context.insert(try makeConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 0, note: "admin-active"))
        try await context.save()

        // note ENDS WITH "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .endsWith("?note", "active")
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 2)  // "user-active" and "admin-active"
    }

    // MARK: - Empty Result Test

    @Test("Empty result: filter excludes all entities")
    func testEmptyResult() async throws {
        let container = try await setupContainer()
        let context = DatabaseContext(container: container)

        let alice = uniqueID("alice")

        try context.insert(try makeConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: ""))
        try context.insert(try makeConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 20, note: ""))
        try await context.save()

        // score > 100 (no matches)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.rdfTerm(try Self.resource(alice))),
                    predicate: .value(.rdfTerm(try Self.predicate("knows"))),
                    object: .variable("?target")
                )
            ]),
            .greaterThan("?score", .int64(100))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.isEmpty)
    }
}
#endif
