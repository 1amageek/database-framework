/// SPARQLPropertyFilterEdgeCaseTests.swift
/// Edge case and complex scenario tests for SPARQL property filtering

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
@testable import GraphIndex

@Persistable
fileprivate struct EdgeCaseConnection {
    #Directory<EdgeCaseConnection>("test", "edge_case")
    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var relation: String = ""
    var score: Int = 0
    var note: String = ""

    #Index(GraphIndexKind<EdgeCaseConnection>(
        from: \.from,
        edge: \.relation,
        to: \.target,
        graph: nil,
        strategy: .tripleStore
    ), storedFields: [
        \EdgeCaseConnection.score,
        \EdgeCaseConnection.note
    ], name: "edge_case_graph")
}

@Suite("SPARQL Property Filter Edge Case Tests", .serialized)
struct SPARQLPropertyFilterEdgeCaseTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([EdgeCaseConnection.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "edge_case"])

        // Set index to readable
        let subspace = try await container.resolveDirectory(for: EdgeCaseConnection.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in EdgeCaseConnection.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)
            if currentState == .disabled {
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            } else if currentState == .writeOnly {
                try await indexStateManager.makeReadable(descriptor.name)
            }
        }

        return container
    }

    // MARK: - Complex Filter Tests (post-scan evaluation)

    @Test("Complex filter: OR expression (post-scan)")
    func testOrExpressionPostScan() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: "friend"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 50, note: "colleague"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 90, note: "family"))
        try await context.save()

        // OR: score < 20 OR score > 80 (both sides are post-scan)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 50, note: "active"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 60, note: "inactive"))
        try await context.save()

        // NOT(note = "inactive")
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .not(.equals("?note", .string("inactive")))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 1)
        #expect(result.bindings[0]["?note"] == .string("active"))
    }

    @Test("Complex filter: regex (post-scan)")
    func testRegexPostScan() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: "active-premium"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 20, note: "inactive"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 30, note: "active"))
        try await context.save()

        // note =~ /^active/
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 100, note: "test"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 200, note: "test"))
        try await context.save()

        // SELECT ?score WHERE { ... } FILTER(?score > 150)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        #expect(result.bindings[0]["?score"] == .int64(200))
        #expect(result.bindings[0]["?target"] == nil)  // Structure variable excluded
        #expect(result.bindings[0]["?note"] == nil)    // Other property excluded
    }

    @Test("SELECT mix of structure and property variables")
    func testSelectMixedVariables() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        context.insert(EdgeCaseConnection(from: alice, target: bob, relation: "knows", score: 100, note: "friend"))
        try await context.save()

        // SELECT ?target ?score WHERE { ... }
        let result = try await context.executeSPARQLPattern(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            on: EdgeCaseConnection.self,
            projection: ["?target", "?score"]
        )

        #expect(result.bindings.count == 1)
        #expect(result.bindings[0]["?target"] == .string(bob))
        #expect(result.bindings[0]["?score"] == .int64(100))
        #expect(result.bindings[0]["?note"] == nil)  // Not in projection
    }

    // MARK: - Comparison Operator Coverage

    @Test("Comparison operators: lessThanOrEqual")
    func testLessThanOrEqual() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        for score in [10, 20, 30, 40, 50] {
            context.insert(EdgeCaseConnection(from: alice, target: uniqueID("user\(score)"), relation: "knows", score: score, note: ""))
        }
        try await context.save()

        // score <= 30
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        for score in [10, 20, 30, 40, 50] {
            context.insert(EdgeCaseConnection(from: alice, target: uniqueID("user\(score)"), relation: "knows", score: score, note: ""))
        }
        try await context.save()

        // score > 30
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 50, note: "test"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 100, note: "test"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 50, note: "test"))
        try await context.save()

        // score != 50
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
                )
            ]),
            .notEquals("?score", .int64(50))
        )

        let result = try await context.executeSPARQLPattern(
            pattern,
            on: EdgeCaseConnection.self
        )

        #expect(result.bindings.count == 1)
        #expect(result.bindings[0]["?score"] == .int64(100))
    }

    @Test("String operators: hasPrefix")
    func testHasPrefix() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 0, note: "active-user"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 0, note: "inactive-user"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 0, note: "active-admin"))
        try await context.save()

        // note STARTS WITH "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 0, note: "user-active"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 0, note: "user-disabled"))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("dave"), relation: "knows", score: 0, note: "admin-active"))
        try await context.save()

        // note ENDS WITH "active"
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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

    @Test("Empty result: filter excludes all records")
    func testEmptyResult() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("bob"), relation: "knows", score: 10, note: ""))
        context.insert(EdgeCaseConnection(from: alice, target: uniqueID("carol"), relation: "knows", score: 20, note: ""))
        try await context.save()

        // score > 100 (no matches)
        let pattern = ExecutionPattern.filter(
            .basic([
                ExecutionTriple(
                    subject: .value(.string(alice)),
                    predicate: .value(.string("knows")),
                    object: .variable("?target"),
                    graph: nil
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
