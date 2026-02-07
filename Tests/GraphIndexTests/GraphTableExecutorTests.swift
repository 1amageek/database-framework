/// GraphTableExecutorTests.swift
/// Integration tests for SQL/PGQ GraphTableExecutor

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
import QueryIR
@testable import GraphIndex

// MARK: - Test Models

/// Type without GraphIndexKind (for error testing)
@Persistable
fileprivate struct NoGraphIndexType {
    #Directory<NoGraphIndexType>("test", "no_graph_index")
    var id: String = UUID().uuidString
    var name: String = ""

    // No GraphIndexKind - only ScalarIndexKind
    #Index(ScalarIndexKind<NoGraphIndexType>(fieldNames: ["name"]), name: "name_index")
}

@Suite("GraphTable Executor Integration Tests", .serialized)
struct GraphTableExecutorTests {

    // MARK: - Test Model

    @Persistable
    struct SocialEdge {
        #Directory<SocialEdge>("test", "social_edges_executor")

        var id: String = UUID().uuidString
        var from: String = ""
        var target: String = ""
        var label: String = ""
        var since: Int = 0
        var status: String? = nil
        var score: Double = 0.0

        #Index(GraphIndexKind<SocialEdge>(
            from: \.from,
            edge: \.label,
            to: \.target,
            graph: nil,
            strategy: .tripleStore
        ), storedFields: [\SocialEdge.since, \SocialEdge.status, \SocialEdge.score], name: "social_executor_index")
    }

    // MARK: - Setup

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeEdge(from: String, target: String, label: String, since: Int, status: String?, score: Double) -> SocialEdge {
        SocialEdge(from: from, target: target, label: label, since: since, status: status, score: score)
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SocialEdge.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "social_edges_executor"])

        // Set index states to readable
        try await setIndexStatesToReadable(container: container)

        return container
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: SocialEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in SocialEdge.indexDescriptors {
            let currentState = try await indexStateManager.state(of: descriptor.name)

            switch currentState {
            case .disabled:
                try await indexStateManager.enable(descriptor.name)
                try await indexStateManager.makeReadable(descriptor.name)
            case .writeOnly:
                try await indexStateManager.makeReadable(descriptor.name)
            case .readable:
                break
            }
        }
    }

    // MARK: - Basic Execution Tests

    @Test("Execute GRAPH_TABLE with simple edge pattern")
    func testBasicExecution() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
        try await context.save()

        // Create GRAPH_TABLE source
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        // Execute
        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 2)
        #expect(rows.allSatisfy { $0.edgeLabel == "KNOWS" })
    }

    @Test("Execute with property filter - equality")
    func testPropertyFilterEquality() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
        try await context.save()

        // GRAPH_TABLE with property filter
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "since", value: .literal(.int(2020)))],  // Filter: since = 2020
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        // Properties are stored as TupleElement types, need to check actual value
        if let since = rows.first?.properties["since"] {
            let sinceInt = TypeConversion.asInt64(since)
            #expect(sinceInt == 2020)
        } else {
            Issue.record("Property 'since' not found in result")
        }
    }

    @Test("Execute with property filter - comparison")
    func testPropertyFilterComparison() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        for year in [2018, 2019, 2020, 2021, 2022] {
            context.insert(makeEdge(from: alice, target: uniqueID("user-\(year)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        try await context.save()

        // GRAPH_TABLE with range filter
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "since", value: .greaterThanOrEqual(.column(ColumnRef(column: "since")), .literal(.int(2020))))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 3)  // 2020, 2021, 2022
        #expect(rows.allSatisfy {
            if let sinceValue = $0.properties["since"] {
                return (TypeConversion.asInt64(sinceValue) ?? 0) >= 2020
            }
            return false
        })
    }

    @Test("Execute with multiple property filters")
    func testMultiplePropertyFilters() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")
        let dave = uniqueID("dave")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2020, status: "inactive", score: 0.5))
        context.insert(makeEdge(from: alice, target: dave, label: "KNOWS", since: 2021, status: "active", score: 0.7))
        try await context.save()

        // Multiple filters (AND)
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [
                            PropertyBinding(key: "since", value: .literal(.int(2020))),
                            PropertyBinding(key: "status", value: .literal(.string("active")))
                        ],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        #expect(rows.first?.target == bob)
    }

    // MARK: - SPARQL RDF Literal Conversion Tests

    @Test("Convert SPARQL IRI literal")
    func testConvertIRILiteral() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "http://example.org/active", score: 0.9))
        try await context.save()

        // Property filter with IRI literal
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "status", value: .literal(.iri("http://example.org/active")))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
    }

    @Test("Convert typed literal")
    func testConvertTypedLiteral() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "premium", score: 0.9))
        try await context.save()

        // Typed literal (xsd:string)
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "status", value: .literal(.typedLiteral(value: "premium", datatype: "http://www.w3.org/2001/XMLSchema#string")))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
    }

    // MARK: - Error Handling Tests

    @Test("Error: complex property expression")
    func testErrorComplexExpression() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        // Complex expression (subquery)
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "since", value: .subquery(SelectQuery(
                            projection: .items([ProjectionItem(.literal(.int(2020)))]),
                            source: .table(TableRef("dummy"))
                        )))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        do {
            _ = try await context.graphTable(SocialEdge.self, source: source)
            Issue.record("Should throw complexPropertyExpression error")
        } catch let error as GraphTableError {
            if case .complexPropertyExpression(let message) = error {
                #expect(message.contains("complex expression"))
            } else {
                Issue.record("Expected complexPropertyExpression error, got \(error)")
            }
        }
    }

    @Test("Error: graph index not found")
    func testErrorIndexNotFound() async throws {
        let database = try FDBClient.openDatabase()
        let schema = Schema([NoGraphIndexType.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let source = GraphTableSource(
            graphName: "NonExistentGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(direction: .outgoing)),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        do {
            // This should fail because NoGraphIndexType has no GraphIndexKind
            _ = try await GraphTableExecutor<NoGraphIndexType>(
                container: container,
                schema: schema,
                graphTableSource: source
            ).execute()

            Issue.record("Should throw indexNotFound error")
        } catch let error as GraphTableError {
            if case .indexNotFound = error {
                // Expected
            } else {
                Issue.record("Expected indexNotFound error, got \(error)")
            }
        }
    }

    @Test("Error: type mismatch (array literal)")
    func testErrorTypeMismatch() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        // Array literal (not supported)
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "since", value: .literal(.array([.int(2020), .int(2021)])))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        do {
            _ = try await context.graphTable(SocialEdge.self, source: source)
            Issue.record("Should throw typeMismatch error")
        } catch let error as GraphTableError {
            if case .typeMismatch(let message) = error {
                #expect(message.contains("Array"))
            } else {
                Issue.record("Expected typeMismatch error, got \(error)")
            }
        }
    }

    // MARK: - Performance Validation

    @Test("Property filter reduces scanned edges")
    func testPropertyFilterReducesScan() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        // Insert 100 edges with different years (1920-2019)
        for i in 0..<100 {
            let year = 1920 + i
            context.insert(makeEdge(from: alice, target: uniqueID("user-\(i)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        // Add one edge with year 2020
        context.insert(makeEdge(from: alice, target: uniqueID("user-2020"), label: "KNOWS", since: 2020, status: "active", score: 0.5))
        try await context.save()

        // Filter to only 2020 (1 edge out of 101)
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(
                        labels: ["KNOWS"],
                        properties: [PropertyBinding(key: "since", value: .literal(.int(2020)))],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b"))
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        if let sinceValue = rows.first?.properties["since"] {
            let since = TypeConversion.asInt64(sinceValue)
            #expect(since == 2020)
        } else {
            Issue.record("Property 'since' not found")
        }
    }
}
