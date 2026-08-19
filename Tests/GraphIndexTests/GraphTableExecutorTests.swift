#if FOUNDATION_DB
/// GraphTableExecutorTests.swift
/// Integration tests for SQL/PGQ GraphTableExecutor

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

// MARK: - Test Models

/// Type without a property-graph index (for error testing)
@Persistable
private struct NoGraphIndexType {
    #Directory<NoGraphIndexType>("graph_table_no_graph_index")
    var id: String = UUID().uuidString
    var name: String = ""

    #Index(
        .ordered(name: "name_index", keys: [.ascending(\NoGraphIndexType.name)], unique: false))
}

@Suite("GraphTable Executor Integration Tests", .serialized, .foundationDBScenario, .heartbeat)
struct GraphTableExecutorTests {

    // MARK: - Test Model

    @Persistable
    struct SocialEdge {
        #Directory<SocialEdge>("graph_table_social_edges_executor")

        var id: String = UUID().uuidString
        var from: String = ""
        var target: String = ""
        var label: String = ""
        var since: Int64 = 0
        var status: String? = nil
        var score: Double = 0.0

        #Index(
            .graph(
                name: "social_executor_index",
                definition: .property(
                    source: \SocialEdge.from, label: .field(\SocialEdge.label),
                    target: \SocialEdge.target,
                    graph: nil, strategy: .tripleStore),
                includedFields: [
                \SocialEdge.since,
                \SocialEdge.status,
                \SocialEdge.score]))
    }

    // MARK: - Setup

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeEdge(from: String, target: String, label: String, since: Int64, status: String?, score: Double) -> SocialEdge {
        SocialEdge(from: from, target: target, label: label, since: since, status: status, score: score)
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try SocialEdge.schemaEntity
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialEdge.self), try DatabaseFrameworkRuntime.entity(NoGraphIndexType.self),
                ]),
            security: .testingDisabled,
        )

        let subspace = try await container.testBaseDirectory(for: SocialEdge.self)
        let (begin, end) = subspace.range()
        try await database.withTransaction { transaction in
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
        try await container.ensureTestBaseIndexesReady()

        return container
    }

    // MARK: - Basic Execution Tests

    @Test("Execute GRAPH_TABLE with simple edge pattern")
    func testBasicExecution() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        try context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        try context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
        try await context.save()

        // Create GRAPH_TABLE source
        let source = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                    .node(NodePattern(variable: "b")),
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
        let context = container.testBaseContext()

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        try context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        try context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
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
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        if let since = rows.first?.properties["since"] {
            #expect(since.int64Value == 2020)
        } else {
            Issue.record("Property 'since' not found in result")
        }
    }

    @Test("Execute with property filter - comparison")
    func testPropertyFilterComparison() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")

        for year: Int64 in [2018, 2019, 2020, 2021, 2022] {
            try context.insert(makeEdge(from: alice, target: uniqueID("user-\(year)"), label: "KNOWS", since: year, status: "active", score: 0.5))
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
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 3)  // 2020, 2021, 2022
        #expect(rows.allSatisfy {
            guard let since = $0.properties["since"]?.int64Value else { return false }
            return since >= 2020
        })
    }

    @Test("Execute with multiple property filters")
    func testMultiplePropertyFilters() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")
        let dave = uniqueID("dave")

        try context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        try context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2020, status: "inactive", score: 0.5))
        try context.insert(makeEdge(from: alice, target: dave, label: "KNOWS", since: 2021, status: "active", score: 0.7))
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
                            PropertyBinding(key: "status", value: .literal(.string("active"))),
                            ],
                        direction: .outgoing
                    )),
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        #expect(rows.first?.target == bob)
    }

    // MARK: - Error Handling Tests

    @Test("Error: complex property expression")
    func testErrorComplexExpression() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

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
                    .node(NodePattern(variable: "b")),
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
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try NoGraphIndexType.schemaEntity
            ],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialEdge.self), try DatabaseFrameworkRuntime.entity(NoGraphIndexType.self),
                ]),
            security: .testingDisabled,
        )
        let subspace = try await container.testBaseDirectory(for: NoGraphIndexType.self)
        let (begin, end) = subspace.range()
        try await database.withTransaction { transaction in
            try transaction.clearRange(beginKey: begin, endKey: end)
        }

        let source = GraphTableSource(
            graphName: "NonExistentGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(direction: .outgoing)),
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        do {
            // This should fail because the type has no property-graph index.
            _ = try await container.testBaseContext().graphTable(
                NoGraphIndexType.self,
                source: source
            )

            Issue.record("Should throw indexNotFound error")
        } catch let error as GraphTableError {
            if case .indexNotFound = error {
                // Expected
            } else {
                Issue.record("Expected indexNotFound error, got \(error)")
            }
        }
    }

    @Test("Array literal does not match scalar property")
    func testArrayLiteralDoesNotMatchScalarProperty() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        try context.insert(
            makeEdge(
                from: uniqueID("alice"),
                target: uniqueID("bob"),
                label: "KNOWS",
                since: 2020,
                status: "active",
                score: 0.9
            )
        )
        try await context.save()

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
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)
        #expect(rows.isEmpty)
    }

    // MARK: - Performance Validation

    @Test("Property filter emits matching edges only")
    func testPropertyFilterEmitsMatchingEdgesOnly() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")

        // Insert 100 edges with different years (1920-2019)
        for i in 0..<100 {
            let year = Int64(1920) + Int64(i)
            try context.insert(makeEdge(from: alice, target: uniqueID("user-\(i)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        // Add one edge with year 2020
        try context.insert(makeEdge(from: alice, target: uniqueID("user-2020"), label: "KNOWS", since: 2020, status: "active", score: 0.5))
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
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let rows = try await context.graphTable(SocialEdge.self, source: source)

        #expect(rows.count == 1)
        if let sinceValue = rows.first?.properties["since"] {
            #expect(sinceValue.int64Value == 2020)
        } else {
            Issue.record("Property 'since' not found")
        }
    }
}
#endif
