#if FOUNDATION_DB
// GraphPropertyScannerTests.swift
// Tests for GraphPropertyScanner - property-aware edge scanning

import Testing
import Foundation
import DatabaseKit
import DatabaseRuntime
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("GraphPropertyScanner Tests", .serialized, .foundationDBScenario, .heartbeat)
struct GraphPropertyScannerTests {

    // MARK: - Test Models

    @Persistable
    struct SocialEdge {
        #Directory<SocialEdge>("graph_property_scanner_social_edges")

        var id: String = UUID().uuidString
        var from: String = ""
        var target: String = ""
        var label: String = ""
        var since: Int64 = 0
        var status: String? = nil
        var score: Double = 0.0

        #Index(
            .graph(
                name: "social_graph_index",
                definition: .property(
                    source: \SocialEdge.from, label: .field(\SocialEdge.label),
                    target: \SocialEdge.target,
                    graph: nil, strategy: .tripleStore),
                includedFields: [
                \SocialEdge.since,
                \SocialEdge.status,
                \SocialEdge.score]))

        #Index(
            .graph(
                name: "adjacency_graph_index",
                definition: .property(
                    source: \SocialEdge.from, label: .field(\SocialEdge.label),
                    target: \SocialEdge.target,
            graph: \SocialEdge.id, strategy: .adjacency),
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

    private func makeEdge(from: String, target: String, label: String, since: Int64, status: String?, score: Double, graphId: String? = nil) -> SocialEdge {
        var edge = SocialEdge(from: from, target: target, label: label, since: since, status: status, score: score)
        if let graphId {
            edge.id = graphId
        }
        return edge
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
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(SocialEdge.self)]),
            security: .testingDisabled,
        )

        try await container.resetTestBaseData()

        return container
    }

    private func makeScanner(
        container: DBContainer,
        indexName: String,
        strategy: GraphIndexStrategy
    ) async throws -> GraphPropertyScanner {
        let subspace = try await container.testBaseDirectory(for: SocialEdge.self)
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: subspace
        )
        return GraphPropertyScanner(
            indexSubspace: try lifecycleStore.indexSubspace(for: indexName),
            strategy: strategy,
            includedFieldNames: ["since", "status", "score"]
        )
    }

    // MARK: - Basic Scanning Tests

    @Test("Scan edges with properties (no filter)")
    func testScanWithPropertiesNoFilter() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        let edge1 = makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9)
        let edge2 = makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5)
        try context.insert(edge1)
        try context.insert(edge2)
        try await context.save()

        let scanner = try await makeScanner(
            container: container,
            indexName: "social_graph_index",
            strategy: .tripleStore
        )
        try await container.engine.withTransaction { transaction in
            let edges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: nil,
                    transaction: transaction
                )
            )

            #expect(edges.count == 2)
            let edge1 = edges.first { $0.target == .identifier(bob) }
            let edge2 = edges.first { $0.target == .identifier(carol) }

            if let e1 = edge1 {
                #expect(e1.properties["since"] == .int64(2020))
                #expect(e1.properties["status"] == .string("active"))
                #expect(e1.properties["score"] == .float64(0.9))
            }

            if let e2 = edge2 {
                #expect(e2.properties["since"] == .int64(2021))
                #expect(e2.properties["status"] == .string("inactive"))
                #expect(e2.properties["score"] == .float64(0.5))
            }
        }
    }

    @Test("Property filter: equality")
    func testPropertyFilterEquality() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")

        try context.insert(makeEdge(from: alice, target: uniqueID("bob"), label: "KNOWS", since: 2020, status: "active", score: 0.9))
        try context.insert(makeEdge(from: alice, target: uniqueID("carol"), label: "KNOWS", since: 2020, status: "inactive", score: 0.5))
        try context.insert(makeEdge(from: alice, target: uniqueID("dave"), label: "KNOWS", since: 2021, status: "active", score: 0.7))
        try await context.save()

        let scanner = try await makeScanner(
            container: container,
            indexName: "social_graph_index",
            strategy: .tripleStore
        )
        try await container.engine.withTransaction { transaction in
            let filters = [PropertyFilter(fieldName: "since", op: .equal, value: .int64(2020))]

            let edges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: filters,
                    transaction: transaction
                )
            )

            #expect(edges.count == 2)
            #expect(edges.allSatisfy { $0.properties["since"] == .int64(2020) })
        }
    }

    @Test("Property filter: range")
    func testPropertyFilterRange() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")

        for year: Int64 in [2018, 2019, 2020, 2021, 2022] {
            try context.insert(makeEdge(from: alice, target: uniqueID("user-\(year)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        try await context.save()

        let scanner = try await makeScanner(
            container: container,
            indexName: "social_graph_index",
            strategy: .tripleStore
        )
        try await container.engine.withTransaction { transaction in
            let filters = [PropertyFilter(fieldName: "since", op: .greaterThanOrEqual, value: .int64(2020))]

            let edges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: filters,
                    transaction: transaction
                )
            )

            #expect(edges.count == 3)
            #expect(edges.allSatisfy {
                guard case .int64(let year) = $0.properties["since"] else {
                    return false
                }
                return year >= 2020
            })
        }
    }

    // MARK: - Bug Fix Verification Tests

    @Test("Bug Fix 1: nil vs empty string distinction")
    func testNilPropertyFiltering() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")

        // Create edges with nil, empty string, and non-empty string status
        try context.insert(makeEdge(from: alice, target: uniqueID("bob"), label: "KNOWS", since: 2020, status: nil, score: 0.9))
        try context.insert(makeEdge(from: alice, target: uniqueID("carol"), label: "KNOWS", since: 2020, status: "", score: 0.5))
        try context.insert(makeEdge(from: alice, target: uniqueID("dave"), label: "KNOWS", since: 2020, status: "active", score: 0.7))
        try await context.save()

        let scanner = try await makeScanner(
            container: container,
            indexName: "social_graph_index",
            strategy: .tripleStore
        )
        try await container.engine.withTransaction { transaction in
            // Test .isNil operator - should match only nil values
            let nilFilters = [PropertyFilter(fieldName: "status", op: .isNil, value: .null)]
            let nilEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: nilFilters,
                    transaction: transaction
                )
            )

            #expect(nilEdges.count == 1, "Should find exactly 1 edge with nil status")
            #expect(nilEdges.allSatisfy { $0.properties["status"] == .null }, "All matched edges should preserve the stored null value")

            // Test .isNotNil operator - should match non-nil values (including empty string)
            let notNilFilters = [PropertyFilter(fieldName: "status", op: .isNotNil, value: .null)]
            let notNilEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: notNilFilters,
                    transaction: transaction
                )
            )

            #expect(notNilEdges.count == 2, "Should find 2 edges with non-nil status (empty string and 'active')")

            // Test .equal("") - should match only empty string, not nil
            let emptyFilters = [PropertyFilter(fieldName: "status", op: .equal, value: .string(""))]
            let emptyEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    propertyFilters: emptyFilters,
                    transaction: transaction
                )
            )

            #expect(emptyEdges.count == 1, "Should find exactly 1 edge with empty string status")
            #expect(emptyEdges.allSatisfy { $0.properties["status"] == .string("") }, "Matched edge should have empty string status")
        }
    }

    @Test("Bug Fix 2 & 3: adjacency strategy with Named Graph support")
    func testAdjacencyWithNamedGraph() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        // Create edges with different graph IDs
        let edge1 = makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9, graphId: "graph-social")
        let edge2 = makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5, graphId: "graph-work")

        try context.insert(edge1)
        try context.insert(edge2)
        try await context.save()

        let scanner = try await makeScanner(
            container: container,
            indexName: "adjacency_graph_index",
            strategy: .adjacency
        )
        try await container.engine.withTransaction { transaction in
            // Test: Scan with graph filter (should only return edges in "graph-social")
            let socialEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    graphTarget: .named(.identifier("graph-social")),
                    propertyFilters: nil,
                    transaction: transaction
                )
            )

            #expect(socialEdges.count == 1, "Should find exactly 1 edge in graph-social")
            #expect(socialEdges.first?.graph == .identifier("graph-social"), "Graph field should be correctly read from adjacency index")
            #expect(socialEdges.first?.target == .identifier(bob), "Should find edge to Bob")

            // Test: Scan with different graph filter
            let workEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    graphTarget: .named(.identifier("graph-work")),
                    propertyFilters: nil,
                    transaction: transaction
                )
            )

            #expect(workEdges.count == 1, "Should find exactly 1 edge in graph-work")
            #expect(workEdges.first?.graph == .identifier("graph-work"), "Graph field should be correctly read")
            #expect(workEdges.first?.target == .identifier(carol), "Should find edge to Carol")

            // Test: Scan without graph filter (should return all edges)
            let allEdges = try await collectEdges(
                scanner.scanEdges(
                    from: .identifier(alice),
                    edge: "KNOWS",
                    to: nil,
                    graphTarget: .all,
                    propertyFilters: nil,
                    transaction: transaction
                )
            )

            #expect(allEdges.count == 2, "Should find all 2 edges when no graph filter is specified")
        }
    }

    private func collectEdges(
        _ scan: GraphPropertyScan
    ) async throws -> [GraphEdgeWithProperties] {
        var cursor = scan.makeCursor()
        var edges: [GraphEdgeWithProperties] = []
        while let edge = try await cursor.next() {
            edges.append(edge)
        }
        return edges
    }
}
#endif
