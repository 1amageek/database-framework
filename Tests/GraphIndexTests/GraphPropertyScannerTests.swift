// GraphPropertyScannerTests.swift
// Tests for GraphPropertyScanner - property-aware edge scanning

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
@testable import GraphIndex

@Suite("GraphPropertyScanner Tests", .serialized)
struct GraphPropertyScannerTests {

    // MARK: - Test Models

    @Persistable
    struct SocialEdge {
        #Directory<SocialEdge>("test", "social_edges")

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
        ), storedFields: [\SocialEdge.since, \SocialEdge.status, \SocialEdge.score], name: "social_graph_index")

        #Index(GraphIndexKind<SocialEdge>(
            from: \.from,
            edge: \.label,
            to: \.target,
            graph: \.id,
            strategy: .adjacency
        ), storedFields: [\SocialEdge.since, \SocialEdge.status, \SocialEdge.score], name: "adjacency_graph_index")
    }

    // MARK: - Setup

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func makeEdge(from: String, target: String, label: String, since: Int, status: String?, score: Double, graphId: String? = nil) -> SocialEdge {
        var edge = SocialEdge(from: from, target: target, label: label, since: since, status: status, score: score)
        if let graphId {
            edge.id = graphId
        }
        return edge
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([SocialEdge.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)

        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["test", "social_edges"])

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
                break  // Already readable
            }
        }
    }

    // MARK: - Basic Scanning Tests

    @Test("Scan edges with properties (no filter)")
    func testScanWithPropertiesNoFilter() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        let edge1 = makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9)
        let edge2 = makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5)
        context.insert(edge1)
        context.insert(edge2)
        try await context.save()

        try await container.database.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SocialEdge.self)
            let indexSubspace = subspace.subspace("I")
            let graphIndexSubspace = indexSubspace.subspace("social_graph_index")

            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .tripleStore,
                storedFieldNames: ["since", "status", "score"]
            )

            var edges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: nil, transaction: transaction) {
                edges.append(edge)
            }

            #expect(edges.count == 2)
            let edge1 = edges.first { $0.target == bob }
            let edge2 = edges.first { $0.target == carol }

            if let e1 = edge1 {
                #expect(e1.properties["since"] as? Int64 == 2020)
                #expect(e1.properties["status"] as? String == "active")
                #expect(e1.properties["score"] as? Double == 0.9)
            }

            if let e2 = edge2 {
                #expect(e2.properties["since"] as? Int64 == 2021)
                #expect(e2.properties["status"] as? String == "inactive")
                #expect(e2.properties["score"] as? Double == 0.5)
            }
        }
    }

    @Test("Property filter: equality")
    func testPropertyFilterEquality() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        context.insert(makeEdge(from: alice, target: uniqueID("bob"), label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: uniqueID("carol"), label: "KNOWS", since: 2020, status: "inactive", score: 0.5))
        context.insert(makeEdge(from: alice, target: uniqueID("dave"), label: "KNOWS", since: 2021, status: "active", score: 0.7))
        try await context.save()

        try await container.database.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SocialEdge.self)
            let indexSubspace = subspace.subspace("I")
            let graphIndexSubspace = indexSubspace.subspace("social_graph_index")

            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .tripleStore,
                storedFieldNames: ["since", "status", "score"]
            )

            let filters = [PropertyFilter(fieldName: "since", op: .equal, value: .int64(2020))]

            var edges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: filters, transaction: transaction) {
                edges.append(edge)
            }

            #expect(edges.count == 2)
            #expect(edges.allSatisfy { $0.properties["since"] as? Int64 == 2020 })
        }
    }

    @Test("Property filter: range")
    func testPropertyFilterRange() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        for year in [2018, 2019, 2020, 2021, 2022] {
            context.insert(makeEdge(from: alice, target: uniqueID("user-\(year)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        try await context.save()

        try await container.database.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SocialEdge.self)
            let indexSubspace = subspace.subspace("I")
            let graphIndexSubspace = indexSubspace.subspace("social_graph_index")

            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .tripleStore,
                storedFieldNames: ["since", "status", "score"]
            )

            let filters = [PropertyFilter(fieldName: "since", op: .greaterThanOrEqual, value: .int64(2020))]

            var edges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: filters, transaction: transaction) {
                edges.append(edge)
            }

            #expect(edges.count == 3)
            #expect(edges.allSatisfy { ($0.properties["since"] as? Int64 ?? 0) >= 2020 })
        }
    }

    // MARK: - Bug Fix Verification Tests

    @Test("Bug Fix 1: nil vs empty string distinction")
    func testNilPropertyFiltering() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        // Create edges with nil, empty string, and non-empty string status
        context.insert(makeEdge(from: alice, target: uniqueID("bob"), label: "KNOWS", since: 2020, status: nil, score: 0.9))
        context.insert(makeEdge(from: alice, target: uniqueID("carol"), label: "KNOWS", since: 2020, status: "", score: 0.5))
        context.insert(makeEdge(from: alice, target: uniqueID("dave"), label: "KNOWS", since: 2020, status: "active", score: 0.7))
        try await context.save()

        try await container.database.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SocialEdge.self)
            let indexSubspace = subspace.subspace("I")
            let graphIndexSubspace = indexSubspace.subspace("social_graph_index")

            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .tripleStore,
                storedFieldNames: ["since", "status", "score"]
            )

            // Test .isNil operator - should match only nil values
            let nilFilters = [PropertyFilter(fieldName: "status", op: .isNil, value: .null)]
            var nilEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: nilFilters, transaction: transaction) {
                nilEdges.append(edge)
            }

            #expect(nilEdges.count == 1, "Should find exactly 1 edge with nil status")
            #expect(nilEdges.allSatisfy { $0.properties["status"] == nil }, "All matched edges should have nil status")

            // Test .isNotNil operator - should match non-nil values (including empty string)
            let notNilFilters = [PropertyFilter(fieldName: "status", op: .isNotNil, value: .null)]
            var notNilEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: notNilFilters, transaction: transaction) {
                notNilEdges.append(edge)
            }

            #expect(notNilEdges.count == 2, "Should find 2 edges with non-nil status (empty string and 'active')")

            // Test .equal("") - should match only empty string, not nil
            let emptyFilters = [PropertyFilter(fieldName: "status", op: .equal, value: .string(""))]
            var emptyEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(from: alice, edge: "KNOWS", to: nil, propertyFilters: emptyFilters, transaction: transaction) {
                emptyEdges.append(edge)
            }

            #expect(emptyEdges.count == 1, "Should find exactly 1 edge with empty string status")
            #expect(emptyEdges.allSatisfy { $0.properties["status"] as? String == "" }, "Matched edge should have empty string status")
        }
    }

    @Test("Bug Fix 2 & 3: adjacency strategy with Named Graph support")
    func testAdjacencyWithNamedGraph() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        // Create edges with different graph IDs
        let edge1 = makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9, graphId: "graph-social")
        let edge2 = makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5, graphId: "graph-work")

        context.insert(edge1)
        context.insert(edge2)
        try await context.save()

        try await container.database.withTransaction { transaction in
            let subspace = try await container.resolveDirectory(for: SocialEdge.self)
            let indexSubspace = subspace.subspace("I")
            let graphIndexSubspace = indexSubspace.subspace("adjacency_graph_index")

            let scanner = GraphPropertyScanner(
                indexSubspace: graphIndexSubspace,
                strategy: .adjacency,
                storedFieldNames: ["since", "status", "score"]
            )

            // Test: Scan with graph filter (should only return edges in "graph-social")
            var socialEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(
                from: alice,
                edge: "KNOWS",
                to: nil,
                graph: "graph-social",
                propertyFilters: nil,
                transaction: transaction
            ) {
                socialEdges.append(edge)
            }

            #expect(socialEdges.count == 1, "Should find exactly 1 edge in graph-social")
            #expect(socialEdges.first?.graph == "graph-social", "Graph field should be correctly read from adjacency index")
            #expect(socialEdges.first?.target == bob, "Should find edge to Bob")

            // Test: Scan with different graph filter
            var workEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(
                from: alice,
                edge: "KNOWS",
                to: nil,
                graph: "graph-work",
                propertyFilters: nil,
                transaction: transaction
            ) {
                workEdges.append(edge)
            }

            #expect(workEdges.count == 1, "Should find exactly 1 edge in graph-work")
            #expect(workEdges.first?.graph == "graph-work", "Graph field should be correctly read")
            #expect(workEdges.first?.target == carol, "Should find edge to Carol")

            // Test: Scan without graph filter (should return all edges)
            var allEdges: [GraphEdgeWithProperties] = []
            for try await edge in scanner.scanEdges(
                from: alice,
                edge: "KNOWS",
                to: nil,
                graph: nil,
                propertyFilters: nil,
                transaction: transaction
            ) {
                allEdges.append(edge)
            }

            #expect(allEdges.count == 2, "Should find all 2 edges when no graph filter is specified")
        }
    }
}
