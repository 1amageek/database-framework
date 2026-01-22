// GraphQueryBuilderTests.swift
// GraphIndex - Tests for GraphQueryBuilder API
//
// Tests the graph() query builder API and error handling.

import Testing
import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Graph
import TestSupport
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct GraphQueryTestEdge: Equatable {
    #Directory<GraphQueryTestEdge>("test", "graphquerybuilder", "edges")

    var id: String = UUID().uuidString
    var source: String = ""
    var predicate: String = ""
    var target: String = ""

    #Index(GraphIndexKind<GraphQueryTestEdge>(
        from: \.source,
        edge: \.predicate,
        to: \.target,
        strategy: .adjacency
    ))
}

// MARK: - Test Suite

@Suite("GraphQueryBuilder Tests", .serialized)
struct GraphQueryBuilderTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([GraphQueryTestEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "graphquerybuilder", "edges"])
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: GraphQueryTestEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in GraphQueryTestEdge.indexDescriptors {
            try await indexStateManager.enable(descriptor.name)
            try await indexStateManager.makeReadable(descriptor.name)
        }
    }

    private func makeEdge(source: String, predicate: String, target: String) -> GraphQueryTestEdge {
        var edge = GraphQueryTestEdge()
        edge.source = source
        edge.predicate = predicate
        edge.target = target
        return edge
    }

    // MARK: - executeItems() Error Tests

    @Test("executeItems() throws executeItemsNotSupported error")
    func executeItemsThrowsError() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")
        let edge1 = makeEdge(source: alice, predicate: "knows", target: uniqueID("Bob"))

        context.insert(edge1)
        try await context.save()

        // Test: executeItems() should throw
        await #expect(throws: GraphQueryError.self) {
            _ = try await context.graph(GraphQueryTestEdge.self)
                .defaultIndex()
                .from(alice)
                .executeItems()
        }
    }

    @Test("executeItems() error has correct description")
    func executeItemsErrorDescription() async throws {
        let error = GraphQueryError.executeItemsNotSupported
        let description = error.description

        #expect(description.contains("executeItems()"))
        #expect(description.contains("not supported"))
        #expect(description.contains("graph indexes"))
    }

    // MARK: - execute() Tests (verify prefix construction works)

    @Test("execute() returns edges with from pattern")
    func executeWithFromPattern() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")

        let edge1 = makeEdge(source: alice, predicate: "knows", target: bob)
        let edge2 = makeEdge(source: alice, predicate: "knows", target: carol)
        let edge3 = makeEdge(source: bob, predicate: "knows", target: carol)

        context.insert(edge1)
        context.insert(edge2)
        context.insert(edge3)
        try await context.save()

        // Test: Query edges from Alice
        let edges = try await context.graph(GraphQueryTestEdge.self)
            .defaultIndex()
            .from(alice)
            .execute()

        #expect(edges.count == 2)
        #expect(edges.allSatisfy { $0.from == alice })
    }

    @Test("execute() returns edges with from and edge pattern")
    func executeWithFromAndEdgePattern() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")

        let edge1 = makeEdge(source: alice, predicate: "knows", target: bob)
        let edge2 = makeEdge(source: alice, predicate: "likes", target: carol)

        context.insert(edge1)
        context.insert(edge2)
        try await context.save()

        // Test: Query edges from Alice with "knows" predicate
        let edges = try await context.graph(GraphQueryTestEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("knows")
            .execute()

        #expect(edges.count == 1)
        #expect(edges.first?.from == alice)
        #expect(edges.first?.edge == "knows")
        #expect(edges.first?.to == bob)
    }

    @Test("execute() returns edges with to pattern")
    func executeWithToPattern() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")

        let edge1 = makeEdge(source: alice, predicate: "knows", target: carol)
        let edge2 = makeEdge(source: bob, predicate: "likes", target: carol)

        context.insert(edge1)
        context.insert(edge2)
        try await context.save()

        // Test: Query edges to Carol
        let edges = try await context.graph(GraphQueryTestEdge.self)
            .defaultIndex()
            .to(carol)
            .execute()

        #expect(edges.count == 2)
        #expect(edges.allSatisfy { $0.to == carol })
    }

    @Test("execute() returns empty when no matches")
    func executeReturnsEmptyWhenNoMatches() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")
        let nonExistent = uniqueID("NonExistent")

        let edge = makeEdge(source: alice, predicate: "knows", target: uniqueID("Bob"))

        context.insert(edge)
        try await context.save()

        // Test: Query with non-existent source
        let edges = try await context.graph(GraphQueryTestEdge.self)
            .defaultIndex()
            .from(nonExistent)
            .execute()

        #expect(edges.isEmpty)
    }

    @Test("execute() respects limit")
    func executeRespectsLimit() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        try await setIndexStatesToReadable(container: container)

        let context = FDBContext(container: container)

        let alice = uniqueID("Alice")

        // Setup: Save many edges
        for i in 1...10 {
            let edge = makeEdge(source: alice, predicate: "knows", target: uniqueID("Person\(i)"))
            context.insert(edge)
        }
        try await context.save()

        // Test: Query with limit
        let edges = try await context.graph(GraphQueryTestEdge.self)
            .defaultIndex()
            .from(alice)
            .limit(5)
            .execute()

        #expect(edges.count == 5)
    }

    // MARK: - Error Cases

    @Test("execute() throws when index not found")
    func executeThrowsWhenIndexNotFound() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = FDBContext(container: container)

        // Test: Using field combination that doesn't have an index should throw
        // The actual index is defined on (source, predicate, target), not (id, source, target)
        await #expect(throws: GraphQueryError.self) {
            _ = try await context.graph(GraphQueryTestEdge.self)
                .index(\.id, \.source, \.target)
                .from("Alice")
                .execute()
        }
    }
}
