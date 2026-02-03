// GraphQueryPropertyFilterTests.swift
// Tests for GraphQuery property filtering integration

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import TestSupport
@testable import GraphIndex

@Suite("GraphQuery Property Filter Tests", .serialized)
struct GraphQueryPropertyFilterTests {

    // MARK: - Test Model

    @Persistable
    struct SocialEdge {
        #Directory<SocialEdge>("test", "social_edges_query")

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
        ), storedFields: [\SocialEdge.since, \SocialEdge.status, \SocialEdge.score], name: "social_graph_query_index")
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
        try? await directoryLayer.remove(path: ["test", "social_edges_query"])

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

    // MARK: - Property Filter Tests

    @Test("Type-safe property filter: equality")
    func testTypeSafeEqualityFilter() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
        try await context.save()

        // Query with property filter
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .where(\.since, .equal, 2020)
            .execute()

        #expect(results.count == 1)
        #expect(results.first?.to == bob)
    }

    @Test("Type-safe property filter: range")
    func testTypeSafeRangeFilter() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")

        for year in [2018, 2019, 2020, 2021, 2022] {
            context.insert(makeEdge(from: alice, target: uniqueID("user-\(year)"), label: "KNOWS", since: year, status: "active", score: 0.5))
        }
        try await context.save()

        // Query with range filter
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .where(\.since, .greaterThanOrEqual, 2020)
            .execute()

        #expect(results.count == 3)
    }

    @Test("Multiple property filters (AND)")
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

        // Query with multiple filters (AND)
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .where(\.since, .equal, 2020)
            .where(\.status, .equal, "active")
            .execute()

        #expect(results.count == 1)
        #expect(results.first?.to == bob)
    }

    @Test("Type-erased property filter (whereRaw)")
    func testTypeErasedPropertyFilter() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2019, status: "inactive", score: 0.5))
        try await context.save()

        // Query with type-erased filter
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .whereRaw(fieldName: "since", .greaterThanOrEqual, 2020)
            .execute()

        #expect(results.count == 1)
        #expect(results.first?.to == bob)
    }

    @Test("Property filter with nil values")
    func testPropertyFilterWithNilValues() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")
        let dave = uniqueID("dave")

        // Create edges with nil, empty string, and non-empty status
        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: nil, score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2020, status: "", score: 0.5))
        context.insert(makeEdge(from: alice, target: dave, label: "KNOWS", since: 2020, status: "active", score: 0.7))
        try await context.save()

        // Filter by non-nil status
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .whereRaw(fieldName: "status", .isNotNil, 0)  // Value ignored for isNotNil
            .execute()

        #expect(results.count == 2)  // Empty string and "active"
    }

    @Test("Backward compatibility: no property filters")
    func testBackwardCompatibility() async throws {
        let container = try await setupContainer()
        let context = FDBContext(container: container)

        let alice = uniqueID("alice")
        let bob = uniqueID("bob")
        let carol = uniqueID("carol")

        context.insert(makeEdge(from: alice, target: bob, label: "KNOWS", since: 2020, status: "active", score: 0.9))
        context.insert(makeEdge(from: alice, target: carol, label: "KNOWS", since: 2021, status: "inactive", score: 0.5))
        try await context.save()

        // Query without property filters (should work as before)
        let results = try await context.graph(SocialEdge.self)
            .defaultIndex()
            .from(alice)
            .edge("KNOWS")
            .execute()

        #expect(results.count == 2)
    }
}
