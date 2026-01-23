// PropertyPathTests.swift
// GraphIndexTests - Tests for SPARQL Property Paths

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct EdgeForPropertyPath {
    #Directory<EdgeForPropertyPath>("test", "sparql", "propertypath")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""

    #Index(GraphIndexKind<EdgeForPropertyPath>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("SPARQL Property Path Tests", .serialized)
struct PropertyPathTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([EdgeForPropertyPath.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: EdgeForPropertyPath.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in EdgeForPropertyPath.indexDescriptors {
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

    private func insertEdges(_ edges: [EdgeForPropertyPath], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> EdgeForPropertyPath {
        var edge = EdgeForPropertyPath()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    // MARK: - Simple IRI Path Tests

    @Test("Simple IRI path")
    func testSimpleIRIPath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = uniqueID("knows")

        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: alice, relationship: predicate, to: carol),
            makeEdge(from: bob, relationship: predicate, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Simple IRI path: Alice knows ?friend
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .iri(predicate), "?friend")
            .execute()

        #expect(result.count == 2)
        let friends = result.bindings.compactMap { $0["?friend"] }
        #expect(friends.contains(bob))
        #expect(friends.contains(carol))
    }

    // MARK: - Inverse Path Tests

    @Test("Inverse path (^)")
    func testInversePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = uniqueID("knows")

        // Create edges: Alice knows Bob, Carol knows Bob
        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: carol, relationship: predicate, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // SPARQL semantics: ?s ^p ?o matches if (?o, p, ?s) exists
        // So Bob ^knows ?person means: find ?person where (?person, knows, Bob) exists
        // This should return Alice and Carol who know Bob
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(bob, path: .inverse(.iri(predicate)), "?person")
            .execute()

        #expect(result.count == 2)
        let persons = result.bindings.compactMap { $0["?person"] }
        #expect(persons.contains(alice))
        #expect(persons.contains(carol))
    }

    // MARK: - Sequence Path Tests

    @Test("Sequence path (/)")
    func testSequencePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let predicate = uniqueID("knows")

        // Alice -> Bob -> Carol, Dave
        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: bob, relationship: predicate, to: carol),
            makeEdge(from: bob, relationship: predicate, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // Sequence path: Alice knows/knows ?fof (friends of friends)
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .sequence(.iri(predicate), .iri(predicate)), "?fof")
            .execute()

        #expect(result.count == 2)
        let fofs = result.bindings.compactMap { $0["?fof"] }
        #expect(fofs.contains(carol))
        #expect(fofs.contains(dave))
    }

    // MARK: - Alternative Path Tests

    @Test("Alternative path (|)")
    func testAlternativePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let knowsPred = uniqueID("knows")
        let likesPred = uniqueID("likes")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: likesPred, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Alternative: Alice (knows|likes) ?related
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .alternative(.iri(knowsPred), .iri(likesPred)), "?related")
            .execute()

        #expect(result.count == 2)
        let related = result.bindings.compactMap { $0["?related"] }
        #expect(related.contains(bob))
        #expect(related.contains(carol))
    }

    // MARK: - Transitive Path Tests

    @Test("One or more path (+)")
    func testOneOrMorePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let predicate = uniqueID("parentOf")

        // Linear chain: Alice -> Bob -> Carol -> Dave
        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: bob, relationship: predicate, to: carol),
            makeEdge(from: carol, relationship: predicate, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // One or more: Alice parentOf+ ?descendant
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .oneOrMore(.iri(predicate)), "?descendant")
            .execute()

        // Should find Bob (1 hop), Carol (2 hops), Dave (3 hops)
        #expect(result.count == 3)
        let descendants = result.bindings.compactMap { $0["?descendant"] }
        #expect(descendants.contains(bob))
        #expect(descendants.contains(carol))
        #expect(descendants.contains(dave))
    }

    @Test("Zero or more path (*)")
    func testZeroOrMorePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = uniqueID("parentOf")

        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: bob, relationship: predicate, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Zero or more: Alice parentOf* ?descendant
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .zeroOrMore(.iri(predicate)), "?descendant")
            .execute()

        // Should include Alice (0 hops), Bob (1 hop), Carol (2 hops)
        #expect(result.count == 3)
        let descendants = result.bindings.compactMap { $0["?descendant"] }
        #expect(descendants.contains(alice))  // Zero hop = self
        #expect(descendants.contains(bob))
        #expect(descendants.contains(carol))
    }

    @Test("Zero or one path (?)")
    func testZeroOrOnePath() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let predicate = uniqueID("knows")

        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Zero or one: Alice knows? ?target
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .zeroOrOne(.iri(predicate)), "?target")
            .execute()

        // Should include Alice (0 hops) and Bob (1 hop)
        #expect(result.count == 2)
        let targets = result.bindings.compactMap { $0["?target"] }
        #expect(targets.contains(alice))  // Zero hop = self
        #expect(targets.contains(bob))
    }

    // MARK: - Cycle Detection Tests

    @Test("Transitive path with cycle")
    func testTransitivePathWithCycle() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = uniqueID("knows")

        // Create a cycle: Alice -> Bob -> Carol -> Alice
        let edges = [
            makeEdge(from: alice, relationship: predicate, to: bob),
            makeEdge(from: bob, relationship: predicate, to: carol),
            makeEdge(from: carol, relationship: predicate, to: alice),
        ]

        try await insertEdges(edges, context: context)

        // One or more with cycle detection
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: .oneOrMore(.iri(predicate)), "?reachable")
            .execute()

        // Should find Bob, Carol, Alice (loop back) - each only once
        #expect(result.count == 3)
        let reachable = result.bindings.compactMap { $0["?reachable"] }
        #expect(reachable.contains(bob))
        #expect(reachable.contains(carol))
        #expect(reachable.contains(alice))
    }

    // MARK: - Complex Path Tests

    @Test("Combined path: sequence of alternatives")
    func testCombinedSequenceAlternative() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let knowsPred = uniqueID("knows")
        let likesPred = uniqueID("likes")
        let worksWithPred = uniqueID("worksWith")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: likesPred, to: carol),
            makeEdge(from: bob, relationship: worksWithPred, to: dave),
            makeEdge(from: carol, relationship: worksWithPred, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // Path: (knows|likes) / worksWith
        let path = PropertyPath.sequence(
            .alternative(.iri(knowsPred), .iri(likesPred)),
            .iri(worksWithPred)
        )

        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(alice, path: path, "?colleague")
            .execute()

        // Both paths lead to Dave
        #expect(result.count >= 1)
        let colleagues = result.bindings.compactMap { $0["?colleague"] }
        #expect(colleagues.contains(dave))
    }

    // MARK: - Property Path Type Tests

    @Test("PropertyPath type operations")
    func testPropertyPathOperations() async throws {
        // Test isRecursive
        #expect(PropertyPath.iri("test").isRecursive == false)
        #expect(PropertyPath.oneOrMore(.iri("test")).isRecursive == true)
        #expect(PropertyPath.zeroOrMore(.iri("test")).isRecursive == true)
        #expect(PropertyPath.zeroOrOne(.iri("test")).isRecursive == true)

        // Test isSimpleIRI
        #expect(PropertyPath.iri("test").isSimpleIRI == true)
        #expect(PropertyPath.inverse(.iri("test")).isSimpleIRI == false)

        // Test simpleIRI
        #expect(PropertyPath.iri("test").simpleIRI == "test")
        #expect(PropertyPath.inverse(.iri("test")).simpleIRI == nil)

        // Test allIRIs
        let path = PropertyPath.sequence(.iri("a"), .alternative(.iri("b"), .iri("c")))
        #expect(path.allIRIs == Set(["a", "b", "c"]))

        // Test complexityEstimate
        #expect(PropertyPath.iri("test").complexityEstimate == 1)
        #expect(PropertyPath.oneOrMore(.iri("test")).complexityEstimate > 1)

        // Test normalization (double inverse)
        let doubleInverse = PropertyPath.inverse(.inverse(.iri("test")))
        #expect(doubleInverse.normalized() == .iri("test"))

        // Test description
        #expect(PropertyPath.iri("knows").description == "knows")
        #expect(PropertyPath.inverse(.iri("knows")).description == "^knows")
        #expect(PropertyPath.oneOrMore(.iri("knows")).description == "knows+")
    }

    @Test("PropertyPath builder methods")
    func testPropertyPathBuilders() async throws {
        let knows = PropertyPath.iri("knows")

        // Test inverted()
        #expect(knows.inverted() == .inverse(.iri("knows")))

        // Test then()
        let worksAt = PropertyPath.iri("worksAt")
        #expect(knows.then(worksAt) == .sequence(.iri("knows"), .iri("worksAt")))

        // Test or()
        let likes = PropertyPath.iri("likes")
        #expect(knows.or(likes) == .alternative(.iri("knows"), .iri("likes")))

        // Test star(), plus(), optional()
        #expect(knows.star() == .zeroOrMore(.iri("knows")))
        #expect(knows.plus() == .oneOrMore(.iri("knows")))
        #expect(knows.optional() == .zeroOrOne(.iri("knows")))
    }
}
