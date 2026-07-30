#if FOUNDATION_DB
// PropertyPathTests.swift
// GraphIndexTests - Tests for SPARQL Property Paths

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Test Model

@Persistable
struct EdgeForPropertyPath {
    #Directory<EdgeForPropertyPath>("property_path_tests")
    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var relationship: RDFTerm = .iri(.xsdString)
    var to: RDFTerm = .iri(.xsdString)

    #Index(
        .rdfDataset,
        from: \EdgeForPropertyPath.from,
        edge: \EdgeForPropertyPath.relationship,
        to: \EdgeForPropertyPath.to
    )
}

// MARK: - Test Suite

@Suite("SPARQL Property Path Tests", .serialized, .foundationDBScenario, .heartbeat)
struct PropertyPathTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "https://example.invalid/node/\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func predicateIRI(_ localName: String) throws -> RDFPredicateIRI {
        try RDFPredicateIRI("https://example.invalid/property/\(localName)")
    }

    private func uniquePredicate(_ prefix: String) throws -> RDFPredicateIRI {
        try RDFPredicateIRI(
            "https://example.invalid/property/\(prefix)-\(UUID().uuidString.prefix(8))"
        )
    }

    private func setupContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [try EdgeForPropertyPath.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(EdgeForPropertyPath.self)]),
            security: .disabled,
        )
    }

    private func insertEdges(_ edges: [EdgeForPropertyPath], context: DatabaseContext) async throws {
        for edge in edges {
            try context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(
        from: String,
        relationship: RDFPredicateIRI,
        to: String
    ) throws -> EdgeForPropertyPath {
        var edge = EdgeForPropertyPath()
        edge.from = try .iri(validating: from)
        edge.relationship = relationship.term
        edge.to = try .iri(validating: to)
        return edge
    }

    private func iriTerm(_ value: String) throws -> ExecutionTerm {
        .value(.rdfTerm(try .iri(validating: value)))
    }

    private func iriValue(
        _ binding: VariableBinding,
        for variable: String
    ) -> String? {
        guard case .rdfTerm(.iri(let value)) = binding[variable] else {
            return nil
        }
        return value.rawValue
    }

    // MARK: - Simple IRI Path Tests

    @Test("Simple IRI path")
    func testSimpleIRIPath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = try uniquePredicate("knows")

        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: alice, relationship: predicate, to: carol),
            try makeEdge(from: bob, relationship: predicate, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Simple IRI path: Alice knows ?friend
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(try iriTerm(alice), path: .iri(predicate), .variable("?friend"))
            .execute()

        #expect(result.count == 2)
        let friends = result.bindings.compactMap { iriValue($0, for: "?friend") }
        #expect(friends.contains(bob))
        #expect(friends.contains(carol))
    }

    // MARK: - Inverse Path Tests

    @Test("Inverse path (^)")
    func testInversePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = try uniquePredicate("knows")

        // Create edges: Alice knows Bob, Carol knows Bob
        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: carol, relationship: predicate, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // SPARQL semantics: ?s ^p ?o matches if (?o, p, ?s) exists
        // So Bob ^knows ?person means: find ?person where (?person, knows, Bob) exists
        // This should return Alice and Carol who know Bob
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(try iriTerm(bob), path: .inverse(.iri(predicate)), .variable("?person"))
            .execute()

        #expect(result.count == 2)
        let persons = result.bindings.compactMap { iriValue($0, for: "?person") }
        #expect(persons.contains(alice))
        #expect(persons.contains(carol))
    }

    // MARK: - Sequence Path Tests

    @Test("Sequence path (/)")
    func testSequencePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let predicate = try uniquePredicate("knows")

        // Alice -> Bob -> Carol, Dave
        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: bob, relationship: predicate, to: carol),
            try makeEdge(from: bob, relationship: predicate, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // Sequence path: Alice knows/knows ?fof (friends of friends)
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .sequence(.iri(predicate), .iri(predicate)),
                .variable("?fof")
            )
            .execute()

        #expect(result.count == 2)
        let fofs = result.bindings.compactMap { iriValue($0, for: "?fof") }
        #expect(fofs.contains(carol))
        #expect(fofs.contains(dave))
    }

    // MARK: - Alternative Path Tests

    @Test("Alternative path (|)")
    func testAlternativePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let knowsPred = try uniquePredicate("knows")
        let likesPred = try uniquePredicate("likes")

        let edges = [
            try makeEdge(from: alice, relationship: knowsPred, to: bob),
            try makeEdge(from: alice, relationship: likesPred, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Alternative: Alice (knows|likes) ?related
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .alternative(.iri(knowsPred), .iri(likesPred)),
                .variable("?related")
            )
            .execute()

        #expect(result.count == 2)
        let related = result.bindings.compactMap { iriValue($0, for: "?related") }
        #expect(related.contains(bob))
        #expect(related.contains(carol))
    }

    // MARK: - Transitive Path Tests

    @Test("One or more path (+)")
    func testOneOrMorePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let predicate = try uniquePredicate("parentOf")

        // Linear chain: Alice -> Bob -> Carol -> Dave
        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: bob, relationship: predicate, to: carol),
            try makeEdge(from: carol, relationship: predicate, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // One or more: Alice parentOf+ ?descendant
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .oneOrMore(.iri(predicate)),
                .variable("?descendant")
            )
            .execute()

        // Should find Bob (1 hop), Carol (2 hops), Dave (3 hops)
        #expect(result.count == 3)
        let descendants = result.bindings.compactMap { iriValue($0, for: "?descendant") }
        #expect(descendants.contains(bob))
        #expect(descendants.contains(carol))
        #expect(descendants.contains(dave))
    }

    @Test("Zero or more path (*)")
    func testZeroOrMorePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = try uniquePredicate("parentOf")

        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: bob, relationship: predicate, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // Zero or more: Alice parentOf* ?descendant
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .zeroOrMore(.iri(predicate)),
                .variable("?descendant")
            )
            .execute()

        // Should include Alice (0 hops), Bob (1 hop), Carol (2 hops)
        #expect(result.count == 3)
        let descendants = result.bindings.compactMap { iriValue($0, for: "?descendant") }
        #expect(descendants.contains(alice))  // Zero hop = self
        #expect(descendants.contains(bob))
        #expect(descendants.contains(carol))
    }

    @Test("Zero or one path (?)")
    func testZeroOrOnePath() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let predicate = try uniquePredicate("knows")

        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Zero or one: Alice knows? ?target
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .zeroOrOne(.iri(predicate)),
                .variable("?target")
            )
            .execute()

        // Should include Alice (0 hops) and Bob (1 hop)
        #expect(result.count == 2)
        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
        #expect(targets.contains(alice))  // Zero hop = self
        #expect(targets.contains(bob))
    }

    // MARK: - Cycle Detection Tests

    @Test("Transitive path with cycle")
    func testTransitivePathWithCycle() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let predicate = try uniquePredicate("knows")

        // Create a cycle: Alice -> Bob -> Carol -> Alice
        let edges = [
            try makeEdge(from: alice, relationship: predicate, to: bob),
            try makeEdge(from: bob, relationship: predicate, to: carol),
            try makeEdge(from: carol, relationship: predicate, to: alice),
        ]

        try await insertEdges(edges, context: context)

        // One or more with cycle detection
        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .oneOrMore(.iri(predicate)),
                .variable("?reachable")
            )
            .execute()

        // Should find Bob, Carol, Alice (loop back) - each only once
        #expect(result.count == 3)
        let reachable = result.bindings.compactMap { iriValue($0, for: "?reachable") }
        #expect(reachable.contains(bob))
        #expect(reachable.contains(carol))
        #expect(reachable.contains(alice))
    }

    // MARK: - Complex Path Tests

    @Test("Combined path: sequence of alternatives")
    func testCombinedSequenceAlternative() async throws {
        let container = try await setupContainer()

        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let knowsPred = try uniquePredicate("knows")
        let likesPred = try uniquePredicate("likes")
        let worksWithPred = try uniquePredicate("worksWith")

        let edges = [
            try makeEdge(from: alice, relationship: knowsPred, to: bob),
            try makeEdge(from: alice, relationship: likesPred, to: carol),
            try makeEdge(from: bob, relationship: worksWithPred, to: dave),
            try makeEdge(from: carol, relationship: worksWithPred, to: dave),
        ]

        try await insertEdges(edges, context: context)

        // Path: (knows|likes) / worksWith
        let path = ExecutionPropertyPath.sequence(
            .alternative(.iri(knowsPred), .iri(likesPred)),
            .iri(worksWithPred)
        )

        let result = try await context.sparql(EdgeForPropertyPath.self)
            .defaultIndex()
            .wherePath(try iriTerm(alice), path: path, .variable("?colleague"))
            .execute()

        // Both paths lead to Dave
        #expect(result.count >= 1)
        let colleagues = result.bindings.compactMap { iriValue($0, for: "?colleague") }
        #expect(colleagues.contains(dave))
    }

    // MARK: - Property Path Type Tests

    @Test("PropertyPath type operations")
    func testPropertyPathOperations() async throws {
        let test = try predicateIRI("test")
        let a = try predicateIRI("a")
        let b = try predicateIRI("b")
        let c = try predicateIRI("c")
        let knows = try predicateIRI("knows")

        // Test isRecursive
        #expect(ExecutionPropertyPath.iri(test).isRecursive == false)
        #expect(ExecutionPropertyPath.oneOrMore(.iri(test)).isRecursive == true)
        #expect(ExecutionPropertyPath.zeroOrMore(.iri(test)).isRecursive == true)
        #expect(ExecutionPropertyPath.zeroOrOne(.iri(test)).isRecursive == false)

        // Test isSimpleIRI
        #expect(ExecutionPropertyPath.iri(test).isSimpleIRI == true)
        #expect(ExecutionPropertyPath.inverse(.iri(test)).isSimpleIRI == false)

        // Test simpleIRI
        #expect(ExecutionPropertyPath.iri(test).simpleIRI == test)
        #expect(ExecutionPropertyPath.inverse(.iri(test)).simpleIRI == nil)

        // Test allIRIs
        let path = ExecutionPropertyPath.sequence(.iri(a), .alternative(.iri(b), .iri(c)))
        #expect(path.allIRIs == Set([a, b, c]))

        // Test complexityEstimate
        #expect(ExecutionPropertyPath.iri(test).complexityEstimate == 1)
        #expect(ExecutionPropertyPath.oneOrMore(.iri(test)).complexityEstimate > 1)

        // Test normalization (double inverse)
        let doubleInverse = ExecutionPropertyPath.inverse(.inverse(.iri(test)))
        #expect(doubleInverse.normalized() == .iri(test))

        // Test description
        #expect(ExecutionPropertyPath.iri(knows).description == knows.rawValue)
        #expect(ExecutionPropertyPath.inverse(.iri(knows)).description == "^\(knows.rawValue)")
        #expect(ExecutionPropertyPath.oneOrMore(.iri(knows)).description == "\(knows.rawValue)+")
    }

    @Test("PropertyPath builder methods")
    func testPropertyPathBuilders() async throws {
        let knowsIRI = try predicateIRI("knows")
        let worksAtIRI = try predicateIRI("worksAt")
        let likesIRI = try predicateIRI("likes")
        let knows = ExecutionPropertyPath.iri(knowsIRI)

        // Test inverted()
        #expect(knows.inverted() == .inverse(.iri(knowsIRI)))

        // Test then()
        let worksAt = ExecutionPropertyPath.iri(worksAtIRI)
        #expect(knows.then(worksAt) == .sequence(.iri(knowsIRI), .iri(worksAtIRI)))

        // Test or()
        let likes = ExecutionPropertyPath.iri(likesIRI)
        #expect(knows.or(likes) == .alternative(.iri(knowsIRI), .iri(likesIRI)))

        // Test star(), plus(), optional()
        #expect(knows.star() == .zeroOrMore(.iri(knowsIRI)))
        #expect(knows.plus() == .oneOrMore(.iri(knowsIRI)))
        #expect(knows.optional() == .zeroOrOne(.iri(knowsIRI)))
    }
}
#endif
