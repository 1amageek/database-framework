// PropertyPathAdvancedTests.swift
// GraphIndexTests - Advanced tests for SPARQL Property Paths
//
// Coverage: Negated property sets, complex quantifiers, cycle limits, inverse+quantifier, large graphs

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex
@testable import QueryAST

// Disambiguate PropertyPath - use GraphIndex version
typealias PropertyPath = GraphIndex.PropertyPath

// MARK: - Test Model

@Persistable
struct AdvancedPathEdge {
    #Directory<AdvancedPathEdge>("test", "sparql", "advancedpath")
    var id: String = UUID().uuidString
    var from: String = ""
    var relationship: String = ""
    var to: String = ""

    #Index(GraphIndexKind<AdvancedPathEdge>(
        from: \.from,
        edge: \.relationship,
        to: \.to,
        strategy: .tripleStore
    ))
}

// MARK: - Test Suite

@Suite("Property Path Advanced Tests", .serialized)
struct PropertyPathAdvancedTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Helpers

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> FDBContainer {
        let database = try FDBClient.openDatabase()
        let schema = Schema([AdvancedPathEdge.self], version: Schema.Version(1, 0, 0))
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func setIndexStatesToReadable(container: FDBContainer) async throws {
        let subspace = try await container.resolveDirectory(for: AdvancedPathEdge.self)
        let indexStateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in AdvancedPathEdge.indexDescriptors {
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

    private func insertEdges(_ edges: [AdvancedPathEdge], context: FDBContext) async throws {
        for edge in edges {
            context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(from: String, relationship: String, to: String) -> AdvancedPathEdge {
        var edge = AdvancedPathEdge()
        edge.from = from
        edge.relationship = relationship
        edge.to = to
        return edge
    }

    // MARK: - Negated Property Set Tests

    @Test("Negated property set - basic")
    func testNegatedPropertySetBasic() async throws {
        // SPARQL: SELECT ?s ?o WHERE { ?s !(ex:knows|ex:hates) ?o }
        // Match any edge that is NOT knows or hates

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let knowsPred = uniqueID("knows")
        let hatesPred = uniqueID("hates")
        let likesPred = uniqueID("likes")
        let worksPred = uniqueID("worksWith")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: hatesPred, to: carol),
            makeEdge(from: alice, relationship: likesPred, to: dave),
            makeEdge(from: alice, relationship: worksPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Negated property set: exclude knows and hates
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(alice, path: .negatedPropertySet([knowsPred, hatesPred]), "?target")
            .execute()

        // Should only find targets via likes and worksWith
        #expect(result.count == 2)
        let targets = result.bindings.compactMap { $0.string("?target") }
        #expect(targets.contains(dave))  // via likes
        #expect(targets.contains(bob))   // via worksWith
    }

    @Test("Negated property set - empty result")
    func testNegatedPropertySetEmpty() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let knowsPred = uniqueID("knows")
        let likesPred = uniqueID("likes")

        let edges = [
            makeEdge(from: alice, relationship: knowsPred, to: bob),
            makeEdge(from: alice, relationship: likesPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Negated property set that excludes all edges
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(alice, path: .negatedPropertySet([knowsPred, likesPred]), "?target")
            .execute()

        #expect(result.isEmpty)
    }

    // MARK: - Property Path with FILTER Tests

    @Test("Property path with FILTER on result")
    func testPropertyPathWithFilter() async throws {
        // SPARQL: SELECT ?ancestor WHERE {
        //   :Alice :parent+ ?ancestor .
        //   FILTER(?ancestor != :Alice)
        // }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let parentPred = uniqueID("parent")

        let edges = [
            makeEdge(from: alice, relationship: parentPred, to: bob),
            makeEdge(from: bob, relationship: parentPred, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // One or more path to find ancestors
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(alice, path: .oneOrMore(.iri(parentPred)), "?ancestor")
            .execute()

        // Filter out Alice (won't appear with + anyway, but test the path)
        #expect(result.count == 2)
        let ancestors = result.bindings.compactMap { $0.string("?ancestor") }
        #expect(!ancestors.contains(alice))
        #expect(ancestors.contains(bob))
        #expect(ancestors.contains(carol))
    }

    // MARK: - Complex Quantifier Tests

    @Test("Complex quantifier: (a/b+)*")
    func testComplexQuantifierSequenceOneOrMore() async throws {
        // SPARQL: SELECT ?x ?y WHERE { ?x (:a/:b+)* ?y }
        // Path: (sequence of a then one-or-more b), repeated zero or more times

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let n4 = uniqueID("N4")
        let predA = uniqueID("a")
        let predB = uniqueID("b")

        // N1 -a-> N2 -b-> N3 -b-> N4
        let edges = [
            makeEdge(from: n1, relationship: predA, to: n2),
            makeEdge(from: n2, relationship: predB, to: n3),
            makeEdge(from: n3, relationship: predB, to: n4),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a / b+)*
        let innerPath = PropertyPath.sequence(.iri(predA), .oneOrMore(.iri(predB)))
        let complexPath = PropertyPath.zeroOrMore(innerPath)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(n1, path: complexPath, "?target")
            .execute()

        // Zero-or-more includes N1 itself (zero repetitions)
        let targets = result.bindings.compactMap { $0.string("?target") }
        #expect(targets.contains(n1))  // Zero repetitions: start node
        // After one iteration of (a/b+): N1 -a-> N2 -b-> N3 and N2 -b-> N3 -b-> N4
        #expect(targets.contains(n3))  // One iteration: a then b (one hop)
        #expect(targets.contains(n4))  // One iteration: a then b+ (two hops)
    }

    @Test("Transitive closure on linear chain (link+)")
    func testTransitiveClosureLinearChain() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let n0 = uniqueID("N0")
        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let n4 = uniqueID("N4")
        let n5 = uniqueID("N5")
        let linkPred = uniqueID("link")

        // Linear chain: N0 -> N1 -> N2 -> N3 -> N4 -> N5
        let edges = [
            makeEdge(from: n0, relationship: linkPred, to: n1),
            makeEdge(from: n1, relationship: linkPred, to: n2),
            makeEdge(from: n2, relationship: linkPred, to: n3),
            makeEdge(from: n3, relationship: linkPred, to: n4),
            makeEdge(from: n4, relationship: linkPred, to: n5),
        ]

        try await insertEdges(edges, context: context)

        // Transitive closure: link+ - one or more hops
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(n0, path: .oneOrMore(.iri(linkPred)), "?target")
            .execute()

        let targets = result.bindings.compactMap { $0.string("?target") }
        // Should find exactly all 5 reachable nodes (N1-N5), not N0 itself (oneOrMore excludes start)
        #expect(targets.count == 5)
        #expect(targets.contains(n1))
        #expect(targets.contains(n2))
        #expect(targets.contains(n3))
        #expect(targets.contains(n4))
        #expect(targets.contains(n5))
    }

    // MARK: - Cycle Detection and Depth Limit Tests

    @Test("Cycle detection with max depth")
    func testCycleDetectionMaxDepth() async throws {
        // SPARQL: SELECT ?node WHERE { :start :link{1,10} ?node }
        // Test that cycles don't cause infinite loops

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let linkPred = uniqueID("link")

        // Create a cycle: N1 -> N2 -> N3 -> N1
        let edges = [
            makeEdge(from: n1, relationship: linkPred, to: n2),
            makeEdge(from: n2, relationship: linkPred, to: n3),
            makeEdge(from: n3, relationship: linkPred, to: n1),
        ]

        try await insertEdges(edges, context: context)

        // Transitive closure with cycle - should not loop infinitely
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(n1, path: .oneOrMore(.iri(linkPred)), "?target")
            .execute()

        // With cycle detection, should visit each node at most once
        // N2 at depth 1, N3 at depth 2, N1 at depth 3 (cycle back to start)
        // Each unique node should appear
        let targets = result.bindings.compactMap { $0.string("?target") }
        #expect(targets.contains(n2))
        #expect(targets.contains(n3))
        #expect(targets.contains(n1))  // Can reach N1 via cycle
    }

    @Test("Deep transitive closure without cycle")
    func testDeepTransitiveClosure() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let linkPred = uniqueID("link")
        let basePrefix = uniqueID("N")

        // Create a linear chain of 20 nodes
        var edges: [AdvancedPathEdge] = []
        for i in 0..<20 {
            edges.append(makeEdge(from: "\(basePrefix)-\(i)", relationship: linkPred, to: "\(basePrefix)-\(i+1)"))
        }

        try await insertEdges(edges, context: context)

        // One or more from start
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("\(basePrefix)-0", path: .oneOrMore(.iri(linkPred)), "?target")
            .execute()

        // Should find all 20 nodes (N1 through N20)
        #expect(result.count == 20)
    }

    // MARK: - Inverse Path with Quantifier Tests

    @Test("Inverse with one or more (^parent+)")
    func testInverseWithOneOrMore() async throws {
        // SPARQL: SELECT ?descendant WHERE { ?descendant ^:parent+ :Root }
        // Find all descendants (inverse of parent)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let root = uniqueID("Root")
        let child1 = uniqueID("Child1")
        let child2 = uniqueID("Child2")
        let grandchild = uniqueID("Grandchild")
        let parentPred = uniqueID("parent")

        // Edges represent "X parent Y" (X is child of Y)
        let edges = [
            makeEdge(from: child1, relationship: parentPred, to: root),
            makeEdge(from: child2, relationship: parentPred, to: root),
            makeEdge(from: grandchild, relationship: parentPred, to: child1),
        ]

        try await insertEdges(edges, context: context)

        // ^parent+ from Root finds all descendants
        // Semantically: find ?d where (?d, parent+, Root) exists
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(root, path: .inverse(.oneOrMore(.iri(parentPred))), "?descendant")
            .execute()

        let descendants = result.bindings.compactMap { $0.string("?descendant") }
        #expect(descendants.contains(child1))
        #expect(descendants.contains(child2))
        #expect(descendants.contains(grandchild))
    }

    @Test("Inverse with zero or more (^knows*)")
    func testInverseWithZeroOrMore() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let knowsPred = uniqueID("knows")

        // Bob knows Alice, Carol knows Bob
        let edges = [
            makeEdge(from: bob, relationship: knowsPred, to: alice),
            makeEdge(from: carol, relationship: knowsPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // ^knows* from Alice: find who can reach Alice via inverse knows
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(alice, path: .inverse(.zeroOrMore(.iri(knowsPred))), "?person")
            .execute()

        let persons = result.bindings.compactMap { $0.string("?person") }
        // Zero hops: Alice itself
        // One hop inverse: Bob (Bob knows Alice)
        // Two hops inverse: Carol (Carol knows Bob knows Alice)
        #expect(persons.contains(alice))
        #expect(persons.contains(bob))
        #expect(persons.contains(carol))
    }

    // MARK: - Complex Combined Path Tests

    @Test("Sequence of alternatives")
    func testSequenceOfAlternatives() async throws {
        // SPARQL: SELECT ?x ?z WHERE { ?x (:a|:b)/(:c|:d) ?z }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let start = uniqueID("Start")
        let mid1 = uniqueID("Mid1")
        let mid2 = uniqueID("Mid2")
        let end1 = uniqueID("End1")
        let end2 = uniqueID("End2")
        let predA = uniqueID("a")
        let predB = uniqueID("b")
        let predC = uniqueID("c")
        let predD = uniqueID("d")

        let edges = [
            makeEdge(from: start, relationship: predA, to: mid1),
            makeEdge(from: start, relationship: predB, to: mid2),
            makeEdge(from: mid1, relationship: predC, to: end1),
            makeEdge(from: mid2, relationship: predD, to: end2),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a|b) / (c|d)
        let path = PropertyPath.sequence(
            .alternative(.iri(predA), .iri(predB)),
            .alternative(.iri(predC), .iri(predD))
        )

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(start, path: path, "?end")
            .execute()

        let ends = result.bindings.compactMap { $0.string("?end") }
        #expect(ends.contains(end1))  // via a/c
        #expect(ends.contains(end2))  // via b/d
    }

    @Test("Alternative of sequences")
    func testAlternativeOfSequences() async throws {
        // SPARQL: SELECT ?x ?z WHERE { ?x ((:a/:b)|(:c/:d)) ?z }

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let start = uniqueID("Start")
        let mid1 = uniqueID("Mid1")
        let mid2 = uniqueID("Mid2")
        let end1 = uniqueID("End1")
        let end2 = uniqueID("End2")
        let predA = uniqueID("a")
        let predB = uniqueID("b")
        let predC = uniqueID("c")
        let predD = uniqueID("d")

        let edges = [
            makeEdge(from: start, relationship: predA, to: mid1),
            makeEdge(from: mid1, relationship: predB, to: end1),
            makeEdge(from: start, relationship: predC, to: mid2),
            makeEdge(from: mid2, relationship: predD, to: end2),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a/b) | (c/d)
        let path = PropertyPath.alternative(
            .sequence(.iri(predA), .iri(predB)),
            .sequence(.iri(predC), .iri(predD))
        )

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(start, path: path, "?end")
            .execute()

        let ends = result.bindings.compactMap { $0.string("?end") }
        #expect(ends.contains(end1))  // via a/b
        #expect(ends.contains(end2))  // via c/d
    }

    // MARK: - Zero-Length Path Tests

    @Test("Zero-length path (zeroOrMore with no matches)")
    func testZeroLengthPath() async throws {
        // SPARQL: SELECT ?x WHERE { :node :link* ?x }
        // :node itself should be in the result even if no edges exist

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let node = uniqueID("Node")
        let linkPred = uniqueID("link")

        // No edges from node
        let edges: [AdvancedPathEdge] = []
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(node, path: .zeroOrMore(.iri(linkPred)), "?target")
            .execute()

        // Should include node itself (zero hops)
        let targets = result.bindings.compactMap { $0.string("?target") }
        #expect(targets.contains(node))
    }

    @Test("Zero-length path with existing edges")
    func testZeroLengthPathWithEdges() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let node = uniqueID("Node")
        let target = uniqueID("Target")
        let linkPred = uniqueID("link")

        let edges = [
            makeEdge(from: node, relationship: linkPred, to: target),
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(node, path: .zeroOrMore(.iri(linkPred)), "?x")
            .execute()

        let targets = result.bindings.compactMap { $0.string("?x") }
        #expect(targets.contains(node))    // Zero hops
        #expect(targets.contains(target))  // One hop
    }

    // MARK: - Performance Tests

    @Test("Property path performance on moderate graph (100 nodes)")
    func testPropertyPathPerformance100Nodes() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let linkPred = uniqueID("link")
        let prefix = uniqueID("N")

        // Create a graph with 100 nodes in a binary tree structure
        // Each node i has children 2i+1 and 2i+2 (for i < 50)
        var edges: [AdvancedPathEdge] = []
        for i in 0..<50 {
            let parent = "\(prefix)-\(i)"
            let child1 = "\(prefix)-\(2*i + 1)"
            let child2 = "\(prefix)-\(2*i + 2)"
            edges.append(makeEdge(from: parent, relationship: linkPred, to: child1))
            edges.append(makeEdge(from: parent, relationship: linkPred, to: child2))
        }

        try await insertEdges(edges, context: context)

        // Query all descendants of root
        let startTime = CFAbsoluteTimeGetCurrent()

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("\(prefix)-0", path: .oneOrMore(.iri(linkPred)), "?descendant")
            .execute()

        let elapsed = CFAbsoluteTimeGetCurrent() - startTime

        // Should find 100 descendants (all except root)
        // Binary tree: parents 0-49 have children 2i+1, 2i+2. Max child = 2*49+2 = 100.
        // Total distinct nodes: 0-100 = 101 nodes. oneOrMore excludes root → 100 results.
        #expect(result.count == 100)

        // Performance sanity check (should complete in reasonable time)
        #expect(elapsed < 30.0)  // 30 seconds max
    }

    @Test("Property path with branching factor")
    func testPropertyPathBranchingFactor() async throws {
        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let linkPred = uniqueID("link")
        let prefix = uniqueID("N")

        // Create a graph with high branching factor
        // Root connects to 10 nodes, each of those connects to 5 more
        var edges: [AdvancedPathEdge] = []
        let root = "\(prefix)-root"

        for i in 0..<10 {
            let level1 = "\(prefix)-L1-\(i)"
            edges.append(makeEdge(from: root, relationship: linkPred, to: level1))

            for j in 0..<5 {
                let level2 = "\(prefix)-L2-\(i)-\(j)"
                edges.append(makeEdge(from: level1, relationship: linkPred, to: level2))
            }
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(root, path: .oneOrMore(.iri(linkPred)), "?target")
            .execute()

        // Should find 10 (level 1) + 50 (level 2) = 60 nodes
        #expect(result.count == 60)
    }

    // MARK: - PropertyPath AST Tests

    @Test("PropertyPath description serialization")
    func testPropertyPathSerialization() throws {
        // Simple IRI
        let simple = PropertyPath.iri("http://example.org/knows")
        #expect(simple.description == "http://example.org/knows")

        // Inverse
        let inverse = PropertyPath.inverse(.iri("http://example.org/parent"))
        #expect(inverse.description == "^http://example.org/parent")

        // Sequence
        let sequence = PropertyPath.sequence(
            .iri("http://example.org/knows"),
            .iri("http://example.org/lives")
        )
        #expect(sequence.description == "http://example.org/knows/http://example.org/lives")

        // Alternative
        let alternative = PropertyPath.alternative(
            .iri("http://example.org/knows"),
            .iri("http://example.org/likes")
        )
        #expect(alternative.description == "http://example.org/knows|http://example.org/likes")

        // Quantifiers
        let zeroOrMore = PropertyPath.zeroOrMore(.iri("http://example.org/link"))
        #expect(zeroOrMore.description == "http://example.org/link*")

        let oneOrMore = PropertyPath.oneOrMore(.iri("http://example.org/link"))
        #expect(oneOrMore.description == "http://example.org/link+")

        let zeroOrOne = PropertyPath.zeroOrOne(.iri("http://example.org/link"))
        #expect(zeroOrOne.description == "http://example.org/link?")

        // Negation
        let negation = PropertyPath.negatedPropertySet(["http://example.org/knows", "http://example.org/hates"])
        #expect(negation.description.contains("!"))
    }

    @Test("PropertyPath complexity estimate")
    func testPropertyPathComplexity() throws {
        // Simple IRI: cost = 1
        #expect(PropertyPath.iri("test").complexityEstimate == 1)

        // Negated property set: cost = 10 (requires scanning all edges)
        #expect(PropertyPath.negatedPropertySet(["a", "b"]).complexityEstimate == 10)

        // Inverse: inner + 1
        #expect(PropertyPath.inverse(.iri("test")).complexityEstimate == 2)

        // Sequence: sum of parts (1 + 1 = 2)
        let sequence = PropertyPath.sequence(.iri("a"), .iri("b"))
        #expect(sequence.complexityEstimate == 2)

        // Alternative: sum of parts (1 + 1 = 2)
        let alternative = PropertyPath.alternative(.iri("a"), .iri("b"))
        #expect(alternative.complexityEstimate == 2)

        // Recursive paths: inner * 100
        #expect(PropertyPath.oneOrMore(.iri("test")).complexityEstimate == 100)
        #expect(PropertyPath.zeroOrMore(.iri("test")).complexityEstimate == 100)

        // ZeroOrOne: inner + 1
        #expect(PropertyPath.zeroOrOne(.iri("test")).complexityEstimate == 2)

        // Complex nested: zeroOrMore(sequence(a, alternative(b, oneOrMore(c))))
        // = (1 + (1 + 1*100)) * 100 = 10200
        let complex = PropertyPath.zeroOrMore(
            .sequence(.iri("a"), .alternative(.iri("b"), .oneOrMore(.iri("c"))))
        )
        #expect(complex.complexityEstimate == 10200)
    }

    @Test("PropertyPath normalization")
    func testPropertyPathNormalization() throws {
        // Double inverse should normalize to original
        let doubleInverse = PropertyPath.inverse(.inverse(.iri("test")))
        #expect(doubleInverse.normalized() == .iri("test"))

        // Triple inverse should normalize to single inverse
        let tripleInverse = PropertyPath.inverse(.inverse(.inverse(.iri("test"))))
        #expect(tripleInverse.normalized() == .inverse(.iri("test")))

        // Non-inverse paths stay the same
        let sequence = PropertyPath.sequence(.iri("a"), .iri("b"))
        #expect(sequence.normalized() == sequence)

        // Alternative flattening: left-associative → right-associative
        // (a|b)|c should normalize to a|(b|c)
        let leftAssoc = PropertyPath.alternative(
            .alternative(.iri("a"), .iri("b")),
            .iri("c")
        )
        let rightAssoc = PropertyPath.alternative(
            .iri("a"),
            .alternative(.iri("b"), .iri("c"))
        )
        #expect(leftAssoc.normalized() == rightAssoc)

        // Nested alternatives: ((a|b)|(c|d)) → a|(b|(c|d))
        let nested = PropertyPath.alternative(
            .alternative(.iri("a"), .iri("b")),
            .alternative(.iri("c"), .iri("d"))
        )
        let nestedExpected = PropertyPath.alternative(
            .iri("a"),
            .alternative(
                .iri("b"),
                .alternative(.iri("c"), .iri("d"))
            )
        )
        #expect(nested.normalized() == nestedExpected)

        // Already right-associative stays the same
        let alreadyRight = PropertyPath.alternative(
            .iri("x"),
            .alternative(.iri("y"), .iri("z"))
        )
        #expect(alreadyRight.normalized() == alreadyRight)

        // Inverse over sequence: ^(a/b) = (^b)/(^a)
        let inverseSeq = PropertyPath.inverse(.sequence(.iri("a"), .iri("b")))
        let expectedInverseSeq = PropertyPath.sequence(.inverse(.iri("b")), .inverse(.iri("a")))
        #expect(inverseSeq.normalized() == expectedInverseSeq)

        // Inverse over alternative: ^(a|b) = (^a)|(^b)
        let inverseAlt = PropertyPath.inverse(.alternative(.iri("a"), .iri("b")))
        let expectedInverseAlt = PropertyPath.alternative(.inverse(.iri("a")), .inverse(.iri("b")))
        #expect(inverseAlt.normalized() == expectedInverseAlt)
    }

    @Test("PropertyPath allIRIs extraction")
    func testPropertyPathAllIRIs() throws {
        let path = PropertyPath.sequence(
            .alternative(.iri("a"), .iri("b")),
            .oneOrMore(.inverse(.iri("c")))
        )

        let iris = path.allIRIs
        #expect(iris.contains("a"))
        #expect(iris.contains("b"))
        #expect(iris.contains("c"))
        #expect(iris.count == 3)
    }

    // MARK: - BFS Origin Tracking Tests

    @Test("BFS transitive: unbound subject + bound object returns origin node (linear chain)")
    func testBFSOriginTrackingLinearChain() async throws {
        // Graph: A→B→C (via "link")
        // Query: ?person (link)+ C → should return ?person=A and ?person=B
        // (A reaches C via 2 hops, B reaches C via 1 hop)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("?person", path: .oneOrMore(.iri(link)), c)
            .execute()

        let persons = Set(result.bindings.compactMap { $0.string("?person") })
        // Both A and B can reach C
        #expect(persons.contains(a), "A should reach C via 2 hops")
        #expect(persons.contains(b), "B should reach C via 1 hop")
        #expect(persons.count == 2)
    }

    @Test("BFS transitive: unbound subject + bound object with branching graph")
    func testBFSOriginTrackingBranching() async throws {
        // Graph: A→B→D, C→D (via "link")
        // Query: ?x (link)+ D → should return ?x=A, ?x=B, ?x=C

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: b, relationship: link, to: d),
            makeEdge(from: c, relationship: link, to: d),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("?x", path: .oneOrMore(.iri(link)), d)
            .execute()

        let xs = Set(result.bindings.compactMap { $0.string("?x") })
        #expect(xs.contains(a), "A reaches D via A→B→D")
        #expect(xs.contains(b), "B reaches D via B→D")
        #expect(xs.contains(c), "C reaches D via C→D")
        #expect(xs.count == 3)
    }

    @Test("BFS transitive: bound subject + unbound object still works (regression)")
    func testBFSBoundSubjectUnboundObject() async throws {
        // Graph: A→B→C (via "link")
        // Query: A (link)+ ?target → should return ?target=B and ?target=C

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(a, path: .oneOrMore(.iri(link)), "?target")
            .execute()

        let targets = Set(result.bindings.compactMap { $0.string("?target") })
        #expect(targets.contains(b), "B is reachable from A")
        #expect(targets.contains(c), "C is reachable from A via B")
        #expect(targets.count == 2)
    }

    @Test("BFS transitive: unbound subject + unbound object preserves origin at depth 2+ (C1 fix)")
    func testBFSUnboundSubjectUnboundObject() async throws {
        // Graph: A→B→C (via "link")
        // Query: ?s (link)+ ?o → should return:
        //   {?s=A, ?o=B} (depth 1)
        //   {?s=B, ?o=C} (depth 1)
        //   {?s=A, ?o=C} (depth 2 — C1 bug: before fix, ?s was missing or wrong)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("?s", path: .oneOrMore(.iri(link)), "?o")
            .execute()

        // Collect all (subject, object) pairs
        let pairs = result.bindings.compactMap { binding -> (String, String)? in
            guard let s = binding.string("?s"), let o = binding.string("?o") else { return nil }
            return (s, o)
        }

        // Verify all expected pairs exist
        let pairSet = Set(pairs.map { "\($0.0)|\($0.1)" })
        #expect(pairSet.contains("\(a)|\(b)"), "A→B should be found (depth 1)")
        #expect(pairSet.contains("\(b)|\(c)"), "B→C should be found (depth 1)")
        #expect(pairSet.contains("\(a)|\(c)"), "A→C should be found (depth 2, origin=A not B)")
        #expect(pairs.count == 3, "Exactly 3 pairs expected")

        // Verify no result has ?s missing
        for binding in result.bindings {
            #expect(binding.string("?s") != nil, "Every result must have ?s bound")
            #expect(binding.string("?o") != nil, "Every result must have ?o bound")
        }
    }

    @Test("BFS transitive: unbound subject + unbound object branching (C1 fix)")
    func testBFSUnboundSubjectUnboundObjectBranching() async throws {
        // Graph: A→B→D, A→C→D (via "link")
        // Query: ?s (link)+ ?o → should include {?s=A, ?o=D} (reachable via both paths)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: a, relationship: link, to: c),
            makeEdge(from: b, relationship: link, to: d),
            makeEdge(from: c, relationship: link, to: d),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath("?s", path: .oneOrMore(.iri(link)), "?o")
            .execute()

        let pairs = result.bindings.compactMap { binding -> (String, String)? in
            guard let s = binding.string("?s"), let o = binding.string("?o") else { return nil }
            return (s, o)
        }
        let pairSet = Set(pairs.map { "\($0.0)|\($0.1)" })

        // Depth 1 results
        #expect(pairSet.contains("\(a)|\(b)"), "A→B depth 1")
        #expect(pairSet.contains("\(a)|\(c)"), "A→C depth 1")
        #expect(pairSet.contains("\(b)|\(d)"), "B→D depth 1")
        #expect(pairSet.contains("\(c)|\(d)"), "C→D depth 1")
        // Depth 2 result — the C1 bug: ?s must be A (origin), not B or C
        #expect(pairSet.contains("\(a)|\(d)"), "A→D depth 2 (origin must be A)")

        // Verify no result has ?s missing
        for binding in result.bindings {
            #expect(binding.string("?s") != nil, "Every result must have ?s bound")
        }
    }

    @Test("BFS transitive: bound subject + bound object (regression)")
    func testBFSBoundSubjectBoundObject() async throws {
        // Graph: A→B→C (via "link")
        // Query: A (link)+ C → should match (A can reach C)

        let container = try await setupContainer()
        try await setIndexStatesToReadable(container: container)
        let context = container.newContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = uniqueID("link")

        let edges = [
            makeEdge(from: a, relationship: link, to: b),
            makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(a, path: .oneOrMore(.iri(link)), c)
            .execute()

        // A can reach C → at least one result (empty binding since both are bound)
        #expect(!result.isEmpty, "A should reach C via A→B→C")
    }
}
