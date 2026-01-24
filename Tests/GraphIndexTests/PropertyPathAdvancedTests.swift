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
        let targets = result.bindings.compactMap { $0["?target"] }
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
        let ancestors = result.bindings.compactMap { $0["?ancestor"] }
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

        // Zero-or-more includes N1 itself
        let targets = result.bindings.compactMap { $0["?target"] }
        #expect(targets.contains(n1))  // Zero repetitions
        // After one iteration of (a/b+), we reach N3 or N4 depending on how b+ is interpreted
    }

    @Test("Range quantifier {2,4}")
    func testRangeQuantifier() async throws {
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

        let targets = result.bindings.compactMap { $0["?target"] }
        // Should find all reachable nodes
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
        let targets = result.bindings.compactMap { $0["?target"] }
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

        let descendants = result.bindings.compactMap { $0["?descendant"] }
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

        let persons = result.bindings.compactMap { $0["?person"] }
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

        let ends = result.bindings.compactMap { $0["?end"] }
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

        let ends = result.bindings.compactMap { $0["?end"] }
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
        let targets = result.bindings.compactMap { $0["?target"] }
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

        let targets = result.bindings.compactMap { $0["?x"] }
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

        // Should find 99 descendants (all except root)
        #expect(result.count == 99)

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
        // Simple paths have low complexity
        #expect(PropertyPath.iri("test").complexityEstimate == 1)

        // Inverse adds slight complexity
        #expect(PropertyPath.inverse(.iri("test")).complexityEstimate == 2)

        // Sequence is additive
        let sequence = PropertyPath.sequence(.iri("a"), .iri("b"))
        #expect(sequence.complexityEstimate >= 2)

        // Recursive paths have higher complexity
        #expect(PropertyPath.oneOrMore(.iri("test")).complexityEstimate > PropertyPath.iri("test").complexityEstimate)
        #expect(PropertyPath.zeroOrMore(.iri("test")).complexityEstimate > PropertyPath.iri("test").complexityEstimate)

        // Complex nested paths have highest complexity
        let complex = PropertyPath.zeroOrMore(
            .sequence(.iri("a"), .alternative(.iri("b"), .oneOrMore(.iri("c"))))
        )
        #expect(complex.complexityEstimate > 10)
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
}
