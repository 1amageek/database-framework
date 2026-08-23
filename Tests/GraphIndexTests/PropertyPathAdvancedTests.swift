#if FOUNDATION_DB
// PropertyPathAdvancedTests.swift
// GraphIndexTests - Advanced tests for SPARQL Property Paths
//
// Coverage: Negated property sets, complex quantifiers, cycle limits, inverse+quantifier, large graphs

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@_spi(DatabaseExecution) @testable import GraphIndex
@testable import QueryAST

// Disambiguate PropertyPath - use GraphIndex version
typealias PropertyPath = GraphIndex.ExecutionPropertyPath

// MARK: - Test Model

@Persistable
struct AdvancedPathEdge {
    #Directory<AdvancedPathEdge>("property_path_advanced", "edges")
    var id: String = UUID().uuidString
    var from: RDFTerm = .iri(.xsdString)
    var relationship: RDFTerm = .iri(.xsdString)
    var to: RDFTerm = .iri(.xsdString)

    #Index(
        .graph(
            name: "AdvancedPathEdge_rdf_quad_from_relationship_to",
            definition: .rdf(
                subject: \AdvancedPathEdge.from, predicate: \AdvancedPathEdge.relationship,
                object: \AdvancedPathEdge.to, graph: nil)))
}

// MARK: - Test Suite

@Suite("Property Path Advanced Tests", .serialized, .foundationDBScenario, .heartbeat)
struct PropertyPathAdvancedTests {

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
            entities: [try AdvancedPathEdge.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(AdvancedPathEdge.self)]),
            security: .testingDisabled,
        )
    }

    private func insertEdges(_ edges: [AdvancedPathEdge], context: DatabaseContext) async throws {
        for edge in edges {
            try context.insert(edge)
        }
        try await context.save()
    }

    private func makeEdge(
        from: String,
        relationship: RDFPredicateIRI,
        to: String
    ) throws -> AdvancedPathEdge {
        var edge = AdvancedPathEdge()
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

    private func execute(
        _ pattern: ExecutionPattern,
        container: DBContainer,
        context: DatabaseContext,
        configuration: ExecutionPropertyPathConfiguration,
        limit: Int? = nil
    ) async throws -> ([VariableBinding], ExecutionStatistics) {
        let selections = try AdvancedPathEdge.indexDescriptors.compactMap(
            RDFDatasetIndexSelection.init(descriptor:)
        )
        guard selections.count == 1 else {
            throw SPARQLQueryError.indexNotConfigured
        }
        let selection = selections[0]
        let workMeter = DatabaseWorkMeter(
            budget: .init(),
            monotonicClock: TestProcessMonotonicClock()
        )
        return try await context.indexQueryContext.withReadableIndex(
            named: selection.indexName,
            indexType: selection.indexType,
            for: AdvancedPathEdge.self,
            authorization: IndexReadAuthorization(
                limit: limit,
                offset: 0,
                orderBy: nil
            )
        ) { readableIndex, transaction in
            guard let readableIndex else {
                throw SPARQLQueryError.indexNotConfigured
            }
            let source = try RDFDatasetSource(
                entityName: AdvancedPathEdge.persistableType,
                selection: selection,
                indexSubspace: readableIndex.subspace
            )
            let executor = SPARQLQueryExecutor(
                monotonicClock: container.monotonicClock,
                wallClock: FixedTestWallClock(),
                sources: [source],
                propertyPathConfiguration: configuration
            )
            return try await executor.executeInTransaction(
                pattern: pattern,
                transaction: transaction,
                limit: limit,
                offset: 0,
                workMeter: workMeter
            )
        }
    }

    // MARK: - Negated Property Set Tests

    @Test("Negated property set - basic")
    func testNegatedPropertySetBasic() async throws {
        // SPARQL: SELECT ?s ?o WHERE { ?s !(ex:knows|ex:hates) ?o }
        // Match any edge that is NOT knows or hates

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let dave = uniqueID("Dave")
        let knowsPred = try uniquePredicate("knows")
        let hatesPred = try uniquePredicate("hates")
        let likesPred = try uniquePredicate("likes")
        let worksPred = try uniquePredicate("worksWith")

        let edges = [
            try makeEdge(from: alice, relationship: knowsPred, to: bob),
            try makeEdge(from: alice, relationship: hatesPred, to: carol),
            try makeEdge(from: alice, relationship: likesPred, to: dave),
            try makeEdge(from: alice, relationship: worksPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Negated property set: exclude knows and hates
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .negatedPropertySet(
                    try PropertyPathNegatedSet(forward: [knowsPred, hatesPred])
                ),
                .variable("?target")
            )
            .execute()

        // Should only find targets via likes and worksWith
        #expect(result.count == 2)
        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
        #expect(targets.contains(dave))  // via likes
        #expect(targets.contains(bob))   // via worksWith
    }

    @Test("Negated property set - empty result")
    func testNegatedPropertySetEmpty() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let knowsPred = try uniquePredicate("knows")
        let likesPred = try uniquePredicate("likes")

        let edges = [
            try makeEdge(from: alice, relationship: knowsPred, to: bob),
            try makeEdge(from: alice, relationship: likesPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // Negated property set that excludes all edges
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .negatedPropertySet(
                    try PropertyPathNegatedSet(forward: [knowsPred, likesPred])
                ),
                .variable("?target")
            )
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

        let context = container.testBaseContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let parentPred = try uniquePredicate("parent")

        let edges = [
            try makeEdge(from: alice, relationship: parentPred, to: bob),
            try makeEdge(from: bob, relationship: parentPred, to: carol),
        ]

        try await insertEdges(edges, context: context)

        // One or more path to find ancestors
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .oneOrMore(.iri(parentPred)),
                .variable("?ancestor")
            )
            .execute()

        // Filter out Alice (won't appear with + anyway, but test the path)
        #expect(result.count == 2)
        let ancestors = result.bindings.compactMap { iriValue($0, for: "?ancestor") }
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

        let context = container.testBaseContext()

        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let n4 = uniqueID("N4")
        let predA = try uniquePredicate("a")
        let predB = try uniquePredicate("b")

        // N1 -a-> N2 -b-> N3 -b-> N4
        let edges = [
            try makeEdge(from: n1, relationship: predA, to: n2),
            try makeEdge(from: n2, relationship: predB, to: n3),
            try makeEdge(from: n3, relationship: predB, to: n4),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a / b+)*
        let innerPath = PropertyPath.sequence(.iri(predA), .oneOrMore(.iri(predB)))
        let complexPath = PropertyPath.zeroOrMore(innerPath)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(try iriTerm(n1), path: complexPath, .variable("?target"))
            .execute()

        // Zero-or-more includes N1 itself (zero repetitions)
        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
        #expect(targets.contains(n1))  // Zero repetitions: start node
        // After one iteration of (a/b+): N1 -a-> N2 -b-> N3 and N2 -b-> N3 -b-> N4
        #expect(targets.contains(n3))  // One iteration: a then b (one hop)
        #expect(targets.contains(n4))  // One iteration: a then b+ (two hops)
    }

    @Test("Transitive closure on linear chain (link+)")
    func testTransitiveClosureLinearChain() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let n0 = uniqueID("N0")
        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let n4 = uniqueID("N4")
        let n5 = uniqueID("N5")
        let linkPred = try uniquePredicate("link")

        // Linear chain: N0 -> N1 -> N2 -> N3 -> N4 -> N5
        let edges = [
            try makeEdge(from: n0, relationship: linkPred, to: n1),
            try makeEdge(from: n1, relationship: linkPred, to: n2),
            try makeEdge(from: n2, relationship: linkPred, to: n3),
            try makeEdge(from: n3, relationship: linkPred, to: n4),
            try makeEdge(from: n4, relationship: linkPred, to: n5),
        ]

        try await insertEdges(edges, context: context)

        // Transitive closure: link+ - one or more hops
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(n0),
                path: .oneOrMore(.iri(linkPred)),
                .variable("?target")
            )
            .execute()

        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
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

        let context = container.testBaseContext()

        let n1 = uniqueID("N1")
        let n2 = uniqueID("N2")
        let n3 = uniqueID("N3")
        let linkPred = try uniquePredicate("link")

        // Create a cycle: N1 -> N2 -> N3 -> N1
        let edges = [
            try makeEdge(from: n1, relationship: linkPred, to: n2),
            try makeEdge(from: n2, relationship: linkPred, to: n3),
            try makeEdge(from: n3, relationship: linkPred, to: n1),
        ]

        try await insertEdges(edges, context: context)

        // Transitive closure with cycle - should not loop infinitely
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(n1),
                path: .oneOrMore(.iri(linkPred)),
                .variable("?target")
            )
            .execute()

        // With cycle detection, should visit each node at most once
        // N2 at depth 1, N3 at depth 2, N1 at depth 3 (cycle back to start)
        // Each unique node should appear
        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
        #expect(targets.contains(n2))
        #expect(targets.contains(n3))
        #expect(targets.contains(n1))  // Can reach N1 via cycle
    }

    @Test("Deep transitive closure without cycle")
    func testDeepTransitiveClosure() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let linkPred = try uniquePredicate("link")
        let basePrefix = uniqueID("N")

        // Create a linear chain of 20 nodes
        var edges: [AdvancedPathEdge] = []
        for i in 0..<20 {
            edges.append(try makeEdge(from: "\(basePrefix)-\(i)", relationship: linkPred, to: "\(basePrefix)-\(i+1)"))
        }

        try await insertEdges(edges, context: context)

        // One or more from start
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm("\(basePrefix)-0"),
                path: .oneOrMore(.iri(linkPred)),
                .variable("?target")
            )
            .execute()

        // Should find all 20 nodes (N1 through N20)
        #expect(result.count == 20)
    }

    @Test("Expression depth limit rejects the whole property-path evaluation")
    func testExpressionDepthLimitRejectsPartialSuccess() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()
        let start = uniqueID("ExpressionDepthStart")
        let target = uniqueID("ExpressionDepthTarget")
        let predicate = try uniquePredicate("expression-depth")
        try await insertEdges(
            [try makeEdge(from: start, relationship: predicate, to: target)],
            context: context
        )

        let pattern = ExecutionPattern.propertyPath(
            subject: try iriTerm(start),
            path: .oneOrMore(.inverse(.inverse(.iri(predicate)))),
            object: .variable("?target")
        )

        do {
            _ = try await execute(
                pattern,
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 1,
                    maximumTraversalDepth: 10,
                    maximumResults: 10
                )
            )
            Issue.record("Expected the expression depth limit to reject the query")
        } catch let error as SPARQLQueryError {
            guard case .propertyPathExpressionDepthLimitExceeded(let maximum) = error else {
                Issue.record("Unexpected SPARQL error: \(error)")
                return
            }
            #expect(maximum == 1)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Traversal depth limit rejects a reachable result prefix")
    func testTraversalDepthLimitRejectsPartialSuccess() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()
        let start = uniqueID("TraversalDepthStart")
        let middle = uniqueID("TraversalDepthMiddle")
        let end = uniqueID("TraversalDepthEnd")
        let predicate = try uniquePredicate("traversal-depth")
        try await insertEdges(
            [
                try makeEdge(from: start, relationship: predicate, to: middle),
                try makeEdge(from: middle, relationship: predicate, to: end),
            ],
            context: context
        )

        let pattern = ExecutionPattern.propertyPath(
            subject: try iriTerm(start),
            path: .oneOrMore(.iri(predicate)),
            object: .variable("?target")
        )

        do {
            _ = try await execute(
                pattern,
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 10,
                    maximumTraversalDepth: 1,
                    maximumResults: 10
                )
            )
            Issue.record("Expected the traversal depth limit to reject the query")
        } catch let error as SPARQLQueryError {
            guard case .propertyPathTraversalDepthLimitExceeded(let maximum) = error else {
                Issue.record("Unexpected SPARQL error: \(error)")
                return
            }
            #expect(maximum == 1)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Result limit rejects a property-path result prefix")
    func testResultLimitRejectsPartialSuccess() async throws {
        let container = try await setupContainer()
        let context = container.testBaseContext()
        let start = uniqueID("ResultLimitStart")
        let first = uniqueID("ResultLimitFirst")
        let second = uniqueID("ResultLimitSecond")
        let predicate = try uniquePredicate("result-limit")
        try await insertEdges(
            [
                try makeEdge(from: start, relationship: predicate, to: first),
                try makeEdge(from: start, relationship: predicate, to: second),
            ],
            context: context
        )

        let pattern = ExecutionPattern.propertyPath(
            subject: try iriTerm(start),
            path: .oneOrMore(.iri(predicate)),
            object: .variable("?target")
        )

        do {
            _ = try await execute(
                pattern,
                container: container,
                context: context,
                configuration: ExecutionPropertyPathConfiguration(
                    maximumExpressionDepth: 10,
                    maximumTraversalDepth: 10,
                    maximumResults: 1
                )
            )
            Issue.record("Expected the result limit to reject the query")
        } catch let error as SPARQLQueryError {
            guard case .propertyPathResultLimitExceeded(let maximum) = error else {
                Issue.record("Unexpected SPARQL error: \(error)")
                return
            }
            #expect(maximum == 1)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    // MARK: - Inverse Path with Quantifier Tests

    @Test("Inverse with one or more (^parent+)")
    func testInverseWithOneOrMore() async throws {
        // SPARQL: SELECT ?descendant WHERE { ?descendant ^:parent+ :Root }
        // Find all descendants (inverse of parent)

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let root = uniqueID("Root")
        let child1 = uniqueID("Child1")
        let child2 = uniqueID("Child2")
        let grandchild = uniqueID("Grandchild")
        let parentPred = try uniquePredicate("parent")

        // Edges represent "X parent Y" (X is child of Y)
        let edges = [
            try makeEdge(from: child1, relationship: parentPred, to: root),
            try makeEdge(from: child2, relationship: parentPred, to: root),
            try makeEdge(from: grandchild, relationship: parentPred, to: child1),
        ]

        try await insertEdges(edges, context: context)

        // ^parent+ from Root finds all descendants
        // Semantically: find ?d where (?d, parent+, Root) exists
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(root),
                path: .inverse(.oneOrMore(.iri(parentPred))),
                .variable("?descendant")
            )
            .execute()

        let descendants = result.bindings.compactMap { iriValue($0, for: "?descendant") }
        #expect(descendants.contains(child1))
        #expect(descendants.contains(child2))
        #expect(descendants.contains(grandchild))
    }

    @Test("Inverse with zero or more (^knows*)")
    func testInverseWithZeroOrMore() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let alice = uniqueID("Alice")
        let bob = uniqueID("Bob")
        let carol = uniqueID("Carol")
        let knowsPred = try uniquePredicate("knows")

        // Bob knows Alice, Carol knows Bob
        let edges = [
            try makeEdge(from: bob, relationship: knowsPred, to: alice),
            try makeEdge(from: carol, relationship: knowsPred, to: bob),
        ]

        try await insertEdges(edges, context: context)

        // ^knows* from Alice: find who can reach Alice via inverse knows
        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(alice),
                path: .inverse(.zeroOrMore(.iri(knowsPred))),
                .variable("?person")
            )
            .execute()

        let persons = result.bindings.compactMap { iriValue($0, for: "?person") }
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

        let context = container.testBaseContext()

        let start = uniqueID("Start")
        let mid1 = uniqueID("Mid1")
        let mid2 = uniqueID("Mid2")
        let end1 = uniqueID("End1")
        let end2 = uniqueID("End2")
        let predA = try uniquePredicate("a")
        let predB = try uniquePredicate("b")
        let predC = try uniquePredicate("c")
        let predD = try uniquePredicate("d")

        let edges = [
            try makeEdge(from: start, relationship: predA, to: mid1),
            try makeEdge(from: start, relationship: predB, to: mid2),
            try makeEdge(from: mid1, relationship: predC, to: end1),
            try makeEdge(from: mid2, relationship: predD, to: end2),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a|b) / (c|d)
        let path = PropertyPath.sequence(
            .alternative(.iri(predA), .iri(predB)),
            .alternative(.iri(predC), .iri(predD))
        )

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(try iriTerm(start), path: path, .variable("?end"))
            .execute()

        let ends = result.bindings.compactMap { iriValue($0, for: "?end") }
        #expect(ends.contains(end1))  // via a/c
        #expect(ends.contains(end2))  // via b/d
    }

    @Test("Alternative of sequences")
    func testAlternativeOfSequences() async throws {
        // SPARQL: SELECT ?x ?z WHERE { ?x ((:a/:b)|(:c/:d)) ?z }

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let start = uniqueID("Start")
        let mid1 = uniqueID("Mid1")
        let mid2 = uniqueID("Mid2")
        let end1 = uniqueID("End1")
        let end2 = uniqueID("End2")
        let predA = try uniquePredicate("a")
        let predB = try uniquePredicate("b")
        let predC = try uniquePredicate("c")
        let predD = try uniquePredicate("d")

        let edges = [
            try makeEdge(from: start, relationship: predA, to: mid1),
            try makeEdge(from: mid1, relationship: predB, to: end1),
            try makeEdge(from: start, relationship: predC, to: mid2),
            try makeEdge(from: mid2, relationship: predD, to: end2),
        ]

        try await insertEdges(edges, context: context)

        // Build path: (a/b) | (c/d)
        let path = PropertyPath.alternative(
            .sequence(.iri(predA), .iri(predB)),
            .sequence(.iri(predC), .iri(predD))
        )

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(try iriTerm(start), path: path, .variable("?end"))
            .execute()

        let ends = result.bindings.compactMap { iriValue($0, for: "?end") }
        #expect(ends.contains(end1))  // via a/b
        #expect(ends.contains(end2))  // via c/d
    }

    // MARK: - Zero-Length Path Tests

    @Test("Zero-length path for a graph node with no matching edge")
    func testZeroLengthPath() async throws {
        // SPARQL: SELECT ?x WHERE { :node :link* ?x }
        // :node itself should be in the result when it occurs in the active graph,
        // even if no edge matches :link.

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let node = uniqueID("Node")
        let unrelatedTarget = uniqueID("UnrelatedTarget")
        let linkPred = try uniquePredicate("link")
        let unrelatedPred = try uniquePredicate("unrelated")

        try await insertEdges(
            [
                try makeEdge(
                    from: node,
                    relationship: unrelatedPred,
                    to: unrelatedTarget
                )
            ],
            context: context
        )

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(node),
                path: .zeroOrMore(.iri(linkPred)),
                .variable("?target")
            )
            .execute()

        // Should include node itself (zero hops)
        let targets = result.bindings.compactMap { iriValue($0, for: "?target") }
        #expect(targets.contains(node))
    }

    @Test("Zero-length path with existing edges")
    func testZeroLengthPathWithEdges() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let node = uniqueID("Node")
        let target = uniqueID("Target")
        let linkPred = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: node, relationship: linkPred, to: target)
        ]

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(node),
                path: .zeroOrMore(.iri(linkPred)),
                .variable("?x")
            )
            .execute()

        let targets = result.bindings.compactMap { iriValue($0, for: "?x") }
        #expect(targets.contains(node))    // Zero hops
        #expect(targets.contains(target))  // One hop
    }

    @Test("Property path traverses a branching graph without duplicates")
    func propertyPathTraversesBranchingGraph() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let linkPred = try uniquePredicate("link")
        let prefix = uniqueID("N")

        // Build a compact binary tree. Scale measurements belong to the
        // independent benchmark package.
        var edges: [AdvancedPathEdge] = []
        for i in 0..<7 {
            let parent = "\(prefix)-\(i)"
            let child1 = "\(prefix)-\(2*i + 1)"
            let child2 = "\(prefix)-\(2*i + 2)"
            edges.append(try makeEdge(from: parent, relationship: linkPred, to: child1))
            edges.append(try makeEdge(from: parent, relationship: linkPred, to: child2))
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm("\(prefix)-0"),
                path: .oneOrMore(.iri(linkPred)),
                .variable("?descendant")
            )
            .execute()

        #expect(result.count == 14)
    }

    @Test("Property path with branching factor")
    func testPropertyPathBranchingFactor() async throws {
        let container = try await setupContainer()

        let context = container.testBaseContext()

        let linkPred = try uniquePredicate("link")
        let prefix = uniqueID("N")

        // Create a graph with high branching factor
        // Root connects to 10 nodes, each of those connects to 5 more
        var edges: [AdvancedPathEdge] = []
        let root = "\(prefix)-root"

        for i in 0..<10 {
            let level1 = "\(prefix)-L1-\(i)"
            edges.append(try makeEdge(from: root, relationship: linkPred, to: level1))

            for j in 0..<5 {
                let level2 = "\(prefix)-L2-\(i)-\(j)"
                edges.append(try makeEdge(from: level1, relationship: linkPred, to: level2))
            }
        }

        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(root),
                path: .oneOrMore(.iri(linkPred)),
                .variable("?target")
            )
            .execute()

        // Should find 10 (level 1) + 50 (level 2) = 60 nodes
        #expect(result.count == 60)
    }

    // MARK: - PropertyPath AST Tests

    @Test("PropertyPath description serialization")
    func testPropertyPathSerialization() throws {
        let knows = try predicateIRI("knows")
        let parent = try predicateIRI("parent")
        let lives = try predicateIRI("lives")
        let likes = try predicateIRI("likes")
        let link = try predicateIRI("link")
        let hates = try predicateIRI("hates")

        // Simple IRI
        let simple = PropertyPath.iri(knows)
        #expect(simple.description == knows.rawValue)

        // Inverse
        let inverse = PropertyPath.inverse(.iri(parent))
        #expect(inverse.description == "^\(parent.rawValue)")

        // Sequence
        let sequence = PropertyPath.sequence(
            .iri(knows),
            .iri(lives)
        )
        #expect(sequence.description == "\(knows.rawValue)/\(lives.rawValue)")

        // Alternative
        let alternative = PropertyPath.alternative(
            .iri(knows),
            .iri(likes)
        )
        #expect(alternative.description == "\(knows.rawValue)|\(likes.rawValue)")

        // Quantifiers
        let zeroOrMore = PropertyPath.zeroOrMore(.iri(link))
        #expect(zeroOrMore.description == "\(link.rawValue)*")

        let oneOrMore = PropertyPath.oneOrMore(.iri(link))
        #expect(oneOrMore.description == "\(link.rawValue)+")

        let zeroOrOne = PropertyPath.zeroOrOne(.iri(link))
        #expect(zeroOrOne.description == "\(link.rawValue)?")

        // Negation
        let negation = PropertyPath.negatedPropertySet(
            try PropertyPathNegatedSet(forward: [knows, hates])
        )
        #expect(negation.description.contains("!"))
    }

    @Test("PropertyPath complexity estimate")
    func testPropertyPathComplexity() throws {
        let test = try predicateIRI("test")
        let a = try predicateIRI("a")
        let b = try predicateIRI("b")
        let c = try predicateIRI("c")

        // Simple IRI: cost = 1
        #expect(PropertyPath.iri(test).complexityEstimate == 1)

        // Negated property set: cost = 10 (requires scanning all edges)
        #expect(
            PropertyPath.negatedPropertySet(
                try PropertyPathNegatedSet(forward: [a, b])
            ).complexityEstimate == 10
        )

        // Inverse: inner + 1
        #expect(PropertyPath.inverse(.iri(test)).complexityEstimate == 2)

        // Sequence: sum of parts (1 + 1 = 2)
        let sequence = PropertyPath.sequence(.iri(a), .iri(b))
        #expect(sequence.complexityEstimate == 2)

        // Alternative: sum of parts (1 + 1 = 2)
        let alternative = PropertyPath.alternative(.iri(a), .iri(b))
        #expect(alternative.complexityEstimate == 2)

        // Recursive paths: inner * 100
        #expect(PropertyPath.oneOrMore(.iri(test)).complexityEstimate == 100)
        #expect(PropertyPath.zeroOrMore(.iri(test)).complexityEstimate == 100)

        // ZeroOrOne: inner + 1
        #expect(PropertyPath.zeroOrOne(.iri(test)).complexityEstimate == 2)

        // Complex nested: zeroOrMore(sequence(a, alternative(b, oneOrMore(c))))
        // = (1 + (1 + 1*100)) * 100 = 10200
        let complex = PropertyPath.zeroOrMore(
            .sequence(.iri(a), .alternative(.iri(b), .oneOrMore(.iri(c))))
        )
        #expect(complex.complexityEstimate == 10200)
    }

    @Test("PropertyPath normalization")
    func testPropertyPathNormalization() throws {
        let test = try predicateIRI("test")
        let a = try predicateIRI("a")
        let b = try predicateIRI("b")
        let c = try predicateIRI("c")
        let d = try predicateIRI("d")
        let x = try predicateIRI("x")
        let y = try predicateIRI("y")
        let z = try predicateIRI("z")

        // Double inverse should normalize to original
        let doubleInverse = PropertyPath.inverse(.inverse(.iri(test)))
        #expect(doubleInverse.normalized() == .iri(test))

        // Triple inverse should normalize to single inverse
        let tripleInverse = PropertyPath.inverse(.inverse(.inverse(.iri(test))))
        #expect(tripleInverse.normalized() == .inverse(.iri(test)))

        // Non-inverse paths stay the same
        let sequence = PropertyPath.sequence(.iri(a), .iri(b))
        #expect(sequence.normalized() == sequence)

        // Alternative flattening: left-associative → right-associative
        // (a|b)|c should normalize to a|(b|c)
        let leftAssoc = PropertyPath.alternative(
            .alternative(.iri(a), .iri(b)),
            .iri(c)
        )
        let rightAssoc = PropertyPath.alternative(
            .iri(a),
            .alternative(.iri(b), .iri(c))
        )
        #expect(leftAssoc.normalized() == rightAssoc)

        // Nested alternatives: ((a|b)|(c|d)) → a|(b|(c|d))
        let nested = PropertyPath.alternative(
            .alternative(.iri(a), .iri(b)),
            .alternative(.iri(c), .iri(d))
        )
        let nestedExpected = PropertyPath.alternative(
            .iri(a),
            .alternative(
                .iri(b),
                .alternative(.iri(c), .iri(d))
            )
        )
        #expect(nested.normalized() == nestedExpected)

        // Already right-associative stays the same
        let alreadyRight = PropertyPath.alternative(
            .iri(x),
            .alternative(.iri(y), .iri(z))
        )
        #expect(alreadyRight.normalized() == alreadyRight)

        // Inverse over sequence: ^(a/b) = (^b)/(^a)
        let inverseSeq = PropertyPath.inverse(.sequence(.iri(a), .iri(b)))
        let expectedInverseSeq = PropertyPath.sequence(.inverse(.iri(b)), .inverse(.iri(a)))
        #expect(inverseSeq.normalized() == expectedInverseSeq)

        // Inverse over alternative: ^(a|b) = (^a)|(^b)
        let inverseAlt = PropertyPath.inverse(.alternative(.iri(a), .iri(b)))
        let expectedInverseAlt = PropertyPath.alternative(.inverse(.iri(a)), .inverse(.iri(b)))
        #expect(inverseAlt.normalized() == expectedInverseAlt)
    }

    @Test("PropertyPath allIRIs extraction")
    func testPropertyPathAllIRIs() throws {
        let a = try predicateIRI("a")
        let b = try predicateIRI("b")
        let c = try predicateIRI("c")
        let path = PropertyPath.sequence(
            .alternative(.iri(a), .iri(b)),
            .oneOrMore(.inverse(.iri(c)))
        )

        let iris = path.allIRIs
        #expect(iris.contains(a))
        #expect(iris.contains(b))
        #expect(iris.contains(c))
        #expect(iris.count == 3)
    }

    // MARK: - BFS Origin Tracking Tests

    @Test("BFS transitive: unbound subject + bound object returns origin node (linear chain)")
    func testBFSOriginTrackingLinearChain() async throws {
        // Graph: A→B→C (via "link")
        // Query: ?person (link)+ C → should return ?person=A and ?person=B
        // (A reaches C via 2 hops, B reaches C via 1 hop)

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                .variable("?person"),
                path: .oneOrMore(.iri(link)),
                try iriTerm(c)
            )
            .execute()

        let persons = Set(result.bindings.compactMap { iriValue($0, for: "?person") })
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

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: b, relationship: link, to: d),
            try makeEdge(from: c, relationship: link, to: d),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                .variable("?x"),
                path: .oneOrMore(.iri(link)),
                try iriTerm(d)
            )
            .execute()

        let xs = Set(result.bindings.compactMap { iriValue($0, for: "?x") })
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

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(a),
                path: .oneOrMore(.iri(link)),
                .variable("?target")
            )
            .execute()

        let targets = Set(result.bindings.compactMap { iriValue($0, for: "?target") })
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

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                .variable("?s"),
                path: .oneOrMore(.iri(link)),
                .variable("?o")
            )
            .execute()

        // Collect all (subject, object) pairs
        let pairs = result.bindings.compactMap { binding -> (String, String)? in
            guard let s = iriValue(binding, for: "?s"),
                  let o = iriValue(binding, for: "?o") else {
                return nil
            }
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
            #expect(iriValue(binding, for: "?s") != nil, "Every result must have ?s bound")
            #expect(iriValue(binding, for: "?o") != nil, "Every result must have ?o bound")
        }
    }

    @Test("BFS transitive: unbound subject + unbound object branching (C1 fix)")
    func testBFSUnboundSubjectUnboundObjectBranching() async throws {
        // Graph: A→B→D, A→C→D (via "link")
        // Query: ?s (link)+ ?o → should include {?s=A, ?o=D} (reachable via both paths)

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let d = uniqueID("D")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: a, relationship: link, to: c),
            try makeEdge(from: b, relationship: link, to: d),
            try makeEdge(from: c, relationship: link, to: d),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                .variable("?s"),
                path: .oneOrMore(.iri(link)),
                .variable("?o")
            )
            .execute()

        let pairs = result.bindings.compactMap { binding -> (String, String)? in
            guard let s = iriValue(binding, for: "?s"),
                  let o = iriValue(binding, for: "?o") else {
                return nil
            }
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
            #expect(iriValue(binding, for: "?s") != nil, "Every result must have ?s bound")
        }
    }

    @Test("BFS transitive: bound subject + bound object (regression)")
    func testBFSBoundSubjectBoundObject() async throws {
        // Graph: A→B→C (via "link")
        // Query: A (link)+ C → should match (A can reach C)

        let container = try await setupContainer()

        let context = container.testBaseContext()

        let a = uniqueID("A")
        let b = uniqueID("B")
        let c = uniqueID("C")
        let link = try uniquePredicate("link")

        let edges = [
            try makeEdge(from: a, relationship: link, to: b),
            try makeEdge(from: b, relationship: link, to: c),
        ]
        try await insertEdges(edges, context: context)

        let result = try await context.sparql(AdvancedPathEdge.self)
            .defaultIndex()
            .wherePath(
                try iriTerm(a),
                path: .oneOrMore(.iri(link)),
                try iriTerm(c)
            )
            .execute()

        // A can reach C → at least one result (empty binding since both are bound)
        #expect(!result.isEmpty, "A should reach C via A→B→C")
    }
}
#endif
