#if FOUNDATION_DB
// NamedGraphUnitTests.swift
// Unit tests for Named Graph (Quad) support
//
// Layer 1: Pure logic tests (no FDB required)
// Tests the SPARQL graph algebra and GraphIndexKind metadata.
//
// NOTE: GraphPatternConverter tests are in Database module scope,
// not GraphIndex. They belong in a Database-level test target.

import Testing
import TestHeartbeat
import Foundation
import Core
import DatabaseValue
import Graph
import QueryIR
@testable import GraphIndex

// MARK: - Canonical ExecutionTriple Tests

@Suite("Canonical ExecutionTriple Tests", .heartbeat)
struct CanonicalExecutionTripleTests {

    private let alice: ExecutionTerm = .value(
        .rdfTerm(.iri("https://example.com/people/alice"))
    )
    private let knows: ExecutionTerm = .value(
        .rdfTerm(.iri("https://example.com/vocabulary/knows"))
    )
    private let bob: ExecutionTerm = .value(
        .rdfTerm(.iri("https://example.com/people/bob"))
    )

    @Test("Triple stores canonical RDF terms")
    func testTripleStoresCanonicalRDFTerms() {
        let triple = ExecutionTriple(
            subject: alice,
            predicate: knows,
            object: bob
        )

        #expect(triple.subject == alice)
        #expect(triple.predicate == knows)
        #expect(triple.object == bob)
        #expect(triple.isFullyBound)
    }

    @Test("Triple variables contain only subject predicate and object variables")
    func testTripleVariablesContainOnlyTriplePositions() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: knows,
            object: .variable("?o")
        )

        #expect(triple.variables == Set(["?s", "?o"]))
    }

    @Test("Substitution preserves canonical RDF values")
    func testSubstitutionPreservesCanonicalRDFValues() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: knows,
            object: .variable("?o")
        )
        let binding = VariableBinding()
            .binding("?s", to: .rdfTerm(.iri("https://example.com/people/alice")))
            .binding("?o", to: .rdfTerm(.iri("https://example.com/people/bob")))

        let substituted = triple.substitute(binding)

        #expect(substituted.subject == alice)
        #expect(substituted.predicate == knows)
        #expect(substituted.object == bob)
        #expect(substituted.isFullyBound)
    }
}

// MARK: - ExecutionPattern Named Graph Tests

@Suite("ExecutionPattern Named Graph Tests", .heartbeat)
struct ExecutionPatternNamedGraphTests {

    private let knows: ExecutionTerm = .value(
        .rdfTerm(.iri("https://example.com/vocabulary/knows"))
    )
    private let name: ExecutionTerm = .value(
        .rdfTerm(.iri("https://example.com/vocabulary/name"))
    )

    private func graphName(_ component: String) throws -> RDFGraphName {
        try RDFGraphName(iri: "https://example.com/graphs/\(component)")
    }

    @Test("Named graph scopes a basic graph pattern")
    func testNamedGraphScopesBasicPattern() throws {
        let graph = try graphName("social")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            ),
        ])
        let pattern = ExecutionPattern.graph(.named(graph), basic)

        #expect(pattern == .graph(.named(graph), basic))
        #expect(pattern.outputVariables == Set(["?s", "?o"]))
        #expect(pattern.requiredOutputVariables == Set(["?s", "?o"]))
        #expect(pattern.patternCount == 1)
        #expect(pattern.description.contains("GRAPH"))
    }

    @Test("Graph variable is part of the algebra binding domain")
    func testGraphVariableIsPartOfBindingDomain() {
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            ),
        ])
        let pattern = ExecutionPattern.graph(.variable("?g"), basic)

        #expect(pattern.outputVariables == Set(["?g", "?s", "?o"]))
        #expect(pattern.requiredOutputVariables == Set(["?g", "?s", "?o"]))
        #expect(pattern.description.contains("?g"))
    }

    @Test("Graph scopes a composed algebra subtree")
    func testGraphScopesComposedAlgebraSubtree() throws {
        let graph = try graphName("social")
        let left = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            ),
        ])
        let right = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?o"),
                predicate: name,
                object: .variable("?name")
            ),
        ])
        let joined = ExecutionPattern.join(left, right)
        let pattern = ExecutionPattern.graph(.named(graph), joined)

        guard case .graph(.named(let actualGraph), let scopedPattern) = pattern else {
            Issue.record("Expected a named graph algebra node")
            return
        }
        #expect(actualGraph == graph)
        #expect(scopedPattern == joined)
        #expect(pattern.patternCount == 2)
    }

    @Test("Graph scope applies to property paths")
    func testGraphScopeAppliesToPropertyPaths() throws {
        let graph = try graphName("social")
        let propertyPath = ExecutionPattern.propertyPath(
            subject: .variable("?s"),
            path: .oneOrMore(
                .iri(
                    try DatabaseRDFPredicateIRI(
                        "https://example.com/vocabulary/knows"
                    )
                )
            ),
            object: .variable("?o")
        )
        let pattern = ExecutionPattern.graph(.named(graph), propertyPath)

        #expect(pattern == .graph(.named(graph), propertyPath))
        #expect(pattern.outputVariables == Set(["?s", "?o"]))
        #expect(pattern.patternCount == 1)
    }

    @Test("Distinct graph names produce distinct algebra")
    func testDistinctGraphNamesProduceDistinctAlgebra() throws {
        let socialGraph = try graphName("social")
        let archiveGraph = try graphName("archive")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            ),
        ])

        #expect(
            ExecutionPattern.graph(.named(socialGraph), basic)
                != ExecutionPattern.graph(.named(archiveGraph), basic)
        )
    }

    @Test("Nested graph scopes remain explicit algebra nodes")
    func testNestedGraphScopesRemainExplicit() throws {
        let outerGraph = try graphName("outer")
        let innerGraph = try graphName("inner")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            ),
        ])
        let nested = ExecutionPattern.graph(
            .named(outerGraph),
            .graph(.named(innerGraph), basic)
        )

        guard case .graph(.named(let actualOuter), let child) = nested,
              case .graph(.named(let actualInner), let leaf) = child else {
            Issue.record("Expected nested graph algebra nodes")
            return
        }
        #expect(actualOuter == outerGraph)
        #expect(actualInner == innerGraph)
        #expect(leaf == basic)
    }
}

// MARK: - GraphIndexKind Named Graph Tests

@Suite("GraphIndexKind Named Graph Tests", .heartbeat)
struct GraphIndexKindNamedGraphTests {

    @Test("fieldNames includes graph field when set")
    func testFieldNamesIncludesGraphField() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            graphField: "graph",
            strategy: .tripleStore
        )
        #expect(kind.fieldNames.contains("graph"))
        #expect(kind.fieldNames.count == 4)
        #expect(kind.fieldNames.last == "graph")
    }

    @Test("fieldNames excludes graph when nil")
    func testFieldNamesExcludesGraphWhenNil() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .tripleStore
        )
        #expect(!kind.fieldNames.contains("graph"))
        #expect(kind.fieldNames.count == 3)
    }

    @Test("indexName includes graph field")
    func testIndexNameIncludesGraphField() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            graphField: "graph",
            strategy: .tripleStore
        )
        #expect(kind.indexName.hasSuffix("_graph"))
    }

    @Test("indexName excludes graph when nil")
    func testIndexNameExcludesGraphWhenNil() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .tripleStore
        )
        #expect(!kind.indexName.hasSuffix("_graph"))
    }

    @Test("hasGraphField is true when set")
    func testHasGraphFieldTrueWhenSet() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            graphField: "graph",
            strategy: .tripleStore
        )
        #expect(kind.hasGraphField)
    }

    @Test("hasGraphField is false when nil")
    func testHasGraphFieldFalseWhenNil() {
        let kind = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .tripleStore
        )
        #expect(!kind.hasGraphField)
    }

    @Test("Codable round-trip with graph field")
    func testCodableRoundTripWithGraph() throws {
        let original = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            graphField: "graph",
            strategy: .tripleStore
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(GraphIndexKind<GraphIndexEdge>.self, from: data)
        #expect(decoded.graphField == "graph")
        #expect(decoded.fromField == "source")
        #expect(decoded.edgeField == "label")
        #expect(decoded.toField == "target")
        #expect(decoded.strategy == .tripleStore)
    }

    @Test("Codable round-trip without graph field")
    func testCodableRoundTripWithoutGraph() throws {
        let original = GraphIndexKind<GraphIndexEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .hexastore
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(GraphIndexKind<GraphIndexEdge>.self, from: data)
        #expect(decoded.graphField == nil)
        #expect(decoded.strategy == .hexastore)
    }

    @Test("Hashable equality with same graph field")
    func testHashableEqualityWithSameGraph() {
        let a = GraphIndexKind<GraphIndexEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        let b = GraphIndexKind<GraphIndexEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        #expect(a == b)
        #expect(a.hashValue == b.hashValue)
    }

    @Test("Hashable inequality with different graph field")
    func testHashableInequalityWithDifferentGraph() {
        let a = GraphIndexKind<GraphIndexEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        let b = GraphIndexKind<GraphIndexEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: nil,
            strategy: .tripleStore
        )
        #expect(a != b)
    }
}
#endif
