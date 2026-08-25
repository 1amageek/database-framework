#if !os(WASI)
// NamedGraphUnitTests.swift
// Unit tests for Named Graph (Quad) support
//
// Layer 1: Pure logic tests (no FDB required)
// Tests the SPARQL graph algebra and canonical property-graph metadata.
//
// NOTE: GraphPatternConverter tests are in Database module scope,
// not GraphIndex. They belong in a Database-level test target.

import Testing
import TestHeartbeat
import Foundation
import DatabaseKit
import DatabaseTypes
@testable import GraphIndex

// MARK: - Canonical ExecutionTriple Tests

@Suite("Canonical ExecutionTriple Tests", .heartbeat)
struct CanonicalExecutionTripleTests {
    private func term(_ value: String) throws -> ExecutionTerm {
        .value(.rdfTerm(try .iri(validating: value)))
    }

    @Test("Triple stores canonical RDF terms")
    func testTripleStoresCanonicalRDFTerms() throws {
        let alice = try term("https://example.com/people/alice")
        let knows = try term("https://example.com/vocabulary/knows")
        let bob = try term("https://example.com/people/bob")
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
    func testTripleVariablesContainOnlyTriplePositions() throws {
        let knows = try term("https://example.com/vocabulary/knows")
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: knows,
            object: .variable("?o")
        )

        #expect(triple.variables == Set(["?s", "?o"]))
    }

    @Test("Substitution preserves canonical RDF values")
    func testSubstitutionPreservesCanonicalRDFValues() throws {
        let alice = try term("https://example.com/people/alice")
        let knows = try term("https://example.com/vocabulary/knows")
        let bob = try term("https://example.com/people/bob")
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: knows,
            object: .variable("?o")
        )
        let binding = VariableBinding()
            .binding(
                "?s",
                to: .rdfTerm(
                    try .iri(validating: "https://example.com/people/alice")
                )
            )
            .binding(
                "?o",
                to: .rdfTerm(
                    try .iri(validating: "https://example.com/people/bob")
                )
            )

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
    private func term(_ value: String) throws -> ExecutionTerm {
        .value(.rdfTerm(try .iri(validating: value)))
    }

    private func graphName(_ component: String) throws -> RDFGraphName {
        try RDFGraphName(iri: "https://example.com/graphs/\(component)")
    }

    @Test("Named graph targets a basic graph pattern")
    func testNamedGraphTargetsBasicPattern() throws {
        let graph = try graphName("social")
        let knows = try term("https://example.com/vocabulary/knows")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            )
        ])
        let pattern = ExecutionPattern.graph(.named(graph), basic)

        guard case .graph(
            .named(let actualGraph),
            .basic(let actualTriples)
        ) = pattern else {
            Issue.record("Expected a named graph with a basic pattern")
            return
        }
        #expect(actualGraph == graph)
        #expect(actualTriples.count == 1)
        #expect(pattern.outputVariables == Set(["?s", "?o"]))
        #expect(pattern.requiredOutputVariables == Set(["?s", "?o"]))
        #expect(pattern.patternCount == 1)
        #expect(pattern.description.contains("GRAPH"))
    }

    @Test("Graph variable is part of the algebra binding domain")
    func testGraphVariableIsPartOfBindingDomain() throws {
        let knows = try term("https://example.com/vocabulary/knows")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            )
        ])
        let pattern = ExecutionPattern.graph(.variable("?g"), basic)

        #expect(pattern.outputVariables == Set(["?g", "?s", "?o"]))
        #expect(pattern.requiredOutputVariables == Set(["?g", "?s", "?o"]))
        #expect(pattern.description.contains("?g"))
    }

    @Test("Graph scopes a composed algebra subtree")
    func testGraphTargetsComposedAlgebraSubtree() throws {
        let graph = try graphName("social")
        let knows = try term("https://example.com/vocabulary/knows")
        let name = try term("https://example.com/vocabulary/name")
        let left = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            )
        ])
        let right = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?o"),
                predicate: name,
                object: .variable("?name")
            )
        ])
        let joined = ExecutionPattern.join(left, right)
        let pattern = ExecutionPattern.graph(.named(graph), joined)

        guard case .graph(.named(let actualGraph), let scopedPattern) = pattern else {
            Issue.record("Expected a named graph algebra node")
            return
        }
        guard case .join(
            .basic(let actualLeft),
            .basic(let actualRight)
        ) = scopedPattern else {
            Issue.record("Expected the graph target to retain the join")
            return
        }
        #expect(actualGraph == graph)
        #expect(actualLeft.count == 1)
        #expect(actualRight.count == 1)
        #expect(pattern.patternCount == 2)
    }

    @Test("Graph target applies to property paths")
    func testGraphTargetAppliesToPropertyPaths() throws {
        let graph = try graphName("social")
        let propertyPath = ExecutionPattern.propertyPath(
            subject: .variable("?s"),
            path: .oneOrMore(
                .iri(
                    try RDFPredicateIRI(
                        "https://example.com/vocabulary/knows"
                    )
                )
            ),
            object: .variable("?o")
        )
        let pattern = ExecutionPattern.graph(.named(graph), propertyPath)

        guard case .graph(
            .named(let actualGraph),
            .propertyPath(let subject, let path, let object)
        ) = pattern else {
            Issue.record("Expected a named graph with a property path")
            return
        }
        #expect(actualGraph == graph)
        #expect(subject == .variable("?s"))
        #expect(path == .oneOrMore(.iri(
            try RDFPredicateIRI("https://example.com/vocabulary/knows")
        )))
        #expect(object == .variable("?o"))
        #expect(pattern.outputVariables == Set(["?s", "?o"]))
        #expect(pattern.patternCount == 1)
    }

    @Test("Graph scopes preserve distinct graph names")
    func testGraphTargetsPreserveDistinctGraphNames() throws {
        let socialGraph = try graphName("social")
        let archiveGraph = try graphName("archive")
        let knows = try term("https://example.com/vocabulary/knows")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            )
        ])

        let social = ExecutionPattern.graph(.named(socialGraph), basic)
        let archive = ExecutionPattern.graph(.named(archiveGraph), basic)
        guard case .graph(.named(let actualSocial), _) = social,
              case .graph(.named(let actualArchive), _) = archive else {
            Issue.record("Expected named graph algebra nodes")
            return
        }
        #expect(actualSocial == socialGraph)
        #expect(actualArchive == archiveGraph)
        #expect(actualSocial != actualArchive)
    }

    @Test("Nested graph targets remain explicit algebra nodes")
    func testNestedGraphTargetsRemainExplicit() throws {
        let outerGraph = try graphName("outer")
        let innerGraph = try graphName("inner")
        let knows = try term("https://example.com/vocabulary/knows")
        let basic = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?s"),
                predicate: knows,
                object: .variable("?o")
            )
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
        guard case .basic(let leafTriples) = leaf else {
            Issue.record("Expected the nested graph leaf to remain basic")
            return
        }
        #expect(leafTriples.count == 1)
    }
}

// MARK: - Canonical Named Graph Definition Tests

@Suite("Canonical named graph definitions", .heartbeat)
struct CanonicalNamedGraphDefinitionTests {
    @Test("A graph field is the fourth canonical key")
    func graphFieldIsCanonical() {
        let definition = IndexDefinition<String>.graph(
            .property(
                source: "source",
                label: .field("label"),
                target: "target",
                graph: "graph",
            strategy: .tripleStore
            ),
            includedFields: []
        )

        #expect(
            definition.keys.map(\.field) == [
            "source",
            "label",
            "target",
            "graph",
        ])
        #expect(definition.type == .graph(.property))
    }

    @Test("The default graph omits the fourth key")
    func defaultGraphOmitsField() {
        let definition = IndexDefinition<String>.graph(
            .property(
                source: "source",
                label: .field("label"),
                target: "target",
                graph: nil,
                strategy: .hexastore
            ),
            includedFields: []
        )

        #expect(
            definition.keys.map(\.field) == ["source", "label",
                "target",
            ])
    }

    @Test("Definition equality includes graph identity")
    func equalityIncludesGraphIdentity() {
        let named = IndexDefinition<String>.graph(
            .property(
                source: "source",
                label: .field("label"),
                target: "target",
                graph: "graph",
            strategy: .tripleStore
            ),
            includedFields: []
        )
        let defaultGraph = IndexDefinition<String>.graph(
            .property(
                source: "source",
                label: .field("label"),
                target: "target",
                graph: nil,
                strategy: .tripleStore
            ),
            includedFields: []
        )

        #expect(named != defaultGraph)
    }
}
#endif
