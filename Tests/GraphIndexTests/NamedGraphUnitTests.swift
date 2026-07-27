#if FOUNDATION_DB
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

    @Test("Named graph scopes a basic graph pattern")
    func testNamedGraphScopesBasicPattern() throws {
        let graph = try graphName("social")
        let knows = try term("https://example.com/vocabulary/knows")
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
    func testGraphVariableIsPartOfBindingDomain() throws {
        let knows = try term("https://example.com/vocabulary/knows")
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
        let knows = try term("https://example.com/vocabulary/knows")
        let name = try term("https://example.com/vocabulary/name")
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
                    try RDFPredicateIRI(
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
        let knows = try term("https://example.com/vocabulary/knows")
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
        let knows = try term("https://example.com/vocabulary/knows")
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

// MARK: - Canonical Named Graph Metadata Tests

@Suite("Canonical Named Graph Metadata", .heartbeat)
struct CanonicalNamedGraphMetadataTests {
    @Test("A namespace field is the fourth canonical field")
    func namespaceFieldIsCanonical() throws {
        let kind = propertyGraphIndexMetadata(
            sourceFieldName: "source",
            labelFieldName: "label",
            targetFieldName: "target",
            namespaceFieldName: "graph",
            strategy: .tripleStore
        )
        let metadata = try PropertyGraphIndexMetadata(
            canonical: kind
        )

        #expect(kind.fieldNames == [
            "source",
            "label",
            "target",
            "graph",
        ])
        #expect(metadata.namespaceFieldName == "graph")
        #expect(metadata.strategy == .tripleStore)
    }

    @Test("The default namespace omits the fourth field")
    func defaultNamespaceOmitsField() throws {
        let kind = propertyGraphIndexMetadata(
            sourceFieldName: "source",
            labelFieldName: "label",
            targetFieldName: "target",
            strategy: .hexastore
        )
        let metadata = try PropertyGraphIndexMetadata(
            canonical: kind
        )

        #expect(kind.fieldNames == ["source", "label", "target"])
        #expect(metadata.namespaceFieldName == nil)
        #expect(metadata.strategy == .hexastore)
    }

    @Test("Canonical metadata equality includes namespace identity")
    func equalityIncludesNamespace() {
        let named = propertyGraphIndexMetadata(
            sourceFieldName: "source",
            labelFieldName: "label",
            targetFieldName: "target",
            namespaceFieldName: "graph",
            strategy: .tripleStore
        )
        let defaultGraph = propertyGraphIndexMetadata(
            sourceFieldName: "source",
            labelFieldName: "label",
            targetFieldName: "target",
            strategy: .tripleStore
        )

        #expect(named != defaultGraph)
    }
}
#endif
