// NamedGraphUnitTests.swift
// Unit tests for Named Graph (Quad) support
//
// Layer 1: Pure logic tests (no FDB required)
// Tests ExecutionTriple/ExecutionPattern/GraphIndexKind
// for correct Named Graph modeling.
//
// NOTE: GraphPatternConverter tests are in Database module scope,
// not GraphIndex. They belong in a Database-level test target.

import Testing
import Foundation
import Core
import Graph
import QueryIR
@testable import GraphIndex

// MARK: - ExecutionTriple Named Graph Tests

@Suite("ExecutionTriple Named Graph Tests")
struct ExecutionTripleNamedGraphTests {

    @Test("Triple with graph stores graph term")
    func testTripleWithGraphStoresGraphTerm() {
        let triple = ExecutionTriple(
            subject: .value("Alice"),
            predicate: .value("knows"),
            object: .value("Bob"),
            graph: .value("g1")
        )
        #expect(triple.graph == .value("g1"))
        #expect(triple.subject == .value("Alice"))
        #expect(triple.predicate == .value("knows"))
        #expect(triple.object == .value("Bob"))
    }

    @Test("Triple without graph has nil graph")
    func testTripleWithoutGraphHasNilGraph() {
        let triple = ExecutionTriple("?s", "?p", "?o")
        #expect(triple.graph == nil)
    }

    @Test("Graph variable included in variables")
    func testGraphVariableIncludedInVariables() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value("knows"),
            object: .variable("?o"),
            graph: .variable("?g")
        )
        let vars = triple.variables
        #expect(vars.contains("?g"))
        #expect(vars.contains("?s"))
        #expect(vars.contains("?o"))
        #expect(vars.count == 3)
    }

    @Test("Graph bound value not in variables")
    func testGraphBoundValueNotInVariables() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value("knows"),
            object: .variable("?o"),
            graph: .value("g1")
        )
        let vars = triple.variables
        #expect(!vars.contains("g1"))
        #expect(vars.count == 2)
    }

    @Test("withGraph sets graph term")
    func testWithGraphSetsGraphTerm() {
        let original = ExecutionTriple("?s", "knows", "?o")
        #expect(original.graph == nil)

        let withGraph = original.withGraph(.value("g1"))
        #expect(withGraph.graph == .value("g1"))
        #expect(withGraph.subject == original.subject)
        #expect(withGraph.predicate == original.predicate)
        #expect(withGraph.object == original.object)
    }

    @Test("Substitute replaces graph variable")
    func testSubstituteReplacesGraphVariable() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value("knows"),
            object: .variable("?o"),
            graph: .variable("?g")
        )
        var binding = VariableBinding()
        binding = binding.binding("?g", to: "socialGraph")

        let substituted = triple.substitute(binding)
        #expect(substituted.graph == .value("socialGraph"))
        #expect(substituted.subject == .variable("?s"))
    }

    @Test("Substitute preserves graph value")
    func testSubstitutePreservesGraphValue() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value("knows"),
            object: .variable("?o"),
            graph: .value("g1")
        )
        var binding = VariableBinding()
        binding = binding.binding("?s", to: "Alice")

        let substituted = triple.substitute(binding)
        #expect(substituted.graph == .value("g1"))
        #expect(substituted.subject == .value("Alice"))
    }

    @Test("Description with graph includes GRAPH prefix")
    func testDescriptionWithGraph() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value("knows"),
            object: .variable("?o"),
            graph: .value("g1")
        )
        #expect(triple.description.contains("GRAPH"))
    }

    @Test("Description without graph omits GRAPH")
    func testDescriptionWithoutGraph() {
        let triple = ExecutionTriple("?s", "knows", "?o")
        #expect(!triple.description.contains("GRAPH"))
    }

    @Test("Equality with same graph")
    func testEqualityWithSameGraph() {
        let a = ExecutionTriple(
            subject: .value("Alice"), predicate: .value("knows"),
            object: .value("Bob"), graph: .value("g1")
        )
        let b = ExecutionTriple(
            subject: .value("Alice"), predicate: .value("knows"),
            object: .value("Bob"), graph: .value("g1")
        )
        #expect(a == b)
    }

    @Test("Inequality with different graph")
    func testInequalityWithDifferentGraph() {
        let a = ExecutionTriple(
            subject: .value("Alice"), predicate: .value("knows"),
            object: .value("Bob"), graph: .value("g1")
        )
        let b = ExecutionTriple(
            subject: .value("Alice"), predicate: .value("knows"),
            object: .value("Bob"), graph: .value("g2")
        )
        #expect(a != b)
    }
}

// MARK: - ExecutionPattern withGraph Tests

@Suite("ExecutionPattern Named Graph Tests")
struct ExecutionPatternNamedGraphTests {

    private let graphTerm: ExecutionTerm = .value("g1")

    private func extractAllGraphs(from pattern: ExecutionPattern) -> [ExecutionTerm?] {
        pattern.allExecutionTriples.map { $0.graph }
    }

    @Test("withGraph propagates to basic pattern")
    func testWithGraphPropagatesToBasic() {
        let pattern = ExecutionPattern.basic([
            ExecutionTriple("?s", "knows", "?o"),
            ExecutionTriple("?s", "name", "?name"),
        ])
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
        #expect(graphs.count == 2)
    }

    @Test("withGraph propagates to join")
    func testWithGraphPropagatesToJoin() {
        let left = ExecutionPattern.basic([ExecutionTriple("?s", "knows", "?o")])
        let right = ExecutionPattern.basic([ExecutionTriple("?o", "name", "?name")])
        let pattern = ExecutionPattern.join(left, right)
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
        #expect(graphs.count == 2)
    }

    @Test("withGraph propagates to optional")
    func testWithGraphPropagatesToOptional() {
        let left = ExecutionPattern.basic([ExecutionTriple("?s", "knows", "?o")])
        let right = ExecutionPattern.basic([ExecutionTriple("?s", "email", "?email")])
        let pattern = ExecutionPattern.optional(left, right)
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
        #expect(graphs.count == 2)
    }

    @Test("withGraph propagates to union")
    func testWithGraphPropagatesToUnion() {
        let left = ExecutionPattern.basic([ExecutionTriple("?s", "knows", "?o")])
        let right = ExecutionPattern.basic([ExecutionTriple("?s", "follows", "?o")])
        let pattern = ExecutionPattern.union(left, right)
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
        #expect(graphs.count == 2)
    }

    @Test("withGraph propagates to filter")
    func testWithGraphPropagatesToFilter() {
        let inner = ExecutionPattern.basic([ExecutionTriple("?s", "age", "?age")])
        let pattern = ExecutionPattern.filter(inner, .numeric("?age", ">=", 18))
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
    }

    @Test("withGraph propagates to minus")
    func testWithGraphPropagatesToMinus() {
        let left = ExecutionPattern.basic([ExecutionTriple("?s", "knows", "?o")])
        let right = ExecutionPattern.basic([ExecutionTriple("?s", "blocks", "?o")])
        let pattern = ExecutionPattern.minus(left, right)
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
        #expect(graphs.count == 2)
    }

    @Test("withGraph propagates to groupBy")
    func testWithGraphPropagatesToGroupBy() {
        let inner = ExecutionPattern.basic([ExecutionTriple("?s", "type", "?type")])
        let pattern = ExecutionPattern.groupBy(inner, groupVariables: ["?type"], aggregates: [], having: nil)
        let result = pattern.withGraph(graphTerm)
        let graphs = extractAllGraphs(from: result)
        #expect(graphs.allSatisfy { $0 == graphTerm })
    }

    @Test("withGraph does not affect propertyPath")
    func testWithGraphDoesNotAffectPropertyPath() {
        let pattern = ExecutionPattern.propertyPath(
            subject: .variable("?s"),
            path: .iri("knows"),
            object: .variable("?o")
        )
        let result = pattern.withGraph(graphTerm)
        // propertyPath has no triples, so allExecutionTriples is empty
        let triples = result.allExecutionTriples
        #expect(triples.isEmpty)
        // Verify the pattern structure is preserved
        if case .propertyPath(let s, _, let o) = result {
            #expect(s == .variable("?s"))
            #expect(o == .variable("?o"))
        } else {
            Issue.record("Expected propertyPath case")
        }
    }
}

// MARK: - GraphIndexKind Named Graph Tests

@Suite("GraphIndexKind Named Graph Tests")
struct GraphIndexKindNamedGraphTests {

    @Test("fieldNames includes graph field when set")
    func testFieldNamesIncludesGraphField() {
        let kind = GraphIndexKind<TestEdge>(
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
        let kind = GraphIndexKind<TestEdge>(
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
        let kind = GraphIndexKind<TestEdge>(
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
        let kind = GraphIndexKind<TestEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .tripleStore
        )
        #expect(!kind.indexName.hasSuffix("_graph"))
    }

    @Test("hasGraphField is true when set")
    func testHasGraphFieldTrueWhenSet() {
        let kind = GraphIndexKind<TestEdge>(
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
        let kind = GraphIndexKind<TestEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .tripleStore
        )
        #expect(!kind.hasGraphField)
    }

    @Test("Codable round-trip with graph field")
    func testCodableRoundTripWithGraph() throws {
        let original = GraphIndexKind<TestEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            graphField: "graph",
            strategy: .tripleStore
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(GraphIndexKind<TestEdge>.self, from: data)
        #expect(decoded.graphField == "graph")
        #expect(decoded.fromField == "source")
        #expect(decoded.edgeField == "label")
        #expect(decoded.toField == "target")
        #expect(decoded.strategy == .tripleStore)
    }

    @Test("Codable round-trip without graph field")
    func testCodableRoundTripWithoutGraph() throws {
        let original = GraphIndexKind<TestEdge>(
            fromField: "source",
            edgeField: "label",
            toField: "target",
            strategy: .hexastore
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(GraphIndexKind<TestEdge>.self, from: data)
        #expect(decoded.graphField == nil)
        #expect(decoded.strategy == .hexastore)
    }

    @Test("Hashable equality with same graph field")
    func testHashableEqualityWithSameGraph() {
        let a = GraphIndexKind<TestEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        let b = GraphIndexKind<TestEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        #expect(a == b)
        #expect(a.hashValue == b.hashValue)
    }

    @Test("Hashable inequality with different graph field")
    func testHashableInequalityWithDifferentGraph() {
        let a = GraphIndexKind<TestEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: "graph",
            strategy: .tripleStore
        )
        let b = GraphIndexKind<TestEdge>(
            fromField: "source", edgeField: "label",
            toField: "target", graphField: nil,
            strategy: .tripleStore
        )
        #expect(a != b)
    }
}
