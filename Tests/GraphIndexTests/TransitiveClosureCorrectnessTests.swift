// TransitiveClosureCorrectnessTests.swift
// Tests for the Warshall-based transitive closure in ReasoningGraphQueryBuilder.
//
// These tests validate that the transitive closure correctly handles
// arbitrary chain lengths (including 5+ node chains) and various
// graph topologies.

import Testing
import Graph
@testable import GraphIndex

@Suite("Transitive Closure Correctness")
struct TransitiveClosureCorrectnessTests {

    // MARK: - RoleHierarchy closure tests

    @Test("RoleHierarchy: in-degree based topological sort produces correct closure")
    func roleHierarchyTopologicalSort() {
        var rh = RoleHierarchy()
        // Chain: p1 ⊑ p2 ⊑ p3 ⊑ p4 ⊑ p5
        rh.addSubRole(sub: "p1", super: "p2")
        rh.addSubRole(sub: "p2", super: "p3")
        rh.addSubRole(sub: "p3", super: "p4")
        rh.addSubRole(sub: "p4", super: "p5")
        rh.ensureClosuresComputed()

        // Sub-roles of p5 should include p1..p4
        let subs = rh.subRolesPrecomputed(of: "p5")
        #expect(subs == Set(["p1", "p2", "p3", "p4"]))

        // Sub-roles of p3 should include p1, p2
        let subs3 = rh.subRolesPrecomputed(of: "p3")
        #expect(subs3 == Set(["p1", "p2"]))
    }

    @Test("RoleHierarchy: cyclic hierarchy handled by DFS fallback")
    func roleHierarchyCyclicFallback() {
        var rh = RoleHierarchy()
        // Cycle: a ⊑ b ⊑ c ⊑ a
        rh.addSubRole(sub: "a", super: "b")
        rh.addSubRole(sub: "b", super: "c")
        rh.addSubRole(sub: "c", super: "a")
        rh.ensureClosuresComputed()

        // In a cycle, all roles are mutual sub-roles (excluding self)
        let subsA = rh.subRolesPrecomputed(of: "a")
        #expect(subsA.contains("b"))
        #expect(subsA.contains("c"))
    }

    @Test("RoleHierarchy: wide hierarchy (many children)")
    func roleHierarchyWide() {
        var rh = RoleHierarchy()
        for i in 0..<50 {
            rh.addSubRole(sub: "child\(i)", super: "parent")
        }
        rh.ensureClosuresComputed()

        let subs = rh.subRolesPrecomputed(of: "parent")
        #expect(subs.count == 50)
    }

    // MARK: - OWLDatatypeValidator NaN tests

    @Test("NaN comparison returns nil (incomparable)")
    func nanComparison() {
        let validator = OWLDatatypeValidator()
        let nan = OWLLiteral(lexicalForm: "NaN", datatype: "xsd:double")
        let five = OWLLiteral(lexicalForm: "5.0", datatype: "xsd:double")

        // NaN is incomparable with any value
        #expect(validator.compare(nan, five) == nil)
        #expect(validator.compare(five, nan) == nil)
        #expect(validator.compare(nan, nan) == nil)
    }

    @Test("Normal numeric comparison still works")
    func normalNumericComparison() {
        let validator = OWLDatatypeValidator()
        let three = OWLLiteral(lexicalForm: "3.0", datatype: "xsd:double")
        let five = OWLLiteral(lexicalForm: "5.0", datatype: "xsd:double")

        #expect(validator.compare(three, five) == .orderedAscending)
        #expect(validator.compare(five, three) == .orderedDescending)
        #expect(validator.compare(five, five) == .orderedSame)
    }

    // MARK: - ExpansionRules facet-aware witness tests

    @Test("Facet-aware witness for datatypeRestriction with minInclusive")
    func facetAwareWitnessMinInclusive() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(10)]
        )

        // The witness should satisfy minInclusive >= 10
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness, let v = w.doubleValue {
            #expect(v >= 10.0)
        }
    }

    @Test("Facet-aware witness for range [5, 15]")
    func facetAwareWitnessRange() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(5), .maxInclusive(15)]
        )

        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness, let v = w.doubleValue {
            #expect(v >= 5.0)
            #expect(v <= 15.0)
        }
    }

    /// Helper to access witness generation via ExpansionRules
    private func testGenerateWitness(for dataRange: OWLDataRange) -> OWLLiteral? {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: dataRange
        )
        graph.addConcept(concept, to: nodeID)

        let changed = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if changed {
            if let values = graph.node(nodeID)?.dataValues["test:prop"] {
                return values.first
            }
        }

        return nil
    }

    // MARK: - CompletionGraph processed flag undo

    @Test("Undo of addedConcept clears processed flags")
    func undoConceptClearsProcessedFlags() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        let trailBefore = graph.trailPosition

        // Add an intersection concept
        let concept = OWLClassExpression.intersection([.named("A"), .named("B")])
        graph.addConcept(concept, to: nodeID)

        // Simulate that intersection rule was applied
        graph.node(nodeID)?.processedIntersections.insert(concept)

        // Undo
        graph.undoToPosition(trailBefore)

        // The concept should be gone AND the processed flag should be cleared
        let node = graph.node(nodeID)
        #expect(node?.concepts.contains(concept) != true)
        #expect(node?.processedIntersections.contains(concept) != true)
    }

    @Test("Undo of addedConcept allows re-application of rules")
    func undoAllowsReapplication() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        let trailBefore = graph.trailPosition

        // Add and process
        let concept = OWLClassExpression.union([.named("A"), .named("B")])
        graph.addConcept(concept, to: nodeID)
        graph.node(nodeID)?.processedUnions.insert(concept)

        // Undo
        graph.undoToPosition(trailBefore)

        // Re-add the same concept
        graph.addConcept(concept, to: nodeID)

        // processedUnions should NOT contain the concept (was cleared by undo)
        #expect(graph.node(nodeID)?.processedUnions.contains(concept) != true)
    }
}
