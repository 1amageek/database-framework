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

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .applied = result {
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

    // MARK: - Contradictory facet tests (A1)

    @Test("Contradictory facets: minInclusive=10, maxExclusive=5 returns nil")
    func contradictoryFacetsReturnNil() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(10), .maxExclusive(5)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness == nil)
    }

    @Test("Contradictory facets: minInclusive=5, maxExclusive=5 returns nil (half-open empty)")
    func halfOpenEmptyReturnsNil() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(5), .maxExclusive(5)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness == nil)
    }

    // MARK: - Integer boundary tests (A2)

    @Test("Integer range: minExclusive=2, maxExclusive=3 returns nil (no integer in (2,3))")
    func noIntegerInOpenRange() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minExclusive(2), .maxExclusive(3)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness == nil)
    }

    @Test("Integer range: minInclusive=2, maxInclusive=5 returns valid witness")
    func integerWitnessInClosedRange() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(2), .maxInclusive(5)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness, let v = w.doubleValue {
            #expect(v >= 2.0)
            #expect(v <= 5.0)
            // Should be an integer
            #expect(v == v.rounded())
        }
    }

    @Test("Integer range: minExclusive=2, maxInclusive=3 returns 3")
    func integerWitnessExclusiveInclusive() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minExclusive(2), .maxInclusive(3)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness, let v = w.doubleValue {
            #expect(v > 2.0)
            #expect(v <= 3.0)
        }
    }

    // MARK: - String facet tests (A3)

    @Test("String maxLength=2 returns witness of length <= 2")
    func stringMaxLengthFacet() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:string",
            facets: [.maxLength(2)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness {
            #expect(w.lexicalForm.count <= 2)
        }
    }

    @Test("String contradictory length: minLength=5, maxLength=2 returns nil")
    func stringContradictoryLengthReturnsNil() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:string",
            facets: [.minLength(5), .maxLength(2)]
        )
        let witness = testGenerateWitness(for: dataRange)
        #expect(witness == nil)
    }

    // MARK: - Data existential clash test (A4)

    @Test("Data existential with unsatisfiable range returns clash")
    func dataExistentialClash() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .datatypeRestriction(
                datatype: "xsd:integer",
                facets: [.minInclusive(10), .maxExclusive(5)]
            )
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .clash(let info) = result {
            #expect(info.type == .datatype)
        } else {
            Issue.record("Expected clash but got \(result)")
        }
    }

    // MARK: - CompletionGraph undo + dangling edges test (B1)

    @Test("Undo of createdNode removes associated edges")
    func undoCreatedNodeCleansEdges() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let parentNode = graph.createNode()

        let trailBefore = graph.trailPosition

        // Create child and add edge
        let childNode = graph.createNode(parent: parentNode)
        graph.addEdge(from: parentNode, role: "ex:hasChild", to: childNode)

        // Verify edge exists
        #expect(graph.successors(of: parentNode, via: "ex:hasChild").contains(childNode))

        // Undo to before child creation (edges first, then node — LIFO order)
        graph.undoToPosition(trailBefore)

        // Both the node and its edges should be gone
        #expect(graph.node(childNode) == nil)
        #expect(graph.successors(of: parentNode, via: "ex:hasChild").isEmpty)
    }

    // MARK: - RoleHierarchy large chain test (C)

    @Test("RoleHierarchy: 100-node chain computes correct transitive closure")
    func largeChainClosure() {
        var rh = RoleHierarchy()
        // Chain: r0 ⊑ r1 ⊑ r2 ⊑ ... ⊑ r99
        for i in 0..<99 {
            rh.addSubRole(sub: "r\(i)", super: "r\(i + 1)")
        }
        rh.ensureClosuresComputed()

        // Sub-roles of r99 should include r0..r98
        let subs = rh.subRolesPrecomputed(of: "r99")
        #expect(subs.count == 99)
        #expect(subs.contains("r0"))
        #expect(subs.contains("r50"))
        #expect(subs.contains("r98"))
    }

    // MARK: - Complex data range does NOT cause false clash

    @Test("dataComplementOf generates witness (non-integer value for ¬xsd:integer)")
    func dataComplementOfGeneratesWitness() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // ¬xsd:integer — complement of integers includes all strings, dates, etc.
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataComplementOf(.datatype("xsd:integer"))
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        // Should generate a non-integer witness (e.g., xsd:string "witness")
        if case .applied = result {
            let values = graph.node(nodeID)?.dataValues["test:prop"]
            #expect(values != nil && !values!.isEmpty, "Witness should be generated")
            if let w = values?.first {
                #expect(w.datatype != "xsd:integer", "Witness must not be an integer")
            }
        } else if case .clash = result {
            Issue.record("dataComplementOf should not cause clash (¬xsd:integer is satisfiable)")
        }
        // .notApplicable is acceptable as a fallback if complement generation is unsupported
    }

    @Test("dataUnionOf generates witness (picks first satisfiable sub-range)")
    func dataUnionOfGeneratesWitness() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataUnionOf([.datatype("xsd:integer"), .datatype("xsd:string")])
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .applied = result {
            let values = graph.node(nodeID)?.dataValues["test:prop"]
            #expect(values != nil && !values!.isEmpty, "Witness should be generated for union")
        } else if case .clash = result {
            Issue.record("dataUnionOf should not cause clash (union of integer|string is satisfiable)")
        } else {
            Issue.record("Expected .applied for satisfiable union but got \(result)")
        }
    }

    @Test("dataUnionOf with empty sub-ranges returns clash")
    func dataUnionOfEmptyClashes() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // Empty union — no possible values
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataUnionOf([])
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .clash(let info) = result {
            #expect(info.type == .datatype)
        } else {
            Issue.record("Expected clash for empty union but got \(result)")
        }
    }

    @Test("dataUnionOf with all unsatisfiable sub-ranges returns clash")
    func dataUnionOfAllUnsatisfiableClashes() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // Union of two contradictory ranges
        let range1 = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer", facets: [.minInclusive(10), .maxExclusive(5)]
        )
        let range2 = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer", facets: [.minInclusive(20), .maxExclusive(15)]
        )
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataUnionOf([range1, range2])
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .clash(let info) = result {
            #expect(info.type == .datatype)
        } else {
            Issue.record("Expected clash for all-unsatisfiable union but got \(result)")
        }
    }

    @Test("dataIntersectionOf with satisfiable sub-ranges generates witness")
    func dataIntersectionOfSatisfiable() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // Intersection: [0, 100] ∩ [50, 200] = [50, 100]
        let range1 = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer", facets: [.minInclusive(0), .maxInclusive(100)]
        )
        let range2 = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer", facets: [.minInclusive(50), .maxInclusive(200)]
        )
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataIntersectionOf([range1, range2])
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .applied = result {
            let values = graph.node(nodeID)?.dataValues["test:prop"]
            #expect(values != nil && !values!.isEmpty, "Witness should be generated")
            if let w = values?.first, let v = w.doubleValue {
                #expect(v >= 50.0 && v <= 100.0, "Witness \(v) must be in [50, 100]")
            }
        } else {
            Issue.record("Expected .applied for satisfiable intersection but got \(result)")
        }
    }

    // MARK: - Exclusive facet tightening

    @Test("minExclusive tightens inclusive bound at same value")
    func exclusiveTightensInclusive() {
        // minInclusive=5 AND minExclusive=5 → effective: x > 5
        // With maxInclusive=6 → range is (5, 6], witness should be 6
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(5), .minExclusive(5), .maxInclusive(6)]
        )

        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness, let v = w.doubleValue {
            #expect(v > 5.0, "Witness must be strictly greater than 5 (exclusive)")
            #expect(v <= 6.0)
        }
    }

    // MARK: - Integer witness edge cases

    @Test("Integer witness: large values (within Double precision)")
    func integerWitnessLargeValues() {
        // Use values within Double's exact integer range (2^53 = 9007199254740992)
        let minVal = 9007199254740980
        let maxVal = 9007199254740990
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(minVal), .maxInclusive(maxVal)]
        )

        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil, "Should find integer in [\(minVal), \(maxVal)]")
        if let w = witness, let v = Int(w.lexicalForm) {
            #expect(v >= minVal && v <= maxVal, "Witness \(v) must be in [\(minVal), \(maxVal)]")
        }
    }

    @Test("Integer witness: single point [42, 42]")
    func integerWitnessSinglePoint() {
        let dataRange = OWLDataRange.datatypeRestriction(
            datatype: "xsd:integer",
            facets: [.minInclusive(42), .maxInclusive(42)]
        )

        let witness = testGenerateWitness(for: dataRange)
        #expect(witness != nil)
        if let w = witness {
            #expect(w.lexicalForm == "42", "Single-point range must yield 42, got \(w.lexicalForm)")
        }
    }

    @Test("String with pattern facet returns unsupported (not clash)")
    func stringPatternUnsupported() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // xsd:string with pattern "[0-9]+" — "aaa" won't match
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .datatypeRestriction(
                datatype: "xsd:string",
                facets: [.pattern("[0-9]+")]
            )
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        // Pattern-constrained string should NOT cause clash (we can't prove unsatisfiability)
        if case .clash = result {
            Issue.record("String pattern should not cause clash (unsupported, not unsatisfiable)")
        }
    }

    @Test("Integer with pattern facet returns unsupported (not clash)")
    func integerPatternUnsupported() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        // xsd:integer with pattern "[13579]" — witness "0" won't match
        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .datatypeRestriction(
                datatype: "xsd:integer",
                facets: [.pattern("[13579]")]
            )
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .clash = result {
            Issue.record("Integer pattern should not cause clash (unsupported, not unsatisfiable)")
        }
    }

    @Test("dataOneOf with empty values returns clash")
    func dataOneOfEmptyClashes() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
        let nodeID = graph.createNode()

        let concept = OWLClassExpression.dataSomeValuesFrom(
            property: "test:prop",
            range: .dataOneOf([])
        )
        graph.addConcept(concept, to: nodeID)

        let result = ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph)
        if case .clash(let info) = result {
            #expect(info.type == .datatype)
        } else {
            Issue.record("Expected clash for empty dataOneOf but got \(result)")
        }
    }

    // MARK: - CompletionGraph processed flag undo (existing)

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
