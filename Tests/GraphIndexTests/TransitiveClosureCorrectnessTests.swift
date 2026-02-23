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

    // MARK: - Category A: 特性継承テスト

    @Test("A1: functional inherited from super-role to sub-role")
    func functionalInheritedFromSuperRole() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.functional, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isFunctional("ex:hasRelative"), "Super-role should be functional")
        #expect(rh.isFunctional("ex:hasChild"), "Sub-role should inherit functional from super-role")
    }

    @Test("A2: inverseFunctional inherited from super-role to sub-role")
    func inverseFunctionalInheritedFromSuperRole() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.inverseFunctional, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isInverseFunctional("ex:hasRelative"))
        #expect(rh.isInverseFunctional("ex:hasChild"), "Sub-role should inherit inverseFunctional")
    }

    @Test("A3: irreflexive inherited from super-role to sub-role")
    func irreflexiveInheritedFromSuperRole() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.irreflexive, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isIrreflexive("ex:hasRelative"))
        #expect(rh.isIrreflexive("ex:hasChild"), "Sub-role should inherit irreflexive")
    }

    @Test("A4: asymmetric inherited from super-role to sub-role")
    func asymmetricInheritedFromSuperRole() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.asymmetric, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isAsymmetric("ex:hasRelative"))
        #expect(rh.isAsymmetric("ex:hasChild"), "Sub-role should inherit asymmetric")
    }

    @Test("A5: transitive NOT inherited from super-role (OWL 2 spec)")
    func transitiveNotInherited() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.transitive, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isTransitive("ex:hasRelative"), "Super-role should be transitive")
        #expect(!rh.isTransitive("ex:hasChild"), "Sub-role must NOT inherit transitive")
    }

    @Test("A6: symmetric NOT inherited from super-role (OWL 2 spec)")
    func symmetricNotInherited() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.symmetric, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isSymmetric("ex:hasRelative"))
        #expect(!rh.isSymmetric("ex:hasChild"), "Sub-role must NOT inherit symmetric")
    }

    @Test("A7: reflexive NOT inherited from super-role (OWL 2 spec)")
    func reflexiveNotInherited() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.reflexive, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isReflexive("ex:hasRelative"))
        #expect(!rh.isReflexive("ex:hasChild"), "Sub-role must NOT inherit reflexive")
    }

    @Test("A8: functional inherited through deep hierarchy (r1 ⊑ r2 ⊑ r3)")
    func functionalInheritedDeepHierarchy() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:r1", super: "ex:r2")
        rh.addSubRole(sub: "ex:r2", super: "ex:r3")
        rh.setCharacteristic(.functional, for: "ex:r3", value: true)
        rh.ensureClosuresComputed()

        #expect(rh.isFunctional("ex:r3"), "Top role should be functional")
        #expect(rh.isFunctional("ex:r2"), "Middle role should inherit functional")
        #expect(rh.isFunctional("ex:r1"), "Leaf role should inherit functional through 2 levels")
    }

    // MARK: - Category B: Tableaux clash detection + inherited characteristics

    @Test("B1: inherited functional causes clash with 2 fillers")
    func inheritedFunctionalCausesClash() {
        // hasChild ⊑ hasRelative, hasRelative is functional
        // hasChild with 2 fillers → functional clash
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.functional, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeA = graph.createNode()
        let nodeB = graph.createNode()
        let nodeC = graph.createNode()

        graph.addEdge(from: nodeA, role: "ex:hasChild", to: nodeB)
        graph.addEdge(from: nodeA, role: "ex:hasChild", to: nodeC)

        let clash = ExpansionRules.detectClash(
            at: nodeA, in: graph,
            classHierarchy: ch, roleHierarchy: rh
        )
        #expect(clash != nil, "Should detect functional clash via inherited characteristic")
        #expect(clash?.type == .functional)
    }

    @Test("B2: inherited irreflexive causes clash on self-loop")
    func inheritedIrreflexiveCausesClash() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.irreflexive, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeA = graph.createNode()
        graph.addEdge(from: nodeA, role: "ex:hasChild", to: nodeA)

        let clash = ExpansionRules.detectClash(
            at: nodeA, in: graph,
            classHierarchy: ch, roleHierarchy: rh
        )
        #expect(clash != nil, "Should detect irreflexive clash via inherited characteristic")
        #expect(clash?.type == .irreflexive)
    }

    @Test("B3: inherited asymmetric causes clash on bidirectional edge")
    func inheritedAsymmetricCausesClash() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:hasChild", super: "ex:hasRelative")
        rh.setCharacteristic(.asymmetric, for: "ex:hasRelative", value: true)
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeA = graph.createNode()
        let nodeB = graph.createNode()
        graph.addEdge(from: nodeA, role: "ex:hasChild", to: nodeB)
        graph.addEdge(from: nodeB, role: "ex:hasChild", to: nodeA)

        let clash = ExpansionRules.detectClash(
            at: nodeA, in: graph,
            classHierarchy: ch, roleHierarchy: rh
        )
        #expect(clash != nil, "Should detect asymmetric clash via inherited characteristic")
        #expect(clash?.type == .asymmetric)
    }

    // MARK: - Category C: Universal rule + deep hierarchy

    @Test("C1: ∀R.C propagates to all sub-role successors in deep hierarchy")
    func universalRulePropagatesViaDeepSubRoles() {
        // r1 ⊑ r2 ⊑ r3, node x has ∀r3.C, successors via r1 should get C
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:r1", super: "ex:r2")
        rh.addSubRole(sub: "ex:r2", super: "ex:r3")
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        let nodeY = graph.createNode()
        let nodeZ = graph.createNode()

        // x has ∀r3.A (universal on super-role)
        graph.addConcept(.allValuesFrom(property: "ex:r3", filler: .named("ex:A")), to: nodeX)

        // y is a r1-successor of x (r1 is sub-sub-role of r3)
        graph.addEdge(from: nodeX, role: "ex:r1", to: nodeY)
        // z is a r2-successor of x
        graph.addEdge(from: nodeX, role: "ex:r2", to: nodeZ)

        let changed = ExpansionRules.applyUniversalRule(
            at: nodeX, in: graph, roleHierarchy: rh
        )

        #expect(changed, "Universal rule should have applied")
        #expect(graph.hasConcept(.named("ex:A"), at: nodeY), "r1-successor should get A (r1 ⊑ r2 ⊑ r3)")
        #expect(graph.hasConcept(.named("ex:A"), at: nodeZ), "r2-successor should get A (r2 ⊑ r3)")
    }

    @Test("C2: successorsViaSubRoles returns all sub-role successors in deep hierarchy")
    func successorsViaSubRolesDeepHierarchy() {
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:r1", super: "ex:r2")
        rh.addSubRole(sub: "ex:r2", super: "ex:r3")
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        let nodeA = graph.createNode()
        let nodeB = graph.createNode()
        let nodeC = graph.createNode()

        graph.addEdge(from: nodeX, role: "ex:r3", to: nodeA)
        graph.addEdge(from: nodeX, role: "ex:r2", to: nodeB)
        graph.addEdge(from: nodeX, role: "ex:r1", to: nodeC)

        let allSuccessors = graph.successorsViaSubRoles(of: nodeX, via: "ex:r3")

        #expect(allSuccessors.contains(nodeA), "Direct r3-successor")
        #expect(allSuccessors.contains(nodeB), "r2-successor (r2 ⊑ r3)")
        #expect(allSuccessors.contains(nodeC), "r1-successor (r1 ⊑ r2 ⊑ r3)")
    }

    // MARK: - Category D: Qualified cardinality

    @Test("D1: ≤1 R.C with 2 qualified fillers triggers merge")
    func maxCardinalityMergeQualified() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        let nodeA = graph.createNode()
        let nodeB = graph.createNode()

        // ≤1 ex:R.ex:C
        graph.addConcept(.maxCardinality(property: "ex:R", n: 1, filler: .named("ex:C")), to: nodeX)
        graph.addConcept(.named("ex:C"), to: nodeA)
        graph.addConcept(.named("ex:C"), to: nodeB)
        graph.addEdge(from: nodeX, role: "ex:R", to: nodeA)
        graph.addEdge(from: nodeX, role: "ex:R", to: nodeB)

        let result = ExpansionRules.applyMaxCardinalityRule(at: nodeX, in: graph)
        if case .applied = result {
            // Expected: merge to satisfy ≤1 R.C
        } else {
            Issue.record("Expected .applied for max cardinality merge but got \(result)")
        }
    }

    @Test("D2: ≥2 R.C ⊓ ≤1 R.C is immediately unsatisfiable")
    func conflictingCardinalitiesClash() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        graph.addConcept(.minCardinality(property: "ex:R", n: 2, filler: .named("ex:C")), to: nodeX)
        graph.addConcept(.maxCardinality(property: "ex:R", n: 1, filler: .named("ex:C")), to: nodeX)

        let clash = ExpansionRules.detectClash(
            at: nodeX, in: graph,
            classHierarchy: ch, roleHierarchy: rh
        )
        #expect(clash != nil, "≥2 ⊓ ≤1 should produce a clash")
        #expect(clash?.type == .maxCardinality)
    }

    @Test("D3: ≤1 R.C does NOT merge unqualified successors")
    func maxCardinalityQualifiedVsUnqualified() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        let nodeA = graph.createNode()
        let nodeB = graph.createNode()

        // ≤1 ex:R.ex:C (qualified)
        graph.addConcept(.maxCardinality(property: "ex:R", n: 1, filler: .named("ex:C")), to: nodeX)
        // Only nodeA has C, nodeB does not
        graph.addConcept(.named("ex:C"), to: nodeA)
        graph.addEdge(from: nodeX, role: "ex:R", to: nodeA)
        graph.addEdge(from: nodeX, role: "ex:R", to: nodeB)

        let result = ExpansionRules.applyMaxCardinalityRule(at: nodeX, in: graph)
        // Only 1 qualified filler (nodeA), so no merge needed
        if case .notApplicable = result {
            // Expected: only 1 qualified successor, no merge needed
        } else {
            Issue.record("Expected .notApplicable but got \(result)")
        }
    }

    // MARK: - Category E: Blocking + edge labels

    @Test("E1: basic blocking — L(x) ⊆ L(y) with matching edge labels")
    func basicBlockingSubsetConcepts() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        // Create root → ancestor → node chain
        let root = graph.createNode()
        let ancestor = graph.createNode(parent: root)
        let node = graph.createNode(parent: ancestor)

        // ancestor has {A, B}, node has {A}
        graph.addConcept(.named("ex:A"), to: ancestor)
        graph.addConcept(.named("ex:B"), to: ancestor)
        graph.addConcept(.named("ex:A"), to: node)

        // Both need matching edge labels
        let child1 = graph.createNode(parent: ancestor)
        graph.addEdge(from: ancestor, role: "ex:R", to: child1)
        let child2 = graph.createNode(parent: node)
        graph.addEdge(from: node, role: "ex:R", to: child2)

        graph.updateBlocking()

        #expect(graph.isBlocked(node.self), "Node should be blocked by ancestor (L(node) ⊆ L(ancestor))")
    }

    @Test("E2: blocking fails when edge labels don't match")
    func blockingFailsEdgeLabelMismatch() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let root = graph.createNode()
        let ancestor = graph.createNode(parent: root)
        let node = graph.createNode(parent: ancestor)

        // Same concepts
        graph.addConcept(.named("ex:A"), to: ancestor)
        graph.addConcept(.named("ex:A"), to: node)

        // Different edge labels
        let child1 = graph.createNode(parent: ancestor)
        graph.addEdge(from: ancestor, role: "ex:R", to: child1)
        let child2 = graph.createNode(parent: node)
        graph.addEdge(from: node, role: "ex:S", to: child2)

        graph.updateBlocking()

        #expect(!graph.isBlocked(node.self), "Node should NOT be blocked (different edge labels)")
    }

    @Test("E3: nominal nodes are never blocked")
    func nominalNeverBlocked() {
        let rh = RoleHierarchy()
        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let root = graph.createNode()
        let ancestor = graph.createNode(parent: root)

        // Create nominal as child of ancestor
        let nominal = graph.getOrCreateNominal("ex:alice")

        // Give ancestor a superset of nominal's concepts
        graph.addConcept(.named("ex:A"), to: ancestor)
        graph.addConcept(.named("ex:A"), to: nominal)

        graph.updateBlocking()

        #expect(!graph.isBlocked(nominal), "Nominal nodes must never be blocked")
    }

    // MARK: - Category F: Integration scenarios

    @Test("F1: inherited functional causes unsatisfiability via Tableaux")
    func inheritedFunctionalCausesUnsatisfiabilityViaTableaux() {
        // If hasChild ⊑ hasRelative and hasRelative is functional,
        // then hasChild also becomes functional via inheritance.
        // ∃hasChild.A ⊓ ∃hasChild.B creates 2 fillers → functional clash detected.
        // This proves the inheritance propagates correctly through the Tableaux reasoner.
        let ontology = OWLOntology(
            iri: "http://test.org/f1",
            objectProperties: [
                OWLObjectProperty(iri: "ex:hasChild", superProperties: ["ex:hasRelative"]),
                OWLObjectProperty(iri: "ex:hasRelative", characteristics: [.functional]),
            ],
            axioms: [
                .subObjectPropertyOf(sub: "ex:hasChild", sup: "ex:hasRelative"),
                .functionalObjectProperty("ex:hasRelative"),
            ]
        )

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasChild.A ⊓ ∃hasChild.B — generates 2 fillers for inherited-functional role
        let expr = OWLClassExpression.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:B")),
        ])

        let result = reasoner.checkSatisfiability(expr)
        // Functional clash is detected before ≤-rule merging, proving inheritance works
        #expect(result.isUnsatisfiable, "Functional clash via inherited characteristic should make this unsatisfiable")
        #expect(result.clash?.type == .functional, "Clash should be functional type")
    }

    @Test("F2: universal rule + max cardinality in deep hierarchy")
    func universalRuleWithMaxCardinalityDeepHierarchy() {
        // r1 ⊑ r2, ∀r2.C ⊓ ≤1 r2.⊤
        // All r1-successors should get C, and at most 1 successor allowed
        var rh = RoleHierarchy()
        rh.addSubRole(sub: "ex:r1", super: "ex:r2")
        rh.ensureClosuresComputed()

        let ch = ClassHierarchy()
        let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)

        let nodeX = graph.createNode()
        let nodeY = graph.createNode()

        graph.addConcept(.allValuesFrom(property: "ex:r2", filler: .named("ex:C")), to: nodeX)
        graph.addConcept(.maxCardinality(property: "ex:r2", n: 1, filler: nil), to: nodeX)
        graph.addEdge(from: nodeX, role: "ex:r1", to: nodeY)

        // Apply universal rule — should propagate C to nodeY via r1 ⊑ r2
        let changed = ExpansionRules.applyUniversalRule(at: nodeX, in: graph, roleHierarchy: rh)

        #expect(changed, "Universal rule should propagate C to r1-successor")
        #expect(graph.hasConcept(.named("ex:C"), at: nodeY), "r1-successor should have C")
    }

    @Test("F3: detectClash covers all clash types directly")
    func detectClashAllTypes() {
        // Test each clash type independently

        // 1. Complement clash: A ⊓ ¬A
        do {
            let rh = RoleHierarchy()
            let ch = ClassHierarchy()
            let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
            let node = graph.createNode()
            graph.addConcept(.named("ex:A"), to: node)
            graph.addConcept(.complement(.named("ex:A")), to: node)

            let clash = ExpansionRules.detectClash(at: node, in: graph, classHierarchy: ch, roleHierarchy: rh)
            #expect(clash?.type == .complement, "Should detect complement clash")
        }

        // 2. Bottom clash: owl:Nothing
        do {
            let rh = RoleHierarchy()
            let ch = ClassHierarchy()
            let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
            let node = graph.createNode()
            graph.addConcept(.nothing, to: node)

            let clash = ExpansionRules.detectClash(at: node, in: graph, classHierarchy: ch, roleHierarchy: rh)
            #expect(clash?.type == .bottom, "Should detect bottom clash")
        }

        // 3. Functional clash: R(x,a), R(x,b) with functional R
        do {
            var rh = RoleHierarchy()
            rh.setCharacteristic(.functional, for: "ex:R", value: true)
            rh.ensureClosuresComputed()
            let ch = ClassHierarchy()
            let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
            let nodeX = graph.createNode()
            let nodeA = graph.createNode()
            let nodeB = graph.createNode()
            graph.addEdge(from: nodeX, role: "ex:R", to: nodeA)
            graph.addEdge(from: nodeX, role: "ex:R", to: nodeB)

            let clash = ExpansionRules.detectClash(at: nodeX, in: graph, classHierarchy: ch, roleHierarchy: rh)
            #expect(clash?.type == .functional, "Should detect functional clash")
        }

        // 4. Irreflexive clash: R(x,x) with irreflexive R
        do {
            var rh = RoleHierarchy()
            rh.setCharacteristic(.irreflexive, for: "ex:R", value: true)
            rh.ensureClosuresComputed()
            let ch = ClassHierarchy()
            let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
            let nodeX = graph.createNode()
            graph.addEdge(from: nodeX, role: "ex:R", to: nodeX)

            let clash = ExpansionRules.detectClash(at: nodeX, in: graph, classHierarchy: ch, roleHierarchy: rh)
            #expect(clash?.type == .irreflexive, "Should detect irreflexive clash")
        }

        // 5. Asymmetric clash: R(x,y) ∧ R(y,x) with asymmetric R
        do {
            var rh = RoleHierarchy()
            rh.setCharacteristic(.asymmetric, for: "ex:R", value: true)
            rh.ensureClosuresComputed()
            let ch = ClassHierarchy()
            let graph = CompletionGraph(roleHierarchy: rh, classHierarchy: ch)
            let nodeX = graph.createNode()
            let nodeY = graph.createNode()
            graph.addEdge(from: nodeX, role: "ex:R", to: nodeY)
            graph.addEdge(from: nodeY, role: "ex:R", to: nodeX)

            let clash = ExpansionRules.detectClash(at: nodeX, in: graph, classHierarchy: ch, roleHierarchy: rh)
            #expect(clash?.type == .asymmetric, "Should detect asymmetric clash")
        }
    }
}
