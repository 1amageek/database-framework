// ReasonerPhase1CorrectnessTests.swift
// Phase 1 correctness tests for OWL Reasoner bug fixes:
// - B-1: Nominal merge prohibition (UNA violation)
// - B-2: conceptSignature restoration on backtrack
// - B-6: OWL2RLMaterializer depth limit enforcement
// - B-9: reachableIndividuals() property chain support

import Testing
import Foundation
import Graph
import FoundationDB
@testable import GraphIndex

// MARK: - B-1: Nominal Merge Tests

@Suite("B-1: Nominal Merge Prohibition")
struct NominalMergeTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Merging distinct nominals returns nominalClash")
    func distinctNominalMergeClash() {
        let graph = createGraph()
        let nomA = graph.getOrCreateNominal("ex:alice")
        let nomB = graph.getOrCreateNominal("ex:bob")

        let result = graph.mergeNodes(survivor: nomA, merged: nomB)

        if case .nominalClash(let survivor, let merged) = result {
            #expect(survivor == nomA)
            #expect(merged == nomB)
        } else {
            Issue.record("Expected nominalClash, got \(result)")
        }
    }

    @Test("Merging nominal and generated node succeeds")
    func nominalAndGeneratedMergeSuccess() {
        let graph = createGraph()
        let nominal = graph.getOrCreateNominal("ex:alice")
        let generated = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: generated)

        let result = graph.mergeNodes(survivor: nominal, merged: generated)
        #expect(result == .success)

        // Verify concepts were transferred
        #expect(graph.hasConcept(.named("ex:Person"), at: nominal))
    }

    @Test("Merging two generated nodes succeeds")
    func generatedNodesMerge() {
        let graph = createGraph()
        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addConcept(.named("ex:A"), to: n1)
        graph.addConcept(.named("ex:B"), to: n2)

        let result = graph.mergeNodes(survivor: n1, merged: n2)
        #expect(result == .success)

        // Verify concepts were merged
        #expect(graph.hasConcept(.named("ex:A"), at: n1))
        #expect(graph.hasConcept(.named("ex:B"), at: n1))
    }

    @Test("Merging same nominal with itself succeeds")
    func sameNominalMerge() {
        let graph = createGraph()
        let nom = graph.getOrCreateNominal("ex:alice")

        // Same nominal: survivor == merged, not "distinct"
        let result = graph.mergeNodes(survivor: nom, merged: nom)
        #expect(result == .success)
    }

    @Test("≤-rule returns clash when only nominals exceed cardinality")
    func maxCardinalityClashWithNominals() {
        let graph = createGraph()
        let root = graph.createNode()

        // Create two nominal R-successors
        let nomA = graph.getOrCreateNominal("ex:alice")
        let nomB = graph.getOrCreateNominal("ex:bob")

        graph.addEdge(from: root, role: "ex:knows", to: nomA)
        graph.addEdge(from: root, role: "ex:knows", to: nomB)

        // ≤1 ex:knows.⊤ — needs to merge 2 down to 1
        graph.addConcept(.maxCardinality(property: "ex:knows", n: 1, filler: nil), to: root)

        let result = ExpansionRules.applyMaxCardinalityRule(at: root, in: graph)

        if case .clash(let info) = result {
            #expect(info.type == .nominal)
        } else {
            Issue.record("Expected clash from merging distinct nominals, got \(result)")
        }
    }

    @Test("≤-rule succeeds when non-nominals can satisfy cardinality")
    func maxCardinalityWithMixedNodes() {
        let graph = createGraph()
        let root = graph.createNode()

        // One nominal and one generated R-successor
        let nom = graph.getOrCreateNominal("ex:alice")
        let gen = graph.createNode()

        graph.addEdge(from: root, role: "ex:knows", to: nom)
        graph.addEdge(from: root, role: "ex:knows", to: gen)

        // ≤1 ex:knows.⊤ — can merge generated into nominal
        graph.addConcept(.maxCardinality(property: "ex:knows", n: 1, filler: nil), to: root)

        let result = ExpansionRules.applyMaxCardinalityRule(at: root, in: graph)

        switch result {
        case .applied:
            // generated should be merged into nominal
            #expect(graph.node(gen) == nil)
            #expect(graph.node(nom) != nil)
        default:
            Issue.record("Expected .applied, got \(result)")
        }
    }
}

// MARK: - B-2: Signature Restoration Tests

@Suite("B-2: conceptSignature Backtrack Restoration")
struct SignatureRestorationTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Signature is recomputed after concept undo")
    func signatureRecomputedAfterUndo() {
        let graph = createGraph()
        let nodeID = graph.createNode()

        let conceptA = OWLClassExpression.named("ex:A")
        let conceptB = OWLClassExpression.named("ex:B")

        graph.addConcept(conceptA, to: nodeID)
        graph.addConcept(conceptB, to: nodeID)

        let sigWithAB = graph.node(nodeID)!.conceptSignature

        // Save position, add C, then undo
        let pos = graph.trailPosition
        let conceptC = OWLClassExpression.named("ex:C")
        graph.addConcept(conceptC, to: nodeID)

        // Signature should have changed (new bit for C)
        let sigWithABC = graph.node(nodeID)!.conceptSignature

        graph.undoToPosition(pos)

        let sigAfterUndo = graph.node(nodeID)!.conceptSignature
        #expect(sigAfterUndo == sigWithAB)

        // Verify concept C is gone
        #expect(!graph.hasConcept(conceptC, at: nodeID))
        #expect(graph.hasConcept(conceptA, at: nodeID))
        #expect(graph.hasConcept(conceptB, at: nodeID))

        // If C's bit was distinct, verify it's actually been cleared
        if sigWithABC != sigWithAB {
            #expect(sigAfterUndo != sigWithABC)
        }
    }

    @Test("Signature is accurate after multiple add-undo cycles")
    func signatureAccurateAfterMultipleCycles() {
        let graph = createGraph()
        let nodeID = graph.createNode()

        let base = OWLClassExpression.named("ex:Base")
        graph.addConcept(base, to: nodeID)
        let baseSignature = graph.node(nodeID)!.conceptSignature

        // Cycle 1: add and undo
        let pos1 = graph.trailPosition
        graph.addConcept(.named("ex:Temp1"), to: nodeID)
        graph.undoToPosition(pos1)
        #expect(graph.node(nodeID)!.conceptSignature == baseSignature)

        // Cycle 2: add and undo
        let pos2 = graph.trailPosition
        graph.addConcept(.named("ex:Temp2"), to: nodeID)
        graph.undoToPosition(pos2)
        #expect(graph.node(nodeID)!.conceptSignature == baseSignature)

        // Cycle 3: add two, undo both
        let pos3 = graph.trailPosition
        graph.addConcept(.named("ex:Temp3"), to: nodeID)
        graph.addConcept(.named("ex:Temp4"), to: nodeID)
        graph.undoToPosition(pos3)
        #expect(graph.node(nodeID)!.conceptSignature == baseSignature)
    }

    @Test("Signature recomputed after merge undo")
    func signatureRecomputedAfterMergeUndo() {
        let graph = createGraph()
        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addConcept(.named("ex:A"), to: n1)
        graph.addConcept(.named("ex:B"), to: n2)

        let sig1Before = graph.node(n1)!.conceptSignature
        let sig2Before = graph.node(n2)!.conceptSignature

        let pos = graph.trailPosition
        let mergeResult = graph.mergeNodes(survivor: n1, merged: n2)
        #expect(mergeResult == .success)

        // Undo merge
        graph.undoToPosition(pos)

        // Both nodes should have their original signatures
        #expect(graph.node(n1)!.conceptSignature == sig1Before)
        #expect(graph.node(n2) != nil)
        #expect(graph.node(n2)!.conceptSignature == sig2Before)
    }

    @Test("Blocker not missed due to stale signature after backtrack")
    func blockerNotMissedAfterBacktrack() {
        let graph = createGraph()

        // Create ancestor Y with concepts {A, B}
        let ancestorID = graph.createNode()
        graph.addConcept(.named("ex:A"), to: ancestorID)
        graph.addConcept(.named("ex:B"), to: ancestorID)

        // Create node X (child of Y) with concepts {A, B}
        let nodeID = graph.createNode(parent: ancestorID)
        graph.addConcept(.named("ex:A"), to: nodeID)
        graph.addConcept(.named("ex:B"), to: nodeID)

        // X should be blockable by Y (L(X) ⊆ L(Y))
        graph.updateBlocking()
        #expect(graph.isBlocked(nodeID))

        // Now add concept C to X, then backtrack
        let pos = graph.trailPosition
        graph.addConcept(.named("ex:C"), to: nodeID)
        graph.updateBlocking()
        // X is no longer blocked (has C that Y doesn't have)
        #expect(!graph.isBlocked(nodeID))

        // Backtrack: remove C
        graph.undoToPosition(pos)
        graph.updateBlocking()

        // X should be blocked by Y again
        // Before fix: stale bit from C would cause signature pre-check to fail
        #expect(graph.isBlocked(nodeID))
    }
}

// MARK: - B-6: Materializer Depth Limit Tests

@Suite("B-6: OWL2RLMaterializer Depth Limit")
struct MaterializerDepthLimitTests {

    @Test("Configuration maxInferenceDepth default value")
    func defaultMaxInferenceDepth() {
        let config = OWL2RLMaterializer.Configuration()
        #expect(config.maxInferenceDepth == 10)
    }

    @Test("Configuration maxInferenceDepth custom value")
    func customMaxInferenceDepth() {
        let config = OWL2RLMaterializer.Configuration(maxInferenceDepth: 3)
        #expect(config.maxInferenceDepth == 3)
    }

    @Test("InferenceStatistics has depthLimitReached field")
    func statisticsHasDepthLimitField() {
        var stats = InferenceStatistics()
        #expect(stats.depthLimitReached == false)
        stats.depthLimitReached = true
        #expect(stats.depthLimitReached == true)
    }

    @Test("Zero depth limit materializer can be created")
    func zeroDepthMaterializerCreation() {
        let subspace = OntologySubspace(base: Subspace(prefix: Array("T".utf8)))
        let store = OntologyStore(subspace: subspace)
        let config = OWL2RLMaterializer.Configuration(maxInferenceDepth: 0)
        let materializer = OWL2RLMaterializer(ontologyStore: store, configuration: config)
        _ = materializer
    }
}

// MARK: - B-9: Property Chain Tests

@Suite("B-9: reachableIndividuals Property Chain Support")
struct PropertyChainReachabilityTests {

    @Test("Two-step property chain reachability")
    func twoStepChain() {
        // hasUncle ← chain(hasParent, hasBrother)
        var ontology = OWLOntology(iri: "http://test.org/family")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasBrother"))
        ontology.objectProperties.append(OWLObjectProperty(
            iri: "ex:hasUncle",
            propertyChains: [["ex:hasParent", "ex:hasBrother"]]
        ))
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasBrother"],
            sup: "ex:hasUncle"
        ))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:Alice",
            property: "ex:hasParent",
            object: "ex:Bob"
        ))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:Bob",
            property: "ex:hasBrother",
            object: "ex:Charlie"
        ))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(
            from: "ex:Alice",
            via: "ex:hasUncle",
            includeInferred: true
        )

        #expect(reachable.contains("ex:Charlie"))
    }

    @Test("Three-step property chain")
    func threeStepChain() {
        var ontology = OWLOntology(iri: "http://test.org/family")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasSibling"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasChild"))
        ontology.objectProperties.append(OWLObjectProperty(
            iri: "ex:hasCousin",
            propertyChains: [["ex:hasParent", "ex:hasSibling", "ex:hasChild"]]
        ))
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasSibling", "ex:hasChild"],
            sup: "ex:hasCousin"
        ))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:A", property: "ex:hasParent", object: "ex:B"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:B", property: "ex:hasSibling", object: "ex:C"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:C", property: "ex:hasChild", object: "ex:D"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:A", via: "ex:hasCousin", includeInferred: true)

        #expect(reachable.contains("ex:D"))
    }

    @Test("Broken chain returns empty set")
    func brokenChain() {
        var ontology = OWLOntology(iri: "http://test.org/family")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasBrother"))
        ontology.objectProperties.append(OWLObjectProperty(
            iri: "ex:hasUncle",
            propertyChains: [["ex:hasParent", "ex:hasBrother"]]
        ))
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasBrother"],
            sup: "ex:hasUncle"
        ))
        // Alice hasParent Bob, but Bob has no hasBrother
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Alice", property: "ex:hasParent", object: "ex:Bob"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:Alice", via: "ex:hasUncle", includeInferred: true)

        #expect(reachable.isEmpty)
    }

    @Test("Chain with sub-property at chain step")
    func chainWithSubProperty() {
        var ontology = OWLOntology(iri: "http://test.org/family")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasFather", superProperties: ["ex:hasParent"]))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasBrother"))
        ontology.objectProperties.append(OWLObjectProperty(
            iri: "ex:hasUncle",
            propertyChains: [["ex:hasParent", "ex:hasBrother"]]
        ))
        ontology.axioms.append(.subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"))
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasBrother"],
            sup: "ex:hasUncle"
        ))
        // Alice hasFather Bob (subProp of hasParent), Bob hasBrother Charlie
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Alice", property: "ex:hasFather", object: "ex:Bob"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Bob", property: "ex:hasBrother", object: "ex:Charlie"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:Alice", via: "ex:hasUncle", includeInferred: true)

        // hasFather is a subProperty of hasParent, so the chain should work
        #expect(reachable.contains("ex:Charlie"))
    }

    @Test("Without includeInferred, chain is not applied")
    func noChainWithoutInferred() {
        var ontology = OWLOntology(iri: "http://test.org/family")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasBrother"))
        ontology.objectProperties.append(OWLObjectProperty(
            iri: "ex:hasUncle",
            propertyChains: [["ex:hasParent", "ex:hasBrother"]]
        ))
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasBrother"],
            sup: "ex:hasUncle"
        ))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Alice", property: "ex:hasParent", object: "ex:Bob"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Bob", property: "ex:hasBrother", object: "ex:Charlie"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:Alice", via: "ex:hasUncle", includeInferred: false)

        // No direct hasUncle assertions
        #expect(reachable.isEmpty)
    }
}

// MARK: - MergeResult Equatable (for tests)

extension MergeResult: Equatable {
    public static func == (lhs: MergeResult, rhs: MergeResult) -> Bool {
        switch (lhs, rhs) {
        case (.success, .success):
            return true
        case (.nominalClash(let ls, let lm), .nominalClash(let rs, let rm)):
            return ls == rs && lm == rm
        default:
            return false
        }
    }
}
