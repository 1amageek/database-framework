// CompletionGraphTests.swift
// Tests for CompletionGraph - trail-based state management

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - Node Operations Tests

@Suite("CompletionGraph Node Operations")
struct CompletionGraphNodeTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Create node increments counter")
    func createNodeIncrementsCounter() {
        let graph = createGraph()

        let n1 = graph.createNode()
        let n2 = graph.createNode()
        let n3 = graph.createNode()

        #expect(n1 != n2)
        #expect(n2 != n3)
        #expect(n1 != n3)
    }

    @Test("Created node is accessible")
    func createdNodeAccessible() {
        let graph = createGraph()
        let nodeID = graph.createNode()

        let node = graph.node(nodeID)
        #expect(node != nil)
        #expect(node?.id == nodeID)
    }

    @Test("Child node has parent reference")
    func childNodeHasParent() {
        let graph = createGraph()
        let parent = graph.createNode()
        let child = graph.createNode(parent: parent)

        let childNode = graph.node(child)
        #expect(childNode?.parent == parent)
    }

    @Test("Child node has correct depth")
    func childNodeDepth() {
        let graph = createGraph()
        let root = graph.createNode()
        let child1 = graph.createNode(parent: root)
        let child2 = graph.createNode(parent: child1)

        #expect(graph.node(root)?.depth == 0)
        #expect(graph.node(child1)?.depth == 1)
        #expect(graph.node(child2)?.depth == 2)
    }

    @Test("Nominal nodes are created correctly")
    func nominalNodeCreation() {
        let graph = createGraph()
        let nominalID = graph.getOrCreateNominal("ex:john")

        #expect(nominalID.isNominalNode)
        #expect(nominalID.iri == "ex:john")
        #expect(graph.nominals.contains(nominalID))
    }

    @Test("Same IRI returns same nominal node")
    func sameIRISameNominal() {
        let graph = createGraph()
        let id1 = graph.getOrCreateNominal("ex:john")
        let id2 = graph.getOrCreateNominal("ex:john")

        #expect(id1 == id2)
    }
}

// MARK: - Concept Operations Tests

@Suite("CompletionGraph Concept Operations")
struct CompletionGraphConceptTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Add concept returns true when new")
    func addConceptReturnsTrue() {
        let graph = createGraph()
        let node = graph.createNode()

        let result = graph.addConcept(.named("ex:Person"), to: node)
        #expect(result == true)
    }

    @Test("Add duplicate concept returns false")
    func addDuplicateReturnsFalse() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: node)
        let result = graph.addConcept(.named("ex:Person"), to: node)

        #expect(result == false)
    }

    @Test("hasConcept returns true for existing concept")
    func hasConceptTrue() {
        let graph = createGraph()
        let node = graph.createNode()
        graph.addConcept(.named("ex:Person"), to: node)

        #expect(graph.hasConcept(.named("ex:Person"), at: node))
    }

    @Test("hasConcept returns false for missing concept")
    func hasConceptFalse() {
        let graph = createGraph()
        let node = graph.createNode()

        #expect(!graph.hasConcept(.named("ex:Person"), at: node))
    }

    @Test("concepts returns all concepts at node")
    func conceptsReturnsAll() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: node)
        graph.addConcept(.named("ex:Employee"), to: node)
        graph.addConcept(.thing, to: node)

        let concepts = graph.concepts(at: node)
        #expect(concepts.count == 3)
        #expect(concepts.contains(.named("ex:Person")))
        #expect(concepts.contains(.named("ex:Employee")))
        #expect(concepts.contains(.thing))
    }
}

// MARK: - Edge Operations Tests

@Suite("CompletionGraph Edge Operations")
struct CompletionGraphEdgeTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Add edge creates connection")
    func addEdgeCreatesConnection() {
        let graph = createGraph()
        let n1 = graph.createNode()
        let n2 = graph.createNode()

        let result = graph.addEdge(from: n1, role: "ex:hasChild", to: n2)

        #expect(result == true)
        #expect(graph.successors(of: n1, via: "ex:hasChild").contains(n2))
        #expect(graph.predecessors(of: n2, via: "ex:hasChild").contains(n1))
    }

    @Test("Duplicate edge returns false")
    func duplicateEdgeReturnsFalse() {
        let graph = createGraph()
        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addEdge(from: n1, role: "ex:hasChild", to: n2)
        let result = graph.addEdge(from: n1, role: "ex:hasChild", to: n2)

        #expect(result == false)
    }

    @Test("Multiple edges from same source")
    func multipleEdgesFromSource() {
        let graph = createGraph()
        let parent = graph.createNode()
        let child1 = graph.createNode()
        let child2 = graph.createNode()

        graph.addEdge(from: parent, role: "ex:hasChild", to: child1)
        graph.addEdge(from: parent, role: "ex:hasChild", to: child2)

        let children = graph.successors(of: parent, via: "ex:hasChild")
        #expect(children.count == 2)
        #expect(children.contains(child1))
        #expect(children.contains(child2))
    }

    @Test("Different roles create separate edges")
    func differentRolesSeparateEdges() {
        let graph = createGraph()
        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addEdge(from: n1, role: "ex:hasChild", to: n2)
        graph.addEdge(from: n1, role: "ex:knows", to: n2)

        #expect(graph.successors(of: n1, via: "ex:hasChild").count == 1)
        #expect(graph.successors(of: n1, via: "ex:knows").count == 1)
    }
}

// MARK: - Role Characteristics Tests

@Suite("CompletionGraph Role Characteristics")
struct CompletionGraphRoleCharacteristicsTests {

    @Test("Symmetric role creates inverse edge")
    func symmetricRoleCreatesInverse() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setCharacteristic(.symmetric, for: "ex:knows", value: true)

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addEdge(from: n1, role: "ex:knows", to: n2)

        // Check both directions exist
        #expect(graph.successors(of: n1, via: "ex:knows").contains(n2))
        #expect(graph.successors(of: n2, via: "ex:knows").contains(n1))
    }

    @Test("Inverse role creates counterpart")
    func inverseRoleCreatesCounterpart() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setInverse("ex:hasChild", "ex:hasParent")

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let parent = graph.createNode()
        let child = graph.createNode()

        graph.addEdge(from: parent, role: "ex:hasChild", to: child)

        // Check inverse exists
        #expect(graph.successors(of: child, via: "ex:hasParent").contains(parent))
    }
}

// MARK: - Backtracking Tests

@Suite("CompletionGraph Backtracking")
struct CompletionGraphBacktrackingTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Choice point is created correctly")
    func choicePointCreation() {
        let graph = createGraph()
        let node = graph.createNode()
        graph.addConcept(.named("ex:A"), to: node)

        // Create choice point for union: A ⊔ B ⊔ C
        _ = graph.createChoicePoint(
            nodeID: node,
            expression: .union([.named("ex:A"), .named("ex:B"), .named("ex:C")]),
            alternatives: [.named("ex:A"), .named("ex:B"), .named("ex:C")]
        )

        #expect(graph.hasChoicePoints)
    }

    @Test("Backtrack restores state")
    func backtrackRestoresState() {
        let graph = createGraph()
        let node = graph.createNode()

        // Add initial concept
        graph.addConcept(.named("ex:Base"), to: node)

        // Create choice point
        _ = graph.createChoicePoint(
            nodeID: node,
            expression: .union([.named("ex:A"), .named("ex:B")]),
            alternatives: [.named("ex:A"), .named("ex:B")]
        )

        // Add concept after choice point
        graph.addConcept(.named("ex:A"), to: node)
        graph.addConcept(.named("ex:Extra"), to: node)

        // Verify state before backtrack
        #expect(graph.hasConcept(.named("ex:Base"), at: node))
        #expect(graph.hasConcept(.named("ex:A"), at: node))
        #expect(graph.hasConcept(.named("ex:Extra"), at: node))

        // Backtrack
        let result = graph.backtrack()

        // Should return next alternative
        #expect(result != nil)
        #expect(result?.choice == .named("ex:B"))

        // State should be restored - concepts added after choice point removed
        #expect(graph.hasConcept(.named("ex:Base"), at: node))
        #expect(!graph.hasConcept(.named("ex:A"), at: node))
        #expect(!graph.hasConcept(.named("ex:Extra"), at: node))
    }

    @Test("Backtrack with no choice points returns nil")
    func backtrackNoChoicePoints() {
        let graph = createGraph()
        let node = graph.createNode()
        graph.addConcept(.named("ex:A"), to: node)

        let result = graph.backtrack()
        #expect(result == nil)
    }

    @Test("Backtrack exhausts all alternatives")
    func backtrackExhaustsAlternatives() {
        let graph = createGraph()
        let node = graph.createNode()

        // Create choice point with 2 alternatives
        _ = graph.createChoicePoint(
            nodeID: node,
            expression: .union([.named("ex:A"), .named("ex:B")]),
            alternatives: [.named("ex:A"), .named("ex:B")]
        )

        // First backtrack returns second alternative
        let result1 = graph.backtrack()
        #expect(result1?.choice == .named("ex:B"))

        // Second backtrack returns nil (exhausted)
        let result2 = graph.backtrack()
        #expect(result2 == nil)
    }
}

// MARK: - Blocking Tests

@Suite("CompletionGraph Blocking")
struct CompletionGraphBlockingTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Node with subset concepts is blocked")
    func subsetConceptsBlocked() {
        let graph = createGraph()

        // Create ancestor with concepts
        let ancestor = graph.createNode()
        graph.addConcept(.named("ex:Person"), to: ancestor)
        graph.addConcept(.named("ex:Employee"), to: ancestor)

        // Create descendant with same concepts (subset)
        let descendant = graph.createNode(parent: ancestor)
        graph.addConcept(.named("ex:Person"), to: descendant)
        graph.addConcept(.named("ex:Employee"), to: descendant)

        // Update blocking
        graph.updateBlocking()

        let descendantNode = graph.node(descendant)
        #expect(descendantNode?.isBlocked == true)
        #expect(descendantNode?.blockedBy == ancestor)
    }

    @Test("Nominal nodes are never blocked")
    func nominalNeverBlocked() {
        let graph = createGraph()

        // Create blocker
        let blocker = graph.createNode()
        graph.addConcept(.named("ex:Person"), to: blocker)

        // Create nominal with same concepts
        let nominal = graph.getOrCreateNominal("ex:john")
        graph.addConcept(.named("ex:Person"), to: nominal)

        graph.updateBlocking()

        let nominalNode = graph.node(nominal)
        #expect(nominalNode?.isBlocked == false)
    }

    @Test("Root nodes are never blocked")
    func rootNeverBlocked() {
        let graph = createGraph()

        let root = graph.createNode()
        graph.addConcept(.named("ex:Person"), to: root)

        graph.updateBlocking()

        let rootNode = graph.node(root)
        #expect(rootNode?.isBlocked == false)
    }

    @Test("Node with superset concepts is not blocked")
    func supersetNotBlocked() {
        let graph = createGraph()

        // Create ancestor
        let ancestor = graph.createNode()
        graph.addConcept(.named("ex:Person"), to: ancestor)

        // Create descendant with MORE concepts (superset)
        let descendant = graph.createNode(parent: ancestor)
        graph.addConcept(.named("ex:Person"), to: descendant)
        graph.addConcept(.named("ex:Employee"), to: descendant)  // Extra concept

        graph.updateBlocking()

        let descendantNode = graph.node(descendant)
        // Not blocked because descendant has concepts not in ancestor
        #expect(descendantNode?.isBlocked == false)
    }
}

// MARK: - Node Merging Tests

@Suite("CompletionGraph Node Merging")
struct CompletionGraphMergingTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Merging combines concepts")
    func mergingCombinesConcepts() {
        let graph = createGraph()

        let n1 = graph.createNode()
        let n2 = graph.createNode()

        graph.addConcept(.named("ex:A"), to: n1)
        graph.addConcept(.named("ex:B"), to: n2)

        graph.mergeNodes(survivor: n1, merged: n2)

        let survivorConcepts = graph.concepts(at: n1)
        #expect(survivorConcepts.contains(.named("ex:A")))
        #expect(survivorConcepts.contains(.named("ex:B")))
    }
}

// MARK: - Transitive Role Tests

@Suite("CompletionGraph Transitive Roles")
struct CompletionGraphTransitiveTests {

    @Test("Transitive role expansion")
    func transitiveRoleExpansion() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setCharacteristic(.transitive, for: "ex:ancestorOf", value: true)

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let alice = graph.createNode()
        let bob = graph.createNode()
        let carol = graph.createNode()

        graph.addEdge(from: alice, role: "ex:ancestorOf", to: bob)
        graph.addEdge(from: bob, role: "ex:ancestorOf", to: carol)

        // Expand transitive closure
        let expanded = graph.expandTransitiveRole("ex:ancestorOf")

        // Should create alice -> carol edge
        #expect(expanded == true)
        #expect(graph.successors(of: alice, via: "ex:ancestorOf").contains(carol))
    }
}

// MARK: - Property Chain Tests

@Suite("CompletionGraph Property Chains")
struct CompletionGraphPropertyChainTests {

    @Test("Property chain inference")
    func propertyChainInference() {
        var roleHierarchy = RoleHierarchy()
        // hasGrandparent ⊑ hasParent ∘ hasParent
        roleHierarchy.addPropertyChain(["ex:hasParent", "ex:hasParent"], implies: "ex:hasGrandparent")

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let child = graph.createNode()
        let parent = graph.createNode()
        let grandparent = graph.createNode()

        graph.addEdge(from: child, role: "ex:hasParent", to: parent)
        graph.addEdge(from: parent, role: "ex:hasParent", to: grandparent)

        // Apply property chain
        let applied = graph.applyPropertyChain(["ex:hasParent", "ex:hasParent"], implies: "ex:hasGrandparent")

        #expect(applied == true)
        #expect(graph.successors(of: child, via: "ex:hasGrandparent").contains(grandparent))
    }
}
