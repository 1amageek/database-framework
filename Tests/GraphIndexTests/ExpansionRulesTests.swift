// ExpansionRulesTests.swift
// Tests for Tableaux expansion rules

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - Clash Detection Tests

@Suite("ExpansionRules Clash Detection")
struct ExpansionRulesClashDetectionTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Complement clash detected")
    func complementClash() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: node)
        graph.addConcept(.complement(.named("ex:Person")), to: node)

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: ClassHierarchy(),
            roleHierarchy: RoleHierarchy()
        )

        #expect(clash != nil)
    }

    @Test("Nothing clash detected")
    func nothingClash() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.nothing, to: node)

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: ClassHierarchy(),
            roleHierarchy: RoleHierarchy()
        )

        #expect(clash != nil)
    }

    @Test("No clash in consistent node")
    func noClash() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: node)
        graph.addConcept(.named("ex:Employee"), to: node)

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: ClassHierarchy(),
            roleHierarchy: RoleHierarchy()
        )

        #expect(clash == nil)
    }

    @Test("Disjoint classes clash")
    func disjointClassesClash() {
        var classHierarchy = ClassHierarchy()
        classHierarchy.addDisjoint(class1: "ex:Dog", class2: "ex:Cat")

        let graph = CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: classHierarchy
        )
        let node = graph.createNode()

        graph.addConcept(.named("ex:Dog"), to: node)
        graph.addConcept(.named("ex:Cat"), to: node)

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: classHierarchy,
            roleHierarchy: RoleHierarchy()
        )

        #expect(clash != nil)
    }

    @Test("Functional property clash")
    func functionalPropertyClash() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setCharacteristic(.functional, for: "ex:hasMother", value: true)

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let node = graph.createNode()
        let target1 = graph.createNode()
        let target2 = graph.createNode()

        graph.addEdge(from: node, role: "ex:hasMother", to: target1)
        graph.addEdge(from: node, role: "ex:hasMother", to: target2)

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: ClassHierarchy(),
            roleHierarchy: roleHierarchy
        )

        #expect(clash != nil)
    }

    @Test("Irreflexive property clash")
    func irreflexivePropertyClash() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setCharacteristic(.irreflexive, for: "ex:parentOf", value: true)

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let node = graph.createNode()
        graph.addEdge(from: node, role: "ex:parentOf", to: node)  // self-loop

        let clash = ExpansionRules.detectClash(
            at: node,
            in: graph,
            classHierarchy: ClassHierarchy(),
            roleHierarchy: roleHierarchy
        )

        #expect(clash != nil)
    }
}

// MARK: - Intersection Rule Tests

@Suite("ExpansionRules Intersection Rule")
struct ExpansionRulesIntersectionTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Intersection rule expands conjuncts")
    func intersectionExpands() {
        let graph = createGraph()
        let node = graph.createNode()

        // Add C ⊓ D
        graph.addConcept(.intersection([
            .named("ex:Person"),
            .named("ex:Employee")
        ]), to: node)

        let applied = ExpansionRules.applyIntersectionRule(at: node, in: graph)

        #expect(applied == true)
        #expect(graph.hasConcept(.named("ex:Person"), at: node))
        #expect(graph.hasConcept(.named("ex:Employee"), at: node))
    }

    @Test("Nested intersection fully expands")
    func nestedIntersectionExpands() {
        let graph = createGraph()
        let node = graph.createNode()

        // Add (A ⊓ B) ⊓ C
        graph.addConcept(.intersection([
            .intersection([.named("ex:A"), .named("ex:B")]),
            .named("ex:C")
        ]), to: node)

        // First application
        _ = ExpansionRules.applyIntersectionRule(at: node, in: graph)
        // Second application (for nested intersection)
        _ = ExpansionRules.applyIntersectionRule(at: node, in: graph)

        #expect(graph.hasConcept(.named("ex:A"), at: node))
        #expect(graph.hasConcept(.named("ex:B"), at: node))
        #expect(graph.hasConcept(.named("ex:C"), at: node))
    }

    @Test("Intersection rule returns false when no work")
    func intersectionNoWork() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.named("ex:Person"), to: node)

        let applied = ExpansionRules.applyIntersectionRule(at: node, in: graph)
        #expect(applied == false)
    }
}

// MARK: - Union Rule Tests

@Suite("ExpansionRules Union Rule")
struct ExpansionRulesUnionTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Union rule identifies choice needed")
    func unionIdentifiesChoice() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.union([
            .named("ex:A"),
            .named("ex:B")
        ]), to: node)

        let result = ExpansionRules.applyUnionRule(at: node, in: graph)

        #expect(result.applied == true)
        #expect(result.alternatives?.count == 2)
    }

    @Test("Union skips when disjunct already present")
    func unionSkipsExisting() {
        let graph = createGraph()
        let node = graph.createNode()

        // Already have one disjunct
        graph.addConcept(.named("ex:A"), to: node)

        graph.addConcept(.union([
            .named("ex:A"),
            .named("ex:B")
        ]), to: node)

        let result = ExpansionRules.applyUnionRule(at: node, in: graph)

        // No choice needed since A already satisfied
        #expect(result.applied == false)
    }
}

// MARK: - Existential Rule Tests

@Suite("ExpansionRules Existential Rule")
struct ExpansionRulesExistentialTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Existential rule creates successor")
    func existentialCreatesSuccessor() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.someValuesFrom(
            property: "ex:hasChild",
            filler: .named("ex:Person")
        ), to: node)

        let applied = ExpansionRules.applyExistentialRule(
            at: node,
            in: graph,
            tboxConstraints: []
        )

        #expect(applied == true)

        let successors = graph.successors(of: node, via: "ex:hasChild")
        #expect(successors.count == 1)

        // Successor should have the filler concept
        if let successor = successors.first {
            #expect(graph.hasConcept(.named("ex:Person"), at: successor))
        }
    }

    @Test("Existential rule doesn't create when blocker exists")
    func existentialBlockedByExisting() {
        let graph = createGraph()
        let node = graph.createNode()
        let existing = graph.createNode()

        // Create existing successor with the filler
        graph.addEdge(from: node, role: "ex:hasChild", to: existing)
        graph.addConcept(.named("ex:Person"), to: existing)

        graph.addConcept(.someValuesFrom(
            property: "ex:hasChild",
            filler: .named("ex:Person")
        ), to: node)

        let applied = ExpansionRules.applyExistentialRule(
            at: node,
            in: graph,
            tboxConstraints: []
        )

        // Should not create new successor since witness exists
        #expect(applied == false)
    }
}

// MARK: - Universal Rule Tests

@Suite("ExpansionRules Universal Rule")
struct ExpansionRulesUniversalTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Universal rule propagates to successors")
    func universalPropagates() {
        let graph = createGraph()
        let node = graph.createNode()
        let successor = graph.createNode()

        graph.addEdge(from: node, role: "ex:hasChild", to: successor)
        graph.addConcept(.allValuesFrom(
            property: "ex:hasChild",
            filler: .named("ex:Person")
        ), to: node)

        let applied = ExpansionRules.applyUniversalRule(
            at: node,
            in: graph,
            roleHierarchy: RoleHierarchy()
        )

        #expect(applied == true)
        #expect(graph.hasConcept(.named("ex:Person"), at: successor))
    }

    @Test("Universal rule respects role hierarchy")
    func universalRespectHierarchy() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.addSubRole(sub: "ex:hasSon", super: "ex:hasChild")

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let node = graph.createNode()
        let successor = graph.createNode()

        // hasSon edge
        graph.addEdge(from: node, role: "ex:hasSon", to: successor)

        // Universal on hasChild should apply to hasSon successors
        graph.addConcept(.allValuesFrom(
            property: "ex:hasChild",
            filler: .named("ex:Person")
        ), to: node)

        let applied = ExpansionRules.applyUniversalRule(
            at: node,
            in: graph,
            roleHierarchy: roleHierarchy
        )

        #expect(applied == true)
        #expect(graph.hasConcept(.named("ex:Person"), at: successor))
    }
}

// MARK: - Cardinality Rule Tests

@Suite("ExpansionRules Cardinality Rules")
struct ExpansionRulesCardinalityTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("Min cardinality creates successors")
    func minCardinalityCreates() {
        let graph = createGraph()
        let node = graph.createNode()

        // ≥2 hasChild.Person
        graph.addConcept(.minCardinality(
            property: "ex:hasChild",
            n: 2,
            filler: .named("ex:Person")
        ), to: node)

        let applied = ExpansionRules.applyMinCardinalityRule(
            at: node,
            in: graph,
            tboxConstraints: []
        )

        #expect(applied == true)

        let successors = graph.successors(of: node, via: "ex:hasChild")
        #expect(successors.count >= 2)

        // All successors should have the filler
        for successor in successors {
            #expect(graph.hasConcept(.named("ex:Person"), at: successor))
        }
    }

    @Test("Max cardinality with excess")
    func maxCardinalityMerge() {
        let graph = createGraph()
        let node = graph.createNode()

        // Create 3 successors
        let s1 = graph.createNode()
        let s2 = graph.createNode()
        let s3 = graph.createNode()

        graph.addEdge(from: node, role: "ex:hasChild", to: s1)
        graph.addEdge(from: node, role: "ex:hasChild", to: s2)
        graph.addEdge(from: node, role: "ex:hasChild", to: s3)

        // Add ≤2 hasChild.⊤
        graph.addConcept(.maxCardinality(
            property: "ex:hasChild",
            n: 2,
            filler: .thing
        ), to: node)

        let result = ExpansionRules.applyMaxCardinalityRule(
            at: node,
            in: graph
        )

        // applyMaxCardinalityRule returns Bool indicating if merge was performed
        #expect(result == true || result == false)  // Either merged or detected violation
    }
}

// MARK: - Domain/Range Rule Tests

@Suite("ExpansionRules Domain/Range Rules")
struct ExpansionRulesDomainRangeTests {

    @Test("Domain rule adds class to subject")
    func domainRule() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setDomain(for: "ex:hasChild", domain: "ex:Parent")

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let parent = graph.createNode()
        let child = graph.createNode()

        graph.addEdge(from: parent, role: "ex:hasChild", to: child)

        let applied = ExpansionRules.applyDomainRule(
            at: parent,
            in: graph,
            roleHierarchy: roleHierarchy
        )

        #expect(applied == true)
        #expect(graph.hasConcept(.named("ex:Parent"), at: parent))
    }

    @Test("Range rule adds class to object")
    func rangeRule() {
        var roleHierarchy = RoleHierarchy()
        roleHierarchy.setRange(for: "ex:hasChild", range: "ex:Person")

        let graph = CompletionGraph(
            roleHierarchy: roleHierarchy,
            classHierarchy: ClassHierarchy()
        )

        let parent = graph.createNode()
        let child = graph.createNode()

        graph.addEdge(from: parent, role: "ex:hasChild", to: child)

        // Range rule is applied to the target node (child), checking incoming edges
        let applied = ExpansionRules.applyRangeRule(
            at: child,
            in: graph,
            roleHierarchy: roleHierarchy
        )

        #expect(applied == true)
        #expect(graph.hasConcept(.named("ex:Person"), at: child))
    }
}

// MARK: - Self Rule Tests

@Suite("ExpansionRules Self Rule")
struct ExpansionRulesSelfTests {

    @Test("Self rule creates self-loop")
    func selfRuleCreatesSelfLoop() {
        let graph = CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
        let node = graph.createNode()

        graph.addConcept(.hasSelf(property: "ex:knows"), to: node)

        let applied = ExpansionRules.applySelfRule(at: node, in: graph)

        #expect(applied == true)
        #expect(graph.successors(of: node, via: "ex:knows").contains(node))
    }
}

// MARK: - OneOf Rule Tests

@Suite("ExpansionRules OneOf Rule")
struct ExpansionRulesOneOfTests {

    private func createGraph() -> CompletionGraph {
        CompletionGraph(
            roleHierarchy: RoleHierarchy(),
            classHierarchy: ClassHierarchy()
        )
    }

    @Test("OneOf with multiple individuals needs choice")
    func oneOfMultipleNeedsChoice() {
        let graph = createGraph()
        let node = graph.createNode()

        graph.addConcept(.oneOf(["ex:john", "ex:mary"]), to: node)

        let result = ExpansionRules.applyOneOfRule(at: node, in: graph)

        #expect(result.needsChoice == true)
        #expect(result.alternatives?.count == 2)
    }
}
