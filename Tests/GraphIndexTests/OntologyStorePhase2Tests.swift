// OntologyStorePhase2Tests.swift
// Phase 2 tests: OntologyStore data integrity fixes
//
// Validates:
//   A-2: loadOntology() idempotency (clearRange before save)
//   A-3: computeTransitiveClosure() robustness with cycles
//   A-4: Property hierarchy truth source unification
//   A-5: Data property hierarchy materialization

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("OntologyStore Phase 2 Fixes", .serialized)
struct OntologyStorePhase2Tests {

    private static let testOntologyIRI = "http://test.org/phase2"

    // MARK: - Helpers

    private func setupContext() async throws -> FDBContext {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let schema = Schema([OntologyTestDummy.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        return container.newContext()
    }

    private func cleanup(context: FDBContext) async throws {
        try await context.ontology.delete(iri: Self.testOntologyIRI)
    }

    // MARK: - A-2: loadOntology() Idempotency

    @Test("loadOntology() clears old axioms on reload (fewer axioms)")
    func loadOntologyIdempotentAxioms() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // First load: 3 axioms
        var ontology1 = OWLOntology(iri: Self.testOntologyIRI)
        ontology1.classes = [
            OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B"),
            OWLClass(iri: "ex:C"), OWLClass(iri: "ex:D"),
        ]
        ontology1.axioms = [
            .subClassOf(sub: .named("ex:A"), sup: .named("ex:B")),
            .subClassOf(sub: .named("ex:B"), sup: .named("ex:C")),
            .subClassOf(sub: .named("ex:C"), sup: .named("ex:D")),
        ]
        try await context.ontology.load(ontology1)

        // Second load: only 1 axiom (fewer than before)
        var ontology2 = OWLOntology(iri: Self.testOntologyIRI)
        ontology2.classes = [OWLClass(iri: "ex:X"), OWLClass(iri: "ex:Y")]
        ontology2.axioms = [
            .subClassOf(sub: .named("ex:X"), sup: .named("ex:Y")),
        ]
        try await context.ontology.load(ontology2)

        // Verify: only the 1 axiom from second load survives
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.axioms.count == 1)
        #expect(loadedOntology.axioms[0] == .subClassOf(sub: .named("ex:X"), sup: .named("ex:Y")))

        // Verify: old classes are gone
        #expect(loadedOntology.classes.count == 2)
        #expect(Set(loadedOntology.classes.map(\.iri)) == Set(["ex:X", "ex:Y"]))

        try await cleanup(context: context)
    }

    @Test("loadOntology() clears old hierarchy on reload")
    func loadOntologyIdempotentHierarchy() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // First load: A ⊑ B ⊑ C
        var ontology1 = OWLOntology(iri: Self.testOntologyIRI)
        ontology1.classes = [
            OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B"), OWLClass(iri: "ex:C"),
        ]
        ontology1.axioms = [
            .subClassOf(sub: .named("ex:A"), sup: .named("ex:B")),
            .subClassOf(sub: .named("ex:B"), sup: .named("ex:C")),
        ]
        try await context.ontology.load(ontology1)

        // Verify first hierarchy
        let supers1 = try await context.ontology.getSuperClasses(of: "ex:A", in: Self.testOntologyIRI)
        #expect(supers1.contains("ex:B"))
        #expect(supers1.contains("ex:C"))

        // Second load: X ⊑ Y (completely different hierarchy)
        var ontology2 = OWLOntology(iri: Self.testOntologyIRI)
        ontology2.classes = [OWLClass(iri: "ex:X"), OWLClass(iri: "ex:Y")]
        ontology2.axioms = [
            .subClassOf(sub: .named("ex:X"), sup: .named("ex:Y")),
        ]
        try await context.ontology.load(ontology2)

        // Verify: old hierarchy (A ⊑ B ⊑ C) is gone
        let supers2 = try await context.ontology.getSuperClasses(of: "ex:A", in: Self.testOntologyIRI)
        #expect(supers2.isEmpty, "Old hierarchy entries should be cleared")

        // Verify: new hierarchy is correct
        let supersX = try await context.ontology.getSuperClasses(of: "ex:X", in: Self.testOntologyIRI)
        #expect(supersX == Set(["ex:Y"]))

        try await cleanup(context: context)
    }

    @Test("loadOntology() clears old property chains on reload")
    func loadOntologyIdempotentChains() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // First load: hasUncle ← hasParent o hasBrother
        var ontology1 = OWLOntology(iri: Self.testOntologyIRI)
        ontology1.objectProperties = [
            OWLObjectProperty(iri: "ex:hasUncle", propertyChains: [["ex:hasParent", "ex:hasBrother"]]),
            OWLObjectProperty(iri: "ex:hasParent"),
            OWLObjectProperty(iri: "ex:hasBrother"),
        ]
        try await context.ontology.load(ontology1)

        let chains1 = try await context.ontology.getPropertyChains(for: "ex:hasUncle", in: Self.testOntologyIRI)
        #expect(chains1.count == 1)

        // Second load: no property chains
        var ontology2 = OWLOntology(iri: Self.testOntologyIRI)
        ontology2.objectProperties = [
            OWLObjectProperty(iri: "ex:simpleRel"),
        ]
        try await context.ontology.load(ontology2)

        // Old chains should be gone
        let chains2 = try await context.ontology.getPropertyChains(for: "ex:hasUncle", in: Self.testOntologyIRI)
        #expect(chains2.isEmpty, "Old property chains should be cleared on reload")

        try await cleanup(context: context)
    }

    // MARK: - A-3: Transitive Closure Robustness

    @Test("Equivalent class cycle does not include self in superclasses")
    func equivalentClassCycleNoSelf() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // A ≡ B → mutual subClassOf: A ⊑ B and B ⊑ A
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B")]
        ontology.axioms = [
            .equivalentClasses([.named("ex:A"), .named("ex:B")]),
        ]
        try await context.ontology.load(ontology)

        // A's superclasses should be {B}, NOT {A, B}
        let supersA = try await context.ontology.getSuperClasses(of: "ex:A", in: Self.testOntologyIRI)
        #expect(supersA == Set(["ex:B"]), "A should not be its own superclass. Got: \(supersA)")

        let supersB = try await context.ontology.getSuperClasses(of: "ex:B", in: Self.testOntologyIRI)
        #expect(supersB == Set(["ex:A"]), "B should not be its own superclass. Got: \(supersB)")

        try await cleanup(context: context)
    }

    @Test("Three-way equivalent class cycle")
    func threeWayEquivalentClassCycle() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // A ≡ B ≡ C → each has the other two as superclasses
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B"), OWLClass(iri: "ex:C"),
        ]
        ontology.axioms = [
            .equivalentClasses([.named("ex:A"), .named("ex:B"), .named("ex:C")]),
        ]
        try await context.ontology.load(ontology)

        let supersA = try await context.ontology.getSuperClasses(of: "ex:A", in: Self.testOntologyIRI)
        #expect(supersA == Set(["ex:B", "ex:C"]))

        let supersB = try await context.ontology.getSuperClasses(of: "ex:B", in: Self.testOntologyIRI)
        #expect(supersB == Set(["ex:A", "ex:C"]))

        let supersC = try await context.ontology.getSuperClasses(of: "ex:C", in: Self.testOntologyIRI)
        #expect(supersC == Set(["ex:A", "ex:B"]))

        try await cleanup(context: context)
    }

    @Test("Deep chain with cycle: A ⊑ B ⊑ C ≡ A")
    func deepChainWithCycle() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // A ⊑ B, B ⊑ C, C ≡ A
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B"), OWLClass(iri: "ex:C"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:A"), sup: .named("ex:B")),
            .subClassOf(sub: .named("ex:B"), sup: .named("ex:C")),
            .equivalentClasses([.named("ex:C"), .named("ex:A")]),
        ]
        try await context.ontology.load(ontology)

        // A: direct super B, transitive C (but not A itself)
        let supersA = try await context.ontology.getSuperClasses(of: "ex:A", in: Self.testOntologyIRI)
        #expect(supersA.contains("ex:B"))
        #expect(supersA.contains("ex:C"))
        #expect(!supersA.contains("ex:A"), "A should not appear in its own superclasses")

        // B: direct super C, transitive A (via C ≡ A)
        let supersB = try await context.ontology.getSuperClasses(of: "ex:B", in: Self.testOntologyIRI)
        #expect(supersB.contains("ex:C"))
        #expect(supersB.contains("ex:A"))
        #expect(!supersB.contains("ex:B"))

        try await cleanup(context: context)
    }

    // MARK: - A-4: Property Hierarchy Truth Source Unification

    @Test("Property hierarchy includes OWLObjectProperty.superProperties")
    func propertyHierarchyFromPropertyStruct() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // Define hierarchy ONLY via OWLObjectProperty.superProperties (no axioms)
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.objectProperties = [
            OWLObjectProperty(iri: "ex:hasFather", superProperties: ["ex:hasParent"]),
            OWLObjectProperty(iri: "ex:hasParent"),
        ]
        try await context.ontology.load(ontology)

        let supers = try await context.ontology.getSuperProperties(of: "ex:hasFather", in: Self.testOntologyIRI)
        #expect(supers.contains("ex:hasParent"),
                "Hierarchy from OWLObjectProperty.superProperties should be materialized")

        try await cleanup(context: context)
    }

    @Test("Property hierarchy unifies axioms and property struct declarations")
    func propertyHierarchyUnified() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // hasFather ⊑ hasParent (via property struct)
        // hasParent ⊑ hasAncestor (via axiom)
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.objectProperties = [
            OWLObjectProperty(iri: "ex:hasFather", superProperties: ["ex:hasParent"]),
            OWLObjectProperty(iri: "ex:hasParent"),
            OWLObjectProperty(iri: "ex:hasAncestor"),
        ]
        ontology.axioms = [
            .subObjectPropertyOf(sub: "ex:hasParent", sup: "ex:hasAncestor"),
        ]
        try await context.ontology.load(ontology)

        // hasFather should transitively reach hasAncestor
        let supers = try await context.ontology.getSuperProperties(of: "ex:hasFather", in: Self.testOntologyIRI)
        #expect(supers.contains("ex:hasParent"))
        #expect(supers.contains("ex:hasAncestor"),
                "Transitive closure should span both truth sources")

        try await cleanup(context: context)
    }

    // MARK: - A-5: Data Property Hierarchy Materialization

    @Test("subDataPropertyOf axiom materializes data property hierarchy")
    func subDataPropertyHierarchy() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.dataProperties = [
            OWLDataProperty(iri: "ex:firstName"),
            OWLDataProperty(iri: "ex:name"),
        ]
        ontology.axioms = [
            .subDataPropertyOf(sub: "ex:firstName", sup: "ex:name"),
        ]
        try await context.ontology.load(ontology)

        let supers = try await context.ontology.getSuperProperties(of: "ex:firstName", in: Self.testOntologyIRI)
        #expect(supers.contains("ex:name"),
                "Data property hierarchy should be materialized from subDataPropertyOf axiom")

        try await cleanup(context: context)
    }

    @Test("equivalentDataProperties axiom materializes data property hierarchy")
    func equivalentDataPropertyHierarchy() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.dataProperties = [
            OWLDataProperty(iri: "ex:familyName"),
            OWLDataProperty(iri: "ex:lastName"),
        ]
        ontology.axioms = [
            .equivalentDataProperties(["ex:familyName", "ex:lastName"]),
        ]
        try await context.ontology.load(ontology)

        let supers1 = try await context.ontology.getSuperProperties(of: "ex:familyName", in: Self.testOntologyIRI)
        #expect(supers1.contains("ex:lastName"))

        let supers2 = try await context.ontology.getSuperProperties(of: "ex:lastName", in: Self.testOntologyIRI)
        #expect(supers2.contains("ex:familyName"))

        try await cleanup(context: context)
    }

    @Test("Data property hierarchy from OWLDataProperty.superProperties")
    func dataPropertyHierarchyFromPropertyStruct() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.dataProperties = [
            OWLDataProperty(iri: "ex:givenName", superProperties: ["ex:name"]),
            OWLDataProperty(iri: "ex:name"),
        ]
        try await context.ontology.load(ontology)

        let supers = try await context.ontology.getSuperProperties(of: "ex:givenName", in: Self.testOntologyIRI)
        #expect(supers.contains("ex:name"),
                "Data property hierarchy from struct superProperties should be materialized")

        try await cleanup(context: context)
    }

    @Test("Transitive data property hierarchy: firstName ⊑ name ⊑ label")
    func transitiveDataPropertyHierarchy() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.dataProperties = [
            OWLDataProperty(iri: "ex:firstName"),
            OWLDataProperty(iri: "ex:name"),
            OWLDataProperty(iri: "ex:label"),
        ]
        ontology.axioms = [
            .subDataPropertyOf(sub: "ex:firstName", sup: "ex:name"),
            .subDataPropertyOf(sub: "ex:name", sup: "ex:label"),
        ]
        try await context.ontology.load(ontology)

        let supers = try await context.ontology.getSuperProperties(of: "ex:firstName", in: Self.testOntologyIRI)
        #expect(supers.contains("ex:name"))
        #expect(supers.contains("ex:label"),
                "Transitive closure of data property hierarchy should include ex:label")

        try await cleanup(context: context)
    }
}
