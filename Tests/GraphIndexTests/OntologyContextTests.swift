// OntologyContextTests.swift
// Tests for OntologyContext — the bridge between SPARQL executor and OWL ontology.

import Testing
import Graph
@testable import GraphIndex

@Suite("OntologyContext")
struct OntologyContextTests {

    // MARK: - Helpers

    private func makeOntology(
        objectProperties: [OWLObjectProperty] = [],
        axioms: [OWLAxiom] = []
    ) -> OWLOntology {
        var ont = OWLOntology(iri: "http://test.org/onto")
        ont.objectProperties = objectProperties
        ont.axioms = axioms
        return ont
    }

    // MARK: - subProperties

    @Test("subProperties returns direct sub-properties")
    func subPropertiesDirect() {
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"),
            .subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"),
        ])
        let ctx = OntologyContext(ontology: ont)
        let subs = ctx.subProperties(of: "ex:hasParent")
        #expect(subs.contains("ex:hasFather"))
        #expect(subs.contains("ex:hasMother"))
        #expect(subs.count == 2)
    }

    @Test("subProperties returns transitive closure")
    func subPropertiesTransitive() {
        // hasSon ⊑ hasFather ⊑ hasParent
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:hasSon", sup: "ex:hasFather"),
            .subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"),
        ])
        let ctx = OntologyContext(ontology: ont)
        let subs = ctx.subProperties(of: "ex:hasParent")
        #expect(subs.contains("ex:hasFather"))
        #expect(subs.contains("ex:hasSon"))
    }

    @Test("subProperties returns empty for unknown property")
    func subPropertiesUnknown() {
        let ont = makeOntology()
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.subProperties(of: "ex:unknown").isEmpty)
    }

    // MARK: - expandedProperties

    @Test("expandedProperties includes self and all sub-properties")
    func expandedProperties() {
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"),
            .subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"),
        ])
        let ctx = OntologyContext(ontology: ont)
        let expanded = ctx.expandedProperties(of: "ex:hasParent")
        #expect(expanded == Set(["ex:hasParent", "ex:hasFather", "ex:hasMother"]))
    }

    @Test("expandedProperties for leaf returns only self")
    func expandedPropertiesLeaf() {
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"),
        ])
        let ctx = OntologyContext(ontology: ont)
        let expanded = ctx.expandedProperties(of: "ex:hasFather")
        #expect(expanded == Set(["ex:hasFather"]))
    }

    // MARK: - inverseProperty

    @Test("inverseProperty returns declared inverse")
    func inversePropertyDeclared() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:hasChild", inverseOf: "ex:hasParent"),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.inverseProperty(of: "ex:hasChild") == "ex:hasParent")
    }

    @Test("inverseProperty returns self for symmetric property")
    func inversePropertySymmetric() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:knows", characteristics: [.symmetric]),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.inverseProperty(of: "ex:knows") == "ex:knows")
    }

    @Test("inverseProperty prefers declared inverse over symmetric self")
    func inversePropertyDeclaredOverSymmetric() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:marriedTo", characteristics: [.symmetric], inverseOf: "ex:spouseOf"),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.inverseProperty(of: "ex:marriedTo") == "ex:spouseOf")
    }

    @Test("inverseProperty returns nil when no inverse exists")
    func inversePropertyNone() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:likes"),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.inverseProperty(of: "ex:likes") == nil)
    }

    // MARK: - Property characteristics

    @Test("isTransitive detects transitive property")
    func isTransitive() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:ancestorOf", characteristics: [.transitive]),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.isTransitive("ex:ancestorOf") == true)
        #expect(ctx.isTransitive("ex:unknown") == false)
    }

    @Test("isSymmetric detects symmetric property")
    func isSymmetric() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:knows", characteristics: [.symmetric]),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.isSymmetric("ex:knows") == true)
        #expect(ctx.isSymmetric("ex:likes") == false)
    }

    @Test("isFunctional detects functional property")
    func isFunctional() {
        let ont = makeOntology(objectProperties: [
            OWLObjectProperty(iri: "ex:hasBirthPlace", characteristics: [.functional]),
        ])
        let ctx = OntologyContext(ontology: ont)
        #expect(ctx.isFunctional("ex:hasBirthPlace") == true)
        #expect(ctx.isFunctional("ex:knows") == false)
    }

    // MARK: - Value semantics

    @Test("OntologyContext copies independently (value semantics)")
    func valueSemanticsTest() {
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:hasFather", sup: "ex:hasParent"),
        ])
        let ctx1 = OntologyContext(ontology: ont)
        let ctx2 = ctx1  // Copy

        // Both should produce same results
        #expect(ctx1.subProperties(of: "ex:hasParent") == ctx2.subProperties(of: "ex:hasParent"))
    }

    // MARK: - Deep hierarchy

    @Test("Deep 5-level property hierarchy")
    func deepHierarchy() {
        // p1 ⊑ p2 ⊑ p3 ⊑ p4 ⊑ p5
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:p1", sup: "ex:p2"),
            .subObjectPropertyOf(sub: "ex:p2", sup: "ex:p3"),
            .subObjectPropertyOf(sub: "ex:p3", sup: "ex:p4"),
            .subObjectPropertyOf(sub: "ex:p4", sup: "ex:p5"),
        ])
        let ctx = OntologyContext(ontology: ont)

        let subs = ctx.subProperties(of: "ex:p5")
        #expect(subs == Set(["ex:p1", "ex:p2", "ex:p3", "ex:p4"]))

        let expanded = ctx.expandedProperties(of: "ex:p5")
        #expect(expanded == Set(["ex:p1", "ex:p2", "ex:p3", "ex:p4", "ex:p5"]))
    }

    // MARK: - Diamond hierarchy

    @Test("Diamond property hierarchy: p1,p2 ⊑ p3; p3,p4 ⊑ p5")
    func diamondHierarchy() {
        let ont = makeOntology(axioms: [
            .subObjectPropertyOf(sub: "ex:p1", sup: "ex:p3"),
            .subObjectPropertyOf(sub: "ex:p2", sup: "ex:p3"),
            .subObjectPropertyOf(sub: "ex:p3", sup: "ex:p5"),
            .subObjectPropertyOf(sub: "ex:p4", sup: "ex:p5"),
        ])
        let ctx = OntologyContext(ontology: ont)

        let subsOfP5 = ctx.subProperties(of: "ex:p5")
        #expect(subsOfP5 == Set(["ex:p1", "ex:p2", "ex:p3", "ex:p4"]))

        let subsOfP3 = ctx.subProperties(of: "ex:p3")
        #expect(subsOfP3 == Set(["ex:p1", "ex:p2"]))
    }
}
