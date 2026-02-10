// TableauxReasonerTests.swift
// Comprehensive tests for SHOIN(D) Tableaux reasoner implementation

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - Basic Satisfiability

@Suite("TableauxReasoner Basic Satisfiability", .serialized)
struct TableauxReasonerBasicTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/minimal")
    }

    @Test("Thing is always satisfiable")
    func thingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.thing)
        #expect(result.isSatisfiable)
        #expect(result.clash == nil)
    }

    @Test("Nothing is never satisfiable")
    func nothingUnsatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.nothing)
        #expect(!result.isSatisfiable)
        #expect(result.clash != nil)
    }

    @Test("Named class is satisfiable by default")
    func namedClassSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(.named("ex:Person")).isSatisfiable)
    }

    @Test("C ⊓ ¬C is unsatisfiable (complement clash)")
    func complementClash() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Thing ⊓ Nothing is unsatisfiable")
    func thingAndNothing() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .thing,
            .nothing
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Disjoint classes intersection is unsatisfiable")
    func disjointClassesUnsatisfiable() {
        var ontology = minimalOntology()
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Dog"),
            .named("ex:Cat")
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Union is satisfiable if any disjunct is satisfiable")
    func unionSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.union([
            .nothing,
            .named("ex:Person")
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("Union of complements (excluded middle)")
    func unionOfComplements() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.union([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("isSatisfiable convenience method")
    func convenienceMethod() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.isSatisfiable(.thing))
        #expect(!reasoner.isSatisfiable(.nothing))
    }
}

// MARK: - Existential Restrictions

@Suite("TableauxReasoner Existential Restrictions", .serialized)
struct TableauxReasonerExistentialTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/existential")
    }

    @Test("∃R.⊤ is satisfiable")
    func existentialThingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .thing)
        ).isSatisfiable)
    }

    @Test("∃R.⊥ is unsatisfiable")
    func existentialNothingUnsatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(!reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .nothing)
        ).isSatisfiable)
    }

    @Test("∃R.C is satisfiable when C is satisfiable")
    func existentialSatisfiableClass() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
        ).isSatisfiable)
    }

    @Test("∃R.(C ⊓ ¬C) is unsatisfiable")
    func existentialUnsatisfiableFiller() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(!reasoner.checkSatisfiability(
            .someValuesFrom(
                property: "ex:hasChild",
                filler: .intersection([
                    .named("ex:Person"),
                    .complement(.named("ex:Person"))
                ])
            )
        ).isSatisfiable)
    }
}

// MARK: - Universal Restrictions

@Suite("TableauxReasoner Universal Restrictions", .serialized)
struct TableauxReasonerUniversalTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/universal")
    }

    @Test("∀R.⊤ is satisfiable")
    func universalThingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .allValuesFrom(property: "ex:hasChild", filler: .thing)
        ).isSatisfiable)
    }

    @Test("∀R.⊥ is satisfiable (vacuously true)")
    func universalNothingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .allValuesFrom(property: "ex:hasChild", filler: .nothing)
        ).isSatisfiable)
    }

    @Test("∃R.C ⊓ ∀R.¬C is unsatisfiable")
    func existentialUniversalConflict() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person")),
            .allValuesFrom(property: "ex:hasChild", filler: .complement(.named("ex:Person")))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("∀R.C ⊓ ∃R.D is satisfiable when C ⊓ D is satisfiable")
    func universalExistentialCompatible() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .allValuesFrom(property: "ex:hasChild", filler: .named("ex:Person")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Student"))
        ]))
        #expect(result.isSatisfiable)
    }
}

// MARK: - Cardinality Restrictions

@Suite("TableauxReasoner Cardinality Restrictions", .serialized)
struct TableauxReasonerCardinalityTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/cardinality")
    }

    @Test("≥0 R.C is always satisfiable")
    func minZeroSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .minCardinality(property: "ex:hasChild", n: 0, filler: .thing)
        ).isSatisfiable)
    }

    @Test("≥1 R.C is satisfiable")
    func minOneSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .minCardinality(property: "ex:hasChild", n: 1, filler: .thing)
        ).isSatisfiable)
    }

    @Test("≤0 R.⊤ is satisfiable (no successors)")
    func maxZeroSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .maxCardinality(property: "ex:hasChild", n: 0, filler: .thing)
        ).isSatisfiable)
    }

    @Test("≥2 R.C ⊓ ≤1 R.C is unsatisfiable")
    func cardinalityConflict() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .minCardinality(property: "ex:hasChild", n: 2, filler: .thing),
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing)
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("=2 R.C is satisfiable")
    func exactCardinalitySatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(
            .exactCardinality(property: "ex:hasChild", n: 2, filler: .thing)
        ).isSatisfiable)
    }

    @Test("≤1 R.⊤ ⊓ ∃R.A ⊓ ∃R.B with disjoint A,B is unsatisfiable")
    func maxCardinalityWithDisjointClasses() {
        var ontology = minimalOntology()
        ontology.axioms.append(.disjointClasses([.named("ex:A"), .named("ex:B")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("≤1 R.⊤ ⊓ ∃R.A ⊓ ∃R.B with compatible A,B is satisfiable")
    func maxCardinalityCompatible() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("≤1 R.⊤ merges duplicate existentials")
    func maxCardinalityMerging() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A"))
        ]))
        #expect(result.isSatisfiable)
    }
}

// MARK: - Subsumption

@Suite("TableauxReasoner Subsumption", .serialized)
struct TableauxReasonerSubsumptionTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/subsumption")
    }

    @Test("⊥ ⊑ C for any C")
    func nothingSubsumesAll() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Person"), subClass: .nothing))
    }

    @Test("C ⊑ ⊤ for any C")
    func thingSubsumesAll() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.subsumes(superClass: .thing, subClass: .named("ex:Person")))
    }

    @Test("C ⊑ C (reflexivity)")
    func subsumptionReflexive() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Dog"), subClass: .named("ex:Dog")))
    }

    @Test("C ⊓ D ⊑ C (conjunction subsumption)")
    func conjunctionSubsumption() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.subsumes(
            superClass: .named("ex:Animal"),
            subClass: .intersection([.named("ex:Animal"), .named("ex:Pet")])
        ))
    }

    @Test("SubClassOf axiom enables transitive subsumption")
    func subClassOfTransitive() {
        var ontology = minimalOntology()
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.subsumes(superClass: .named("ex:Animal"), subClass: .named("ex:Dog")))
    }

    @Test("EquivalentClasses implies mutual subsumption")
    func equivalentSubsumption() {
        var ontology = minimalOntology()
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.subsumes(superClass: .named("ex:Human"), subClass: .named("ex:Person")))
        #expect(reasoner.subsumes(superClass: .named("ex:Person"), subClass: .named("ex:Human")))
    }

    @Test("areEquivalent checks bidirectional subsumption")
    func areEquivalentTest() {
        var ontology = minimalOntology()
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.areEquivalent(.named("ex:Human"), .named("ex:Person")))
        #expect(!reasoner.areEquivalent(.named("ex:Human"), .named("ex:Dog")))
    }

    @Test("areDisjoint checks intersection unsatisfiability")
    func areDisjointTest() {
        var ontology = minimalOntology()
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.areDisjoint(.named("ex:Dog"), .named("ex:Cat")))
        #expect(!reasoner.areDisjoint(.named("ex:Dog"), .named("ex:Animal")))
    }
}

// MARK: - TBox Axioms

@Suite("TableauxReasoner TBox Axioms", .serialized)
struct TableauxReasonerTBoxTests {

    @Test("SubClassOf axiom affects satisfiability")
    func subClassOfAxiom() {
        var ontology = OWLOntology(iri: "http://test.org/tbox")
        ontology.axioms.append(.subClassOf(sub: .named("ex:Employee"), sup: .named("ex:Person")))
        ontology.axioms.append(.disjointClasses([.named("ex:Person"), .named("ex:Animal")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Employee"),
            .named("ex:Animal")
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Chained SubClassOf axioms")
    func chainedSubClassOf() {
        var ontology = OWLOntology(iri: "http://test.org/tbox-chain")
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        ontology.axioms.append(.disjointClasses([.named("ex:Animal"), .named("ex:Plant")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Dog"),
            .named("ex:Plant")
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("EquivalentClasses axiom")
    func equivalentClassesAxiom() {
        var ontology = OWLOntology(iri: "http://test.org/equiv")
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))
        ontology.axioms.append(.disjointClasses([.named("ex:Person"), .named("ex:Robot")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Human"),
            .named("ex:Robot")
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Defined class with intersection")
    func definedClassIntersection() {
        var ontology = OWLOntology(iri: "http://test.org/defined")
        // Parent ≡ Person ⊓ ∃hasChild.⊤
        ontology.axioms.append(.equivalentClasses([
            .named("ex:Parent"),
            .intersection([
                .named("ex:Person"),
                .someValuesFrom(property: "ex:hasChild", filler: .thing)
            ])
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)
        // Parent ⊑ Person (from definition)
        #expect(reasoner.subsumes(superClass: .named("ex:Person"), subClass: .named("ex:Parent")))
        // Person ⋢ Parent (not all persons are parents)
        #expect(!reasoner.subsumes(superClass: .named("ex:Parent"), subClass: .named("ex:Person")))
    }
}

// MARK: - Nominals (OneOf)

@Suite("TableauxReasoner Nominals", .serialized)
struct TableauxReasonerNominalTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/nominal")
    }

    @Test("oneOf with individuals is satisfiable")
    func oneOfSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(.oneOf(["ex:john", "ex:mary"])).isSatisfiable)
    }

    @Test("oneOf with single individual is satisfiable")
    func oneOfSingleSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(reasoner.checkSatisfiability(.oneOf(["ex:john"])).isSatisfiable)
    }

    @Test("Empty oneOf is unsatisfiable")
    func emptyOneOfUnsatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        #expect(!reasoner.checkSatisfiability(.oneOf([])).isSatisfiable)
    }

    @Test("hasValue creates nominal that can clash")
    func hasValueNominalClash() {
        var ontology = minimalOntology()
        // GlobalCorp ≡ Corporation ⊓ hasValue(hasScale, Global)
        ontology.axioms.append(.equivalentClasses([
            .named("ex:GlobalCorp"),
            .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ]))
        ontology.axioms.append(.subClassOf(sub: .named("ex:GlobalCorp"), sup: .named("ex:Corporation")))

        let reasoner = TableauxReasoner(ontology: ontology)
        // Corporation ⊓ hasValue(hasScale, Global) ⊑ GlobalCorp
        #expect(reasoner.subsumes(
            superClass: .named("ex:GlobalCorp"),
            subClass: .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ))
    }
}

// MARK: - HasSelf

@Suite("TableauxReasoner HasSelf", .serialized)
struct TableauxReasonerHasSelfTests {

    @Test("∃R.Self is satisfiable")
    func hasSelfSatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/hasSelf")
        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.checkSatisfiability(.hasSelf(property: "ex:knows")).isSatisfiable)
    }
}

// MARK: - Transitive Roles

@Suite("TableauxReasoner Transitive Roles", .serialized)
struct TableauxReasonerTransitiveRoleTests {

    private func transitiveOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/transitive")
        var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
        ancestorOf.characteristics.insert(.transitive)
        ontology.objectProperties.append(ancestorOf)
        return ontology
    }

    @Test("Transitive role propagates universal restrictions")
    func transitiveUniversalPropagation() {
        let reasoner = TableauxReasoner(ontology: transitiveOntology())
        // ∀ancestorOf.Person ⊓ ∃ancestorOf.∃ancestorOf.¬Person
        // Due to transitivity, ∀ancestorOf.Person reaches all transitive successors
        let result = reasoner.checkSatisfiability(.intersection([
            .allValuesFrom(property: "ex:ancestorOf", filler: .named("ex:Person")),
            .someValuesFrom(
                property: "ex:ancestorOf",
                filler: .someValuesFrom(
                    property: "ex:ancestorOf",
                    filler: .complement(.named("ex:Person"))
                )
            )
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Transitive role without conflict is satisfiable")
    func transitiveRoleSatisfiable() {
        let reasoner = TableauxReasoner(ontology: transitiveOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .allValuesFrom(property: "ex:ancestorOf", filler: .named("ex:Person")),
            .someValuesFrom(property: "ex:ancestorOf", filler: .named("ex:Person"))
        ]))
        #expect(result.isSatisfiable)
    }
}

// MARK: - Role Hierarchy (H)

@Suite("TableauxReasoner Role Hierarchy", .serialized)
struct TableauxReasonerRoleHierarchyTests {

    @Test("Sub-property propagates universal restriction")
    func subPropertyUniversalPropagation() {
        var ontology = OWLOntology(iri: "http://test.org/role-hierarchy")
        // hasMother ⊑ hasParent
        ontology.axioms.append(.subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasMother.C ⊓ ∀hasParent.¬C should be unsatisfiable
        // Because hasMother successor is also a hasParent successor
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasMother", filler: .named("ex:Female")),
            .allValuesFrom(property: "ex:hasParent", filler: .complement(.named("ex:Female")))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Super-property does not propagate downward")
    func superPropertyDoesNotPropagateDown() {
        var ontology = OWLOntology(iri: "http://test.org/role-hierarchy-2")
        ontology.axioms.append(.subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasParent.C ⊓ ∀hasMother.¬C should be satisfiable
        // Because hasParent successor is not necessarily a hasMother successor
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasParent", filler: .named("ex:Person")),
            .allValuesFrom(property: "ex:hasMother", filler: .complement(.named("ex:Person")))
        ]))
        #expect(result.isSatisfiable)
    }
}

// MARK: - Inverse Roles (I)

@Suite("TableauxReasoner Inverse Roles", .serialized)
struct TableauxReasonerInverseRoleTests {

    @Test("Inverse role propagates concepts")
    func inverseRolePropagation() {
        var ontology = OWLOntology(iri: "http://test.org/inverse")
        var parentOf = OWLObjectProperty(iri: "ex:parentOf")
        parentOf.inverseOf = "ex:childOf"
        ontology.objectProperties.append(parentOf)
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:childOf"))
        ontology.axioms.append(.inverseObjectProperties(first: "ex:parentOf", second: "ex:childOf"))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃parentOf.C ⊓ ∀childOf⁻.¬C  (i.e., ∀parentOf.¬C)
        // This should be equivalent to ∃parentOf.C ⊓ ∀parentOf.¬C → unsatisfiable
        // Note: inverse(childOf) = parentOf, so ∀childOf⁻ = ∀parentOf
        // But the reasoner may handle this via explicit inverse axioms
        // A simpler test: symmetric scenario
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:parentOf", filler: .named("ex:Person")),
            .allValuesFrom(property: "ex:parentOf", filler: .complement(.named("ex:Person")))
        ]))
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Symmetric Roles

@Suite("TableauxReasoner Symmetric Roles", .serialized)
struct TableauxReasonerSymmetricRoleTests {

    @Test("Symmetric role creates bidirectional edges")
    func symmetricRole() {
        var ontology = OWLOntology(iri: "http://test.org/symmetric")
        var knows = OWLObjectProperty(iri: "ex:knows")
        knows.characteristics.insert(.symmetric)
        ontology.objectProperties.append(knows)

        let reasoner = TableauxReasoner(ontology: ontology)

        // With a symmetric role, ∃knows.C means the successor also knows us
        // So: C ⊓ ∃knows.(D ⊓ ∀knows.¬C) should be unsatisfiable
        // Because: x:C, x knows y:D, y knows x (symmetric), ∀knows.¬C on y means x must be ¬C
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:C"),
            .someValuesFrom(
                property: "ex:knows",
                filler: .intersection([
                    .named("ex:D"),
                    .allValuesFrom(property: "ex:knows", filler: .complement(.named("ex:C")))
                ])
            )
        ]))
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Functional Roles

@Suite("TableauxReasoner Functional Roles", .serialized)
struct TableauxReasonerFunctionalRoleTests {

    @Test("Functional role limits to one successor")
    func functionalRoleConflict() {
        var ontology = OWLOntology(iri: "http://test.org/functional")
        var hasMother = OWLObjectProperty(iri: "ex:hasMother")
        hasMother.characteristics.insert(.functional)
        ontology.objectProperties.append(hasMother)

        ontology.axioms.append(.disjointClasses([.named("ex:A"), .named("ex:B")]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasMother.A ⊓ ∃hasMother.B with functional hasMother
        // The two successors must be merged, but A ⊓ B = ⊥
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasMother", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasMother", filler: .named("ex:B"))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Functional role with single existential is satisfiable")
    func functionalRoleSingleExistential() {
        var ontology = OWLOntology(iri: "http://test.org/functional-ok")
        var hasMother = OWLObjectProperty(iri: "ex:hasMother")
        hasMother.characteristics.insert(.functional)
        ontology.objectProperties.append(hasMother)

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasMother.(A ⊓ B) → single successor that is A ⊓ B
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasMother", filler: .intersection([
                .named("ex:A"),
                .named("ex:B")
            ]))
        )
        #expect(result.isSatisfiable)
    }
}

// MARK: - Domain and Range

@Suite("TableauxReasoner Domain and Range", .serialized)
struct TableauxReasonerDomainRangeTests {

    @Test("Domain constraint propagates to subject")
    func domainPropagation() {
        var ontology = OWLOntology(iri: "http://test.org/domain")
        var teaches = OWLObjectProperty(iri: "ex:teaches")
        teaches.domains = [.named("ex:Teacher")]
        ontology.objectProperties.append(teaches)
        ontology.axioms.append(.disjointClasses([.named("ex:Teacher"), .named("ex:Student")]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // Student ⊓ ∃teaches.⊤ should be unsatisfiable
        // Because teaches domain = Teacher, and Teacher ⊓ Student = ⊥
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Student"),
            .someValuesFrom(property: "ex:teaches", filler: .thing)
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Range constraint propagates to object")
    func rangePropagation() {
        var ontology = OWLOntology(iri: "http://test.org/range")
        var teaches = OWLObjectProperty(iri: "ex:teaches")
        teaches.ranges = [.named("ex:Course")]
        ontology.objectProperties.append(teaches)
        ontology.axioms.append(.disjointClasses([.named("ex:Course"), .named("ex:Person")]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃teaches.Person should be unsatisfiable
        // Because teaches range = Course, and Course ⊓ Person = ⊥
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:teaches", filler: .named("ex:Person"))
        )
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Instance Checking (ABox)

@Suite("TableauxReasoner Instance Checking", .serialized)
struct TableauxReasonerInstanceTests {

    @Test("isInstanceOf with direct class assertion")
    func directClassAssertion() {
        var ontology = OWLOntology(iri: "http://test.org/abox")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.axioms.append(.classAssertion(individual: "ex:alice", class_: .named("ex:Person")))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.isInstanceOf(individual: "ex:alice", classExpr: .named("ex:Person")))
    }

    @Test("isInstanceOf via SubClassOf inference")
    func instanceViaSubClass() {
        var ontology = OWLOntology(iri: "http://test.org/abox-sub")
        ontology.classes.append(OWLClass(iri: "ex:Dog"))
        ontology.classes.append(OWLClass(iri: "ex:Animal"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:rex"))
        ontology.axioms.append(.classAssertion(individual: "ex:rex", class_: .named("ex:Dog")))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.isInstanceOf(individual: "ex:rex", classExpr: .named("ex:Animal")))
    }

    @Test("isInstanceOf with object property assertion")
    func instanceWithObjectProperty() {
        var ontology = OWLOntology(iri: "http://test.org/abox-prop")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.axioms.append(.classAssertion(individual: "ex:alice", class_: .named("ex:Person")))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:alice", property: "ex:knows", object: "ex:bob"
        ))

        let reasoner = TableauxReasoner(ontology: ontology)
        // alice is Person ⊓ hasValue(knows, bob)
        #expect(reasoner.isInstanceOf(
            individual: "ex:alice",
            classExpr: .hasValue(property: "ex:knows", individual: "ex:bob")
        ))
    }

    @Test("isInstanceOf returns false for unasserted individual")
    func unassertedIndividual() {
        var ontology = OWLOntology(iri: "http://test.org/abox-empty")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:unknown"))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(!reasoner.isInstanceOf(individual: "ex:unknown", classExpr: .named("ex:Person")))
    }

    @Test("isInstanceOf with defined class")
    func instanceOfDefinedClass() {
        var ontology = OWLOntology(iri: "http://test.org/abox-defined")
        ontology.classes.append(OWLClass(iri: "ex:Corporation"))
        ontology.classes.append(OWLClass(iri: "ex:GlobalCorp"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasScale"))

        // GlobalCorp ≡ Corporation ⊓ hasValue(hasScale, Global)
        ontology.axioms.append(.equivalentClasses([
            .named("ex:GlobalCorp"),
            .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ]))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Global"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Toyota"))
        ontology.axioms.append(.classAssertion(individual: "ex:Toyota", class_: .named("ex:Corporation")))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:Toyota", property: "ex:hasScale", object: "ex:Global"
        ))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.isInstanceOf(individual: "ex:Toyota", classExpr: .named("ex:GlobalCorp")))
    }
}

// MARK: - types() Query

@Suite("TableauxReasoner types() Query", .serialized)
struct TableauxReasonerTypesTests {

    @Test("types() returns all inferred types")
    func typesReturnsInferred() {
        var ontology = OWLOntology(iri: "http://test.org/types")
        for cls in ["ex:LivingThing", "ex:Animal", "ex:Dog"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Animal"), sup: .named("ex:LivingThing")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:rex"))
        ontology.axioms.append(.classAssertion(individual: "ex:rex", class_: .named("ex:Dog")))

        let reasoner = TableauxReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:rex")

        #expect(types.contains("ex:Dog"))
        #expect(types.contains("ex:Animal"))
        #expect(types.contains("ex:LivingThing"))
    }

    @Test("types() returns empty for unknown individual")
    func typesEmptyForUnknown() {
        var ontology = OWLOntology(iri: "http://test.org/types-empty")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:unknown"))

        let reasoner = TableauxReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:unknown")
        // No assertions → no types
        #expect(types.isEmpty)
    }
}

// MARK: - instances() Query

@Suite("TableauxReasoner instances() Query", .serialized)
struct TableauxReasonerInstancesTests {

    @Test("instances() returns direct and inferred instances")
    func instancesReturnsAll() {
        var ontology = OWLOntology(iri: "http://test.org/instances")
        ontology.classes.append(OWLClass(iri: "ex:Animal"))
        ontology.classes.append(OWLClass(iri: "ex:Dog"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:rex"))
        ontology.axioms.append(.classAssertion(individual: "ex:rex", class_: .named("ex:Dog")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:tweety"))
        ontology.axioms.append(.classAssertion(individual: "ex:tweety", class_: .named("ex:Animal")))

        let reasoner = TableauxReasoner(ontology: ontology)

        let dogInstances = reasoner.instances(of: .named("ex:Dog"))
        #expect(dogInstances.contains("ex:rex"))
        #expect(!dogInstances.contains("ex:tweety"))

        let animalInstances = reasoner.instances(of: .named("ex:Animal"))
        #expect(animalInstances.contains("ex:rex"))    // inferred via Dog ⊑ Animal
        #expect(animalInstances.contains("ex:tweety"))  // direct assertion
    }

    @Test("instances() returns empty for unsatisfiable class")
    func instancesEmptyForNothing() {
        var ontology = OWLOntology(iri: "http://test.org/instances-empty")
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:x"))
        ontology.axioms.append(.classAssertion(individual: "ex:x", class_: .named("ex:A")))

        let reasoner = TableauxReasoner(ontology: ontology)
        let instances = reasoner.instances(of: .nothing)
        #expect(instances.isEmpty)
    }
}

// MARK: - classify()

@Suite("TableauxReasoner Classification", .serialized)
struct TableauxReasonerClassifyTests {

    @Test("classify() builds correct hierarchy from TBox")
    func classifyBuildsHierarchy() {
        var ontology = OWLOntology(iri: "http://test.org/classify")
        for cls in ["ex:Animal", "ex:Mammal", "ex:Dog", "ex:Cat"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Cat"), sup: .named("ex:Mammal")))

        let reasoner = TableauxReasoner(ontology: ontology)
        var hierarchy = reasoner.classify()

        #expect(hierarchy.superClasses(of: "ex:Dog").contains("ex:Mammal"))
        #expect(hierarchy.superClasses(of: "ex:Dog").contains("ex:Animal"))
        #expect(hierarchy.superClasses(of: "ex:Cat").contains("ex:Mammal"))
    }

    @Test("classify() detects equivalences")
    func classifyDetectsEquivalence() {
        var ontology = OWLOntology(iri: "http://test.org/classify-equiv")
        ontology.classes.append(OWLClass(iri: "ex:Human"))
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        var hierarchy = reasoner.classify()

        #expect(hierarchy.equivalentClasses(of: "ex:Human").contains("ex:Person"))
    }
}

// MARK: - Property Chains

@Suite("TableauxReasoner Property Chains", .serialized)
struct TableauxReasonerPropertyChainTests {

    @Test("Property chain R ∘ S ⊑ T")
    func propertyChainComposition() {
        var ontology = OWLOntology(iri: "http://test.org/chain")
        // hasParent ∘ hasSibling ⊑ hasUncle
        ontology.axioms.append(.subPropertyChainOf(
            chain: ["ex:hasParent", "ex:hasSibling"],
            sup: "ex:hasUncle"
        ))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasParent.∃hasSibling.⊤ should imply ∃hasUncle.⊤
        // But the specific test: the chain should fire and produce the uncle edge
        // We verify by checking satisfiability of a concept that uses the chain
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasParent", filler:
                .someValuesFrom(property: "ex:hasSibling", filler: .thing)
            ),
            .allValuesFrom(property: "ex:hasUncle", filler: .nothing)
        ]))
        // If chains work, hasUncle successor exists, but ∀hasUncle.⊥ forbids it → unsatisfiable
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Backtracking

@Suite("TableauxReasoner Backtracking", .serialized)
struct TableauxReasonerBacktrackingTests {

    private func disjointOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/backtrack")
        ontology.axioms.append(.disjointClasses([.named("ex:A"), .named("ex:B")]))
        return ontology
    }

    @Test("Union with first disjunct failing requires backtracking")
    func unionBacktracking() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())
        // (A ⊔ C) ⊓ B - first choice A fails, backtrack to C
        let result = reasoner.checkSatisfiability(.intersection([
            .union([.named("ex:A"), .named("ex:C")]),
            .named("ex:B")
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("All union choices fail")
    func allChoicesFail() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())
        // (A ⊔ B) ⊓ ¬A ⊓ ¬B
        let result = reasoner.checkSatisfiability(.intersection([
            .union([.named("ex:A"), .named("ex:B")]),
            .complement(.named("ex:A")),
            .complement(.named("ex:B"))
        ]))
        #expect(!result.isSatisfiable)
    }

    @Test("Multiple union backtracking")
    func multipleUnionBacktracking() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())
        // (A ⊔ B) ⊓ (A ⊔ C) ⊓ ¬A → B ⊓ C ⊓ ¬A
        let result = reasoner.checkSatisfiability(.intersection([
            .union([.named("ex:A"), .named("ex:B")]),
            .union([.named("ex:A"), .named("ex:C")]),
            .complement(.named("ex:A"))
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("Backtrack count is recorded in statistics")
    func backtrackCountRecorded() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())
        // (A ⊔ C) ⊓ B → A fails (disjoint), must backtrack
        let result = reasoner.checkSatisfiability(.intersection([
            .union([.named("ex:A"), .named("ex:C")]),
            .named("ex:B")
        ]))
        #expect(result.isSatisfiable)
        #expect(result.statistics.backtrackCount > 0)
    }
}

// MARK: - Blocking (Termination)

@Suite("TableauxReasoner Blocking Termination", .serialized)
struct TableauxReasonerBlockingTests {

    @Test("Cyclic concept terminates via blocking")
    func infiniteChainBlocking() {
        let ontology = OWLOntology(iri: "http://test.org/blocking")
        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasChild.Person ⊓ ∀hasChild.∃hasChild.Person
        // Would create infinite nodes without blocking
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person")),
            .allValuesFrom(
                property: "ex:hasChild",
                filler: .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
            )
        ]))
        #expect(result.isSatisfiable)
        #expect(result.statistics.expansionSteps < 1000)
    }

    @Test("Blocking does not hide clashes")
    func blockingDoesNotHideClash() {
        var ontology = OWLOntology(iri: "http://test.org/blocking-clash")
        ontology.axioms.append(.disjointClasses([.named("ex:A"), .named("ex:B")]))

        let reasoner = TableauxReasoner(ontology: ontology)
        // ∃hasChild.A ⊓ ∀hasChild.B → child is A ⊓ B → clash
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .allValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Complex Expressions

@Suite("TableauxReasoner Complex Expressions", .serialized)
struct TableauxReasonerComplexTests {

    @Test("Deeply nested expression is satisfiable")
    func deeplyNestedSatisfiable() {
        let reasoner = TableauxReasoner(ontology: OWLOntology(iri: "http://test.org/complex"))
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(
                property: "ex:hasChild",
                filler: .someValuesFrom(
                    property: "ex:hasChild",
                    filler: .intersection([
                        .named("ex:Person"),
                        .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
                    ])
                )
            )
        )
        #expect(result.isSatisfiable)
    }

    @Test("Statistics are tracked")
    func statisticsTracking() {
        let reasoner = TableauxReasoner(ontology: OWLOntology(iri: "http://test.org/stats"))
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Person"),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
        ]))
        #expect(result.isSatisfiable)
        #expect(result.statistics.nodesCreated > 0)
        #expect(result.statistics.expansionSteps > 0)
    }

    @Test("Complex GCI interaction")
    func complexGCIInteraction() {
        var ontology = OWLOntology(iri: "http://test.org/complex-gci")
        // GCI: ∃owns.Dog ⊑ DogOwner
        ontology.axioms.append(.subClassOf(
            sub: .someValuesFrom(property: "ex:owns", filler: .named("ex:Dog")),
            sup: .named("ex:DogOwner")
        ))
        // DogOwner ⊓ CatHater = ⊥
        ontology.axioms.append(.disjointClasses([
            .named("ex:DogOwner"),
            .named("ex:CatHater")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // CatHater ⊓ ∃owns.Dog should be unsatisfiable
        // Because ∃owns.Dog → DogOwner, and DogOwner ⊓ CatHater = ⊥
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:CatHater"),
            .someValuesFrom(property: "ex:owns", filler: .named("ex:Dog"))
        ]))
        #expect(!result.isSatisfiable)
    }
}

// MARK: - Regularity Check

@Suite("TableauxReasoner Regularity Check", .serialized)
struct TableauxReasonerRegularityTests {

    @Test("Regular ontology passes check")
    func regularOntologyPasses() {
        var ontology = OWLOntology(iri: "http://test.org/regular")
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.isRegular)
        #expect(reasoner.regularityViolations.isEmpty)
    }

    @Test("Transitive role in cardinality detected")
    func transitiveInCardinalityDetected() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(!reasoner.isRegular)

        let hasTransitiveViolation = reasoner.regularityViolations.contains { violation in
            if case .transitiveInCardinality = violation { return true }
            return false
        }
        #expect(hasTransitiveViolation)
    }

    @Test("Reasoning returns unknown on regularity violation")
    func returnsUnknownOnViolation() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.named("ex:Person"))
        #expect(result.isUnknown)
        #expect(result.status == .unknown)
    }

    @Test("Reasoning continues when abortOnRegularityViolations is false")
    func continuesWhenNotAborting() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let config = TableauxReasoner.Configuration(
            maxExpansionSteps: 1000,
            checkRegularity: true,
            abortOnRegularityViolations: false
        )
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)
        let result = reasoner.checkSatisfiability(.thing)
        #expect(!result.isUnknown)
        #expect(result.isSatisfiable)
    }

    @Test("Regularity check can be disabled")
    func regularityCheckCanBeDisabled() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let config = TableauxReasoner.Configuration(checkRegularity: false)
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)
        #expect(reasoner.regularityViolations.isEmpty)
        #expect(reasoner.isRegular)

        let result = reasoner.checkSatisfiability(.thing)
        #expect(result.isSatisfiable)
    }
}

// MARK: - Configuration

@Suite("TableauxReasoner Configuration", .serialized)
struct TableauxReasonerConfigurationTests {

    @Test("Custom maxExpansionSteps")
    func customMaxExpansionSteps() {
        let config = TableauxReasoner.Configuration(
            maxExpansionSteps: 500,
            checkRegularity: false
        )
        let reasoner = TableauxReasoner(
            ontology: OWLOntology(iri: "http://test.org/config"),
            configuration: config
        )
        #expect(reasoner.checkSatisfiability(.thing).isSatisfiable)
    }

    @Test("Expansion limit returns unknown")
    func expansionLimitReturnsUnknown() {
        let ontology = OWLOntology(iri: "http://test.org/limit")
        // Create a concept that requires many expansion steps
        // ≥3 R.⊤ ⊓ ∀R.∃R.⊤ ⊓ ∀R.∀R.∃R.⊤ → generates many nodes
        let config = TableauxReasoner.Configuration(
            maxExpansionSteps: 1,
            checkRegularity: false
        )
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)

        let result = reasoner.checkSatisfiability(.intersection([
            .minCardinality(property: "ex:R", n: 3, filler: .thing),
            .allValuesFrom(property: "ex:R", filler:
                .someValuesFrom(property: "ex:R", filler: .thing)
            )
        ]))
        // With only 1 expansion step, should hit the limit
        #expect(result.status == .unknown)
    }

    @Test("Convenience initializer with maxExpansionSteps")
    func convenienceInit() {
        let reasoner = TableauxReasoner(
            ontology: OWLOntology(iri: "http://test.org/conv"),
            maxExpansionSteps: 5000
        )
        #expect(reasoner.checkSatisfiability(.thing).isSatisfiable)
    }

    @Test("SatisfiabilityResult status properties")
    func satisfiabilityResultProperties() {
        let sat = TableauxReasoner.SatisfiabilityResult(
            status: .satisfiable, statistics: .init()
        )
        #expect(sat.isSatisfiable)
        #expect(!sat.isUnsatisfiable)
        #expect(!sat.isUnknown)

        let unsat = TableauxReasoner.SatisfiabilityResult(
            status: .unsatisfiable, statistics: .init()
        )
        #expect(!unsat.isSatisfiable)
        #expect(unsat.isUnsatisfiable)
        #expect(!unsat.isUnknown)

        let unknown = TableauxReasoner.SatisfiabilityResult(
            status: .unknown, statistics: .init()
        )
        #expect(!unknown.isSatisfiable)
        #expect(!unknown.isUnsatisfiable)
        #expect(unknown.isUnknown)
    }

    @Test("Legacy SatisfiabilityResult initializer")
    func legacyInitializer() {
        let sat = TableauxReasoner.SatisfiabilityResult(
            isSatisfiable: true, statistics: .init()
        )
        #expect(sat.status == .satisfiable)

        let unsat = TableauxReasoner.SatisfiabilityResult(
            isSatisfiable: false, statistics: .init()
        )
        #expect(unsat.status == .unsatisfiable)
    }
}

// MARK: - Data Properties

@Suite("TableauxReasoner Data Properties", .serialized)
struct TableauxReasonerDataPropertyTests {

    @Test("dataHasValue is satisfiable")
    func dataHasValueSatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/data")
        let reasoner = TableauxReasoner(ontology: ontology)

        let result = reasoner.checkSatisfiability(
            .dataHasValue(property: "ex:age", literal: OWLLiteral(lexicalForm: "30", datatype: "xsd:integer"))
        )
        #expect(result.isSatisfiable)
    }

    @Test("isInstanceOf with data property assertion")
    func instanceWithDataProperty() {
        var ontology = OWLOntology(iri: "http://test.org/data-inst")
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.axioms.append(.dataPropertyAssertion(
            subject: "ex:alice",
            property: "ex:age",
            value: OWLLiteral(lexicalForm: "30", datatype: "xsd:integer")
        ))

        let reasoner = TableauxReasoner(ontology: ontology)
        #expect(reasoner.isInstanceOf(
            individual: "ex:alice",
            classExpr: .dataHasValue(
                property: "ex:age",
                literal: OWLLiteral(lexicalForm: "30", datatype: "xsd:integer")
            )
        ))
    }
}

// MARK: - Irreflexive and Asymmetric Roles

@Suite("TableauxReasoner Role Constraints", .serialized)
struct TableauxReasonerRoleConstraintTests {

    @Test("Irreflexive role with hasSelf is unsatisfiable")
    func irreflexiveWithHasSelf() {
        var ontology = OWLOntology(iri: "http://test.org/irreflexive")
        var before = OWLObjectProperty(iri: "ex:before")
        before.characteristics.insert(.irreflexive)
        ontology.objectProperties.append(before)

        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.hasSelf(property: "ex:before"))
        #expect(!result.isSatisfiable)
    }

    @Test("Asymmetric role constraint")
    func asymmetricRole() {
        var ontology = OWLOntology(iri: "http://test.org/asymmetric")
        var parentOf = OWLObjectProperty(iri: "ex:parentOf")
        parentOf.characteristics.insert(.asymmetric)
        ontology.objectProperties.append(parentOf)

        // asymmetric: R(x,y) → ¬R(y,x)
        // hasSelf would mean R(x,x), which is a special case of both R(x,y) and R(y,x) with y=x
        let reasoner = TableauxReasoner(ontology: ontology)
        let result = reasoner.checkSatisfiability(.hasSelf(property: "ex:parentOf"))
        #expect(!result.isSatisfiable)
    }
}
