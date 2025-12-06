// TableauxReasonerTests.swift
// Tests for SHOIN(D) Tableaux reasoner implementation

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - TableauxReasoner Basic Tests

@Suite("TableauxReasoner Basic Satisfiability", .serialized)
struct TableauxReasonerBasicTests {

    /// Create a minimal ontology for testing
    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/minimal")
    }

    /// Create ontology with disjoint classes
    private func disjointOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/disjoint")

        // Dog ⊓ Cat = ⊥ (disjoint)
        ontology.axioms.append(.disjointClasses([
            .named("ex:Dog"),
            .named("ex:Cat")
        ]))

        return ontology
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
        let result = reasoner.checkSatisfiability(.named("ex:Person"))

        #expect(result.isSatisfiable)
    }

    @Test("C ⊓ ¬C is unsatisfiable (complement clash)")
    func complementClash() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))

        #expect(!result.isSatisfiable)
        #expect(result.clash != nil)
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
        let reasoner = TableauxReasoner(ontology: disjointOntology())
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
            .nothing,  // unsatisfiable
            .named("ex:Person")  // satisfiable
        ]))

        #expect(result.isSatisfiable)
    }

    @Test("Union of complements (excluded middle)")
    func unionOfComplements() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        // C ⊔ ¬C is always satisfiable (excluded middle)
        let result = reasoner.checkSatisfiability(.union([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))

        #expect(result.isSatisfiable)
    }
}

// MARK: - Existential Restriction Tests

@Suite("TableauxReasoner Existential Restrictions", .serialized)
struct TableauxReasonerExistentialTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/properties")
    }

    @Test("∃R.⊤ is satisfiable")
    func existentialThingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .thing)
        )

        #expect(result.isSatisfiable)
    }

    @Test("∃R.⊥ is unsatisfiable")
    func existentialNothingUnsatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .nothing)
        )

        #expect(!result.isSatisfiable)
    }

    @Test("∃R.C is satisfiable when C is satisfiable")
    func existentialSatisfiableClass() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
        )

        #expect(result.isSatisfiable)
    }

    @Test("∃R.(C ⊓ ¬C) is unsatisfiable")
    func existentialUnsatisfiableFiller() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(
                property: "ex:hasChild",
                filler: .intersection([
                    .named("ex:Person"),
                    .complement(.named("ex:Person"))
                ])
            )
        )

        #expect(!result.isSatisfiable)
    }
}

// MARK: - Universal Restriction Tests

@Suite("TableauxReasoner Universal Restrictions", .serialized)
struct TableauxReasonerUniversalTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/universal")
    }

    @Test("∀R.⊤ is satisfiable")
    func universalThingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .allValuesFrom(property: "ex:hasChild", filler: .thing)
        )

        #expect(result.isSatisfiable)
    }

    @Test("∀R.⊥ is satisfiable (vacuously true when no R-successors)")
    func universalNothingSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        // ∀R.⊥ is satisfiable - it means "no R-successors"
        let result = reasoner.checkSatisfiability(
            .allValuesFrom(property: "ex:hasChild", filler: .nothing)
        )

        #expect(result.isSatisfiable)
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
}

// MARK: - Cardinality Restriction Tests

@Suite("TableauxReasoner Cardinality Restrictions", .serialized)
struct TableauxReasonerCardinalityTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/cardinality")
    }

    @Test("≥0 R.C is always satisfiable")
    func minZeroSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .minCardinality(property: "ex:hasChild", n: 0, filler: .thing)
        )

        #expect(result.isSatisfiable)
    }

    @Test("≥1 R.C is satisfiable")
    func minOneSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .minCardinality(property: "ex:hasChild", n: 1, filler: .thing)
        )

        #expect(result.isSatisfiable)
    }

    @Test("≤0 R.⊤ is satisfiable (no successors)")
    func maxZeroSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .maxCardinality(property: "ex:hasChild", n: 0, filler: .thing)
        )

        #expect(result.isSatisfiable)
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
        let result = reasoner.checkSatisfiability(
            .exactCardinality(property: "ex:hasChild", n: 2, filler: .thing)
        )

        #expect(result.isSatisfiable)
    }
}

// MARK: - Subsumption Tests

@Suite("TableauxReasoner Subsumption", .serialized)
struct TableauxReasonerSubsumptionTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/subsumption")
    }

    @Test("⊥ ⊑ C for any C (Nothing subsumes everything)")
    func nothingSubsumesAll() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Person"),
            subClass: .nothing
        )

        #expect(result)
    }

    @Test("C ⊑ ⊤ for any C (Everything is subsumed by Thing)")
    func thingSubsumesAll() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.subsumes(
            superClass: .thing,
            subClass: .named("ex:Person")
        )

        #expect(result)
    }

    @Test("C ⊑ C (reflexivity)")
    func subsumptionReflexive() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Dog"),
            subClass: .named("ex:Dog")
        )

        #expect(result)
    }

    @Test("C ⊓ D ⊑ C (conjunction subsumption)")
    func conjunctionSubsumption() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Animal"),
            subClass: .intersection([.named("ex:Animal"), .named("ex:Pet")])
        )

        #expect(result)
    }
}

// MARK: - Nominal (OneOf) Tests

@Suite("TableauxReasoner Nominals", .serialized)
struct TableauxReasonerNominalTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/nominal")
    }

    @Test("oneOf with at least one individual is satisfiable")
    func oneOfSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .oneOf(["ex:john", "ex:mary"])
        )

        #expect(result.isSatisfiable)
    }

    @Test("oneOf with single individual is satisfiable")
    func oneOfSingleSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .oneOf(["ex:john"])
        )

        #expect(result.isSatisfiable)
    }

    @Test("Empty oneOf is unsatisfiable")
    func emptyOneOfUnsatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .oneOf([])
        )

        #expect(!result.isSatisfiable)
    }
}

// MARK: - HasSelf Tests

@Suite("TableauxReasoner HasSelf", .serialized)
struct TableauxReasonerHasSelfTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/hasSelf")
    }

    @Test("∃R.Self is satisfiable")
    func hasSelfSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(
            .hasSelf(property: "ex:knows")
        )

        #expect(result.isSatisfiable)
    }
}

// MARK: - Complex Expression Tests

@Suite("TableauxReasoner Complex Expressions", .serialized)
struct TableauxReasonerComplexTests {

    private func minimalOntology() -> OWLOntology {
        OWLOntology(iri: "http://test.org/complex")
    }

    @Test("Deeply nested expression is satisfiable")
    func deeplyNestedSatisfiable() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())

        // ∃hasChild.(∃hasChild.(Person ⊓ ∃hasChild.Person))
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

    @Test("Statistics tracking")
    func statisticsTracking() {
        let reasoner = TableauxReasoner(ontology: minimalOntology())
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Person"),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
        ]))

        #expect(result.isSatisfiable)
        #expect(result.statistics.nodesCreated > 0)
    }
}

// MARK: - Backtracking Tests

@Suite("TableauxReasoner Backtracking", .serialized)
struct TableauxReasonerBacktrackingTests {

    private func disjointOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/backtrack")

        // A and B are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:A"),
            .named("ex:B")
        ]))

        return ontology
    }

    @Test("Union with first disjunct failing requires backtracking")
    func unionBacktracking() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())

        // (A ⊔ C) ⊓ B - first choice A fails (disjoint with B), backtrack to C
        let result = reasoner.checkSatisfiability(.intersection([
            .union([
                .named("ex:A"),
                .named("ex:C")
            ]),
            .named("ex:B")
        ]))

        #expect(result.isSatisfiable)
    }

    @Test("All union choices fail")
    func allChoicesFail() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())

        // (A ⊔ B) ⊓ ¬A ⊓ ¬B - all choices fail
        let result = reasoner.checkSatisfiability(.intersection([
            .union([
                .named("ex:A"),
                .named("ex:B")
            ]),
            .complement(.named("ex:A")),
            .complement(.named("ex:B"))
        ]))

        #expect(!result.isSatisfiable)
    }

    @Test("Multiple union backtracking")
    func multipleUnionBacktracking() {
        let reasoner = TableauxReasoner(ontology: disjointOntology())

        // (A ⊔ B) ⊓ (A ⊔ C) ⊓ ¬A
        // First union: try A -> fail (conflicts with ¬A), try B -> ok
        // Second union: try A -> fail (conflicts with ¬A), try C -> ok
        // Result: B ⊓ C ⊓ ¬A is satisfiable
        let result = reasoner.checkSatisfiability(.intersection([
            .union([.named("ex:A"), .named("ex:B")]),
            .union([.named("ex:A"), .named("ex:C")]),
            .complement(.named("ex:A"))
        ]))

        #expect(result.isSatisfiable)
    }
}

// MARK: - TBox Axiom Tests (Critical Feature Verification)

@Suite("TableauxReasoner TBox Axioms", .serialized)
struct TableauxReasonerTBoxTests {

    @Test("SubClassOf axiom affects satisfiability")
    func subClassOfAxiom() {
        var ontology = OWLOntology(iri: "http://test.org/tbox")

        // TBox: Employee ⊑ Person (every employee is a person)
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Employee"),
            sup: .named("ex:Person")
        ))

        // TBox: Person and Animal are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:Person"),
            .named("ex:Animal")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // Employee ⊓ Animal should be UNSATISFIABLE
        // Because: Employee ⊑ Person, Person ⊓ Animal = ⊥
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Employee"),
            .named("ex:Animal")
        ]))

        #expect(!result.isSatisfiable, "Employee ⊓ Animal should be unsatisfiable due to TBox axiom Employee ⊑ Person")
    }

    @Test("Chained SubClassOf axioms")
    func chainedSubClassOf() {
        var ontology = OWLOntology(iri: "http://test.org/tbox-chain")

        // TBox: Dog ⊑ Mammal, Mammal ⊑ Animal
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Mammal")
        ))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Mammal"),
            sup: .named("ex:Animal")
        ))

        // Animal and Plant are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:Animal"),
            .named("ex:Plant")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // Dog ⊓ Plant should be UNSATISFIABLE (Dog → Mammal → Animal, Animal ⊓ Plant = ⊥)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Dog"),
            .named("ex:Plant")
        ]))

        #expect(!result.isSatisfiable, "Dog ⊓ Plant should be unsatisfiable through TBox chain")
    }

    @Test("EquivalentClasses axiom")
    func equivalentClassesAxiom() {
        var ontology = OWLOntology(iri: "http://test.org/equiv")

        // TBox: Human ≡ Person
        ontology.axioms.append(.equivalentClasses([
            .named("ex:Human"),
            .named("ex:Person")
        ]))

        // Person and Robot are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:Person"),
            .named("ex:Robot")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // Human ⊓ Robot should be UNSATISFIABLE (Human ≡ Person, Person ⊓ Robot = ⊥)
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:Human"),
            .named("ex:Robot")
        ]))

        #expect(!result.isSatisfiable, "Human ⊓ Robot should be unsatisfiable due to equivalence axiom")
    }
}

// MARK: - Max Cardinality Tests (Critical Feature Verification)

@Suite("TableauxReasoner Max Cardinality Enforcement", .serialized)
struct TableauxReasonerMaxCardinalityTests {

    @Test("Max cardinality with disjoint filler classes")
    func maxCardinalityWithDisjointClasses() {
        var ontology = OWLOntology(iri: "http://test.org/maxcard-disjoint")

        // A and B are disjoint - cannot be satisfied by same individual
        ontology.axioms.append(.disjointClasses([
            .named("ex:A"),
            .named("ex:B")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ≤1 hasChild.⊤ ⊓ ∃hasChild.A ⊓ ∃hasChild.B
        // With A ⊓ B = ⊥, the two existentials need DIFFERENT children
        // But max cardinality allows only 1 child
        // → UNSATISFIABLE
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))

        #expect(!result.isSatisfiable, "Max cardinality with disjoint fillers should be unsatisfiable")
    }

    @Test("Max cardinality satisfied through merging")
    func maxCardinalityMerging() {
        let ontology = OWLOntology(iri: "http://test.org/maxcard-merge")
        let reasoner = TableauxReasoner(ontology: ontology)

        // ≤1 hasChild.⊤ ⊓ ∃hasChild.A ⊓ ∃hasChild.A
        // This should be satisfiable - both existentials can be witnessed by the same node
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A"))
        ]))

        #expect(result.isSatisfiable, "Max cardinality should allow merging compatible successors")
    }

    @Test("Max cardinality with compatible fillers is satisfiable")
    func maxCardinalityCompatible() {
        let ontology = OWLOntology(iri: "http://test.org/maxcard-compat")
        let reasoner = TableauxReasoner(ontology: ontology)

        // ≤1 hasChild.⊤ ⊓ ∃hasChild.A ⊓ ∃hasChild.B
        // A and B are NOT disjoint, so one child can be both A and B
        // → SATISFIABLE
        let result = reasoner.checkSatisfiability(.intersection([
            .maxCardinality(property: "ex:hasChild", n: 1, filler: .thing),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))

        #expect(result.isSatisfiable, "Max cardinality with compatible fillers should be satisfiable")
    }
}

// MARK: - Transitive Role Tests (Critical Feature Verification)

@Suite("TableauxReasoner Transitive Roles", .serialized)
struct TableauxReasonerTransitiveRoleTests {

    private func transitiveOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/transitive")

        // ancestorOf is transitive
        var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
        ancestorOf.characteristics.insert(.transitive)
        ontology.objectProperties.append(ancestorOf)

        return ontology
    }

    @Test("Transitive role propagates universal restrictions")
    func transitiveUniversalPropagation() {
        let ontology = transitiveOntology()
        let reasoner = TableauxReasoner(ontology: ontology)

        // If ancestorOf is transitive, then:
        // ∀ancestorOf.Person should propagate through the chain
        // ∃ancestorOf.∃ancestorOf.¬Person should conflict with ∀ancestorOf.Person

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

        // Due to transitivity: ancestorOf(x,y) ∧ ancestorOf(y,z) → ancestorOf(x,z)
        // So ∀ancestorOf.Person should apply to z as well
        // But z must be ¬Person, contradiction
        #expect(!result.isSatisfiable, "Transitive role should propagate universal restrictions")
    }
}

// MARK: - Blocking Tests (Critical Feature Verification)

@Suite("TableauxReasoner Blocking Termination", .serialized)
struct TableauxReasonerBlockingTests {

    @Test("Infinite chain terminates via blocking")
    func infiniteChainBlocking() {
        let ontology = OWLOntology(iri: "http://test.org/blocking")
        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasChild.∃hasChild.∃hasChild... would create infinite nodes
        // But blocking should terminate it
        // Test: ∃hasChild.Self ⊓ ∀hasChild.∃hasChild.Self (cyclic definition)
        // This is actually satisfiable with blocking

        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person")),
            .allValuesFrom(
                property: "ex:hasChild",
                filler: .someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person"))
            )
        ]))

        // Should terminate and be satisfiable (infinite model exists)
        #expect(result.isSatisfiable, "Should terminate via blocking")
        #expect(result.statistics.expansionSteps < 1000, "Should terminate efficiently via blocking")
    }

    @Test("Blocking does not prematurely stop unsatisfiable detection")
    func blockingDoesNotHideClash() {
        var ontology = OWLOntology(iri: "http://test.org/blocking-clash")

        // A and B are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:A"),
            .named("ex:B")
        ]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃hasChild.A ⊓ ∀hasChild.B should be unsatisfiable
        // (the child must be both A and B, but they are disjoint)
        let result = reasoner.checkSatisfiability(.intersection([
            .someValuesFrom(property: "ex:hasChild", filler: .named("ex:A")),
            .allValuesFrom(property: "ex:hasChild", filler: .named("ex:B"))
        ]))

        #expect(!result.isSatisfiable, "Blocking should not hide clashes")
    }
}

// MARK: - Regularity Check Tests

@Suite("TableauxReasoner Regularity Check", .serialized)
struct TableauxReasonerRegularityTests {

    @Test("Regular ontology passes check")
    func regularOntologyPasses() {
        var ontology = OWLOntology(iri: "http://test.org/regular")

        // A regular ontology with simple roles
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Animal")
        ))

        let reasoner = TableauxReasoner(ontology: ontology)

        #expect(reasoner.isRegular)
        #expect(reasoner.regularityViolations.isEmpty)
    }

    @Test("Transitive role in cardinality detected")
    func transitiveInCardinalityDetected() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")

        // Make hasAncestor transitive
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))

        // Use transitive role in cardinality - OWL DL violation!
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let reasoner = TableauxReasoner(ontology: ontology)

        #expect(!reasoner.isRegular)
        #expect(!reasoner.regularityViolations.isEmpty)

        // Should find transitive in cardinality violation
        let hasTransitiveViolation = reasoner.regularityViolations.contains { violation in
            if case .transitiveInCardinality = violation {
                return true
            }
            return false
        }
        #expect(hasTransitiveViolation, "Should detect transitive role in cardinality")
    }

    @Test("Reasoning returns unknown on regularity violation when configured")
    func returnsUnknownOnViolation() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")

        // Make hasAncestor transitive
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))

        // Use transitive role in cardinality - OWL DL violation!
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        // Default configuration: abortOnRegularityViolations = true
        let reasoner = TableauxReasoner(ontology: ontology)

        let result = reasoner.checkSatisfiability(.named("ex:Person"))

        #expect(result.isUnknown, "Should return unknown when ontology has regularity violations")
        #expect(result.status == .unknown)
    }

    @Test("Reasoning continues when abortOnRegularityViolations is false")
    func continuesWhenNotAborting() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")

        // Make hasAncestor transitive
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))

        // Use transitive role in cardinality - OWL DL violation!
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        // Configure to continue despite violations
        let config = TableauxReasoner.Configuration(
            maxExpansionSteps: 1000,
            checkRegularity: true,
            abortOnRegularityViolations: false
        )
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)

        let result = reasoner.checkSatisfiability(.thing)

        // Should not be unknown - reasoning proceeded
        #expect(!result.isUnknown, "Should not be unknown when continuing despite violations")
        #expect(result.isSatisfiable, "Thing should be satisfiable")
    }

    @Test("Regularity check can be disabled")
    func regularityCheckCanBeDisabled() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")

        // Make hasAncestor transitive
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))

        // Use transitive role in cardinality - OWL DL violation!
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        // Disable regularity checking
        let config = TableauxReasoner.Configuration(
            checkRegularity: false
        )
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)

        // No violations stored since checking was disabled
        #expect(reasoner.regularityViolations.isEmpty)
        #expect(reasoner.isRegular) // isRegular = violations.isEmpty

        // Reasoning proceeds normally
        let result = reasoner.checkSatisfiability(.thing)
        #expect(result.isSatisfiable)
    }

    @Test("Configuration with custom maxExpansionSteps")
    func customMaxExpansionSteps() {
        let ontology = OWLOntology(iri: "http://test.org/config")

        let config = TableauxReasoner.Configuration(
            maxExpansionSteps: 500,
            checkRegularity: false,
            abortOnRegularityViolations: false
        )
        let reasoner = TableauxReasoner(ontology: ontology, configuration: config)

        // Basic functionality still works
        let result = reasoner.checkSatisfiability(.thing)
        #expect(result.isSatisfiable)
    }
}
