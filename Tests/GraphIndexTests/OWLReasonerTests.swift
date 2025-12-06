// OWLReasonerTests.swift
// Tests for high-level OWLReasoner API

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - OWLReasoner Initialization Tests

@Suite("OWLReasoner Initialization", .serialized)
struct OWLReasonerInitTests {

    @Test("Create reasoner with empty ontology")
    func createWithEmptyOntology() {
        let ontology = OWLOntology(iri: "http://test.org/empty")
        let reasoner = OWLReasoner(ontology: ontology)

        #expect(reasoner.isConsistent().value == true)
    }

    @Test("Create reasoner with configuration")
    func createWithConfiguration() {
        let ontology = OWLOntology(iri: "http://test.org/config")
        let config = OWLReasoner.Configuration(
            maxExpansionSteps: 500,
            enableIncrementalReasoning: false,
            cacheClassification: true
        )
        let reasoner = OWLReasoner(ontology: ontology, configuration: config)

        #expect(reasoner.isConsistent().value == true)
    }
}

// MARK: - OWLReasoner Consistency Tests

@Suite("OWLReasoner Consistency Checking", .serialized)
struct OWLReasonerConsistencyTests {

    @Test("Consistent ontology")
    func consistentOntology() {
        var ontology = OWLOntology(iri: "http://test.org/consistent")

        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.classes.append(OWLClass(iri: "ex:Employee"))

        // Employee ⊑ Person
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Employee"),
            sup: .named("ex:Person")
        ))

        // Individual
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:john"))
        ontology.axioms.append(.classAssertion(
            individual: "ex:john",
            class_: .named("ex:Employee")
        ))

        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isConsistent()

        #expect(result.value == true)
    }

    @Test("Inconsistent class expression (intersection of disjoint classes)")
    func inconsistentDisjoint() {
        var ontology = OWLOntology(iri: "http://test.org/inconsistent")

        // Dog and Cat are disjoint
        ontology.axioms.append(.disjointClasses([
            .named("ex:Dog"),
            .named("ex:Cat")
        ]))

        let reasoner = OWLReasoner(ontology: ontology)

        // The intersection of disjoint classes is unsatisfiable
        let result = reasoner.isSatisfiable(.intersection([
            .named("ex:Dog"),
            .named("ex:Cat")
        ]))

        #expect(result.value == false)
    }
}

// MARK: - OWLReasoner Satisfiability Tests

@Suite("OWLReasoner Satisfiability", .serialized)
struct OWLReasonerSatisfiabilityTests {

    private func basicOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/basic")

        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.classes.append(OWLClass(iri: "ex:Adult"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasChild"))

        return ontology
    }

    @Test("Satisfiable class")
    func satisfiableClass() {
        let reasoner = OWLReasoner(ontology: basicOntology())
        let result = reasoner.isSatisfiable(.named("ex:Person"))

        #expect(result.value == true)
    }

    @Test("Unsatisfiable class (complement intersection)")
    func unsatisfiableClass() {
        let reasoner = OWLReasoner(ontology: basicOntology())
        let result = reasoner.isSatisfiable(.intersection([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))

        #expect(result.value == false)
    }

    @Test("Caching works for repeated queries")
    func cachingWorks() {
        let config = OWLReasoner.Configuration(cacheClassification: true)
        let reasoner = OWLReasoner(ontology: basicOntology(), configuration: config)

        // First query
        let result1 = reasoner.isSatisfiable(.named("ex:Person"))
        let stats1 = result1.statistics

        // Second query (should hit cache)
        let result2 = reasoner.isSatisfiable(.named("ex:Person"))
        let stats2 = result2.statistics

        #expect(result1.value == result2.value)
        #expect(stats2.cacheHits > stats1.cacheHits)
    }
}

// MARK: - OWLReasoner Subsumption Tests

@Suite("OWLReasoner Subsumption", .serialized)
struct OWLReasonerSubsumptionTests {

    private func hierarchyOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/hierarchy")

        // Hierarchy: Dog ⊑ Mammal ⊑ Animal
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Mammal")
        ))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Mammal"),
            sup: .named("ex:Animal")
        ))

        return ontology
    }

    @Test("Direct subsumption holds")
    func directSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Mammal"),
            subClass: .named("ex:Dog")
        )

        #expect(result.value == true)
    }

    @Test("Transitive subsumption holds")
    func transitiveSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Animal"),
            subClass: .named("ex:Dog")
        )

        #expect(result.value == true)
    }

    @Test("Inverse subsumption fails")
    func inverseSubsumptionFails() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Dog"),
            subClass: .named("ex:Animal")
        )

        #expect(result.value == false)
    }

    @Test("Self subsumption always holds")
    func selfSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        let result = reasoner.subsumes(
            superClass: .named("ex:Dog"),
            subClass: .named("ex:Dog")
        )

        #expect(result.value == true)
    }
}

// MARK: - OWLReasoner Instance Tests

@Suite("OWLReasoner Instance Queries", .serialized)
struct OWLReasonerInstanceTests {

    private func instanceOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/instances")

        // Classes
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.classes.append(OWLClass(iri: "ex:Employee"))

        // Employee ⊑ Person
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Employee"),
            sup: .named("ex:Person")
        ))

        // Individuals
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:john"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:mary"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))

        // John is an Employee
        ontology.axioms.append(.classAssertion(
            individual: "ex:john",
            class_: .named("ex:Employee")
        ))

        // Mary is a Person
        ontology.axioms.append(.classAssertion(
            individual: "ex:mary",
            class_: .named("ex:Person")
        ))

        return ontology
    }

    @Test("Direct instances")
    func directInstances() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let employees = reasoner.instances(of: .named("ex:Employee"))

        #expect(employees.contains("ex:john"))
        #expect(!employees.contains("ex:mary"))
    }

    @Test("Inferred instances through subsumption")
    func inferredInstances() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let persons = reasoner.instances(of: .named("ex:Person"))

        // John is an Employee, which is subclass of Person
        #expect(persons.contains("ex:john"))
        // Mary is directly a Person
        #expect(persons.contains("ex:mary"))
    }

    @Test("Types of individual")
    func typesOfIndividual() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let types = reasoner.types(of: "ex:john")

        #expect(types.contains("ex:Employee"))
        // Should also infer Person through subsumption
        #expect(types.contains("ex:Person"))
    }
}

// MARK: - OWLReasoner Classification Tests

@Suite("OWLReasoner Classification", .serialized)
struct OWLReasonerClassificationTests {

    private func classificationOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/classification")

        // Classes
        ontology.classes.append(OWLClass(iri: "ex:Animal"))
        ontology.classes.append(OWLClass(iri: "ex:Mammal"))
        ontology.classes.append(OWLClass(iri: "ex:Dog"))
        ontology.classes.append(OWLClass(iri: "ex:Cat"))

        // Hierarchy
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Mammal"),
            sup: .named("ex:Animal")
        ))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Mammal")
        ))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Cat"),
            sup: .named("ex:Mammal")
        ))

        return ontology
    }

    @Test("Classification computes hierarchy")
    func classificationComputesHierarchy() {
        let reasoner = OWLReasoner(ontology: classificationOntology())
        var hierarchy = reasoner.classify()

        // Check superclasses
        let dogSupers = hierarchy.superClasses(of: "ex:Dog")
        #expect(dogSupers.contains("ex:Mammal"))
        #expect(dogSupers.contains("ex:Animal"))

        // Check subclasses
        let mammalSubs = hierarchy.subClasses(of: "ex:Mammal")
        #expect(mammalSubs.contains("ex:Dog"))
        #expect(mammalSubs.contains("ex:Cat"))
    }
}

// MARK: - OWLReasoner Property Tests

@Suite("OWLReasoner Property Reasoning", .serialized)
struct OWLReasonerPropertyTests {

    private func propertyOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/properties")

        // Transitive property
        var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
        ancestorOf.characteristics.insert(.transitive)
        ontology.objectProperties.append(ancestorOf)

        // Individuals
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:carol"))

        // alice ancestorOf bob, bob ancestorOf carol
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:alice",
            property: "ex:ancestorOf",
            object: "ex:bob"
        ))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:bob",
            property: "ex:ancestorOf",
            object: "ex:carol"
        ))

        return ontology
    }

    @Test("Transitive reachability")
    func transitiveReachability() {
        let reasoner = OWLReasoner(ontology: propertyOntology())
        let reachable = reasoner.reachableIndividuals(
            from: "ex:alice",
            via: "ex:ancestorOf",
            includeInferred: true
        )

        #expect(reachable.contains("ex:bob"))
        #expect(reachable.contains("ex:carol"))  // Through transitivity
    }
}

// MARK: - OWLReasoner Statistics Tests

@Suite("OWLReasoner Statistics", .serialized)
struct OWLReasonerStatisticsTests {

    @Test("Statistics are tracked")
    func statisticsTracked() {
        let ontology = OWLOntology(iri: "http://test.org/stats")
        let reasoner = OWLReasoner(ontology: ontology)

        // Perform some queries
        _ = reasoner.isConsistent()
        _ = reasoner.isSatisfiable(.named("ex:Person"))
        _ = reasoner.isSatisfiable(.named("ex:Employee"))

        let stats = reasoner.statistics
        #expect(stats.satisfiabilityChecks >= 2)
    }

    @Test("Clear caches")
    func clearCaches() {
        let ontology = OWLOntology(iri: "http://test.org/reset")
        let reasoner = OWLReasoner(ontology: ontology)

        // Perform queries
        _ = reasoner.isSatisfiable(.named("ex:Person"))

        // Clear caches
        reasoner.clearCaches()

        let stats = reasoner.statistics
        #expect(stats.cacheHits == 0)
    }
}
