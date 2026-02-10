// OWLReasonerTests.swift
// Comprehensive tests for high-level OWLReasoner API

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - Initialization

@Suite("OWLReasoner Initialization", .serialized)
struct OWLReasonerInitTests {

    @Test("Create reasoner with empty ontology")
    func createWithEmptyOntology() {
        let ontology = OWLOntology(iri: "http://test.org/empty")
        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isConsistent().value == true)
        #expect(!reasoner.isClassified)
    }

    @Test("Create reasoner with configuration")
    func createWithConfiguration() {
        let ontology = OWLOntology(iri: "http://test.org/config")
        let config = OWLReasoner.Configuration(
            maxExpansionSteps: 500,
            enableIncrementalReasoning: false,
            cacheClassification: true,
            timeout: 30.0
        )
        let reasoner = OWLReasoner(ontology: ontology, configuration: config)
        #expect(reasoner.isConsistent().value == true)
    }
}

// MARK: - Consistency

@Suite("OWLReasoner Consistency", .serialized)
struct OWLReasonerConsistencyTests {

    @Test("Consistent ontology with hierarchy")
    func consistentWithHierarchy() {
        var ontology = OWLOntology(iri: "http://test.org/consistent")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.classes.append(OWLClass(iri: "ex:Employee"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Employee"), sup: .named("ex:Person")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:john"))
        ontology.axioms.append(.classAssertion(individual: "ex:john", class_: .named("ex:Employee")))

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isConsistent().value == true)
    }

    @Test("Intersection of disjoint classes is unsatisfiable")
    func disjointIntersectionUnsatisfiable() {
        var ontology = OWLOntology(iri: "http://test.org/disjoint")
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isSatisfiable(.intersection([.named("ex:Dog"), .named("ex:Cat")]))
        #expect(result.value == false)
    }
}

// MARK: - Satisfiability

@Suite("OWLReasoner Satisfiability", .serialized)
struct OWLReasonerSatisfiabilityTests {

    @Test("Named class is satisfiable")
    func namedClassSatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/sat")
        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isSatisfiable(.named("ex:Person")).value == true)
    }

    @Test("Complement intersection is unsatisfiable")
    func complementIntersectionUnsatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/unsat")
        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isSatisfiable(.intersection([
            .named("ex:Person"),
            .complement(.named("ex:Person"))
        ]))
        #expect(result.value == false)
    }

    @Test("Union with satisfiable disjunct is satisfiable")
    func unionSatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/union")
        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isSatisfiable(.union([.nothing, .named("ex:Person")]))
        #expect(result.value == true)
    }

    @Test("Existential with satisfiable filler")
    func existentialSatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/exist")
        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isSatisfiable(.someValuesFrom(property: "ex:hasChild", filler: .named("ex:Person")))
        #expect(result.value == true)
    }

    @Test("Existential with unsatisfiable filler")
    func existentialUnsatisfiable() {
        let ontology = OWLOntology(iri: "http://test.org/exist-unsat")
        let reasoner = OWLReasoner(ontology: ontology)
        let result = reasoner.isSatisfiable(.someValuesFrom(property: "ex:hasChild", filler: .nothing))
        #expect(result.value == false)
    }

    @Test("Caching works for repeated queries")
    func cachingWorks() {
        let config = OWLReasoner.Configuration(cacheClassification: true)
        let reasoner = OWLReasoner(ontology: OWLOntology(iri: "http://test.org/cache"), configuration: config)

        let r1 = reasoner.isSatisfiable(.named("ex:Person"))
        let s1 = r1.statistics
        let r2 = reasoner.isSatisfiable(.named("ex:Person"))
        let s2 = r2.statistics

        #expect(r1.value == r2.value)
        #expect(s2.cacheHits > s1.cacheHits)
    }

    @Test("Canonicalized cache key ensures hit for equivalent expressions")
    func canonicalizedCacheKey() {
        let config = OWLReasoner.Configuration(cacheClassification: true)
        let reasoner = OWLReasoner(ontology: OWLOntology(iri: "http://test.org/canon"), configuration: config)

        // intersection([A, B]) and intersection([B, A]) should share cache
        _ = reasoner.isSatisfiable(.intersection([.named("ex:A"), .named("ex:B")]))
        let r2 = reasoner.isSatisfiable(.intersection([.named("ex:B"), .named("ex:A")]))
        #expect(r2.statistics.cacheHits >= 1)
    }
}

// MARK: - Subsumption

@Suite("OWLReasoner Subsumption", .serialized)
struct OWLReasonerSubsumptionTests {

    private func hierarchyOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/hierarchy")
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        return ontology
    }

    @Test("Direct subsumption")
    func directSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Mammal"), subClass: .named("ex:Dog")).value == true)
    }

    @Test("Transitive subsumption")
    func transitiveSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Animal"), subClass: .named("ex:Dog")).value == true)
    }

    @Test("Inverse subsumption fails")
    func inverseSubsumptionFails() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Dog"), subClass: .named("ex:Animal")).value == false)
    }

    @Test("Self subsumption")
    func selfSubsumption() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Dog"), subClass: .named("ex:Dog")).value == true)
    }

    @Test("Thing subsumes everything")
    func thingSubsumesAll() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .thing, subClass: .named("ex:Dog")).value == true)
    }

    @Test("Nothing is subsumed by everything")
    func nothingSubsumedByAll() {
        let reasoner = OWLReasoner(ontology: hierarchyOntology())
        #expect(reasoner.subsumes(superClass: .named("ex:Dog"), subClass: .nothing).value == true)
    }
}

// MARK: - Equivalence & Disjointness

@Suite("OWLReasoner Equivalence and Disjointness", .serialized)
struct OWLReasonerEquivDisjointTests {

    @Test("Equivalent classes via axiom")
    func equivalentClasses() {
        var ontology = OWLOntology(iri: "http://test.org/equiv")
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.areEquivalent(.named("ex:Human"), .named("ex:Person")).value == true)
    }

    @Test("Non-equivalent classes")
    func nonEquivalent() {
        let ontology = OWLOntology(iri: "http://test.org/noneq")
        let reasoner = OWLReasoner(ontology: ontology)
        // Unrelated classes are not equivalent (they could have disjoint extensions)
        // subsumes neither way → not equivalent
        #expect(reasoner.areEquivalent(.named("ex:Dog"), .named("ex:Cat")).value == false)
    }

    @Test("Disjoint classes")
    func disjointClasses() {
        var ontology = OWLOntology(iri: "http://test.org/disj")
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.areDisjoint(.named("ex:Dog"), .named("ex:Cat")).value == true)
    }

    @Test("Non-disjoint classes")
    func nonDisjoint() {
        let ontology = OWLOntology(iri: "http://test.org/nondisj")
        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.areDisjoint(.named("ex:Dog"), .named("ex:Cat")).value == false)
    }
}

// MARK: - Instance Reasoning

@Suite("OWLReasoner Instance Queries", .serialized)
struct OWLReasonerInstanceTests {

    private func instanceOntology() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/instances")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.classes.append(OWLClass(iri: "ex:Employee"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Employee"), sup: .named("ex:Person")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:john"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:mary"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.axioms.append(.classAssertion(individual: "ex:john", class_: .named("ex:Employee")))
        ontology.axioms.append(.classAssertion(individual: "ex:mary", class_: .named("ex:Person")))
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
        #expect(persons.contains("ex:john"))
        #expect(persons.contains("ex:mary"))
    }

    @Test("isInstanceOf check")
    func isInstanceOfCheck() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        #expect(reasoner.isInstanceOf(individual: "ex:john", classExpr: .named("ex:Employee")).value == true)
        #expect(reasoner.isInstanceOf(individual: "ex:john", classExpr: .named("ex:Person")).value == true)
        #expect(reasoner.isInstanceOf(individual: "ex:mary", classExpr: .named("ex:Employee")).value == false)
    }

    @Test("Types of individual include hierarchy")
    func typesOfIndividual() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let types = reasoner.types(of: "ex:john")
        #expect(types.contains("ex:Employee"))
        #expect(types.contains("ex:Person"))
        #expect(types.contains("owl:Thing"))
    }

    @Test("Types of unasserted individual is only owl:Thing")
    func typesOfUnassertedIndividual() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let types = reasoner.types(of: "ex:bob")
        #expect(types == Set(["owl:Thing"]))
    }

    @Test("Types of unknown individual is only owl:Thing")
    func typesOfUnknownIndividual() {
        let reasoner = OWLReasoner(ontology: instanceOntology())
        let types = reasoner.types(of: "ex:nonexistent")
        #expect(types == Set(["owl:Thing"]))
    }
}

// MARK: - Defined Class (equivalentClasses) Instance Reasoning

@Suite("OWLReasoner Defined Class Reasoning", .serialized)
struct OWLReasonerDefinedClassTests {

    @Test("Individual classified via defined class")
    func definedClassClassification() {
        var ontology = OWLOntology(iri: "http://test.org/defined")
        ontology.classes.append(OWLClass(iri: "ex:Corporation"))
        ontology.classes.append(OWLClass(iri: "ex:GlobalCorp"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasScale"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Global"))

        // GlobalCorp ≡ Corporation ⊓ hasScale value Global
        ontology.axioms.append(.equivalentClasses([
            .named("ex:GlobalCorp"),
            .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ]))
        ontology.axioms.append(.subClassOf(sub: .named("ex:GlobalCorp"), sup: .named("ex:Corporation")))

        // Toyota matches the definition
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Toyota"))
        ontology.axioms.append(.classAssertion(individual: "ex:Toyota", class_: .named("ex:Corporation")))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Toyota", property: "ex:hasScale", object: "ex:Global"))

        // LocalShop does NOT match
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:LocalShop"))
        ontology.axioms.append(.classAssertion(individual: "ex:LocalShop", class_: .named("ex:Corporation")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Local"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:LocalShop", property: "ex:hasScale", object: "ex:Local"))

        let reasoner = OWLReasoner(ontology: ontology)

        let toyotaTypes = reasoner.types(of: "ex:Toyota")
        #expect(toyotaTypes.contains("ex:GlobalCorp"))
        #expect(toyotaTypes.contains("ex:Corporation"))
        #expect(toyotaTypes.contains("owl:Thing"))

        let shopTypes = reasoner.types(of: "ex:LocalShop")
        #expect(!shopTypes.contains("ex:GlobalCorp"))
        #expect(shopTypes.contains("ex:Corporation"))
    }

    @Test("Defined class superclasses are expanded")
    func definedClassSuperClassExpansion() {
        var ontology = OWLOntology(iri: "http://test.org/defined-super")
        for cls in ["ex:Organization", "ex:Corporation", "ex:GlobalCorp"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Corporation"), sup: .named("ex:Organization")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:GlobalCorp"), sup: .named("ex:Corporation")))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasScale"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Global"))

        ontology.axioms.append(.equivalentClasses([
            .named("ex:GlobalCorp"),
            .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ]))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Acme"))
        ontology.axioms.append(.classAssertion(individual: "ex:Acme", class_: .named("ex:Corporation")))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Acme", property: "ex:hasScale", object: "ex:Global"))

        let reasoner = OWLReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:Acme")

        #expect(types.contains("ex:GlobalCorp"))
        #expect(types.contains("ex:Corporation"))
        #expect(types.contains("ex:Organization"))
        #expect(types.contains("owl:Thing"))
    }
}

// MARK: - Classification

@Suite("OWLReasoner Classification", .serialized)
struct OWLReasonerClassificationTests {

    @Test("Classification computes hierarchy")
    func classificationComputesHierarchy() {
        var ontology = OWLOntology(iri: "http://test.org/classification")
        ontology.classes.append(OWLClass(iri: "ex:Animal"))
        ontology.classes.append(OWLClass(iri: "ex:Mammal"))
        ontology.classes.append(OWLClass(iri: "ex:Dog"))
        ontology.classes.append(OWLClass(iri: "ex:Cat"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Cat"), sup: .named("ex:Mammal")))

        let reasoner = OWLReasoner(ontology: ontology)
        var hierarchy = reasoner.classify()

        #expect(reasoner.isClassified)
        #expect(hierarchy.superClasses(of: "ex:Dog").contains("ex:Mammal"))
        #expect(hierarchy.superClasses(of: "ex:Dog").contains("ex:Animal"))
        #expect(hierarchy.subClasses(of: "ex:Mammal").contains("ex:Dog"))
        #expect(hierarchy.subClasses(of: "ex:Mammal").contains("ex:Cat"))
    }

    @Test("superClasses and subClasses queries")
    func superSubClassQueries() {
        var ontology = OWLOntology(iri: "http://test.org/queries")
        ontology.classes.append(OWLClass(iri: "ex:A"))
        ontology.classes.append(OWLClass(iri: "ex:B"))
        ontology.classes.append(OWLClass(iri: "ex:C"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:C"), sup: .named("ex:B")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:B"), sup: .named("ex:A")))

        let reasoner = OWLReasoner(ontology: ontology)

        // Direct only
        let directSupers = reasoner.superClasses(of: "ex:C", direct: true)
        #expect(directSupers.contains("ex:B"))
        #expect(!directSupers.contains("ex:A"))

        // All supers
        let allSupers = reasoner.superClasses(of: "ex:C")
        #expect(allSupers.contains("ex:B"))
        #expect(allSupers.contains("ex:A"))

        // Subs
        let allSubs = reasoner.subClasses(of: "ex:A")
        #expect(allSubs.contains("ex:B"))
        #expect(allSubs.contains("ex:C"))
    }

    @Test("Equivalent classes query")
    func equivalentClassesQuery() {
        var ontology = OWLOntology(iri: "http://test.org/equiv-query")
        ontology.axioms.append(.equivalentClasses([.named("ex:Human"), .named("ex:Person")]))

        let reasoner = OWLReasoner(ontology: ontology)
        let equivs = reasoner.equivalentClasses(of: "ex:Human")
        #expect(equivs.contains("ex:Person"))
    }

    @Test("Disjoint classes query")
    func disjointClassesQuery() {
        var ontology = OWLOntology(iri: "http://test.org/disj-query")
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        let reasoner = OWLReasoner(ontology: ontology)
        let disjoints = reasoner.disjointClasses(of: "ex:Dog")
        #expect(disjoints.contains("ex:Cat"))
    }
}

// MARK: - Property Reasoning

@Suite("OWLReasoner Property Reasoning", .serialized)
struct OWLReasonerPropertyTests {

    @Test("Transitive property reachability")
    func transitiveReachability() {
        var ontology = OWLOntology(iri: "http://test.org/trans")
        var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
        ancestorOf.characteristics.insert(.transitive)
        ontology.objectProperties.append(ancestorOf)

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:carol"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:alice", property: "ex:ancestorOf", object: "ex:bob"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:bob", property: "ex:ancestorOf", object: "ex:carol"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:alice", via: "ex:ancestorOf", includeInferred: true)
        #expect(reachable.contains("ex:bob"))
        #expect(reachable.contains("ex:carol"))
    }

    @Test("Symmetric property reachability")
    func symmetricReachability() {
        var ontology = OWLOntology(iri: "http://test.org/sym")
        var friendOf = OWLObjectProperty(iri: "ex:friendOf")
        friendOf.characteristics.insert(.symmetric)
        ontology.objectProperties.append(friendOf)

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:alice", property: "ex:friendOf", object: "ex:bob"))

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isSymmetric("ex:friendOf"))

        let reachable = reasoner.reachableIndividuals(from: "ex:bob", via: "ex:friendOf", includeInferred: true)
        #expect(reachable.contains("ex:alice"))
    }

    @Test("Inverse property")
    func inverseProperty() {
        var ontology = OWLOntology(iri: "http://test.org/inv")
        var parentOf = OWLObjectProperty(iri: "ex:parentOf")
        parentOf.inverseOf = "ex:childOf"
        ontology.objectProperties.append(parentOf)
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:childOf"))
        ontology.axioms.append(.inverseObjectProperties(first: "ex:parentOf", second: "ex:childOf"))

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.inverseProperty(of: "ex:parentOf") == "ex:childOf")
    }

    @Test("Functional property check")
    func functionalProperty() {
        var ontology = OWLOntology(iri: "http://test.org/func")
        var hasMother = OWLObjectProperty(iri: "ex:hasMother")
        hasMother.characteristics.insert(.functional)
        ontology.objectProperties.append(hasMother)

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isFunctional("ex:hasMother"))
        #expect(!reasoner.isFunctional("ex:hasChild"))
    }

    @Test("Transitive property check")
    func transitiveProperty() {
        var ontology = OWLOntology(iri: "http://test.org/trans-check")
        var ancestorOf = OWLObjectProperty(iri: "ex:ancestorOf")
        ancestorOf.characteristics.insert(.transitive)
        ontology.objectProperties.append(ancestorOf)

        let reasoner = OWLReasoner(ontology: ontology)
        #expect(reasoner.isTransitive("ex:ancestorOf"))
        #expect(!reasoner.isTransitive("ex:hasChild"))
    }

    @Test("Sub-property hierarchy")
    func subPropertyHierarchy() {
        var ontology = OWLOntology(iri: "http://test.org/subprop")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasMother"))
        ontology.axioms.append(.subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"))

        let reasoner = OWLReasoner(ontology: ontology)
        let superProps = reasoner.superProperties(of: "ex:hasMother")
        #expect(superProps.contains("ex:hasParent"))

        let subProps = reasoner.subProperties(of: "ex:hasParent")
        #expect(subProps.contains("ex:hasMother"))
    }

    @Test("Sub-property reachability")
    func subPropertyReachability() {
        var ontology = OWLOntology(iri: "http://test.org/subprop-reach")
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasParent"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasMother"))
        ontology.axioms.append(.subObjectPropertyOf(sub: "ex:hasMother", sup: "ex:hasParent"))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:eve"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:alice", property: "ex:hasMother", object: "ex:eve"))

        let reasoner = OWLReasoner(ontology: ontology)
        let reachable = reasoner.reachableIndividuals(from: "ex:alice", via: "ex:hasParent", includeInferred: true)
        #expect(reachable.contains("ex:eve"))
    }
}

// MARK: - OWL DL Validation

@Suite("OWLReasoner Validation", .serialized)
struct OWLReasonerValidationTests {

    @Test("Valid ontology passes validation")
    func validOntology() {
        var ontology = OWLOntology(iri: "http://test.org/valid")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Person"), sup: .thing))

        let reasoner = OWLReasoner(ontology: ontology)
        let (isValid, _) = reasoner.validateOWLDL()
        #expect(isValid)
    }

    @Test("Transitive role in cardinality violates OWL DL")
    func transitiveInCardinality() {
        var ontology = OWLOntology(iri: "http://test.org/irregular")
        ontology.axioms.append(.transitiveObjectProperty("ex:hasAncestor"))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:Person"),
            sup: .maxCardinality(property: "ex:hasAncestor", n: 10, filler: nil)
        ))

        let reasoner = OWLReasoner(ontology: ontology)
        let (isValid, violations) = reasoner.validateOWLDL()
        #expect(!isValid)
        #expect(!violations.isEmpty)
    }
}

// MARK: - Statistics

@Suite("OWLReasoner Statistics", .serialized)
struct OWLReasonerStatisticsTests {

    @Test("Statistics are tracked")
    func statisticsTracked() {
        let reasoner = OWLReasoner(ontology: OWLOntology(iri: "http://test.org/stats"))
        _ = reasoner.isConsistent()
        _ = reasoner.isSatisfiable(.named("ex:Person"))
        _ = reasoner.isSatisfiable(.named("ex:Employee"))

        let stats = reasoner.statistics
        #expect(stats.satisfiabilityChecks >= 2)
    }

    @Test("Clear caches resets hit count")
    func clearCachesResetsHitCount() {
        let reasoner = OWLReasoner(ontology: OWLOntology(iri: "http://test.org/reset"))
        _ = reasoner.isSatisfiable(.named("ex:Person"))
        _ = reasoner.isSatisfiable(.named("ex:Person")) // cache hit

        reasoner.clearCaches()
        let stats = reasoner.statistics
        #expect(stats.cacheHits == 0)
    }

    @Test("Subsumption checks are counted")
    func subsumptionChecksCounted() {
        let reasoner = OWLReasoner(ontology: OWLOntology(iri: "http://test.org/sub-stats"))
        _ = reasoner.subsumes(superClass: .thing, subClass: .named("ex:A"))
        _ = reasoner.subsumes(superClass: .named("ex:A"), subClass: .named("ex:B"))

        #expect(reasoner.statistics.subsumptionChecks >= 2)
    }
}
