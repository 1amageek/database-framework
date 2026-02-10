// OWLReasonerBenchmarkTests.swift
// Performance and correctness tests for OWLReasoner optimizations

import Testing
import Foundation
import Graph
@testable import GraphIndex

// MARK: - Ontology Builders

/// Builds ontologies of various sizes for benchmarking
private enum OntologyBuilder {

    /// Balanced tree hierarchy
    /// depth=3, branch=4 → 1 + 4 + 16 + 64 = 85 classes
    static func treeHierarchy(depth: Int, branchingFactor: Int) -> OWLOntology {
        var ontology = OWLOntology(iri: "http://bench.org/tree")
        ontology.classes.append(OWLClass(iri: "ex:Root"))

        func buildLevel(parent: String, currentDepth: Int) {
            guard currentDepth < depth else { return }
            for i in 0..<branchingFactor {
                let child = "\(parent)_\(i)"
                ontology.classes.append(OWLClass(iri: child))
                ontology.axioms.append(.subClassOf(sub: .named(child), sup: .named(parent)))
                buildLevel(parent: child, currentDepth: currentDepth + 1)
            }
        }
        buildLevel(parent: "ex:Root", currentDepth: 0)
        return ontology
    }

    /// Diamond hierarchy: multiple inheritance paths
    ///
    /// ```
    ///        Thing
    ///       /     \
    ///    LivingThing  PhysicalObject
    ///       |    \   /    |
    ///    Animal  Plant  Artifact
    ///      / \          |
    ///   Dog  Cat      Robot
    ///      \  /
    ///     CatDog  (diamond join)
    /// ```
    static func diamondHierarchy() -> OWLOntology {
        var ontology = OWLOntology(iri: "http://bench.org/diamond")
        let classes = [
            "ex:LivingThing", "ex:PhysicalObject",
            "ex:Animal", "ex:Plant", "ex:Artifact",
            "ex:Dog", "ex:Cat", "ex:Robot", "ex:CatDog"
        ]
        for cls in classes {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:LivingThing"), sup: .thing))
        ontology.axioms.append(.subClassOf(sub: .named("ex:PhysicalObject"), sup: .thing))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Animal"), sup: .named("ex:LivingThing")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Plant"), sup: .named("ex:LivingThing")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Plant"), sup: .named("ex:PhysicalObject")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Artifact"), sup: .named("ex:PhysicalObject")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Cat"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Robot"), sup: .named("ex:Artifact")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:CatDog"), sup: .named("ex:Dog")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:CatDog"), sup: .named("ex:Cat")))
        return ontology
    }

    /// Ontology with defined classes, object properties, and disjointness
    static func richOntology(classCount: Int, individualCount: Int) -> OWLOntology {
        var ontology = OWLOntology(iri: "http://bench.org/rich")

        // Base hierarchy
        ontology.classes.append(OWLClass(iri: "ex:Entity"))
        ontology.classes.append(OWLClass(iri: "ex:Agent"))
        ontology.classes.append(OWLClass(iri: "ex:Place"))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Agent"), sup: .named("ex:Entity")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Place"), sup: .named("ex:Entity")))
        ontology.axioms.append(.disjointClasses([.named("ex:Agent"), .named("ex:Place")]))

        // Properties
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:locatedIn"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:worksAt"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasRole"))

        // Generated classes under Agent
        for i in 0..<classCount {
            let cls = "ex:AgentType\(i)"
            ontology.classes.append(OWLClass(iri: cls))
            ontology.axioms.append(.subClassOf(sub: .named(cls), sup: .named("ex:Agent")))

            // Every other class gets an equivalence definition
            if i % 2 == 0 {
                let roleInd = "ex:role\(i)"
                ontology.individuals.append(OWLNamedIndividual(iri: roleInd))
                ontology.axioms.append(.equivalentClasses([
                    .named(cls),
                    .intersection([
                        .named("ex:Agent"),
                        .hasValue(property: "ex:hasRole", individual: roleInd)
                    ])
                ]))
            }
        }

        // Individuals with assertions
        for i in 0..<individualCount {
            let ind = "ex:ind\(i)"
            ontology.individuals.append(OWLNamedIndividual(iri: ind))

            let classIndex = i % classCount
            ontology.axioms.append(.classAssertion(
                individual: ind,
                class_: .named("ex:AgentType\(classIndex)")
            ))

            // Object property assertions for defined class matching
            if classIndex % 2 == 0 {
                ontology.axioms.append(.objectPropertyAssertion(
                    subject: ind,
                    property: "ex:hasRole",
                    object: "ex:role\(classIndex)"
                ))
            }

            // Location assertions
            let placeInd = "ex:place\(i % 3)"
            if i < 3 {
                ontology.individuals.append(OWLNamedIndividual(iri: placeInd))
                ontology.axioms.append(.classAssertion(individual: placeInd, class_: .named("ex:Place")))
            }
            ontology.axioms.append(.objectPropertyAssertion(
                subject: ind,
                property: "ex:locatedIn",
                object: placeInd
            ))
        }

        return ontology
    }
}

// MARK: - Performance Tests

@Suite("OWLReasoner Benchmark", .serialized)
struct OWLReasonerBenchmarkTests {

    @Test("types(of:) with 85-class tree hierarchy", .timeLimit(.minutes(1)))
    func benchmarkTypesOfTreeHierarchy() {
        var ontology = OntologyBuilder.treeHierarchy(depth: 3, branchingFactor: 4)

        // Add 20 individuals at leaf level (depth=3, e.g. ex:Root_0_0_0)
        let leaves = ontology.classes.map(\.iri).filter { iri in
            // Leaf nodes at depth 3 have 3 underscore-separated segments after "ex:Root"
            let suffix = iri.dropFirst("ex:Root".count)
            return suffix.split(separator: "_").count == 3
        }
        #expect(leaves.count == 64, "Should have 64 leaf classes")
        for i in 0..<20 {
            let ind = "ex:ind\(i)"
            ontology.individuals.append(OWLNamedIndividual(iri: ind))
            ontology.axioms.append(.classAssertion(
                individual: ind,
                class_: .named(leaves[i % leaves.count])
            ))
        }

        let reasoner = OWLReasoner(ontology: ontology)

        let start = ContinuousClock.now
        for individual in ontology.individuals {
            let types = reasoner.types(of: individual.iri)
            #expect(types.contains("owl:Thing"))
            #expect(types.contains("ex:Root"))
            // leaf + 2 intermediate ancestors + Root + owl:Thing = 5
            #expect(types.count >= 5, "Leaf individual should have at least 5 types, got \(types.count): \(types)")
        }
        let elapsed = ContinuousClock.now - start

        #expect(elapsed < .seconds(5), "types(of:) on 85-class tree too slow: \(elapsed)")
    }

    @Test("types(of:) with diamond hierarchy (multiple inheritance)", .timeLimit(.minutes(1)))
    func benchmarkDiamondHierarchy() {
        var ontology = OntologyBuilder.diamondHierarchy()

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:buddy"))
        ontology.axioms.append(.classAssertion(individual: "ex:buddy", class_: .named("ex:CatDog")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:fido"))
        ontology.axioms.append(.classAssertion(individual: "ex:fido", class_: .named("ex:Dog")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bonsai"))
        ontology.axioms.append(.classAssertion(individual: "ex:bonsai", class_: .named("ex:Plant")))

        let reasoner = OWLReasoner(ontology: ontology)

        let buddyTypes = reasoner.types(of: "ex:buddy")
        // CatDog ⊑ Dog, Cat ⊑ Animal ⊑ LivingThing ⊑ Thing
        #expect(buddyTypes.contains("ex:CatDog"))
        #expect(buddyTypes.contains("ex:Dog"))
        #expect(buddyTypes.contains("ex:Cat"))
        #expect(buddyTypes.contains("ex:Animal"))
        #expect(buddyTypes.contains("ex:LivingThing"))
        #expect(buddyTypes.contains("owl:Thing"))

        let bonsaiTypes = reasoner.types(of: "ex:bonsai")
        // Plant ⊑ LivingThing, PhysicalObject ⊑ Thing
        #expect(bonsaiTypes.contains("ex:Plant"))
        #expect(bonsaiTypes.contains("ex:LivingThing"))
        #expect(bonsaiTypes.contains("ex:PhysicalObject"))
        #expect(!bonsaiTypes.contains("ex:Animal"))
    }

    @Test("Defined class classification with 10 classes and 20 individuals", .timeLimit(.minutes(1)))
    func benchmarkDefinedClassClassification() {
        let ontology = OntologyBuilder.richOntology(classCount: 10, individualCount: 20)
        let reasoner = OWLReasoner(ontology: ontology)

        let start = ContinuousClock.now
        for individual in ontology.individuals {
            let iri = individual.iri
            guard iri.hasPrefix("ex:ind") else { continue }
            let types = reasoner.types(of: iri)
            #expect(types.contains("owl:Thing"))
            #expect(types.contains("ex:Entity"))
            #expect(types.contains("ex:Agent"))
        }
        let elapsed = ContinuousClock.now - start

        #expect(elapsed < .seconds(10), "Rich ontology types(of:) too slow: \(elapsed)")
    }

    @Test("Cache effectiveness on repeated queries", .timeLimit(.minutes(1)))
    func benchmarkCacheEffectiveness() {
        let ontology = OntologyBuilder.richOntology(classCount: 6, individualCount: 10)
        let reasoner = OWLReasoner(ontology: ontology)

        let inds = ontology.individuals.map(\.iri).filter { $0.hasPrefix("ex:ind") }

        // Cold pass
        let start1 = ContinuousClock.now
        for iri in inds { _ = reasoner.types(of: iri) }
        let cold = ContinuousClock.now - start1

        // Warm pass
        let start2 = ContinuousClock.now
        for iri in inds { _ = reasoner.types(of: iri) }
        let warm = ContinuousClock.now - start2

        #expect(warm <= cold, "Cached pass should not be slower (cold: \(cold), warm: \(warm))")
    }

    @Test("Subsumption check on deep hierarchy (depth=5)", .timeLimit(.minutes(1)))
    func benchmarkSubsumptionDeepHierarchy() {
        var ontology = OWLOntology(iri: "http://bench.org/deep")
        var prev = "ex:Level0"
        ontology.classes.append(OWLClass(iri: prev))
        for i in 1...5 {
            let cls = "ex:Level\(i)"
            ontology.classes.append(OWLClass(iri: cls))
            ontology.axioms.append(.subClassOf(sub: .named(cls), sup: .named(prev)))
            prev = cls
        }

        let reasoner = OWLReasoner(ontology: ontology)

        let start = ContinuousClock.now
        // Check all pairs
        for i in 0...5 {
            for j in 0...5 {
                let subExpr: OWLClassExpression = .named("ex:Level\(j)")
                let supExpr: OWLClassExpression = .named("ex:Level\(i)")
                let result = reasoner.subsumes(superClass: supExpr, subClass: subExpr)
                if j >= i {
                    #expect(result.value, "Level\(j) ⊑ Level\(i) should hold")
                }
            }
        }
        let elapsed = ContinuousClock.now - start
        #expect(elapsed < .seconds(5), "Subsumption on depth-5 chain too slow: \(elapsed)")
    }

    @Test("instances(of:) correctness with hierarchy", .timeLimit(.minutes(1)))
    func benchmarkInstancesOf() {
        var ontology = OWLOntology(iri: "http://bench.org/instances")
        for cls in ["ex:Animal", "ex:Mammal", "ex:Dog", "ex:Cat", "ex:Bird"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Cat"), sup: .named("ex:Mammal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Bird"), sup: .named("ex:Animal")))
        ontology.axioms.append(.disjointClasses([.named("ex:Dog"), .named("ex:Cat")]))

        // 5 dogs, 3 cats, 2 birds = 10 animals, 8 mammals
        for i in 0..<5 {
            let iri = "ex:dog\(i)"
            ontology.individuals.append(OWLNamedIndividual(iri: iri))
            ontology.axioms.append(.classAssertion(individual: iri, class_: .named("ex:Dog")))
        }
        for i in 0..<3 {
            let iri = "ex:cat\(i)"
            ontology.individuals.append(OWLNamedIndividual(iri: iri))
            ontology.axioms.append(.classAssertion(individual: iri, class_: .named("ex:Cat")))
        }
        for i in 0..<2 {
            let iri = "ex:bird\(i)"
            ontology.individuals.append(OWLNamedIndividual(iri: iri))
            ontology.axioms.append(.classAssertion(individual: iri, class_: .named("ex:Bird")))
        }

        let reasoner = OWLReasoner(ontology: ontology)

        let dogs = reasoner.instances(of: .named("ex:Dog"))
        #expect(dogs.count == 5)

        let cats = reasoner.instances(of: .named("ex:Cat"))
        #expect(cats.count == 3)

        let mammals = reasoner.instances(of: .named("ex:Mammal"))
        #expect(mammals.count == 8, "8 mammals = 5 dogs + 3 cats")

        let animals = reasoner.instances(of: .named("ex:Animal"))
        #expect(animals.count == 10, "10 animals = 5 dogs + 3 cats + 2 birds")

        let birds = reasoner.instances(of: .named("ex:Bird"))
        #expect(birds.count == 2)
    }
}

// MARK: - Correctness Tests

@Suite("OWLReasoner types(of:) Correctness", .serialized)
struct OWLReasonerTypesCorrectnessTests {

    @Test("Defined Class classification is correct")
    func correctnessWithDefinedClasses() {
        var ontology = OWLOntology(iri: "http://test.org/defined-correctness")

        ontology.classes.append(OWLClass(iri: "ex:Corporation"))
        ontology.classes.append(OWLClass(iri: "ex:GlobalCorp"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasScale"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Global"))

        ontology.axioms.append(.equivalentClasses([
            .named("ex:GlobalCorp"),
            .intersection([
                .named("ex:Corporation"),
                .hasValue(property: "ex:hasScale", individual: "ex:Global")
            ])
        ]))
        ontology.axioms.append(.subClassOf(sub: .named("ex:GlobalCorp"), sup: .named("ex:Corporation")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Toyota"))
        ontology.axioms.append(.classAssertion(individual: "ex:Toyota", class_: .named("ex:Corporation")))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:Toyota", property: "ex:hasScale", object: "ex:Global"))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:LocalShop"))
        ontology.axioms.append(.classAssertion(individual: "ex:LocalShop", class_: .named("ex:Corporation")))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:Local"))
        ontology.axioms.append(.objectPropertyAssertion(subject: "ex:LocalShop", property: "ex:hasScale", object: "ex:Local"))

        let reasoner = OWLReasoner(ontology: ontology)

        let toyotaTypes = reasoner.types(of: "ex:Toyota")
        #expect(toyotaTypes.contains("ex:Corporation"))
        #expect(toyotaTypes.contains("ex:GlobalCorp"))
        #expect(toyotaTypes.contains("owl:Thing"))

        let shopTypes = reasoner.types(of: "ex:LocalShop")
        #expect(shopTypes.contains("ex:Corporation"))
        #expect(!shopTypes.contains("ex:GlobalCorp"))
    }

    @Test("subClassOf hierarchy is fully expanded")
    func correctnessSubClassHierarchy() {
        var ontology = OWLOntology(iri: "http://test.org/hierarchy")

        for cls in ["ex:LivingThing", "ex:Animal", "ex:Mammal", "ex:Dog"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Animal"), sup: .named("ex:LivingThing")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Mammal"), sup: .named("ex:Animal")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Mammal")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:rex"))
        ontology.axioms.append(.classAssertion(individual: "ex:rex", class_: .named("ex:Dog")))

        let reasoner = OWLReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:rex")

        #expect(types.contains("ex:Dog"))
        #expect(types.contains("ex:Mammal"))
        #expect(types.contains("ex:Animal"))
        #expect(types.contains("ex:LivingThing"))
        #expect(types.contains("owl:Thing"))
    }

    @Test("Optimized types(of:) matches naive TableauxReasoner")
    func correctnessMatchesNaive() {
        var ontology = OWLOntology(iri: "http://test.org/naive-compare")

        for cls in ["ex:Person", "ex:Employee", "ex:Manager"] {
            ontology.classes.append(OWLClass(iri: cls))
        }
        ontology.axioms.append(.subClassOf(sub: .named("ex:Employee"), sup: .named("ex:Person")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Manager"), sup: .named("ex:Employee")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.axioms.append(.classAssertion(individual: "ex:alice", class_: .named("ex:Manager")))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.axioms.append(.classAssertion(individual: "ex:bob", class_: .named("ex:Employee")))

        let optimized = OWLReasoner(ontology: ontology)
        let naive = TableauxReasoner(ontology: ontology)

        for individual in ontology.individuals {
            let iri = individual.iri
            let optimizedTypes = optimized.types(of: iri)
            var naiveTypes = naive.types(of: iri)
            naiveTypes.insert("owl:Thing")

            #expect(optimizedTypes == naiveTypes, "Mismatch for \(iri)")
        }
    }

    @Test("Individual with no assertions returns only owl:Thing")
    func individualWithNoAssertions() {
        var ontology = OWLOntology(iri: "http://test.org/no-assertions")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:unknown"))

        let reasoner = OWLReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:unknown")

        #expect(types == Set(["owl:Thing"]))
    }

    @Test("Multiple defined classes with disjointness")
    func multipleDefinedClassesWithDisjointness() {
        var ontology = OWLOntology(iri: "http://test.org/multi-defined")

        ontology.classes.append(OWLClass(iri: "ex:Vehicle"))
        ontology.classes.append(OWLClass(iri: "ex:Car"))
        ontology.classes.append(OWLClass(iri: "ex:Truck"))
        ontology.objectProperties.append(OWLObjectProperty(iri: "ex:hasType"))

        ontology.individuals.append(OWLNamedIndividual(iri: "ex:sedan"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:pickup"))

        // Car ≡ Vehicle ⊓ hasValue(hasType, sedan)
        ontology.axioms.append(.equivalentClasses([
            .named("ex:Car"),
            .intersection([
                .named("ex:Vehicle"),
                .hasValue(property: "ex:hasType", individual: "ex:sedan")
            ])
        ]))
        // Truck ≡ Vehicle ⊓ hasValue(hasType, pickup)
        ontology.axioms.append(.equivalentClasses([
            .named("ex:Truck"),
            .intersection([
                .named("ex:Vehicle"),
                .hasValue(property: "ex:hasType", individual: "ex:pickup")
            ])
        ]))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Car"), sup: .named("ex:Vehicle")))
        ontology.axioms.append(.subClassOf(sub: .named("ex:Truck"), sup: .named("ex:Vehicle")))

        // mycar: Vehicle with hasType sedan
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:mycar"))
        ontology.axioms.append(.classAssertion(individual: "ex:mycar", class_: .named("ex:Vehicle")))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:mycar", property: "ex:hasType", object: "ex:sedan"
        ))

        // mytruck: Vehicle with hasType pickup
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:mytruck"))
        ontology.axioms.append(.classAssertion(individual: "ex:mytruck", class_: .named("ex:Vehicle")))
        ontology.axioms.append(.objectPropertyAssertion(
            subject: "ex:mytruck", property: "ex:hasType", object: "ex:pickup"
        ))

        let reasoner = OWLReasoner(ontology: ontology)

        let carTypes = reasoner.types(of: "ex:mycar")
        #expect(carTypes.contains("ex:Car"))
        #expect(carTypes.contains("ex:Vehicle"))
        #expect(!carTypes.contains("ex:Truck"))

        let truckTypes = reasoner.types(of: "ex:mytruck")
        #expect(truckTypes.contains("ex:Truck"))
        #expect(truckTypes.contains("ex:Vehicle"))
        #expect(!truckTypes.contains("ex:Car"))
    }

    @Test("Optimized vs naive on rich ontology")
    func optimizedMatchesNaiveRichOntology() {
        let ontology = OntologyBuilder.richOntology(classCount: 6, individualCount: 8)
        let optimized = OWLReasoner(ontology: ontology)
        let naive = TableauxReasoner(ontology: ontology)

        let inds = ontology.individuals.map(\.iri).filter { $0.hasPrefix("ex:ind") }
        for iri in inds {
            let optTypes = optimized.types(of: iri)
            var naiveTypes = naive.types(of: iri)
            naiveTypes.insert("owl:Thing")
            #expect(optTypes == naiveTypes, "Mismatch for \(iri): optimized=\(optTypes.sorted()), naive=\(naiveTypes.sorted())")
        }
    }
}

// MARK: - Tableaux Benchmark

@Suite("TableauxReasoner Benchmark", .serialized)
struct TableauxReasonerBenchmarkTests {

    @Test("Satisfiability with many disjunctions", .timeLimit(.minutes(1)))
    func benchmarkManyDisjunctions() {
        var ontology = OWLOntology(iri: "http://bench.org/disj")
        // 5 pairs of disjoint classes
        for i in 0..<5 {
            ontology.axioms.append(.disjointClasses([
                .named("ex:A\(i)"),
                .named("ex:B\(i)")
            ]))
        }

        let reasoner = TableauxReasoner(ontology: ontology)

        let start = ContinuousClock.now
        // Test union with many disjuncts requiring backtracking
        for i in 0..<5 {
            // (Ai ⊔ Bi) ⊓ ¬Ai → must backtrack to Bi
            let result = reasoner.checkSatisfiability(.intersection([
                .union([.named("ex:A\(i)"), .named("ex:B\(i)")]),
                .complement(.named("ex:A\(i)"))
            ]))
            #expect(result.isSatisfiable)
            #expect(result.statistics.backtrackCount > 0)
        }
        let elapsed = ContinuousClock.now - start
        #expect(elapsed < .seconds(5), "Disjunction backtracking too slow: \(elapsed)")
    }

    @Test("Nested existential-universal interaction", .timeLimit(.minutes(1)))
    func benchmarkNestedRestrictions() {
        var ontology = OWLOntology(iri: "http://bench.org/nested")
        ontology.axioms.append(.disjointClasses([.named("ex:Good"), .named("ex:Bad")]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // ∃R.(∃R.(∃R.Good ⊓ ∀R.Bad)) → 3-deep chain, clash at depth 3
        let result = reasoner.checkSatisfiability(
            .someValuesFrom(property: "ex:R", filler:
                .someValuesFrom(property: "ex:R", filler:
                    .intersection([
                        .someValuesFrom(property: "ex:R", filler: .named("ex:Good")),
                        .allValuesFrom(property: "ex:R", filler: .named("ex:Bad"))
                    ])
                )
            )
        )
        #expect(!result.isSatisfiable)
        #expect(result.statistics.nodesCreated >= 1)
    }

    @Test("Cardinality with multiple roles", .timeLimit(.minutes(1)))
    func benchmarkCardinalityMultipleRoles() {
        let ontology = OWLOntology(iri: "http://bench.org/card")
        let reasoner = TableauxReasoner(ontology: ontology)

        // ≥2 R1.⊤ ⊓ ≥2 R2.⊤ ⊓ ≤3 R1.⊤ ⊓ ≤3 R2.⊤
        // Satisfiable: 2-3 R1-successors and 2-3 R2-successors
        let result = reasoner.checkSatisfiability(.intersection([
            .minCardinality(property: "ex:R1", n: 2, filler: .thing),
            .minCardinality(property: "ex:R2", n: 2, filler: .thing),
            .maxCardinality(property: "ex:R1", n: 3, filler: .thing),
            .maxCardinality(property: "ex:R2", n: 3, filler: .thing)
        ]))
        #expect(result.isSatisfiable)
    }

    @Test("GCI chain propagation", .timeLimit(.minutes(1)))
    func benchmarkGCIChain() {
        var ontology = OWLOntology(iri: "http://bench.org/gci")
        // A ⊑ ∃R.B, B ⊑ ∃R.C, C ⊓ D = ⊥
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:A"),
            sup: .someValuesFrom(property: "ex:R", filler: .named("ex:B"))
        ))
        ontology.axioms.append(.subClassOf(
            sub: .named("ex:B"),
            sup: .someValuesFrom(property: "ex:R", filler: .named("ex:C"))
        ))
        ontology.axioms.append(.disjointClasses([.named("ex:C"), .named("ex:D")]))

        let reasoner = TableauxReasoner(ontology: ontology)

        // A ⊓ ∀R.∀R.D → A must have R-successor B, B must have R-successor C,
        // but ∀R.∀R.D forces C to be D, which contradicts C ⊓ D = ⊥
        let result = reasoner.checkSatisfiability(.intersection([
            .named("ex:A"),
            .allValuesFrom(property: "ex:R", filler:
                .allValuesFrom(property: "ex:R", filler: .named("ex:D"))
            )
        ]))
        #expect(!result.isSatisfiable)
    }
}
