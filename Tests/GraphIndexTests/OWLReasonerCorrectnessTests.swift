#if FOUNDATION_DB
import DatabaseKit
import TestHeartbeat
import TestSupport
import Testing
@testable import GraphIndex
@testable import OntologyIndex

@Suite("OWLReasoner types(of:) correctness", .serialized, .heartbeat)
struct OWLReasonerTypesCorrectnessTests {
    @Test("subClassOf hierarchy is fully expanded")
    func subClassHierarchyIsFullyExpanded() {
        var ontology = OWLOntology(iri: "http://test.org/hierarchy")
        for classIRI in [
            "ex:LivingThing",
            "ex:Animal",
            "ex:Mammal",
            "ex:Dog",
        ] {
            ontology.classes.append(OWLClass(iri: classIRI))
        }
        ontology.axioms.append(
            .subClassOf(
                sub: .named("ex:Animal"),
                sup: .named("ex:LivingThing")
            )
        )
        ontology.axioms.append(
            .subClassOf(
                sub: .named("ex:Mammal"),
                sup: .named("ex:Animal")
            )
        )
        ontology.axioms.append(
            .subClassOf(
                sub: .named("ex:Dog"),
                sup: .named("ex:Mammal")
            )
        )
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:rex"))
        ontology.axioms.append(
            .classAssertion(
                individual: "ex:rex",
                class_: .named("ex:Dog")
            )
        )

        let reasoner = OWLReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )
        let types = reasoner.types(of: "ex:rex")

        #expect(types.contains("ex:Dog"))
        #expect(types.contains("ex:Mammal"))
        #expect(types.contains("ex:Animal"))
        #expect(types.contains("ex:LivingThing"))
        #expect(types.contains("owl:Thing"))
    }

    @Test("Optimized types(of:) matches naive TableauxReasoner")
    func optimizedTypesMatchNaiveReasoner() {
        var ontology = OWLOntology(iri: "http://test.org/naive-compare")
        for classIRI in ["ex:Person", "ex:Employee", "ex:Manager"] {
            ontology.classes.append(OWLClass(iri: classIRI))
        }
        ontology.axioms.append(
            .subClassOf(
                sub: .named("ex:Employee"),
                sup: .named("ex:Person")
            )
        )
        ontology.axioms.append(
            .subClassOf(
                sub: .named("ex:Manager"),
                sup: .named("ex:Employee")
            )
        )
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:alice"))
        ontology.axioms.append(
            .classAssertion(
                individual: "ex:alice",
                class_: .named("ex:Manager")
            )
        )
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:bob"))
        ontology.axioms.append(
            .classAssertion(
                individual: "ex:bob",
                class_: .named("ex:Employee")
            )
        )

        let optimized = OWLReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )
        let naive = TableauxReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )

        for individual in ontology.individuals {
            let optimizedTypes = optimized.types(of: individual.iri)
            var naiveTypes = naive.types(of: individual.iri)
            naiveTypes.insert("owl:Thing")
            #expect(optimizedTypes == naiveTypes)
        }
    }

    @Test("Individual with no assertions returns only owl:Thing")
    func individualWithoutAssertionsReturnsThing() {
        var ontology = OWLOntology(iri: "http://test.org/no-assertions")
        ontology.classes.append(OWLClass(iri: "ex:Person"))
        ontology.individuals.append(OWLNamedIndividual(iri: "ex:unknown"))

        let reasoner = OWLReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )

        #expect(reasoner.types(of: "ex:unknown") == Set(["owl:Thing"]))
    }

    @Test("Multiple defined classes remain distinct")
    func multipleDefinedClassesRemainDistinct() {
        var ontology = OWLOntology(iri: "http://test.org/multi-defined")
        for classIRI in ["ex:Vehicle", "ex:Car", "ex:Truck"] {
            ontology.classes.append(OWLClass(iri: classIRI))
        }
        ontology.objectProperties.append(
            OWLObjectProperty(iri: "ex:hasType")
        )
        for individualIRI in ["ex:sedan", "ex:pickup"] {
            ontology.individuals.append(
                OWLNamedIndividual(iri: individualIRI)
            )
        }
        ontology.axioms.append(
            .equivalentClasses([
                .named("ex:Car"),
                .intersection([
                    .named("ex:Vehicle"),
                    .hasValue(
                        property: "ex:hasType",
                        individual: "ex:sedan"
                    ),
                ]),
            ])
        )
        ontology.axioms.append(
            .equivalentClasses([
                .named("ex:Truck"),
                .intersection([
                    .named("ex:Vehicle"),
                    .hasValue(
                        property: "ex:hasType",
                        individual: "ex:pickup"
                    ),
                ]),
            ])
        )
        ontology.axioms.append(
            .subClassOf(sub: .named("ex:Car"), sup: .named("ex:Vehicle"))
        )
        ontology.axioms.append(
            .subClassOf(sub: .named("ex:Truck"), sup: .named("ex:Vehicle"))
        )
        appendVehicle(
            iri: "ex:mycar",
            type: "ex:sedan",
            to: &ontology
        )
        appendVehicle(
            iri: "ex:mytruck",
            type: "ex:pickup",
            to: &ontology
        )

        let reasoner = OWLReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )
        let carTypes = reasoner.types(of: "ex:mycar")
        let truckTypes = reasoner.types(of: "ex:mytruck")

        #expect(carTypes.contains("ex:Car"))
        #expect(carTypes.contains("ex:Vehicle"))
        #expect(!carTypes.contains("ex:Truck"))
        #expect(truckTypes.contains("ex:Truck"))
        #expect(truckTypes.contains("ex:Vehicle"))
        #expect(!truckTypes.contains("ex:Car"))
    }

    @Test("Optimized and naive reasoners agree on a rich ontology")
    func optimizedAndNaiveReasonersAgreeOnRichOntology() {
        let ontology = makeRichOntology(classCount: 6, individualCount: 8)
        let optimized = OWLReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )
        let naive = TableauxReasoner(
            ontology: ontology,
            clock: TestProcessMonotonicClock()
        )

        for individualIRI in ontology.individuals
            .map(\.iri)
            .filter({ $0.hasPrefix("ex:ind") }) {
            let optimizedTypes = optimized.types(of: individualIRI)
            var naiveTypes = naive.types(of: individualIRI)
            naiveTypes.insert("owl:Thing")
            #expect(optimizedTypes == naiveTypes)
        }
    }

    private func appendVehicle(
        iri: String,
        type: String,
        to ontology: inout OWLOntology
    ) {
        ontology.individuals.append(OWLNamedIndividual(iri: iri))
        ontology.axioms.append(
            .classAssertion(individual: iri, class_: .named("ex:Vehicle"))
        )
        ontology.axioms.append(
            .objectPropertyAssertion(
                subject: iri,
                property: "ex:hasType",
                object: type
            )
        )
    }

    private func makeRichOntology(
        classCount: Int,
        individualCount: Int
    ) -> OWLOntology {
        var ontology = OWLOntology(iri: "http://test.org/rich")
        for classIRI in ["ex:Entity", "ex:Agent", "ex:Place"] {
            ontology.classes.append(OWLClass(iri: classIRI))
        }
        ontology.axioms.append(
            .subClassOf(sub: .named("ex:Agent"), sup: .named("ex:Entity"))
        )
        ontology.axioms.append(
            .subClassOf(sub: .named("ex:Place"), sup: .named("ex:Entity"))
        )
        ontology.axioms.append(
            .disjointClasses([.named("ex:Agent"), .named("ex:Place")])
        )
        for propertyIRI in ["ex:locatedIn", "ex:worksAt", "ex:hasRole"] {
            ontology.objectProperties.append(
                OWLObjectProperty(iri: propertyIRI)
            )
        }

        for index in 0..<classCount {
            let classIRI = "ex:AgentType\(index)"
            ontology.classes.append(OWLClass(iri: classIRI))
            ontology.axioms.append(
                .subClassOf(sub: .named(classIRI), sup: .named("ex:Agent"))
            )
            if index.isMultiple(of: 2) {
                let roleIRI = "ex:role\(index)"
                ontology.individuals.append(OWLNamedIndividual(iri: roleIRI))
                ontology.axioms.append(
                    .equivalentClasses([
                        .named(classIRI),
                        .intersection([
                            .named("ex:Agent"),
                            .hasValue(
                                property: "ex:hasRole",
                                individual: roleIRI
                            ),
                        ]),
                    ])
                )
            }
        }

        for index in 0..<individualCount {
            let individualIRI = "ex:ind\(index)"
            let classIndex = index % classCount
            ontology.individuals.append(
                OWLNamedIndividual(iri: individualIRI)
            )
            ontology.axioms.append(
                .classAssertion(
                    individual: individualIRI,
                    class_: .named("ex:AgentType\(classIndex)")
                )
            )
            if classIndex.isMultiple(of: 2) {
                ontology.axioms.append(
                    .objectPropertyAssertion(
                        subject: individualIRI,
                        property: "ex:hasRole",
                        object: "ex:role\(classIndex)"
                    )
                )
            }
        }
        return ontology
    }
}
#endif
