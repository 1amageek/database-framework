// OntologyPersistenceTests.swift
// Integration tests for ontology save/load round-trip
//
// Validates the full persistence pipeline:
//   OWLOntology → OntologyStore.loadOntology() → FDB → OntologyStore.listAxioms/listClasses/listProperties → OWLOntology
//
// These tests cover the AURORA use case:
//   1. SaveOntologyTool saves ontology (classes, properties, axioms)
//   2. ClassifyTool loads ontology via get(iri:)
//   3. OWLReasoner uses loaded ontology for inference

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

// MARK: - Dummy Entity (required by FDBContainer)

/// Minimal @Persistable entity to satisfy FDBContainer's Schema requirement.
/// Ontology tests do not use this entity — they operate on the ontology subspace (O/).
@Persistable
struct OntologyTestDummy {
    #Directory<OntologyTestDummy>("test", "ontology", "dummy")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<OntologyTestDummy>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .tripleStore
    ))
}

// MARK: - Ontology Persistence Tests

@Suite("Ontology Persistence", .serialized)
struct OntologyPersistenceTests {

    private static let testOntologyIRI = "http://test.org/ontology"

    // MARK: - Helpers

    private func setupContext() async throws -> FDBContext {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([OntologyTestDummy.self], version: Schema.Version(1, 0, 0))
        let container = FDBContainer(database: database, schema: schema, security: .disabled)
        return container.newContext()
    }

    private func cleanup(context: FDBContext) async throws {
        try await context.ontology.delete(iri: Self.testOntologyIRI)
    }

    // MARK: - Axiom Round-Trip

    @Test("subClassOf axioms survive save/load round-trip")
    func subClassOfAxiomRoundTrip() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Animal"),
            OWLClass(iri: "ex:Dog"),
            OWLClass(iri: "ex:GuideDog"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")),
            .subClassOf(sub: .named("ex:GuideDog"), sup: .named("ex:Dog")),
        ]

        // Save
        try await context.ontology.load(ontology)

        // Load
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        // Verify classes
        #expect(loadedOntology.classes.count == 3)

        // Verify axioms
        #expect(loadedOntology.axioms.count == 2)
        #expect(loadedOntology.axioms.contains(.subClassOf(
            sub: .named("ex:Dog"),
            sup: .named("ex:Animal")
        )))
        #expect(loadedOntology.axioms.contains(.subClassOf(
            sub: .named("ex:GuideDog"),
            sup: .named("ex:Dog")
        )))

        try await cleanup(context: context)
    }

    @Test("equivalentClasses axioms survive save/load round-trip")
    func equivalentClassesAxiomRoundTrip() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Organization"),
            OWLClass(iri: "ex:Corporation"),
            OWLClass(iri: "ex:GlobalManufacturer"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:Corporation"), sup: .named("ex:Organization")),
            .equivalentClasses([
                .named("ex:GlobalManufacturer"),
                .intersection([
                    .named("ex:Corporation"),
                    .dataHasValue(property: "ex:scale", literal: .string("Global")),
                ]),
            ]),
        ]

        // Save
        try await context.ontology.load(ontology)

        // Load
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        // Verify axioms
        #expect(loadedOntology.axioms.count == 2)

        let hasEquivalentClass = loadedOntology.axioms.contains(where: { axiom in
            if case .equivalentClasses(let exprs) = axiom {
                return exprs.contains(.named("ex:GlobalManufacturer"))
            }
            return false
        })
        #expect(hasEquivalentClass)

        try await cleanup(context: context)
    }

    // MARK: - Property Round-Trip

    @Test("objectProperties survive save/load round-trip")
    func objectPropertyRoundTrip() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Person"),
            OWLClass(iri: "ex:Company"),
        ]
        ontology.objectProperties = [
            OWLObjectProperty(
                iri: "ex:worksFor",
                domains: [.named("ex:Person")],
                ranges: [.named("ex:Company")]
            ),
        ]
        ontology.dataProperties = [
            OWLDataProperty(
                iri: "ex:name",
                domains: [.named("ex:Person")]
            ),
        ]

        // Save
        try await context.ontology.load(ontology)

        // Load
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        // Verify properties
        #expect(loadedOntology.objectProperties.count == 1)
        #expect(loadedOntology.objectProperties[0].iri == "ex:worksFor")
        #expect(loadedOntology.dataProperties.count == 1)
        #expect(loadedOntology.dataProperties[0].iri == "ex:name")

        try await cleanup(context: context)
    }

    // MARK: - Reasoning After Persistence (AURORA ClassifyTool use case)

    @Test("subClassOf inference works after save/load")
    func subClassOfReasoningAfterPersistence() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // Build ontology: TechCompany subClassOf Company subClassOf Organization
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Organization"),
            OWLClass(iri: "ex:Company"),
            OWLClass(iri: "ex:TechCompany"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:Company"), sup: .named("ex:Organization")),
            .subClassOf(sub: .named("ex:TechCompany"), sup: .named("ex:Company")),
        ]

        // Save then load (simulates FDB persistence cycle)
        try await context.ontology.load(ontology)
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        var reasoningOntology = try #require(loaded)

        // Add ABox assertion: ex:Google rdf:type ex:TechCompany
        reasoningOntology.axioms.append(.classAssertion(
            individual: "ex:Google",
            class_: .named("ex:TechCompany")
        ))
        _ = reasoningOntology.addIndividual(OWLNamedIndividual(iri: "ex:Google"))

        // Reason
        let reasoner = OWLReasoner(ontology: reasoningOntology)
        let inferredTypes = reasoner.types(of: "ex:Google")

        // ex:Google should be inferred as instance of Company AND Organization
        #expect(inferredTypes.contains("ex:TechCompany"))
        #expect(inferredTypes.contains("ex:Company"))
        #expect(inferredTypes.contains("ex:Organization"))

        try await cleanup(context: context)
    }

    @Test("Defined Class classification works after save/load")
    func definedClassReasoningAfterPersistence() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // Build ontology with Defined Class:
        //   GlobalCorp ≡ Corporation ∩ hasValue(scale, "Global")
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Corporation"),
            OWLClass(iri: "ex:GlobalCorp"),
        ]
        ontology.axioms = [
            .equivalentClasses([
                .named("ex:GlobalCorp"),
                .intersection([
                    .named("ex:Corporation"),
                    .dataHasValue(property: "ex:scale", literal: .string("Global")),
                ]),
            ]),
        ]

        // Save then load
        try await context.ontology.load(ontology)
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        var reasoningOntology = try #require(loaded)

        // Add ABox: ex:Toyota rdf:type ex:Corporation, ex:Toyota ex:scale "Global"
        reasoningOntology.axioms.append(.classAssertion(
            individual: "ex:Toyota",
            class_: .named("ex:Corporation")
        ))
        reasoningOntology.axioms.append(.dataPropertyAssertion(
            subject: "ex:Toyota",
            property: "ex:scale",
            value: .string("Global")
        ))
        _ = reasoningOntology.addIndividual(OWLNamedIndividual(iri: "ex:Toyota"))

        // Reason
        let reasoner = OWLReasoner(ontology: reasoningOntology)
        let inferredTypes = reasoner.types(of: "ex:Toyota")

        // ex:Toyota should be classified as GlobalCorp via Defined Class
        #expect(inferredTypes.contains("ex:Corporation"))
        #expect(inferredTypes.contains("ex:GlobalCorp"))

        try await cleanup(context: context)
    }

    // MARK: - Edge Cases

    @Test("empty ontology round-trip")
    func emptyOntologyRoundTrip() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        let ontology = OWLOntology(iri: Self.testOntologyIRI)

        try await context.ontology.load(ontology)
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.classes.isEmpty)
        #expect(loadedOntology.objectProperties.isEmpty)
        #expect(loadedOntology.dataProperties.isEmpty)
        #expect(loadedOntology.axioms.isEmpty)

        try await cleanup(context: context)
    }

    @Test("overwrite preserves latest axioms only")
    func overwriteOntologyAxioms() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // First save: 1 axiom
        var ontology1 = OWLOntology(iri: Self.testOntologyIRI)
        ontology1.classes = [OWLClass(iri: "ex:A"), OWLClass(iri: "ex:B")]
        ontology1.axioms = [
            .subClassOf(sub: .named("ex:A"), sup: .named("ex:B")),
        ]
        try await context.ontology.load(ontology1)

        // Second save: different axiom
        var ontology2 = OWLOntology(iri: Self.testOntologyIRI)
        ontology2.classes = [OWLClass(iri: "ex:X"), OWLClass(iri: "ex:Y")]
        ontology2.axioms = [
            .subClassOf(sub: .named("ex:X"), sup: .named("ex:Y")),
        ]
        try await context.ontology.load(ontology2)

        // Load: should have only the second save's data
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.classes.count == 2)
        #expect(loadedOntology.classes.map(\.iri).sorted() == ["ex:X", "ex:Y"])
        #expect(loadedOntology.axioms.count == 1)
        #expect(loadedOntology.axioms[0] == .subClassOf(sub: .named("ex:X"), sup: .named("ex:Y")))

        try await cleanup(context: context)
    }

    @Test("complex axiom types survive round-trip")
    func complexAxiomRoundTrip() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:A"),
            OWLClass(iri: "ex:B"),
            OWLClass(iri: "ex:C"),
        ]
        ontology.objectProperties = [
            OWLObjectProperty(iri: "ex:rel"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:A"), sup: .named("ex:B")),
            .disjointClasses([.named("ex:A"), .named("ex:C")]),
            .objectPropertyDomain(property: "ex:rel", domain: .named("ex:A")),
            .objectPropertyRange(property: "ex:rel", range: .named("ex:B")),
        ]

        try await context.ontology.load(ontology)
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.axioms.count == 4)

        // Verify each axiom type survived
        #expect(loadedOntology.axioms.contains(.subClassOf(sub: .named("ex:A"), sup: .named("ex:B"))))
        #expect(loadedOntology.axioms.contains(.disjointClasses([.named("ex:A"), .named("ex:C")])))
        #expect(loadedOntology.axioms.contains(.objectPropertyDomain(property: "ex:rel", domain: .named("ex:A"))))
        #expect(loadedOntology.axioms.contains(.objectPropertyRange(property: "ex:rel", range: .named("ex:B"))))

        try await cleanup(context: context)
    }

    // MARK: - In-Memory Reasoner Baseline

    @Test("in-memory reasoner: subClassOf chain does not falsely classify siblings")
    func inMemoryReasonerBaseline() async throws {
        // Test with objectProperty to isolate the trigger
        var ontology = OWLOntology(iri: "http://test.org/inmemory")
        ontology.classes = [
            OWLClass(iri: "ex:Organization"),
            OWLClass(iri: "ex:Company"),
            OWLClass(iri: "ex:TechCompany"),
            OWLClass(iri: "ex:AICompany"),
        ]
        ontology.objectProperties = [
            OWLObjectProperty(
                iri: "ex:regulatedBy",
                domains: [.named("ex:Company")],
                ranges: [.named("ex:Organization")]
            ),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:Company"), sup: .named("ex:Organization")),
            .subClassOf(sub: .named("ex:TechCompany"), sup: .named("ex:Company")),
            .subClassOf(sub: .named("ex:AICompany"), sup: .named("ex:TechCompany")),
            .classAssertion(individual: "ex:Google", class_: .named("ex:TechCompany")),
        ]
        _ = ontology.addIndividual(OWLNamedIndividual(iri: "ex:Google"))

        let tableaux = TableauxReasoner(
            ontology: ontology,
            configuration: .init(checkRegularity: false, abortOnRegularityViolations: false)
        )

        let testExpr = OWLClassExpression.intersection([
            .named("ex:TechCompany"),
            .complement(.named("ex:AICompany")),
        ])
        let result = tableaux.checkSatisfiability(testExpr)
        #expect(result.isSatisfiable,
                "TechCompany ⊓ ¬AICompany should be satisfiable (status: \(result.status))")

        let googleTypes = tableaux.types(of: "ex:Google")
        #expect(!googleTypes.contains("ex:AICompany"),
                "Google (TechCompany) should NOT be AICompany. Got: \(googleTypes)")
    }

    // MARK: - AURORA Full Workflow Simulation

    @Test("AURORA workflow: save ontology -> load -> classify entities")
    func auroraClassifyWorkflow() async throws {
        let context = try await setupContext()
        try await cleanup(context: context)

        // Step 1: SaveOntologyTool builds and saves ontology
        var ontology = OWLOntology(iri: Self.testOntologyIRI)
        ontology.classes = [
            OWLClass(iri: "ex:Organization"),
            OWLClass(iri: "ex:Company"),
            OWLClass(iri: "ex:TechCompany"),
            OWLClass(iri: "ex:AICompany"),
            OWLClass(iri: "ex:RegulatoryAuthority"),
        ]
        ontology.objectProperties = [
            OWLObjectProperty(
                iri: "ex:regulatedBy",
                domains: [.named("ex:Company")],
                ranges: [.named("ex:RegulatoryAuthority")]
            ),
        ]
        ontology.dataProperties = [
            OWLDataProperty(iri: "ex:name"),
            OWLDataProperty(iri: "ex:industry"),
        ]
        ontology.axioms = [
            .subClassOf(sub: .named("ex:Company"), sup: .named("ex:Organization")),
            .subClassOf(sub: .named("ex:TechCompany"), sup: .named("ex:Company")),
            .subClassOf(sub: .named("ex:AICompany"), sup: .named("ex:TechCompany")),
        ]
        try await context.ontology.load(ontology)

        // Step 2: ClassifyTool loads ontology from FDB
        let loaded = try await context.ontology.get(iri: Self.testOntologyIRI)
        let loadedOntology = try #require(loaded)

        // Verify context is complete (ReadStep would use this for SPARQL generation)
        #expect(loadedOntology.classes.count == 5)
        #expect(loadedOntology.objectProperties.count == 1)
        #expect(loadedOntology.dataProperties.count == 2)
        #expect(loadedOntology.axioms.count == 3)

        // Step 3: ClassifyTool adds ABox data and reasons
        var reasoningOntology = loadedOntology

        // Entities from save_entities
        let entities = [
            ("ex:OpenAI", "ex:AICompany"),
            ("ex:Google", "ex:TechCompany"),
            ("ex:CMA", "ex:RegulatoryAuthority"),
        ]
        for (entityIRI, typeIRI) in entities {
            reasoningOntology.axioms.append(.classAssertion(
                individual: entityIRI,
                class_: .named(typeIRI)
            ))
            _ = reasoningOntology.addIndividual(OWLNamedIndividual(iri: entityIRI))
        }

        let reasoner = OWLReasoner(ontology: reasoningOntology)

        // Verify inference: OpenAI (AICompany) -> TechCompany -> Company -> Organization
        let openAITypes = reasoner.types(of: "ex:OpenAI")
        #expect(openAITypes.contains("ex:AICompany"))
        #expect(openAITypes.contains("ex:TechCompany"))
        #expect(openAITypes.contains("ex:Company"))
        #expect(openAITypes.contains("ex:Organization"))

        // Verify inference: Google (TechCompany) -> Company -> Organization
        let googleTypes = reasoner.types(of: "ex:Google")
        #expect(googleTypes.contains("ex:TechCompany"))
        #expect(googleTypes.contains("ex:Company"))
        #expect(googleTypes.contains("ex:Organization"))
        #expect(!googleTypes.contains("ex:AICompany")) // NOT an AI company

        // CMA (RegulatoryAuthority) has no superclass axiom
        let cmaTypes = reasoner.types(of: "ex:CMA")
        #expect(cmaTypes.contains("ex:RegulatoryAuthority"))
        #expect(!cmaTypes.contains("ex:Company"))

        try await cleanup(context: context)
    }
}
