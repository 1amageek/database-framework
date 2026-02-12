// SchemaOntologyIntegrationTests.swift
// Integration test for the full Schema.Ontology → FDBContainer → OntologyStore pipeline
//
// Validates:
//   1. FDBContainer(for: schema) persists Schema.Ontology via SchemaRegistry
//   2. loadSchemaOntology() decodes Schema.Ontology → OWLOntology → OntologyStore
//   3. OntologyStore reflects the loaded ontology

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
struct SchemaOntologyIntegrationDummy {
    #Directory<SchemaOntologyIntegrationDummy>("test", "schema_ontology_integration", "dummy")

    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<SchemaOntologyIntegrationDummy>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .tripleStore
    ))
}

@Suite("Schema.Ontology Integration", .serialized)
struct SchemaOntologyIntegrationTests {

    private static let testIRI = "http://test.org/schema-ontology-integration"

    // MARK: - Helpers

    private func setupContainer(withOntology: Bool) async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()

        var owlOntology: OWLOntology?
        if withOntology {
            var ont = OWLOntology(iri: Self.testIRI)
            ont.classes = [
                OWLClass(iri: "ex:Animal"),
                OWLClass(iri: "ex:Dog"),
            ]
            ont.axioms = [
                .subClassOf(sub: .named("ex:Dog"), sup: .named("ex:Animal")),
            ]
            ont.dataProperties = [
                OWLDataProperty(iri: "ex:name"),
            ]
            owlOntology = ont
        }

        let schema = Schema(
            [SchemaOntologyIntegrationDummy.self],
            ontology: owlOntology?.asSchemaOntology()
        )

        // Use the full init that calls ensureIndexesReady + SchemaRegistry.persist
        return try await FDBContainer(for: schema, security: .disabled)
    }

    private func cleanup(container: FDBContainer) async throws {
        let context = container.newContext()
        try? await context.ontology.delete(iri: Self.testIRI)
    }

    // MARK: - Tests

    @Test("loadSchemaOntology() populates OntologyStore from Schema.Ontology")
    func loadSchemaOntologyPopulatesStore() async throws {
        let container = try await setupContainer(withOntology: true)

        // loadSchemaOntology() decodes Schema.Ontology → OWLOntology → OntologyStore
        try await container.loadSchemaOntology()

        // Verify OntologyStore has the ontology
        let context = container.newContext()
        let loaded = try await context.ontology.get(iri: Self.testIRI)
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.classes.count == 2)
        #expect(loadedOntology.axioms.count == 1)
        #expect(loadedOntology.dataProperties.count == 1)

        try await cleanup(container: container)
    }

    @Test("loadSchemaOntology() is idempotent (skips if already loaded)")
    func loadSchemaOntologyIdempotent() async throws {
        let container = try await setupContainer(withOntology: true)

        // First call loads
        try await container.loadSchemaOntology()

        // Second call should skip (no error)
        try await container.loadSchemaOntology()

        // Verify still correct
        let context = container.newContext()
        let loaded = try await context.ontology.get(iri: Self.testIRI)
        #expect(loaded != nil)
        #expect(loaded?.classes.count == 2)

        try await cleanup(container: container)
    }

    @Test("loadSchemaOntology() is no-op when schema has no ontology")
    func loadSchemaOntologyNoOp() async throws {
        let container = try await setupContainer(withOntology: false)

        // Should not throw, just return
        try await container.loadSchemaOntology()

        // No ontology in store
        let context = container.newContext()
        let loaded = try await context.ontology.get(iri: Self.testIRI)
        #expect(loaded == nil)
    }

    @Test("Full pipeline: Schema.Ontology persisted by SchemaRegistry, loaded into OntologyStore, used for reasoning")
    func fullPipelineWithReasoning() async throws {
        let container = try await setupContainer(withOntology: true)

        // Step 1: Verify SchemaRegistry persisted the ontology
        let registry = SchemaRegistry(database: container.database)
        let schemaOntology = try await registry.loadOntology()
        #expect(schemaOntology != nil)
        #expect(schemaOntology?.iri == Self.testIRI)

        // Step 2: Load into OntologyStore
        try await container.loadSchemaOntology()

        // Step 3: Retrieve and use for reasoning
        let context = container.newContext()
        let loaded = try await context.ontology.get(iri: Self.testIRI)
        var ontology = try #require(loaded)

        // Add ABox assertion
        ontology.axioms.append(.classAssertion(
            individual: "ex:Buddy",
            class_: .named("ex:Dog")
        ))
        _ = ontology.addIndividual(OWLNamedIndividual(iri: "ex:Buddy"))

        let reasoner = OWLReasoner(ontology: ontology)
        let types = reasoner.types(of: "ex:Buddy")

        #expect(types.contains("ex:Dog"))
        #expect(types.contains("ex:Animal"))

        try await cleanup(container: container)
    }
}
