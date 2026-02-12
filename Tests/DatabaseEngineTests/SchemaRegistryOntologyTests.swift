// SchemaRegistryOntologyTests.swift
// Tests for Schema.Ontology persistence via SchemaRegistry
//
// Validates:
//   1. persist(schema) writes ontology atomically with entities
//   2. loadOntology() returns persisted ontology
//   3. loadOntology() returns nil when no ontology is stored
//   4. loadAll() does NOT pick up the ontology key (key isolation)

import Testing
import Foundation
import FoundationDB
import Core
import Graph
import TestSupport
@testable import DatabaseEngine

@Persistable
struct SchemaRegTestDummy {
    #Directory<SchemaRegTestDummy>("test", "schema_registry", "dummy")

    var name: String = ""
}

@Suite("SchemaRegistry Ontology Persistence", .serialized)
struct SchemaRegistryOntologyTests {

    // MARK: - Helpers

    private func setupRegistry() async throws -> (SchemaRegistry, any DatabaseProtocol) {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        return (SchemaRegistry(database: database), database)
    }

    private func cleanup(registry: SchemaRegistry) async throws {
        // Persist a schema without ontology to clear the ontology key
        let schema = Schema([SchemaRegTestDummy.self])
        try await registry.persist(schema)
    }

    // MARK: - Tests

    @Test("persist(schema) writes ontology, loadOntology() reads it back")
    func ontologyPersistAndLoad() async throws {
        let (registry, _) = try await setupRegistry()

        var owlOntology = OWLOntology(iri: "http://test.org/schema-registry")
        owlOntology.classes = [
            OWLClass(iri: "ex:Person"),
            OWLClass(iri: "ex:Company"),
        ]
        owlOntology.axioms = [
            .subClassOf(sub: .named("ex:Company"), sup: .named("ex:Person")),
        ]
        owlOntology.objectProperties = [
            OWLObjectProperty(iri: "ex:worksFor"),
        ]

        let schema = Schema(
            [SchemaRegTestDummy.self],
            ontology: owlOntology.asSchemaOntology()
        )
        try await registry.persist(schema)

        // Load ontology
        let loaded = try await registry.loadOntology()
        let loadedOntology = try #require(loaded)

        #expect(loadedOntology.iri == "http://test.org/schema-registry")
        #expect(loadedOntology.typeIdentifier == "OWLOntology")

        // Decode back to OWLOntology and verify fields
        let restored = try OWLOntology(schemaOntology: loadedOntology)
        #expect(restored.classes.count == 2)
        #expect(restored.axioms.count == 1)
        #expect(restored.objectProperties.count == 1)

        try await cleanup(registry: registry)
    }

    @Test("loadOntology() returns nil when schema has no ontology")
    func ontologyNilWhenAbsent() async throws {
        let (registry, _) = try await setupRegistry()

        // Persist schema without ontology
        let schema = Schema([SchemaRegTestDummy.self])
        try await registry.persist(schema)

        let loaded = try await registry.loadOntology()
        #expect(loaded == nil)
    }

    @Test("loadAll() does not include ontology key in entity results")
    func loadAllExcludesOntology() async throws {
        let (registry, _) = try await setupRegistry()

        let owlOntology = OWLOntology(iri: "http://test.org/isolation")
        let schema = Schema(
            [SchemaRegTestDummy.self],
            ontology: owlOntology.asSchemaOntology()
        )
        try await registry.persist(schema)

        // loadAll should return only entities, not the ontology.
        // Other tests may have persisted their own entities, so we check:
        //   1. Our entity is present
        //   2. No entry has the ontology IRI as its name (key isolation)
        let entities = try await registry.loadAll()
        #expect(entities.contains { $0.name == "SchemaRegTestDummy" })
        #expect(!entities.contains { $0.name.contains("ontology") })

        try await cleanup(registry: registry)
    }

    @Test("persist overwrites previous ontology")
    func ontologyOverwrite() async throws {
        let (registry, _) = try await setupRegistry()

        // First persist with ontology A
        let ontologyA = OWLOntology(iri: "http://test.org/version-a")
        let schemaA = Schema(
            [SchemaRegTestDummy.self],
            ontology: ontologyA.asSchemaOntology()
        )
        try await registry.persist(schemaA)

        let loadedA = try await registry.loadOntology()
        #expect(loadedA?.iri == "http://test.org/version-a")

        // Second persist with ontology B
        let ontologyB = OWLOntology(iri: "http://test.org/version-b")
        let schemaB = Schema(
            [SchemaRegTestDummy.self],
            ontology: ontologyB.asSchemaOntology()
        )
        try await registry.persist(schemaB)

        let loadedB = try await registry.loadOntology()
        #expect(loadedB?.iri == "http://test.org/version-b")

        try await cleanup(registry: registry)
    }
}
