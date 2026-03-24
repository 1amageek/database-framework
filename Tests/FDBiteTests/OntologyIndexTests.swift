// OntologyIndexTests.swift
// Tests for OntologyIndex — automatic @OWLClass entity → SPO triple materialization
//
// Unit tests: OWLTripleIndexMaintainer key generation logic
// Macro tests: @OWLClass auto-generates _owlTripleDescriptors, merged into descriptors
// Integration tests: context.save() → IndexMaintenanceService → SPO entries in storage

import Testing
import Foundation
import Database
import StorageKit

// MARK: - Test Models

/// @OWLClass entity: Person
@Persistable
@OWLClass("ex:Person")
struct OntoPerson: Hashable {
    #Directory<OntoPerson>("test", "ontology", "persons")

    @OWLDataProperty("rdfs:label")
    var name: String = ""

    @OWLDataProperty("ex:email")
    var email: String = ""
}

/// @OWLClass entity: Organization
@Persistable
@OWLClass("ex:Organization")
struct OntoOrganization: Hashable {
    #Directory<OntoOrganization>("test", "ontology", "organizations")

    @OWLDataProperty("rdfs:label")
    var name: String = ""
}

/// Non-OWL entity (no @OWLClass) for comparison
@Persistable
struct PlainItem: Hashable {
    #Directory<PlainItem>("test", "ontology", "plain")

    var name: String = ""
}

// MARK: - Helpers

private func containsSubsequence(_ haystack: [UInt8], _ needle: [UInt8]) -> Bool {
    guard needle.count <= haystack.count else { return false }
    for i in 0...(haystack.count - needle.count) {
        if haystack[i..<(i + needle.count)].elementsEqual(needle) {
            return true
        }
    }
    return false
}

/// Scan all key-value pairs in storage and find entries containing the given string.
private func findEntries(
    engine: any StorageEngine,
    containing text: String
) async throws -> [Bytes] {
    let textBytes = Array(text.utf8)
    var matched: [Bytes] = []
    try await engine.withTransaction { tx in
        for (key, _) in try await tx.collectRange(
            from: .firstGreaterOrEqual([0x00]),
            to: .firstGreaterOrEqual([0xFF]),
            limit: 10000,
            snapshot: true
        ) {
            if containsSubsequence(key, textBytes) {
                matched.append(key)
            }
        }
    }
    return matched
}

// MARK: - Macro Generation Tests

@Suite("OWLClass Macro Descriptor Tests")
struct OWLClassMacroDescriptorTests {

    @Test("@OWLClass auto-generates _owlTripleDescriptors")
    func owlClassGeneratesTripleDescriptors() {
        let descriptors = OntoPerson._owlTripleDescriptors
        #expect(descriptors.count == 1)

        let indexDesc = descriptors[0] as? IndexDescriptor
        #expect(indexDesc != nil)
        #expect(indexDesc?.name == "OntoPerson_owlTriple")
        #expect(indexDesc?.kindIdentifier == "owlTriple")
    }

    @Test("descriptors merges _persistableDescriptors + _owlTripleDescriptors")
    func descriptorsMerge() {
        let all = OntoPerson.descriptors
        let indexDescs = all.compactMap { $0 as? IndexDescriptor }

        // Should contain at least: owlTriple descriptor
        let owlTriple = indexDescs.first { $0.kindIdentifier == "owlTriple" }
        #expect(owlTriple != nil, "descriptors should contain OWLTripleIndexKind descriptor")
        #expect(owlTriple?.name == "OntoPerson_owlTriple")
    }

    @Test("indexDescriptors includes OWLTripleIndexKind")
    func indexDescriptorsIncludeOwlTriple() {
        let indexDescs = OntoPerson.indexDescriptors
        let owlTriple = indexDescs.first { $0.kindIdentifier == "owlTriple" }
        #expect(owlTriple != nil, "indexDescriptors should include owlTriple")
    }

    @Test("Non-OWL entity has no owlTriple descriptor")
    func plainEntityNoOwlTriple() {
        let indexDescs = PlainItem.indexDescriptors
        let owlTriple = indexDescs.first { $0.kindIdentifier == "owlTriple" }
        #expect(owlTriple == nil, "Plain entity should not have owlTriple descriptor")
    }
}

// MARK: - Unit Tests: Maintainer Key Generation

@Suite("OWLTripleIndexMaintainer Unit Tests")
struct OWLTripleIndexMaintainerUnitTests {

    @Test("Generates correct SPO key count for insert")
    func keyCount() async throws {
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl"), graph: "test:default", prefix: "test"
        )
        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // 3 triples (rdf:type + rdfs:label + ex:email) × 3 orderings = 9
        #expect(keys.count == 9)
    }

    @Test("Skips empty string fields")
    func skipsEmpty() async throws {
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl"), graph: "test:default", prefix: "test"
        )
        let person = OntoPerson(name: "", email: "noname@example.com")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // 2 triples (rdf:type + ex:email) × 3 orderings = 6
        #expect(keys.count == 6)
    }

    @Test("Different values produce different keys")
    func diffValues() async throws {
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl"), graph: "test:default", prefix: "test"
        )
        var person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keysV1 = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        person.name = "Bob"
        let keysV2 = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        #expect(keysV1 != keysV2)
        #expect(keysV1.count == keysV2.count)
    }

    @Test("Entity IRI format: {prefix}:{lowercase_type}/{id}")
    func entityIRI() async throws {
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl"), graph: "test:default", prefix: "test"
        )
        let person = OntoPerson(name: "Test")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))
        let iriBytes = Array("test:ontoperson/\(person.id)".utf8)

        #expect(keys.contains { containsSubsequence($0, iriBytes) })
    }

    @Test("Generates rdf:type triple with ontologyClassIRI")
    func rdfType() async throws {
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl"), graph: "test:default", prefix: "test"
        )
        let person = OntoPerson(name: "Alice")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        let rdfTypeBytes = Array("rdf:type".utf8)
        let classBytes = Array("ex:Person".utf8)
        #expect(keys.contains { containsSubsequence($0, rdfTypeBytes) && containsSubsequence($0, classBytes) })
    }

    @Test("Different @OWLClass types produce independent keys")
    func multipleTypes() async throws {
        let personMaintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: Subspace("test"), graph: "default", prefix: "e"
        )
        let orgMaintainer = OWLTripleIndexMaintainer<OntoOrganization>(
            subspace: Subspace("test"), graph: "default", prefix: "e"
        )

        let pKeys = try await personMaintainer.computeIndexKeys(
            for: OntoPerson(name: "A", email: "a@b"), id: Tuple(["1"])
        )
        let oKeys = try await orgMaintainer.computeIndexKeys(
            for: OntoOrganization(name: "X"), id: Tuple(["2"])
        )

        #expect(pKeys.count == 9)  // 3 triples × 3
        #expect(oKeys.count == 6)  // 2 triples × 3
    }
}

// MARK: - Integration Tests: Full Pipeline

@Suite("OntologyIndex Integration Tests")
struct OntologyIndexIntegrationTests {

    private func makeContainer() async throws -> DBContainer {
        let schema = Schema(
            [OntoPerson.self, OntoOrganization.self, PlainItem.self],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    @Test("Insert: context.save() creates SPO entries automatically")
    func insertCreatesSPOEntries() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        context.insert(person)
        try await context.save()

        let entityIRI = "entity:ontoperson/\(person.id)"
        let entries = try await findEntries(engine: container.engine, containing: entityIRI)

        // 3 triples × 3 orderings = 9
        #expect(entries.count == 9, "Expected 9 SPO entries, got \(entries.count)")
    }

    @Test("Insert: empty fields are skipped")
    func insertSkipsEmpty() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        let person = OntoPerson(name: "", email: "x@y")
        context.insert(person)
        try await context.save()

        let entityIRI = "entity:ontoperson/\(person.id)"
        let entries = try await findEntries(engine: container.engine, containing: entityIRI)

        // 2 triples × 3 orderings = 6
        #expect(entries.count == 6, "Expected 6 SPO entries, got \(entries.count)")
    }

    @Test("Update: old SPO entries replaced with new values")
    func updateReplacesSPO() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        var person = OntoPerson(name: "Alice", email: "alice@example.com")
        context.insert(person)
        try await context.save()

        // Update
        person.name = "Alice Smith"
        context.insert(person)  // upsert
        try await context.save()

        let entityIRI = "entity:ontoperson/\(person.id)"
        let entries = try await findEntries(engine: container.engine, containing: entityIRI)

        // Still 3 triples × 3 = 9 (old cleared, new set)
        #expect(entries.count == 9)

        // Old value should not be in keys
        let oldBytes = Array("Alice".utf8)
        let newBytes = Array("Alice Smith".utf8)
        let hasStale = entries.contains {
            containsSubsequence($0, oldBytes) && !containsSubsequence($0, newBytes)
        }
        #expect(!hasStale, "Old value 'Alice' should not remain after update")
    }

    @Test("Delete: all SPO entries removed")
    func deleteRemovesSPO() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        let person = OntoPerson(name: "Bob", email: "bob@example.com")
        context.insert(person)
        try await context.save()

        let entityIRI = "entity:ontoperson/\(person.id)"
        let before = try await findEntries(engine: container.engine, containing: entityIRI)
        #expect(!before.isEmpty)

        context.delete(person)
        try await context.save()

        let after = try await findEntries(engine: container.engine, containing: entityIRI)
        #expect(after.isEmpty, "All SPO entries should be removed after delete")
    }

    @Test("Multiple types: independent SPO entries per entity type")
    func multipleTypesIntegration() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        let person = OntoPerson(name: "Charlie", email: "c@d")
        let org = OntoOrganization(name: "Acme")
        context.insert(person)
        context.insert(org)
        try await context.save()

        let personEntries = try await findEntries(
            engine: container.engine, containing: "entity:ontoperson/\(person.id)"
        )
        let orgEntries = try await findEntries(
            engine: container.engine, containing: "entity:ontoorganization/\(org.id)"
        )

        #expect(personEntries.count == 9)  // 3 × 3
        #expect(orgEntries.count == 6)     // 2 × 3
    }

    @Test("Non-OWL entity: no SPO entries created")
    func plainEntityNoSPO() async throws {
        let container = try await makeContainer()
        let context = container.newContext()

        let item = PlainItem(name: "test")
        context.insert(item)
        try await context.save()

        // PlainItem has no @OWLClass, so no owlTriple entries
        let entries = try await findEntries(
            engine: container.engine, containing: "entity:plainitem/"
        )
        #expect(entries.isEmpty, "Non-OWL entity should not produce SPO entries")
    }
}
