// OntologyIndexTests.swift
// Tests for OntologyIndex — automatic @OWLClass entity → SPO triple materialization
//
// Validates that OWLTripleIndexMaintainer generates and maintains SPO/POS/OSP index
// entries via the standard IndexMaintainer pipeline.
//
// NOTE: Full integration requires @OWLClass macro enhancement in database-kit
// to auto-generate OWLTripleIndexKind descriptors. These tests use the Schema's
// `indexDescriptors:` parameter as a workaround.

import Testing
import Foundation
import FDBite
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

// MARK: - Test Suite

@Suite("OntologyIndex Tests")
struct OntologyIndexTests {

    // MARK: - OWLTripleIndexMaintainer Direct Tests

    @Test("OWLTripleIndexMaintainer generates correct SPO keys for insert")
    func maintainerGeneratesKeys() async throws {
        // Test OWLTripleIndexMaintainer directly with a mock transaction
        let subspace = Subspace("test_owl")
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace,
            graph: "test:default",
            prefix: "test"
        )

        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // 3 triples (rdf:type + rdfs:label + ex:email) × 3 orderings = 9 keys
        #expect(keys.count == 9, "Expected 9 keys, got \(keys.count)")
    }

    @Test("OWLTripleIndexMaintainer skips empty string fields")
    func maintainerSkipsEmptyFields() async throws {
        let subspace = Subspace("test_owl")
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace,
            graph: "test:default",
            prefix: "test"
        )

        let person = OntoPerson(name: "", email: "noname@example.com")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // 2 triples (rdf:type + ex:email, no rdfs:label) × 3 orderings = 6 keys
        #expect(keys.count == 6, "Expected 6 keys, got \(keys.count)")
    }

    @Test("OWLTripleIndexMaintainer generates different keys for different property values")
    func maintainerDiffKeys() async throws {
        let subspace = Subspace("test_owl")
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace,
            graph: "test:default",
            prefix: "test"
        )

        var person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keysV1 = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        person.name = "Bob"
        let keysV2 = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // Keys should differ (different name → different rdfs:label triple keys)
        #expect(keysV1 != keysV2, "Keys should differ when property values change")

        // Same count (same number of non-empty properties)
        #expect(keysV1.count == keysV2.count)
    }

    @Test("Entity IRI uses lowercase type name and entity ID")
    func entityIRIFormat() async throws {
        let subspace = Subspace("test_owl")
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace,
            graph: "test:default",
            prefix: "test"
        )

        let person = OntoPerson(name: "Test")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // Entity IRI: "test:ontoperson/{id}" should be encoded in keys
        let expectedIRI = "test:ontoperson/\(person.id)"
        let iriBytes = Array(expectedIRI.utf8)

        let containsIRI = keys.contains { key in
            containsSubsequence(key, iriBytes)
        }
        #expect(containsIRI, "SPO keys should contain entity IRI: \(expectedIRI)")
    }

    @Test("OWLTripleIndexMaintainer generates rdf:type triple")
    func maintainerGeneratesTypeTriple() async throws {
        let subspace = Subspace("test_owl")
        let maintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace,
            graph: "test:default",
            prefix: "test"
        )

        let person = OntoPerson(name: "Alice")
        let keys = try await maintainer.computeIndexKeys(for: person, id: Tuple([person.id]))

        // Check that rdf:type is in the keys
        let rdfTypeBytes = Array("rdf:type".utf8)
        let classIRIBytes = Array("ex:Person".utf8)

        let hasTypeTriple = keys.contains { key in
            containsSubsequence(key, rdfTypeBytes) && containsSubsequence(key, classIRIBytes)
        }
        #expect(hasTypeTriple, "Keys should contain rdf:type triple for ex:Person")
    }

    @Test("OWLTripleIndexMaintainer works for different @OWLClass types")
    func multipleTypes() async throws {
        let subspace = Subspace("test_owl")

        let personMaintainer = OWLTripleIndexMaintainer<OntoPerson>(
            subspace: subspace, graph: "test:default", prefix: "test"
        )
        let orgMaintainer = OWLTripleIndexMaintainer<OntoOrganization>(
            subspace: subspace, graph: "test:default", prefix: "test"
        )

        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let org = OntoOrganization(name: "Acme")

        let personKeys = try await personMaintainer.computeIndexKeys(for: person, id: Tuple([person.id]))
        let orgKeys = try await orgMaintainer.computeIndexKeys(for: org, id: Tuple([org.id]))

        // Person: 3 properties × 3 = 9
        #expect(personKeys.count == 9)
        // Organization: 2 properties × 3 = 6
        #expect(orgKeys.count == 6)

        // Verify different class IRIs
        let personClassBytes = Array("ex:Person".utf8)
        let orgClassBytes = Array("ex:Organization".utf8)

        #expect(personKeys.contains { containsSubsequence($0, personClassBytes) })
        #expect(orgKeys.contains { containsSubsequence($0, orgClassBytes) })
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
}
