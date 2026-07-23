#if SQLITE
import Testing
import Database
import DatabaseValue
import Graph
import StorageKit
import TestHeartbeat

private enum OntologyPersistenceVocabulary {
    static let classBase = "https://test.example/ontology/"
    static let individualBase = "https://test.example/individual/"
    static let graph = "https://test.example/graph/ontology"
    static let label = "http://www.w3.org/2000/01/rdf-schema#label"
}

@Persistable
@OWLClass(
    "https://test.example/ontology/Organization",
    individualIRIBase: "https://test.example/individual/",
    graph: "https://test.example/graph/ontology"
)
struct OntoOrganization: Hashable {
    #Directory<OntoOrganization>("test", "ontology", "organizations")

    @OWLDataProperty("http://www.w3.org/2000/01/rdf-schema#label")
    var name: String = ""
}

@Persistable
@OWLClass(
    "https://test.example/ontology/Person",
    individualIRIBase: "https://test.example/individual/",
    graph: "https://test.example/graph/ontology"
)
struct OntoPerson: Hashable {
    #Directory<OntoPerson>("test", "ontology", "persons")

    @OWLDataProperty("http://www.w3.org/2000/01/rdf-schema#label")
    var name: String = ""

    @OWLDataProperty("https://test.example/ontology/email")
    var email: String = ""

    @OWLDataProperty(
        "https://test.example/ontology/memberOf",
        to: \OntoOrganization.id
    )
    var organizationID: String? = nil
}

@Persistable
struct PlainItem: Hashable {
    #Directory<PlainItem>("test", "ontology", "plain")

    var name: String = ""
}

private func containsSubsequence(
    _ haystack: [UInt8],
    _ needle: [UInt8]
) -> Bool {
    guard needle.count <= haystack.count else { return false }
    for offset in 0...(haystack.count - needle.count) {
        if haystack[offset..<(offset + needle.count)].elementsEqual(needle) {
            return true
        }
    }
    return false
}

private func findEntries(
    engine: any StorageEngine,
    containing term: DatabaseRDFTerm
) async throws -> [Bytes] {
    let needle = try DatabaseRDFTermCodec.encode(term).copyBytes()
    var matched: [Bytes] = []
    try await engine.withTransaction { transaction in
        for (key, _) in try await transaction.collectRange(
            from: .firstGreaterOrEqual([0x00]),
            to: .firstGreaterOrEqual([0xFF]),
            limit: 10_000,
            snapshot: true
        ) where containsSubsequence(key, needle) {
            matched.append(key)
        }
    }
    return matched
}

@Suite("OWLClass RDF Descriptor Tests", .heartbeat)
struct OWLClassRDFDescriptorTests {
    @Test("@OWLClass registers one canonical RDF projection")
    func generatedDescriptor() {
        let descriptors = OntoPerson._owlRDFDescriptors
        #expect(descriptors.count == 1)

        let index = descriptors[0] as? IndexDescriptor
        #expect(index?.name == "OntoPerson_owl_rdf")
        #expect(index?.kindIdentifier == "owl_class_rdf")
    }

    @Test("Record and RDF descriptors are merged")
    func descriptorsMerge() {
        let rdfIndex = OntoPerson.indexDescriptors.first {
            $0.kindIdentifier == "owl_class_rdf"
        }
        #expect(rdfIndex?.name == "OntoPerson_owl_rdf")
    }

    @Test("A plain record does not register an RDF projection")
    func plainEntityHasNoProjection() {
        #expect(
            PlainItem.indexDescriptors.allSatisfy {
                $0.kindIdentifier != "owl_class_rdf"
            }
        )
    }
}

@Suite("OWLClass RDF Index Maintainer Tests", .heartbeat)
struct OWLClassRDFIndexMaintainerTests {
    @Test("Each projected quad produces six canonical orderings")
    func keyCount() async throws {
        let maintainer = OWLClassRDFIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl")
        )
        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keys = try await maintainer.computeIndexKeys(
            for: person,
            id: Tuple([person.id])
        )

        #expect(keys.count == 18)
    }

    @Test("An empty string remains a valid xsd:string assertion")
    func emptyStringIsRetained() async throws {
        let maintainer = OWLClassRDFIndexMaintainer<OntoPerson>(
            subspace: Subspace("test_owl")
        )
        let person = OntoPerson(name: "", email: "noname@example.com")
        let keys = try await maintainer.computeIndexKeys(
            for: person,
            id: Tuple([person.id])
        )

        #expect(keys.count == 18)
    }

    @Test("The projection preserves typed RDF terms")
    func typedTerms() throws {
        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let quads = try person.ontologyQuads()

        #expect(quads.count == 3)
        #expect(quads.allSatisfy { $0.graph == .iri(OntologyPersistenceVocabulary.graph) })
        #expect(
            quads.contains {
                $0.predicate == OWLRDFVocabulary.rdfType
                    && $0.object == .iri(OntologyPersistenceVocabulary.classBase + "Person")
            }
        )
        #expect(
            quads.contains {
                $0.predicate == .iri(OntologyPersistenceVocabulary.label)
                    && $0.object == OWLRDFVocabulary.literal("Alice", datatype: "string")
            }
        )
    }

    @Test("Object properties project the target individual IRI")
    func objectProperty() throws {
        let organization = OntoOrganization(name: "Database Group")
        let person = OntoPerson(
            name: "Alice",
            email: "alice@example.com",
            organizationID: organization.id
        )
        let target = try organization.ontologySubject()
        let quads = try person.ontologyQuads()

        #expect(quads.count == 4)
        #expect(
            quads.contains {
                $0.predicate == .iri(OntologyPersistenceVocabulary.classBase + "memberOf")
                    && $0.object == target
            }
        )
    }
}

@Suite("OWLClass RDF SQLite Integration Tests", .heartbeat)
struct OWLClassRDFSQLiteIntegrationTests {
    private func makeContainer() async throws -> DBContainer {
        let schema = Schema(
            [OntoPerson.self, OntoOrganization.self, PlainItem.self],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.inMemory(for: schema, security: .disabled)
    }

    @Test("Saving a record atomically creates its RDF projection")
    func insertCreatesProjection() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        let person = OntoPerson(name: "Alice", email: "alice@example.com")

        context.insert(person)
        try await context.save()

        let subject = try person.ontologySubject()
        let entries = try await findEntries(
            engine: container.engine,
            containing: subject
        )
        #expect(entries.count == 18)
    }

    @Test("Updating a record replaces stale RDF assertions")
    func updateReplacesProjection() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        var person = OntoPerson(name: "Alice", email: "alice@example.com")

        context.insert(person)
        try await context.save()
        person.name = "Alice Smith"
        context.insert(person)
        try await context.save()

        let entries = try await findEntries(
            engine: container.engine,
            containing: person.ontologySubject()
        )
        let oldLiteral = try DatabaseRDFTermCodec.encode(
            OWLRDFVocabulary.literal("Alice", datatype: "string")
        ).copyBytes()
        let newLiteral = try DatabaseRDFTermCodec.encode(
            OWLRDFVocabulary.literal("Alice Smith", datatype: "string")
        ).copyBytes()

        #expect(entries.count == 18)
        #expect(entries.allSatisfy { !containsSubsequence($0, oldLiteral) })
        #expect(entries.contains { containsSubsequence($0, newLiteral) })
    }

    @Test("Deleting a record removes its RDF projection")
    func deleteRemovesProjection() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        let person = OntoPerson(name: "Bob", email: "bob@example.com")
        let subject = try person.ontologySubject()

        context.insert(person)
        try await context.save()
        #expect(
            try await !findEntries(
                engine: container.engine,
                containing: subject
            ).isEmpty
        )

        context.delete(person)
        try await context.save()

        #expect(
            try await findEntries(
                engine: container.engine,
                containing: subject
            ).isEmpty
        )
    }
}
#endif
