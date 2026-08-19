#if SQLITE
import Foundation
import Testing
import Database
import DatabaseTypes
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
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

    var id: String = Foundation.UUID().uuidString

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

    var id: String = Foundation.UUID().uuidString

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

    var id: String = Foundation.UUID().uuidString
    var name: String = ""
}

private func containsSubsequence(
    _ haystack: borrowing ByteString,
    _ needle: borrowing [UInt8]
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
    containing term: RDFTerm
) async throws -> [ByteString] {
    let needle = try RDFTermStorageFormat.encode(term).copyBytes()
    return try await engine.withTransaction { transaction in
        var matched: [ByteString] = []
        for (key, _) in try await transaction.collectRange(
            from: .firstGreaterOrEqual([0x00]),
            to: .firstGreaterOrEqual([0xFF]),
            limit: 10_000,
            snapshot: true
        ) where containsSubsequence(key, needle) {
            matched.append(key)
        }
        return matched
    }
}

@Suite("OWLClass RDF Descriptor Tests", .heartbeat)
struct OWLClassRDFDescriptorTests {
    @Test("@OWLClass registers one canonical RDF projection")
    func generatedDescriptor() throws {
        let descriptors = try OntoPerson._owlRDFIndexDescriptors
        #expect(descriptors.count == 1)

        #expect(descriptors[0].name == "OntoPerson_owl_rdf")
        #expect(descriptors[0].type == .graph(.ontologyProjection))
    }

    @Test("Entity and RDF descriptors are merged")
    func descriptorsMerge() throws {
        let rdfIndex = try OntoPerson.indexDescriptors.first {
            $0.type == .graph(.ontologyProjection)
        }
        #expect(rdfIndex?.name == "OntoPerson_owl_rdf")
    }

    @Test("A plain entity does not register an RDF projection")
    func plainEntityHasNoProjection() throws {
        #expect(
            try PlainItem.indexDescriptors.allSatisfy {
                $0.type != .graph(.ontologyProjection)
            }
        )
    }
}

@Suite("OWLClass RDF Index Maintainer Tests", .heartbeat)
struct OWLClassRDFIndexMaintainerTests {
    private func makeMaintainer() -> OWLClassRDFIndexMaintainer {
        OWLClassRDFIndexMaintainer(
            subspace: Subspace("test_owl"),
            entityName: OntoPerson.persistableType,
            classIRI: OntoPerson.ontologyClassIRI,
            individualIRIBase: OntoPerson.ontologyIndividualIRIBase,
            graph: OntoPerson.ontologyGraph,
            properties: OntoPerson.ontologyPropertyDescriptors
        )
    }

    @Test("Each projected quad produces six canonical orderings")
    func keyCount() async throws {
        let maintainer = makeMaintainer()
        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let keys = try await maintainer.computeIndexKeys(
            for: PersistedModel(person),
            id: Tuple([person.id])
        )

        #expect(keys.count == 18)
    }

    @Test("An empty string remains a valid xsd:string assertion")
    func emptyStringIsRetained() async throws {
        let maintainer = makeMaintainer()
        let person = OntoPerson(name: "", email: "noname@example.com")
        let keys = try await maintainer.computeIndexKeys(
            for: PersistedModel(person),
            id: Tuple([person.id])
        )

        #expect(keys.count == 18)
    }

    @Test("Canonical and compiled OWL projections are identical")
    func canonicalProjectionParity() throws {
        let organization = OntoOrganization(name: "Database Group")
        let person = OntoPerson(
            name: "Alice",
            email: "alice@example.com",
            organizationID: organization.id
        )

        let typed = try person.ontologyQuads()
        let canonical = try makeMaintainer().projectedQuads(
            for: PersistedModel(person)
        )

        #expect(canonical == typed)
    }

    @Test("The projection preserves typed RDF terms")
    func typedTerms() throws {
        let person = OntoPerson(name: "Alice", email: "alice@example.com")
        let quads = try person.ontologyQuads()
        let graph = try RDFGraphName(iri: OntologyPersistenceVocabulary.graph)
        let personClass = RDFTerm.iri(
            try RDFIRI(OntologyPersistenceVocabulary.classBase + "Person")
        )
        let label = try RDFPredicateIRI(OntologyPersistenceVocabulary.label)
        let rdfType = try OWLRDFVocabulary.rdfType

        #expect(quads.count == 3)
        #expect(quads.allSatisfy { $0.graph == graph })
        #expect(
            quads.contains {
                $0.predicate == rdfType
                    && $0.object == personClass
            }
        )
        #expect(
            quads.contains {
                $0.predicate == label
                    && $0.object == OWLRDFVocabulary.literal("Alice", datatype: .string)
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
        let memberOf = try RDFPredicateIRI(
            OntologyPersistenceVocabulary.classBase + "memberOf"
        )

        #expect(quads.count == 4)
        let hasMembership = quads.contains { quad in
            quad.predicate == memberOf && quad.object == target.term
        }
        #expect(hasMembership)
    }
}

@Suite("OWLClass RDF SQLite Integration Tests", .heartbeat)
struct OWLClassRDFSQLiteIntegrationTests {
    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [
                try OntoPerson.schemaEntity,
                try OntoOrganization.schemaEntity,
                try PlainItem.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(OntoPerson.self), try DatabaseFrameworkRuntime.entity(OntoOrganization.self), try DatabaseFrameworkRuntime.entity(PlainItem.self),
                ]
            ),
            security: .testingDisabled
        )
    }

    @Test("Saving an entity atomically creates its RDF projection")
    func insertCreatesProjection() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let person = OntoPerson(name: "Alice", email: "alice@example.com")

        try context.insert(person)
        try await context.save()

        let subject = try person.ontologySubject()
        let entries = try await findEntries(
            engine: container.engine,
            containing: subject.term
        )
        #expect(entries.count == 18)
    }

    @Test("Updating an entity replaces stale RDF assertions")
    func updateReplacesProjection() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        var person = OntoPerson(name: "Alice", email: "alice@example.com")

        try context.insert(person)
        try await context.save()
        person.name = "Alice Smith"
        try context.update(person)
        try await context.save()

        let entries = try await findEntries(
            engine: container.engine,
            containing: person.ontologySubject().term
        )
        let oldLiteral = try RDFTermStorageFormat.encode(
            OWLRDFVocabulary.literal("Alice", datatype: .string)
        ).copyBytes()
        let newLiteral = try RDFTermStorageFormat.encode(
            OWLRDFVocabulary.literal("Alice Smith", datatype: .string)
        ).copyBytes()

        #expect(entries.count == 18)
        #expect(entries.allSatisfy { !containsSubsequence($0, oldLiteral) })
        #expect(entries.contains { containsSubsequence($0, newLiteral) })
    }

    @Test("Deleting an entity removes its RDF projection")
    func deleteRemovesProjection() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let person = OntoPerson(name: "Bob", email: "bob@example.com")
        let subject = try person.ontologySubject()

        try context.insert(person)
        try await context.save()
        #expect(
            try await !findEntries(
                engine: container.engine,
                containing: subject.term
            ).isEmpty
        )

        try context.delete(person)
        try await context.save()

        #expect(
            try await findEntries(
                engine: container.engine,
                containing: subject.term
            ).isEmpty
        )
    }
}
#endif
