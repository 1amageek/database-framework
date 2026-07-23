import Core
import DatabaseRuntime
import DatabaseEngine
import DatabaseServer
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical RDF document store", .serialized)
struct DatabaseRDFDocumentStoreTests {
    @Test("replacement is canonical, paged, and revisioned")
    func replacementIsCanonicalAndPaged() async throws {
        let container = try await makeContainer()
        let store = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "ontology"
        )
        let later = try quad(subject: "urn:z")
        let earlier = try quad(subject: "urn:a")

        let firstPage = try await container.engine.withTransaction(
            configuration: .batch
        ) { transaction in
            let revision = try await store.replace(
                identifier: "urn:calendar",
                auxiliaryIdentifiers: ["urn:z-import", "urn:a-import", "urn:z-import"],
                quads: [later, earlier, later],
                expectedRevision: 0,
                transaction: transaction
            )
            #expect(revision == 1)
            return try await store.page(
                identifier: "urn:calendar",
                offset: 0,
                limit: 1,
                transaction: transaction
            )
        }

        #expect(firstPage?.revision == 1)
        #expect(firstPage?.auxiliaryIdentifiers == ["urn:a-import", "urn:z-import"])
        #expect(firstPage?.totalQuadCount == 2)
        #expect(firstPage?.quads.count == 1)
        #expect(firstPage?.nextOffset == 1)

        let secondPage = try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await store.page(
                identifier: "urn:calendar",
                offset: 1,
                limit: 1,
                transaction: transaction
            )
        }
        #expect(secondPage?.quads.count == 1)
        #expect(secondPage?.nextOffset == nil)
        #expect(Set((firstPage?.quads ?? []) + (secondPage?.quads ?? [])) == [earlier, later])
    }

    @Test("revision conflicts and tombstones are deterministic")
    func revisionConflictAndTombstone() async throws {
        let container = try await makeContainer()
        let store = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "shacl"
        )
        try await container.engine.withTransaction(configuration: .batch) { transaction in
            _ = try await store.replace(
                identifier: "urn:shapes",
                auxiliaryIdentifiers: [],
                quads: [try quad(subject: "urn:shape")],
                expectedRevision: nil,
                transaction: transaction
            )
        }

        await #expect(throws: DatabaseRDFDocumentStoreError.self) {
            try await container.engine.withTransaction(configuration: .batch) { transaction in
                _ = try await store.replace(
                    identifier: "urn:shapes",
                    auxiliaryIdentifiers: [],
                    quads: [],
                    expectedRevision: 9,
                    transaction: transaction
                )
            }
        }

        let deletedRevision = try await container.engine.withTransaction(
            configuration: .batch
        ) { transaction in
            try await store.delete(
                identifier: "urn:shapes",
                expectedRevision: 1,
                transaction: transaction
            )
        }
        #expect(deletedRevision == 2)

        let page = try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await store.page(
                identifier: "urn:shapes",
                offset: 0,
                limit: 10,
                transaction: transaction
            )
        }
        #expect(page == nil)

        let recreatedRevision = try await container.engine.withTransaction(
            configuration: .batch
        ) { transaction in
            try await store.replace(
                identifier: "urn:shapes",
                auxiliaryIdentifiers: [],
                quads: [try quad(subject: "urn:replacement")],
                expectedRevision: 2,
                transaction: transaction
            )
        }
        #expect(recreatedRevision == 3)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: Schema(
                [DatabaseEndpointRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func quad(subject: String) throws -> DatabaseRDFQuad {
        try DatabaseRDFQuad(
            subject: .iri(subject),
            predicate: .iri("urn:predicate"),
            object: .literal(
                try .init(
                    lexicalForm: subject,
                    datatype: "http://www.w3.org/2001/XMLSchema#string"
                )
            ),
            graph: .iri("urn:graph")
        )
    }
}
