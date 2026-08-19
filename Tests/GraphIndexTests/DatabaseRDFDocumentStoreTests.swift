@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing

@testable import GraphIndex

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

        let context = container.testBaseContext()
        let firstPage = try await context.withExecutionTransaction(
            configuration: .batch
        ) { transaction in
            let revision = try await store.replace(
                identifier: "urn:calendar",
                auxiliaryIdentifiers: ["urn:z-import", "urn:a-import", "urn:z-import"],
                quads: [later, earlier, later],
                expectedRevision: 0,
                transaction: transaction.executionStorageAccess
            )
            #expect(revision == 1)
            return try await store.page(
                identifier: "urn:calendar",
                offset: 0,
                limit: 1,
                transaction: transaction.executionStorageAccess
            )
        }

        #expect(firstPage?.revision == 1)
        #expect(firstPage?.auxiliaryIdentifiers == ["urn:a-import", "urn:z-import"])
        #expect(firstPage?.totalQuadCount == 2)
        #expect(firstPage?.quads.count == 1)
        #expect(firstPage?.nextOffset == 1)

        let secondPage = try await context.withExecutionTransaction(
            configuration: .readOnly
        ) { transaction in
            try await store.page(
                identifier: "urn:calendar",
                offset: 1,
                limit: 1,
                transaction: transaction.executionStorageAccess
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
        let context = container.testBaseContext()
        try await context.withExecutionTransaction(configuration: .batch) { transaction in
            _ = try await store.replace(
                identifier: "urn:shapes",
                auxiliaryIdentifiers: [],
                quads: [try quad(subject: "urn:shape")],
                expectedRevision: nil,
                transaction: transaction.executionStorageAccess
            )
        }

        await #expect(throws: DatabaseRDFDocumentStoreError.self) {
            try await context.withExecutionTransaction(configuration: .batch) { transaction in
                _ = try await store.replace(
                    identifier: "urn:shapes",
                    auxiliaryIdentifiers: [],
                    quads: [],
                    expectedRevision: 9,
                    transaction: transaction.executionStorageAccess
                )
            }
        }

        let deletedRevision = try await context.withExecutionTransaction(
            configuration: .batch
        ) { transaction in
            try await store.delete(
                identifier: "urn:shapes",
                expectedRevision: 1,
                transaction: transaction.executionStorageAccess
            )
        }
        #expect(deletedRevision == 2)

        let page = try await context.withExecutionTransaction(
            configuration: .readOnly
        ) { transaction in
            try await store.page(
                identifier: "urn:shapes",
                offset: 0,
                limit: 10,
                transaction: transaction.executionStorageAccess
            )
        }
        #expect(page == nil)

        let recreatedRevision = try await context.withExecutionTransaction(
            configuration: .batch
        ) { transaction in
            try await store.replace(
                identifier: "urn:shapes",
                auxiliaryIdentifiers: [],
                quads: [try quad(subject: "urn:replacement")],
                expectedRevision: 2,
                transaction: transaction.executionStorageAccess
            )
        }
        #expect(recreatedRevision == 3)
    }

    @Test("version 1 server metadata is rejected")
    func rejectsLegacyServerMetadataVersion() async throws {
        let container = try await makeContainer()
        let store = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "ontology"
        )
        let identifier = "urn:legacy"
        let metadata = try StorageFrameEncoder.encode {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            encoder.writeUInt16(1)
            try encoder.writeString(identifier)
            encoder.writeUInt64(1)
            encoder.writeBool(false)
            encoder.writeUInt64(0)
            encoder.writeUInt64(0)
        }
        let context = container.testBaseContext()
        try await context.withExecutionTransaction(
            configuration: .batch
        ) { transaction in
            let metadataKey = try container.operationDataSubspace(
                relativePath: [
                    "database-framework",
                    "rdf-documents",
                    "ontology",
                    identifier,
                ]
            ).pack(Tuple("metadata"))
            try transaction.executionStorageAccess.setValue(
                metadata,
                for: metadataKey
            )
        }

        await #expect(throws: DatabaseRDFDocumentStoreError.self) {
            try await context.withExecutionTransaction(
                configuration: .readOnly
            ) { transaction in
                try await store.page(
                    identifier: identifier,
                    offset: 0,
                    limit: 1,
                    transaction: transaction.executionStorageAccess
                )
            }
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [try Player.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Player.self)
                ]
            ),
            security: .testingDisabled
        )
    }

    private func quad(subject: String) throws -> RDFQuad {
        try RDFQuad(
            validatingSubject: try RDFTerm.iri(validating: subject),
            predicate: try RDFTerm.iri(validating: "urn:predicate"),
            object: .literal(
                try .init(
                    lexicalForm: subject,
                    datatype: "http://www.w3.org/2001/XMLSchema#string"
                )
            ),
            graph: try RDFTerm.iri(validating: "urn:graph")
        )
    }
}
