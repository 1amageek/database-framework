#if MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine
@testable import OntologyIndex

@Suite("Ontology Base write authorization")
struct OntologyBaseWriteAuthorizationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let ownerContext: DatabaseContext
        let readerContext: DatabaseContext
    }

    @Test("Ontology mutations require Base write access")
    func ontologyMutationsRequireBaseWriteAccess() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let first = OWLOntology(
            iri: "https://example.com/ontology/first",
            classes: [
                OWLClass(iri: "https://example.com/schema#First")
            ]
        )
        let second = OWLOntology(
            iri: "https://example.com/ontology/second",
            classes: [
                OWLClass(iri: "https://example.com/schema#Second")
            ]
        )
        let timestamp = Timestamp(secondsSinceUnixEpoch: 1_000)

        try await fixture.ownerContext.ontology.load(first, at: timestamp)
        try await fixture.ownerContext.ontology.loadAll(
            [second],
            at: timestamp
        )
        #expect(
            try await fixture.readerContext.ontology.get(iri: first.iri)
                != nil
        )
        #expect(
            Set(try await fixture.readerContext.ontology.list())
                == Set([first.iri, second.iri])
        )

        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.ontology.load(
                first,
                at: timestamp
            )
        }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.ontology.loadAll(
                [second],
                at: timestamp
            )
        }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.ontology.delete(iri: first.iri)
        }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.ontology.deleteAll()
        }

        #expect(
            Set(try await fixture.ownerContext.ontology.list())
                == Set([first.iri, second.iri])
        )
        try await fixture.ownerContext.ontology.deleteAll()
        #expect(try await fixture.ownerContext.ontology.list().isEmpty)
    }

    private func makeFixture() async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID(
            "ontology-write-authorization"
        )
        let placementID = try Base.Placement.ID(
            "ontology-write-authorization"
        )
        let baseID = try Base.ID("ontology-write-authorization")
        let owner = Principal(identifier: "ontology-owner")
        let reader = Principal(identifier: "ontology-reader")
        let engine = InMemoryEngine()
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    rootPath: ["tests", "ontology-write-authorization"],
                    storageEngine: engine
                )
            ],
            placements: [
                DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID
                )
            ],
            defaultPlacementID: placementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "ontology-write-authorization",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Anchor.self)
                ]
            ),
            security: .testingDisabled
        )
        do {
            _ = try await container.provisionBase(
                baseID,
                placementID: placementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(baseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(baseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
        } catch {
            await container.shutdown()
            throw error
        }
        let session = container.session(
            authorization: .authenticated(owner)
        )
        let readerSession = container.session(
            authorization: .authenticated(reader)
        )
        return Fixture(
            container: container,
            ownerContext: session.base(baseID).newContext(),
            readerContext: readerSession.base(baseID).newContext()
        )
    }
}
#endif
