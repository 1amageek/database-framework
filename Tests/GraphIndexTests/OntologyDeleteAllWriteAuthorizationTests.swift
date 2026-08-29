#if MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine
@testable import OntologyIndex

@Suite("Ontology delete-all write authorization")
struct OntologyDeleteAllWriteAuthorizationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    @Test("Write-only access deletes every ontology")
    func writeOnlyAccessDeletesEveryOntology() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let first = OWLOntology(iri: "https://example.com/ontology/first")
        let second = OWLOntology(iri: "https://example.com/ontology/second")
        let timestamp = Timestamp(secondsSinceUnixEpoch: 2_000)

        try await fixture.ownerContext.ontology.load(first, at: timestamp)
        try await fixture.ownerContext.ontology.load(second, at: timestamp)
        try await fixture.writerContext.ontology.deleteAll()
        #expect(try await fixture.ownerContext.ontology.list().isEmpty)
    }

    private func makeFixture() async throws -> (
        container: DBContainer,
        ownerContext: DatabaseContext,
        writerContext: DatabaseContext
    ) {
        let domainID = try DatabaseStorageDomain.ID(
            "ontology-delete-all-authorization"
        )
        let placementID = try Base.Placement.ID(
            "ontology-delete-all-authorization"
        )
        let baseID = try Base.ID("ontology-delete-all-authorization")
        let owner = Principal(identifier: "ontology-delete-all-owner")
        let writer = Principal(identifier: "ontology-delete-all-writer")
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    rootPath: ["tests", "ontology-delete-all"],
                    storageEngine: InMemoryEngine()
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
                name: "ontology-delete-all-authorization",
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
                        subject: .principal(writer.identifier),
                        resource: .base(baseID),
                        access: .write
                    ),
                ],
                expectedRevision: 0
            )
        } catch {
            await container.shutdown()
            throw error
        }
        return (
            container,
            container.session(
                authorization: .authenticated(owner)
            ).base(baseID).newContext(),
            container.session(
                authorization: .authenticated(writer)
            ).base(baseID).newContext()
        )
    }
}
#endif
