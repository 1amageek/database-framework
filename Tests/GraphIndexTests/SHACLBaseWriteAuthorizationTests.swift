#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("SHACL Base write authorization")
struct SHACLBaseWriteAuthorizationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    @Test("SHACL mutations require write access")
    func shaclMutationsRequireWriteAccess() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let first = SHACLShapesGraph(
            iri: "https://example.com/shapes/first",
            shapes: []
        )
        let second = SHACLShapesGraph(
            iri: "https://example.com/shapes/second",
            shapes: []
        )

        try await fixture.writerContext.shacl.loadShapes(first)
        try await fixture.writerContext.shacl.loadShapes(second)
        #expect(
            Set(try await fixture.readerContext.shacl.listShapesGraphs())
                == Set([first.iri, second.iri])
        )
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.shacl.loadShapes(first)
        }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.shacl.deleteShapesGraph(
                iri: first.iri
            )
        }
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await fixture.readerContext.shacl.deleteAllShapesGraphs()
        }

        try await fixture.writerContext.shacl.deleteShapesGraph(iri: first.iri)
        try await fixture.writerContext.shacl.deleteAllShapesGraphs()
        #expect(try await fixture.readerContext.shacl.listShapesGraphs().isEmpty)
    }

    private func makeFixture() async throws -> (
        container: DBContainer,
        readerContext: DatabaseContext,
        writerContext: DatabaseContext
    ) {
        let domainID = try DatabaseStorageDomain.ID(
            "shacl-write-authorization"
        )
        let placementID = try Base.Placement.ID(
            "shacl-write-authorization"
        )
        let baseID = try Base.ID("shacl-write-authorization")
        let owner = Principal(identifier: "shacl-owner")
        let reader = Principal(identifier: "shacl-reader")
        let writer = Principal(identifier: "shacl-writer")
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["tests", "shacl-write-authorization"],
                    storageEngine: InMemoryEngine()
                )
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
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
                name: "shacl-write-authorization",
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
                authorization: .authenticated(reader)
            ).base(baseID).newContext(),
            container.session(
                authorization: .authenticated(writer)
            ).base(baseID).newContext()
        )
    }
}
#endif
