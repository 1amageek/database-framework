#if MultipleBases
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Composition typed query execution")
struct CompositionQueryExecutorTests {
    @Persistable
    struct Item {
        var id: String = ""
        var rank: Int64 = 0
    }

    @Test("Global ordering, windowing, and provenance span domains")
    func globalOrderingWindowAndProvenance() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1), ("primary", 3)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2), ("secondary", 4)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .offset(1)
            .limit(2)
            .execute()

        #expect(results.map(\.value.rank) == [2, 3])
        #expect(
            results.map(\.origin) == [
                .source(fixture.secondaryBaseID),
                .source(fixture.primaryBaseID),
            ]
        )
        #expect(results.allSatisfy {
            $0.compositionID == fixture.compositionID && $0.generation == 1
        })

        let count = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .offset(1)
            .limit(2)
            .count()
        #expect(count == 2)
    }

    @Test("Equal logical IDs remain Base-qualified")
    func equalLogicalIDsRemainBaseQualified() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: true)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )
        try await insert(
            [("shared", 2)],
            baseID: fixture.secondaryBaseID,
            fixture: fixture
        )

        let results = try await fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(fixture.compositionID)
            .query(Item.self)
            .orderBy(#field(\Item.rank))
            .execute()

        #expect(results.map(\.value.id) == ["shared", "shared"])
        #expect(Set(results.map(\.origin)).count == 2)
    }

    @Test("Every member is authorized before results are exposed")
    func everyMemberMustBeAuthorized() async throws {
        let fixture = try await makeFixture(readerCanReadSecondary: false)
        defer { await fixture.container.shutdown() }
        try await insert(
            [("visible-only-if-composition-were-partial", 1)],
            baseID: fixture.primaryBaseID,
            fixture: fixture
        )

        await #expect(throws: DatabaseCompositionAccessError.self) {
            try await fixture.container.session(
                authorization: fixture.readerAuthorization
            ).composition(fixture.compositionID)
                .query(Item.self)
                .execute()
        }
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let primaryBaseID: Base.ID
        let secondaryBaseID: Base.ID
        let compositionID: Base.Composition.ID
        let ownerAuthorization: AuthorizationContext
        let readerAuthorization: AuthorizationContext
    }

    private func makeFixture(
        readerCanReadSecondary: Bool
    ) async throws -> Fixture {
        let controlDomainID = try DatabaseStorageDomain.ID("control")
        let secondaryDomainID = try DatabaseStorageDomain.ID("secondary")
        let primaryPlacementID = try Base.Placement.ID("primary")
        let secondaryPlacementID = try Base.Placement.ID("secondary")
        let primaryBaseID = try Base.ID("company-a")
        let secondaryBaseID = try Base.ID("company-b")
        let compositionID = try Base.Composition.ID("shared")
        let owner = Principal(identifier: "owner")
        let reader = Principal(identifier: "reader")
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: [
                try DatabaseStorageDomain(
                    id: controlDomainID,
                    namespacePath: ["tests", "composition", "control"],
                    storageEngine: InMemoryEngine()
                ),
                try DatabaseStorageDomain(
                    id: secondaryDomainID,
                    namespacePath: ["tests", "composition", "secondary"],
                    storageEngine: InMemoryEngine()
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: primaryPlacementID,
                    domainID: controlDomainID,
                    path: ["bases"]
                ),
                try DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: secondaryDomainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: primaryPlacementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Item.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-typed-query",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Item.self),
                ]
            ),
            security: .testingDisabled
        )
        let ownerAuthorization: AuthorizationContext = .authenticated(owner)
        let readerAuthorization: AuthorizationContext = .authenticated(reader)
        do {
            _ = try await container.provisionBase(
                primaryBaseID,
                placementID: primaryPlacementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(primaryBaseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(primaryBaseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
            var secondaryGrants = [
                Security.Grant(
                    subject: .principal(owner.identifier),
                    resource: .base(secondaryBaseID),
                    access: .all
                ),
            ]
            if readerCanReadSecondary {
                secondaryGrants.append(
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(secondaryBaseID),
                        access: .read
                    )
                )
            }
            _ = try await container.provisionBase(
                secondaryBaseID,
                placementID: secondaryPlacementID,
                initialGrants: secondaryGrants,
                expectedRevision: 0
            )
            _ = try await container.withControlMetadataTransaction {
                transaction in
                try await container.compositionCatalog.create(
                    try Base.Composition(
                        id: compositionID,
                        bases: [primaryBaseID, secondaryBaseID]
                    ),
                    expectedRevision: 0,
                    transaction: transaction.storageAccess
                )
            }
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            primaryBaseID: primaryBaseID,
            secondaryBaseID: secondaryBaseID,
            compositionID: compositionID,
            ownerAuthorization: ownerAuthorization,
            readerAuthorization: readerAuthorization
        )
    }

    private func insert(
        _ values: [(String, Int64)],
        baseID: Base.ID,
        fixture: Fixture
    ) async throws {
        let context = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(baseID).newContext()
        for value in values {
            var item = Item()
            item.id = value.0
            item.rank = value.1
            try context.insert(item)
        }
        try await context.save()
    }
}
#endif
