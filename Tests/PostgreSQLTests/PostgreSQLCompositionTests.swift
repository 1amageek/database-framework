#if POSTGRESQL && MultipleBases
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import PostgreSQLStorage
import StorageKit
import TestSupport
import Testing

@Persistable
private struct PostgreSQLCompositionItem {
    var id: String = ""
    var rank: Int64 = 0
}

@Suite(
    "PostgreSQL Composition domains",
    .serialized,
    .heartbeat,
    .enabled(if: PostgreSQLScenarioCoordinator.isConfigured)
)
struct PostgreSQLCompositionTests {
    @Test("Composition reads two independently owned PostgreSQL domains")
    func readsAcrossTwoPostgreSQLDomains() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let controlDomainID = try DatabaseStorageDomain.ID("pg-control")
            let secondaryDomainID = try DatabaseStorageDomain.ID("pg-secondary")
            let primaryPlacementID = try Base.Placement.ID("pg-primary")
            let secondaryPlacementID = try Base.Placement.ID("pg-secondary")
            let controlEngine = try await PostgreSQLScenarioCoordinator.shared.engine
            let secondaryEngine = try await PostgreSQLScenarioCoordinator.shared.engine
            let topology = try DatabaseStorageTopology(
                controlDomainID: controlDomainID,
                domains: [
                    try DatabaseStorageDomain(
                        id: controlDomainID,
                        namespacePath: ["composition", "control"],
                        storageEngine: controlEngine
                    ),
                    try DatabaseStorageDomain(
                        id: secondaryDomainID,
                        namespacePath: ["composition", "secondary"],
                        storageEngine: secondaryEngine
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
                    entities: [try PostgreSQLCompositionItem.schemaEntity],
                    version: Schema.Version(1, 0, 0)
                ),
                configuration: DBConfiguration(
                    name: "postgresql-cross-domain",
                    storageTopology: topology,
                    monotonicClock: TestProcessMonotonicClock(),
                    wallClock: FixedTestWallClock()
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            PostgreSQLCompositionItem.self
                        ),
                    ]
                ),
                security: .testingDisabled
            )
            defer { await container.shutdown() }
            let principal = Principal(identifier: "postgresql-composition")
            let authorization = AuthorizationContext.authenticated(principal)
            let primaryBaseID = try Base.ID("pg-company-a")
            let secondaryBaseID = try Base.ID("pg-company-b")
            for (baseID, placementID) in [
                (primaryBaseID, primaryPlacementID),
                (secondaryBaseID, secondaryPlacementID),
            ] {
                _ = try await container.provisionBase(
                    baseID,
                    placementID: placementID,
                    initialGrants: [
                        Security.Grant(
                            subject: .principal(principal.identifier),
                            resource: .base(baseID),
                            access: .all
                        ),
                    ],
                    expectedRevision: 0
                )
            }
            let compositionID = try Base.Composition.ID("pg-shared")
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
            for (baseID, rank) in [
                (primaryBaseID, Int64(2)),
                (secondaryBaseID, Int64(1)),
            ] {
                let context = container.session(authorization: authorization)
                    .base(baseID).newContext()
                try context.insert(
                    PostgreSQLCompositionItem(id: "same", rank: rank)
                )
                try await context.save()
            }

            let results = try await container.session(
                authorization: authorization
            ).composition(compositionID)
                .query(PostgreSQLCompositionItem.self)
                .orderBy(#field(\PostgreSQLCompositionItem.rank))
                .execute()

            #expect(results.map(\.value.rank) == [1, 2])
            #expect(Set(results.map(\.origin)).count == 2)
        }
    }
}
#endif
