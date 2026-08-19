#if FOUNDATION_DB
import Testing
import TestHeartbeat
import DatabaseKit
import DatabaseTypes
import StorageKit
@testable import DatabaseEngine
@testable import VersionIndex

@Persistable
struct VersionIndexEntity {
    var id: String
    var title: String
}

func versionIndexDefinition(
    strategy: VersionHistoryStrategy
) -> IndexDefinition<FieldIdentity> {
    .history(
        version: FieldIdentity(name: "id", number: 1),
        retention: strategy
    )
}

@Suite("Version index provider tests", .heartbeat)
struct VersionIndexProviderTests {
    @Test("Provider creates a maintainer from canonical version metadata")
    func providerCreatesMaintainer() throws {
        let definition = versionIndexDefinition(strategy: .keepLast(3))
        #expect(
            definition
                == .history(
                    version: FieldIdentity(name: "id", number: 1),
                    retention: .keepLast(3)
                )
        )

        let index = try ResolvedIndex(
            for: VersionIndexEntity.self,
            name: "VersionIndexEntity_id",
            definition: definition,
            rootExpression: FieldKeyExpression(fieldName: "id")
        )
        let maintainer: any IndexMaintainer<VersionIndexEntity> = try
            VersionIndexMaintainerProvider().makeIndexMaintainer(
                index: index,
                subspace: Subspace(),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: [],
                wallClock: FixedVersionIndexWallClock(
                    now: Timestamp(secondsSinceUnixEpoch: 0)
                )
            )
        #expect(maintainer is VersionIndexMaintainer<VersionIndexEntity>)
    }
}

private struct FixedVersionIndexWallClock: WallClock {
    let now: Timestamp
}
#endif
