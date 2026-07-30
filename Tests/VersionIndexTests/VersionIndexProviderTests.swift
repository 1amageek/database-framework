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

func versionIndexMetadata(
    strategy: VersionHistoryStrategy
) -> IndexKindMetadata {
    let strategyMetadata: [String: FieldValue]
    switch strategy {
    case .keepAll:
        strategyMetadata = ["strategy": .string("keepAll")]
    case .keepLast(let count):
        strategyMetadata = [
            "strategy": .string("keepLast"),
            "strategyCount": .int64(Int64(count)),
        ]
    case .keepForDuration(let duration):
        strategyMetadata = [
            "strategy": .string("keepForDuration"),
            "strategyDuration": .timeSpan(duration),
        ]
    }

    return IndexKindMetadata(
        identifier: "version",
        subspaceStructure: .hierarchical,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: "id", number: 1)
            )
        ],
        metadata: strategyMetadata
    )
}

@Suite("Version index provider tests", .heartbeat)
struct VersionIndexProviderTests {
    @Test("Provider creates a maintainer from canonical version metadata")
    func providerCreatesMaintainer() throws {
        let metadata = versionIndexMetadata(strategy: .keepLast(3))
        let definition = try IndexDefinition(metadata: metadata)
        #expect(definition == .version(strategy: .keepLast(3)))

        let index = Index(
            name: "VersionIndexEntity_id",
            kind: metadata,
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
