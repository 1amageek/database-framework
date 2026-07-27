import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit
import Testing
@testable import RankIndex

@Suite("Rank UInt64 index runtime")
struct RankUInt64IndexRuntimeTests {
    @Test("Provider builds UInt64 runtime and emits exact ordered score keys")
    func providerBuildsFullWidthRuntime() async throws {
        let scores: [UInt64] = [
            UInt64(Int64.max),
            UInt64(Int64.max) + 1,
            UInt64.max,
        ]
        let index = Index(
            name: "UnsignedRankEntity_rank_score",
            kind: rankIndexMetadata(scoreType: .uint64),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "UnsignedRankEntity_rank_score",
            itemTypes: [UnsignedRankEntity.persistableType]
        )
        let indexSubspace = Subspace(
            prefix: Tuple("rank-uint64-runtime").pack()
        )
        let maintainer: any IndexMaintainer<UnsignedRankEntity> = try RankIndexMaintainerProvider()
            .makeIndexMaintainer(
                index: index,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )
        let scoresSubspace = indexSubspace.subspace("scores")

        var keys: [ByteString] = []
        keys.reserveCapacity(scores.count)
        for (offset, score) in scores.enumerated() {
            let entity = UnsignedRankEntity(id: "entity-\(offset)", score: score)
            let computed = try await maintainer.computeIndexKeys(
                for: entity,
                id: Tuple(entity.id)
            )
            #expect(computed.count == 1)
            let key = try #require(computed.first)
            let unpacked = try scoresSubspace.unpack(key)
            let decodedScore = try TupleDecoder.decode(
                unpacked.element(at: 0),
                as: UInt64.self
            )

            #expect(decodedScore == score)
            keys.append(key)
        }

        #expect(keys[0].lexicographicallyPrecedes(keys[1]))
        #expect(keys[1].lexicographicallyPrecedes(keys[2]))
    }

    @Test("Rank runtime rejects NaN before emitting an unordered key")
    func rejectsNaNScore() async throws {
        let index = Index(
            name: "FloatingRankEntity_rank_score",
            kind: rankIndexMetadata(scoreType: .float64),
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "FloatingRankEntity_rank_score",
            itemTypes: [FloatingRankEntity.persistableType]
        )
        let maintainer: any IndexMaintainer<FloatingRankEntity> = try RankIndexMaintainerProvider()
            .makeIndexMaintainer(
                index: index,
                subspace: Subspace(prefix: Tuple("rank-nan-runtime").pack()),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )

        await #expect(
            throws: RankIndexMaintenanceError.unorderedFloatingPoint(
                indexName: "FloatingRankEntity_rank_score"
            )
        ) {
            try await maintainer.computeIndexKeys(
                for: FloatingRankEntity(id: "nan", score: .nan),
                id: Tuple("nan")
            )
        }
    }
}

@Persistable
private struct UnsignedRankEntity {
    let id: String
    let score: UInt64
}

@Persistable
private struct FloatingRankEntity {
    let id: String
    let score: Double
}
