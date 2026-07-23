import Core
import DatabaseValue
import DatabaseEngine
import Rank
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
        let kind = RankIndexKind<UnsignedRankEntity, UInt64>(field: \.score)
        #expect(kind.scoreType == .uint64)

        let index = Index(
            name: "UnsignedRankEntity_rank_score",
            kind: kind,
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

        var keys: [Bytes] = []
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
        let kind = RankIndexKind<FloatingRankEntity, Double>(field: \.score)
        let index = Index(
            name: "FloatingRankEntity_rank_score",
            kind: kind,
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

        await #expect(throws: RankIndexError.self) {
            try await maintainer.computeIndexKeys(
                for: FloatingRankEntity(id: "nan", score: .nan),
                id: Tuple("nan")
            )
        }
    }
}

private struct UnsignedRankEntity: Persistable {
    typealias ID = String

    let id: String
    let score: UInt64

    static var persistableType: String { "UnsignedRankEntity" }
    static var allFields: [String] { ["id", "score"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": id
        case "score": score
        default: nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<UnsignedRankEntity, Value>
    ) -> String {
        switch keyPath {
        case \UnsignedRankEntity.id: "id"
        case \UnsignedRankEntity.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(
        for keyPath: PartialKeyPath<UnsignedRankEntity>
    ) -> String {
        switch keyPath {
        case \UnsignedRankEntity.id: "id"
        case \UnsignedRankEntity.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<UnsignedRankEntity> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}

private struct FloatingRankEntity: Persistable {
    typealias ID = String

    let id: String
    let score: Double

    static var persistableType: String { "FloatingRankEntity" }
    static var allFields: [String] { ["id", "score"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": id
        case "score": score
        default: nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<FloatingRankEntity, Value>
    ) -> String {
        switch keyPath {
        case \FloatingRankEntity.id: "id"
        case \FloatingRankEntity.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(
        for keyPath: PartialKeyPath<FloatingRankEntity>
    ) -> String {
        switch keyPath {
        case \FloatingRankEntity.id: "id"
        case \FloatingRankEntity.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<FloatingRankEntity> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}
