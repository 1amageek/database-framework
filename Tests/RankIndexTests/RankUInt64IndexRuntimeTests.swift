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
        let kind = RankIndexKind<UnsignedRankRecord, UInt64>(field: \.score)
        #expect(kind.scoreType == .uint64)

        let index = Index(
            name: "UnsignedRankRecord_rank_score",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "UnsignedRankRecord_rank_score",
            itemTypes: [UnsignedRankRecord.persistableType]
        )
        let indexSubspace = Subspace(
            prefix: Tuple("rank-uint64-runtime").pack()
        )
        let maintainer: any IndexMaintainer<UnsignedRankRecord> = try RankIndexMaintainerProvider()
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
            let record = UnsignedRankRecord(id: "record-\(offset)", score: score)
            let computed = try await maintainer.computeIndexKeys(
                for: record,
                id: Tuple(record.id)
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
        let kind = RankIndexKind<FloatingRankRecord, Double>(field: \.score)
        let index = Index(
            name: "FloatingRankRecord_rank_score",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "score"),
            subspaceKey: "FloatingRankRecord_rank_score",
            itemTypes: [FloatingRankRecord.persistableType]
        )
        let maintainer: any IndexMaintainer<FloatingRankRecord> = try RankIndexMaintainerProvider()
            .makeIndexMaintainer(
                index: index,
                subspace: Subspace(prefix: Tuple("rank-nan-runtime").pack()),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )

        await #expect(throws: RankIndexError.self) {
            try await maintainer.computeIndexKeys(
                for: FloatingRankRecord(id: "nan", score: .nan),
                id: Tuple("nan")
            )
        }
    }
}

private struct UnsignedRankRecord: Persistable {
    typealias ID = String

    let id: String
    let score: UInt64

    static var persistableType: String { "UnsignedRankRecord" }
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
        for keyPath: KeyPath<UnsignedRankRecord, Value>
    ) -> String {
        switch keyPath {
        case \UnsignedRankRecord.id: "id"
        case \UnsignedRankRecord.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(
        for keyPath: PartialKeyPath<UnsignedRankRecord>
    ) -> String {
        switch keyPath {
        case \UnsignedRankRecord.id: "id"
        case \UnsignedRankRecord.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<UnsignedRankRecord> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}

private struct FloatingRankRecord: Persistable {
    typealias ID = String

    let id: String
    let score: Double

    static var persistableType: String { "FloatingRankRecord" }
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
        for keyPath: KeyPath<FloatingRankRecord, Value>
    ) -> String {
        switch keyPath {
        case \FloatingRankRecord.id: "id"
        case \FloatingRankRecord.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(
        for keyPath: PartialKeyPath<FloatingRankRecord>
    ) -> String {
        switch keyPath {
        case \FloatingRankRecord.id: "id"
        case \FloatingRankRecord.score: "score"
        default: "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<FloatingRankRecord> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}
