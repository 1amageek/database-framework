import Core
import DatabaseValue
import QueryIR
import StorageKit
import Testing
@testable import DatabaseEngine
@testable import RankIndex

@Suite("Rank read result assembly")
struct RankReadResultAssemblerTests {
    @Test("Column sort expressions are preserved")
    func extractsColumnSortFields() throws {
        let fields = try RankReadResultAssembler.orderByFields(
            from: [
                SortKey(.column(ColumnRef(column: "score"))),
                SortKey(.column(ColumnRef(column: "id")))
            ]
        )

        #expect(fields == ["score", "id"])
    }

    @Test("Unsupported sort expressions fail instead of disappearing")
    func rejectsUnsupportedSortExpression() {
        #expect(throws: RankReadError.unsupportedSortExpression) {
            try RankReadResultAssembler.orderByFields(
                from: [SortKey(.literal(.int(1)))]
            )
        }
    }

    @Test("Every ranked key must resolve to a fetched entity")
    func rejectsMissingFetchedEntity() {
        let primaryKey = Tuple(Int64(7), "missing")

        #expect(
            throws: RankReadError.missingFetchedEntity(
                primaryKey: primaryKey.pack()
            )
        ) {
            try RankReadResultAssembler.assemble(
                rankedKeys: [(primaryKey: primaryKey, rank: 0)],
                entities: []
            )
        }
    }

    @Test("Fetched entities retain native rank order")
    func preservesRankOrder() throws {
        let first = RankReadEntity(id: "first", score: 20)
        let second = RankReadEntity(id: "second", score: 10)
        let entities = [
            PolymorphicEntity(
                item: second,
                typeName: RankReadEntity.persistableType,
                typeCode: 7
            ),
            PolymorphicEntity(
                item: first,
                typeName: RankReadEntity.persistableType,
                typeCode: 7
            )
        ]

        let results = try RankReadResultAssembler.assemble(
            rankedKeys: [
                (primaryKey: Tuple(Int64(7), "first"), rank: 0),
                (primaryKey: Tuple(Int64(7), "second"), rank: 1)
            ],
            entities: entities
        )

        let identifiers = results.map { $0.entity.item.id as? String }
        #expect(identifiers == ["first", "second"])
        #expect(results.map(\.rank) == [0, 1])
    }
}

private struct RankReadEntity: Persistable {
    typealias ID = String

    let id: String
    let score: Int64

    static let persistableType = "RankReadEntity"
    static let allFields = ["id", "score"]

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<RankReadEntity, Value>
    ) -> String {
        switch keyPath {
        case \RankReadEntity.id: return "id"
        case \RankReadEntity.score: return "score"
        default: return String(describing: keyPath)
        }
    }
}
