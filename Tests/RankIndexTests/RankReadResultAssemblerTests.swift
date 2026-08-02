import DatabaseKit
import DatabaseTypes
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
                entities: [nil]
            )
        }
    }

    @Test("Transaction-ordered entities retain native rank order")
    func preservesRankOrder() throws {
        let first = RankReadEntity(id: "first", score: 20)
        let second = RankReadEntity(id: "second", score: 10)
        let typeCode = RankReadEntity.typeCode(
            for: RankReadEntity.persistableType
        )
        let entities = [
            PolymorphicEntity(
                item: try PersistedModel(first),
                typeName: RankReadEntity.persistableType,
                typeCode: typeCode,
                polymorphicIdentifier: Tuple(typeCode, first.id)
            ),
            PolymorphicEntity(
                item: try PersistedModel(second),
                typeName: RankReadEntity.persistableType,
                typeCode: typeCode,
                polymorphicIdentifier: Tuple(typeCode, second.id)
            )
        ]

        let results = try RankReadResultAssembler.assemble(
            rankedKeys: [
                (primaryKey: Tuple(typeCode, "first"), rank: 0),
                (primaryKey: Tuple(typeCode, "second"), rank: 1)
            ],
            entities: entities
        )

        let identifiers = try results.map {
            try $0.entity.item.decode(as: RankReadEntity.self).id
        }
        #expect(identifiers == ["first", "second"])
        #expect(results.map { $0.rank } == [0, 1])
    }

    @Test("Fetch result count mismatch fails explicitly")
    func rejectsMismatchedFetchCount() {
        #expect(
            throws: RankReadError.fetchedEntityCountMismatch(
                expected: 1,
                actual: 0
            )
        ) {
            try RankReadResultAssembler.assemble(
                rankedKeys: [(primaryKey: Tuple("missing"), rank: 0)],
                entities: []
            )
        }
    }
}

@Polymorphable(identifier: "RankReadEntity")
@PolymorphicDirectory("rank-read-entity")
private protocol RankReadModel:
    Polymorphable<RankReadModelPolymorphicGroup>
{
    var id: String { get }
    var score: Int64 { get }
}

@Persistable
private struct RankReadEntity: RankReadModel {
    let id: String
    let score: Int64
}
