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

    @Test("Every ranked key must resolve to a fetched record")
    func rejectsMissingFetchedRecord() {
        let primaryKey = Tuple(Int64(7), "missing")

        #expect(
            throws: RankReadError.missingFetchedRecord(
                primaryKey: primaryKey.pack()
            )
        ) {
            try RankReadResultAssembler.assemble(
                rankedKeys: [(primaryKey: primaryKey, rank: 0)],
                records: []
            )
        }
    }

    @Test("Fetched records retain native rank order")
    func preservesRankOrder() throws {
        let first = RankReadRecord(id: "first", score: 20)
        let second = RankReadRecord(id: "second", score: 10)
        let records = [
            PolymorphicRecord(
                item: second,
                typeName: RankReadRecord.persistableType,
                typeCode: 7
            ),
            PolymorphicRecord(
                item: first,
                typeName: RankReadRecord.persistableType,
                typeCode: 7
            )
        ]

        let results = try RankReadResultAssembler.assemble(
            rankedKeys: [
                (primaryKey: Tuple(Int64(7), "first"), rank: 0),
                (primaryKey: Tuple(Int64(7), "second"), rank: 1)
            ],
            records: records
        )

        let identifiers = results.map { $0.record.item.id as? String }
        #expect(identifiers == ["first", "second"])
        #expect(results.map(\.rank) == [0, 1])
    }
}

private struct RankReadRecord: Persistable {
    typealias ID = String

    let id: String
    let score: Int64

    static let persistableType = "RankReadRecord"
    static let allFields = ["id", "score"]

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<RankReadRecord, Value>
    ) -> String {
        switch keyPath {
        case \RankReadRecord.id: return "id"
        case \RankReadRecord.score: return "score"
        default: return String(describing: keyPath)
        }
    }
}
