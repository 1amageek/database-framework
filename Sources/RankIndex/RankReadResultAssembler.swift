import Core
import DatabaseEngine
import QueryIR
import StorageKit

enum RankReadResultAssembler {
    static func orderByFields(from orderBy: [SortKey]?) throws -> [String]? {
        guard let orderBy else { return nil }

        var fields: [String] = []
        fields.reserveCapacity(orderBy.count)
        for sortKey in orderBy {
            guard case .column(let column) = sortKey.expression else {
                throw RankReadError.unsupportedSortExpression
            }
            fields.append(column.column)
        }
        return fields
    }

    static func assemble(
        rankedKeys: [(primaryKey: Tuple, rank: Int)],
        records: [PolymorphicRecord]
    ) throws -> [(record: PolymorphicRecord, rank: Int)] {
        var recordByID: [Bytes: PolymorphicRecord] = [:]
        recordByID.reserveCapacity(records.count)
        for record in records {
            let key = try recordKey(for: record)
            if let _ = recordByID[key] {
                throw RankReadError.duplicateFetchedRecord(primaryKey: key)
            }
            recordByID[key] = record
        }

        var ordered: [(record: PolymorphicRecord, rank: Int)] = []
        ordered.reserveCapacity(rankedKeys.count)
        for rankedKey in rankedKeys {
            let key = rankedKey.primaryKey.pack()
            guard let record = recordByID[key] else {
                throw RankReadError.missingFetchedRecord(primaryKey: key)
            }
            ordered.append((record: record, rank: rankedKey.rank))
        }
        return ordered
    }

    private static func recordKey(for record: PolymorphicRecord) throws -> Bytes {
        let identifier = try record.item.recordIdentifierTuple()
        return Tuple(record.typeCode).appending(identifier).pack()
    }
}
