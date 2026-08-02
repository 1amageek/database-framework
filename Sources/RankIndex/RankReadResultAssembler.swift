import DatabaseTypes
import DatabaseKit
import DatabaseEngine
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
        entities: [PolymorphicEntity?]
    ) throws -> [(entity: PolymorphicEntity, rank: Int)] {
        guard rankedKeys.count == entities.count else {
            throw RankReadError.fetchedEntityCountMismatch(
                expected: rankedKeys.count,
                actual: entities.count
            )
        }

        var ordered: [(entity: PolymorphicEntity, rank: Int)] = []
        ordered.reserveCapacity(rankedKeys.count)
        for (rankedKey, entity) in zip(rankedKeys, entities) {
            guard let entity else {
                throw RankReadError.missingFetchedEntity(
                    primaryKey: rankedKey.primaryKey.pack()
                )
            }
            ordered.append((entity: entity, rank: rankedKey.rank))
        }
        return ordered
    }
}
