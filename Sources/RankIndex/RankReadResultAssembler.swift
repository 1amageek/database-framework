import DatabaseKit
import DatabaseEngine
import DatabaseKit
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
        entities: [PolymorphicEntity]
    ) throws -> [(entity: PolymorphicEntity, rank: Int)] {
        var entityByID: [Bytes: PolymorphicEntity] = [:]
        entityByID.reserveCapacity(entities.count)
        for entity in entities {
            let key = try entityKey(for: entity)
            if let _ = entityByID[key] {
                throw RankReadError.duplicateFetchedEntity(primaryKey: key)
            }
            entityByID[key] = entity
        }

        var ordered: [(entity: PolymorphicEntity, rank: Int)] = []
        ordered.reserveCapacity(rankedKeys.count)
        for rankedKey in rankedKeys {
            let key = rankedKey.primaryKey.pack()
            guard let entity = entityByID[key] else {
                throw RankReadError.missingFetchedEntity(primaryKey: key)
            }
            ordered.append((entity: entity, rank: rankedKey.rank))
        }
        return ordered
    }

    private static func entityKey(for entity: PolymorphicEntity) throws -> Bytes {
        let identifier = try entity.item.persistableIdentifierTuple()
        return Tuple(entity.typeCode).appending(identifier).pack()
    }
}
