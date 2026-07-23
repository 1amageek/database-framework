import DatabaseEngine
import DatabaseValue
import StorageKit

enum CanonicalRelationshipIdentity {
    static func resolve(
        _ identity: RecordIdentity,
        container: DBContainer
    ) throws -> (id: Tuple, partition: AnyDirectoryPath?) {
        guard let entity = container.schema.entity(named: identity.entity),
              let type = entity.persistableType else {
            throw RelationshipReferenceError.unknownRelatedEntity(identity.entity)
        }
        let id: Tuple
        do {
            id = try RecordIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: type.recordIdentifierType
            )
        } catch let error {
            throw RelationshipReferenceError.invalidTargetIdentifier(
                entity: identity.entity,
                reason: error
            )
        }
        let partition: AnyDirectoryPath?
        do {
            partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: type,
                partitions: identity.partitions
            )
        } catch {
            throw RelationshipReferenceError.invalidTargetPartition(
                entity: identity.entity,
                reason: String(describing: error)
            )
        }
        return (id, partition)
    }

}
