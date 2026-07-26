import DatabaseEngine
import DatabaseTypes
import StorageKit

enum CanonicalRelationshipIdentity {
    static func resolve(
        _ identity: EntityReference,
        container: DBContainer
    ) throws -> (id: Tuple, partition: AnyDirectoryPath?) {
        guard container.schema.entity(named: identity.entity) != nil else {
            throw RelationshipReferenceError.unknownRelatedEntity(identity.entity)
        }
        guard let type = container.runtimeConfiguration.persistableTypes.type(
            named: identity.entity
        ) else {
            throw RelationshipReferenceError.relatedEntityHasNoCompiledType(
                identity.entity
            )
        }
        let id: Tuple
        do {
            id = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: type.persistableIdentifierType
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
