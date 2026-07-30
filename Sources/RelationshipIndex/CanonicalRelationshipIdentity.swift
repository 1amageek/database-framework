import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum CanonicalRelationshipIdentity {
    static func resolve(
        _ identity: EntityReference,
        container: DBContainer
    ) throws -> (id: Tuple, partition: AnyDirectoryPath?) {
        guard let entity = container.schema.entity(named: identity.entity) else {
            throw RelationshipReferenceError.unknownRelatedEntity(identity.entity)
        }
        let id = try resolveIdentifier(
            identity,
            expectedType: entity.identifierType
        )
        let partition: AnyDirectoryPath?
        do {
            partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: entity,
                partitions: identity.partitions
            )
        } catch {
            throw RelationshipReferenceError.invalidTargetPartition(
                entity: identity.entity,
                reason: "partition does not match the compiled entity schema"
            )
        }
        return (id, partition)
    }

    private static func resolveIdentifier(
        _ identity: EntityReference,
        expectedType: PersistableIdentifierType
    ) throws(RelationshipReferenceError) -> Tuple {
        do {
            return try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: expectedType
            )
        } catch let error {
            throw .invalidTargetIdentifier(
                entity: identity.entity,
                reason: error
            )
        }
    }

}
