import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

struct ResolvedEntityReference: Sendable {
    let identity: EntityReference
    let persistableType: any Persistable.Type
    let id: Tuple
    let partition: AnyDirectoryPath?
    let partitionPath: [String]

    static func resolve(
        _ identity: EntityReference,
        container: DBContainer,
        model: (any Persistable)? = nil
    ) throws -> Self {
        guard let entity = container.schema.entities.first(where: { $0.name == identity.entity }) else {
            throw DatabaseMutationError.unknownEntity(identity.entity)
        }
        guard let persistableType = container.runtimeConfiguration
            .persistableTypes.type(named: entity.name) else {
            throw DatabaseMutationError.entityHasNoPersistableType(identity.entity)
        }
        let id: Tuple
        do {
            id = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: persistableType.persistableIdentifierType
            )
        } catch {
            throw DatabaseMutationError.invalidPersistableIdentifier(
                entity: identity.entity,
                reason: String(describing: error)
            )
        }
        if let model {
            let modelType = type(of: model).persistableType
            guard modelType == identity.entity else {
                throw DatabaseMutationError.entityTypeMismatch(
                    expected: identity.entity,
                    actual: modelType
                )
            }
            let encodedIdentity: EntityReference
            do {
                encodedIdentity = try EntityReferenceEncoder.encode(model)
            } catch {
                throw DatabaseMutationError.persistableIdentityMismatch(
                    identity
                )
            }
            guard encodedIdentity == identity else {
                throw DatabaseMutationError.persistableIdentityMismatch(identity)
            }
        }

        let partition: AnyDirectoryPath?
        do {
            partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: persistableType,
                partitions: identity.partitions
            )
        } catch CanonicalReadError.invalidPartition(_, let reason) {
            throw DatabaseMutationError.invalidPartition(
                entity: identity.entity,
                reason: reason
            )
        } catch {
            throw DatabaseMutationError.invalidPartition(
                entity: identity.entity,
                reason: String(describing: error)
            )
        }

        let partitionPath = partition?.resolve() ?? []
        return Self(
            identity: identity,
            persistableType: persistableType,
            id: id,
            partition: partition,
            partitionPath: partitionPath
        )
    }

    static func key(
        _ identity: EntityReference,
        container: DBContainer
    ) throws -> Key {
        let resolved = try resolve(identity, container: container)
        return Key(
            entity: identity.entity,
            id: resolved.id.pack(),
            partitionPath: resolved.partitionPath
        )
    }

    struct Key: Sendable, Hashable {
        let entity: String
        let id: ByteString
        let partitionPath: [String]
    }

}
