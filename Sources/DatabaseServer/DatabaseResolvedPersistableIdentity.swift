import Core
import DatabaseEngine
import DatabaseValue
import StorageKit

struct DatabaseResolvedPersistableIdentity: Sendable {
    let identity: PersistableIdentity
    let persistableType: any Persistable.Type
    let id: Tuple
    let partition: AnyDirectoryPath?
    let partitionPath: [String]

    static func resolve(
        _ identity: PersistableIdentity,
        container: DBContainer,
        model: (any Persistable)? = nil
    ) throws -> Self {
        guard let entity = container.schema.entities.first(where: { $0.name == identity.entity }) else {
            throw DatabaseMutationError.unknownEntity(identity.entity)
        }
        guard let persistableType = entity.persistableType else {
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
            let actualType = type(of: model).persistableType
            guard actualType == identity.entity else {
                throw DatabaseMutationError.entityTypeMismatch(
                    expected: identity.entity,
                    actual: actualType
                )
            }
            guard model.persistableIdentifierValue == identity.id else {
                throw DatabaseMutationError.persistableIdentityMismatch(identity)
            }
        }

        let dynamicFieldNames = persistableType.directoryFieldNames
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
        var modelValues: [(name: String, value: any Sendable)] = []
        if let model {
            for name in dynamicFieldNames {
                guard let raw = model[dynamicMember: name] else {
                    throw DatabaseMutationError.persistableIdentityMismatch(identity)
                }
                modelValues.append((name, raw))
            }
            if !dynamicFieldNames.isEmpty {
                let actual = try AnyDirectoryPath(fieldValues: modelValues, type: persistableType)
                try actual.validate()
                guard actual.resolve() == partitionPath else {
                    throw DatabaseMutationError.persistableIdentityMismatch(identity)
                }
            }
        }
        return Self(
            identity: identity,
            persistableType: persistableType,
            id: id,
            partition: partition,
            partitionPath: partitionPath
        )
    }

    static func key(
        _ identity: PersistableIdentity,
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
        let id: Bytes
        let partitionPath: [String]
    }

}
