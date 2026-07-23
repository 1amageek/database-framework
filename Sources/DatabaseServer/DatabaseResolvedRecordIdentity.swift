import Core
import DatabaseEngine
import DatabaseValue
import StorageKit

struct DatabaseResolvedRecordIdentity: Sendable {
    let identity: RecordIdentity
    let persistableType: any Persistable.Type
    let id: Tuple
    let partition: AnyDirectoryPath?
    let partitionPath: [String]

    static func resolve(
        _ identity: RecordIdentity,
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
            id = try RecordIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: persistableType.recordIdentifierType
            )
        } catch {
            throw DatabaseMutationError.invalidRecordIdentifier(
                entity: identity.entity,
                reason: String(describing: error)
            )
        }
        if let model {
            let actualType = type(of: model).persistableType
            guard actualType == identity.entity else {
                throw DatabaseMutationError.recordTypeMismatch(
                    expected: identity.entity,
                    actual: actualType
                )
            }
            guard model.recordIdentifierValue == identity.id else {
                throw DatabaseMutationError.recordIdentityMismatch(identity)
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
                    throw DatabaseMutationError.recordIdentityMismatch(identity)
                }
                modelValues.append((name, raw))
            }
            if !dynamicFieldNames.isEmpty {
                let actual = try AnyDirectoryPath(fieldValues: modelValues, type: persistableType)
                try actual.validate()
                guard actual.resolve() == partitionPath else {
                    throw DatabaseMutationError.recordIdentityMismatch(identity)
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
        _ identity: RecordIdentity,
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
