import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

struct ResolvedEntityReference: Sendable {
    let identity: EntityReference
    let id: Tuple
    let partition: AnyDirectoryPath?
    let partitionPath: [String]

    static func resolve(
        _ identity: EntityReference,
        container: DBContainer,
        model: PersistedModel? = nil
    ) throws -> Self {
        guard let entity = container.schema.entities.first(where: { $0.name == identity.entity }) else {
            throw DatabaseMutationError.unknownEntity(identity.entity)
        }
        guard let runtime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw DatabaseMutationError.entityHasNoPersistableType(identity.entity)
        }
        let id: Tuple
        do {
            id = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: entity.identifierType
            )
        } catch {
            throw DatabaseMutationError.invalidPersistableIdentifier(
                entity: identity.entity,
                reason: "Identifier does not match the compiled entity schema"
            )
        }
        if let model {
            let modelType = model.entity
            guard modelType == identity.entity else {
                throw DatabaseMutationError.entityTypeMismatch(
                    expected: identity.entity,
                    actual: modelType
                )
            }
            let encodedIdentity: EntityReference
            do {
                encodedIdentity = try runtime.identity(for: model)
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
                for: entity,
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
                reason: "Partition does not match the compiled entity schema"
            )
        }

        let partitionPath = partition?.resolve() ?? []
        return Self(
            identity: identity,
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

    struct Key: Sendable, Equatable, Comparable {
        let entity: String
        let id: ByteString
        let partitionPath: [String]

        static func < (lhs: Key, rhs: Key) -> Bool {
            if lhs.entity != rhs.entity {
                return lhs.entity < rhs.entity
            }
            if lhs.id != rhs.id {
                return lhs.id < rhs.id
            }
            for (left, right) in zip(lhs.partitionPath, rhs.partitionPath) {
                if left != right {
                    return left < right
                }
            }
            return lhs.partitionPath.count < rhs.partitionPath.count
        }
    }

}
