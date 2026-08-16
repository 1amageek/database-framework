import DatabaseKit
import DatabaseTypes
import StorageKit

@_spi(DatabaseExecution)
public struct ResolvedEntityReference: Sendable {
    public let identity: EntityReference
    public let id: Tuple
    public let partition: AnyDirectoryPath?
    public let partitionPath: [String]

    public static func resolve(
        _ identity: EntityReference,
        container: DBContainer,
        model: PersistedModel? = nil
    ) throws -> Self {
        guard let entity = container.schema.entities.first(where: {
            $0.name == identity.entity
        }) else {
            throw DatabaseEntityMutationError.unknownEntity(identity.entity)
        }
        guard let runtime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw DatabaseEntityMutationError.entityHasNoPersistableType(
                identity.entity
            )
        }
        let id: Tuple
        do {
            id = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: entity.identifierType
            )
        } catch {
            throw DatabaseEntityMutationError.invalidPersistableIdentifier(
                entity: identity.entity,
                reason: "Identifier does not match the compiled entity schema"
            )
        }
        if let model {
            let modelType = model.entity
            guard modelType == identity.entity else {
                throw DatabaseEntityMutationError.entityTypeMismatch(
                    expected: identity.entity,
                    actual: modelType
                )
            }
            let encodedIdentity: EntityReference
            do {
                encodedIdentity = try runtime.identity(for: model)
            } catch {
                throw DatabaseEntityMutationError.persistableIdentityMismatch(
                    identity
                )
            }
            guard encodedIdentity == identity else {
                throw DatabaseEntityMutationError.persistableIdentityMismatch(
                    identity
                )
            }
        }

        let partition: AnyDirectoryPath?
        do {
            if entity.hasDynamicDirectory || !identity.partitions.isEmpty {
                partition = try AnyDirectoryPath(
                    entity: entity,
                    partitions: identity.partitions
                )
            } else {
                partition = nil
            }
        } catch CanonicalReadError.invalidPartition(_, let reason) {
            throw DatabaseEntityMutationError.invalidPartition(
                entity: identity.entity,
                reason: reason
            )
        } catch {
            throw DatabaseEntityMutationError.invalidPartition(
                entity: identity.entity,
                reason: "Partition does not match the compiled entity schema"
            )
        }

        return Self(
            identity: identity,
            id: id,
            partition: partition,
            partitionPath: partition?.resolve() ?? []
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
