import DatabaseKit
import StorageKit

/// Maintains the shared polymorphic projection for primary Persistable writes.
///
/// The canonical encoded value is retained from the primary write and reused
/// directly. Projection storage and indexes use the same physical transaction.
internal struct PolymorphicProjectionMaintainer: Sendable {
    private let container: DBContainer

    init(container: DBContainer) {
        self.container = container
    }

    func update(
        _ write: PersistableWriteResult,
        transaction: any TransactionAccess
    ) async throws {
        guard let entityRuntime = container.runtimeConfiguration.entityRuntimes
            .registration(named: write.canonicalModel.entity) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: write.canonicalModel.entity
            )
        }
        let entity = entityRuntime.entity
        guard let membership = entity.polymorphicMembership,
              requiresProjection(
                entity: entity,
                membership: membership
              ) else {
            return
        }

        let projection = try await resolveProjection(
            membership: membership,
            transaction: transaction
        )
        let compositeID = try PolymorphicIdentifierKey.tuple(
            for: entity,
            identifier: write.identifier
        )
        let key = projection.items.pack(compositeID)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: projection.blobs
        )
        let previousProjection: PersistedModel?
        if try await storage.read(for: key) != nil {
            guard write.previousCanonicalModel != nil else {
                throw PolymorphicProjectionError.unexpectedProjection(
                    entity: entity.name,
                    group: projection.group.identifier
                )
            }
            previousProjection = write.previousCanonicalModel
        } else {
            guard write.previousCanonicalModel == nil else {
                throw PolymorphicProjectionError.missingProjection(
                    entity: entity.name,
                    group: projection.group.identifier
                )
            }
            previousProjection = nil
        }

        try await storage.write(write.encodedValue, for: key)
        try await projection.indexes.updateIndexesUntyped(
            runtime: entityRuntime,
            oldModel: previousProjection,
            newModel: write.canonicalModel,
            id: compositeID,
            descriptors: container.schema.polymorphicIndexDescriptors(
                identifier: projection.group.identifier,
                memberTypeName: entity.name
            ),
            logicalTypeName: projection.group.identifier,
            transaction: transaction
        )
    }

    func remove(
        _ model: PersistedModel,
        identifier: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        guard let entityRuntime = container.runtimeConfiguration.entityRuntimes
            .registration(named: model.entity) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: model.entity
            )
        }
        let entity = entityRuntime.entity
        guard let membership = entity.polymorphicMembership,
              requiresProjection(
                entity: entity,
                membership: membership
              ) else {
            return
        }

        let projection = try await resolveProjection(
            membership: membership,
            transaction: transaction
        )
        let compositeID = try PolymorphicIdentifierKey.tuple(
            for: entity,
            identifier: identifier
        )
        let key = projection.items.pack(compositeID)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: projection.blobs
        )
        guard let bytes = try await storage.read(for: key) else {
            throw PolymorphicProjectionError.missingProjection(
                entity: entity.name,
                group: projection.group.identifier
            )
        }
        let projectedModel = try DataAccess.deserializePersistedModel(
            bytes,
            expectedEntity: entity.name
        )
        try await projection.indexes.updateIndexesUntyped(
            runtime: entityRuntime,
            oldModel: projectedModel,
            newModel: nil,
            id: compositeID,
            descriptors: container.schema.polymorphicIndexDescriptors(
                identifier: projection.group.identifier,
                memberTypeName: entity.name
            ),
            logicalTypeName: projection.group.identifier,
            transaction: transaction
        )
        try await storage.delete(for: key)
    }

    private func requiresProjection(
        entity: Schema.Entity,
        membership: PolymorphicMembership
    ) -> Bool {
        entity.directoryComponents
            != membership.directoryComponents
    }

    private func resolveProjection(
        membership: PolymorphicMembership,
        transaction: any TransactionAccess
    ) async throws -> Projection {
        let group = try container.polymorphicGroup(
            identifier: membership.identifier
        )
        let subspace = try await container.resolvePolymorphicDirectory(
            for: group.identifier,
            transaction: transaction
        )
        let configurations = group.indexes.flatMap { index in
            container.runtimeConfiguration.indexConfigurations(
                named: index.name
            )
        }
        return Projection(
            group: group,
            items: subspace.subspace(SubspaceKey.items),
            blobs: subspace.subspace(SubspaceKey.blobs),
            indexes: IndexMaintenanceService(
                indexLifecycleStore: IndexLifecycleStore(
                    container: container,
                    subspace: subspace
                ),
                violationTracker: UniquenessViolationTracker(
                    container: container,
                    metadataSubspace: subspace.subspace(
                        SubspaceKey.metadata
                    )
                ),
                configurations: configurations
            )
        )
    }

    private struct Projection: Sendable {
        let group: PolymorphicGroup
        let items: Subspace
        let blobs: Subspace
        let indexes: IndexMaintenanceService
    }
}
