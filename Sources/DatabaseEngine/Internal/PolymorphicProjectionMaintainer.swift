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
        let modelType = type(of: write.model)
        guard let entityRuntime = container.runtimeConfiguration.entityRuntimes
            .registration(named: modelType.persistableType) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: modelType.persistableType
            )
        }
        guard let membership = modelType.polymorphicMembership,
              requiresProjection(
                modelType: modelType,
                membership: membership
              ) else {
            return
        }

        let projection = try await resolveProjection(
            membership: membership,
            transaction: transaction
        )
        let compositeID = try PolymorphicIdentifierKey.tuple(
            for: modelType,
            identifier: write.identifier
        )
        let key = projection.items.pack(compositeID)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: projection.blobs
        )
        let previousProjection: PersistedModel?
        if try await storage.read(for: key) != nil {
            guard write.previousModel != nil else {
                throw PolymorphicProjectionError.unexpectedProjection(
                    entity: modelType.persistableType,
                    group: projection.group.identifier
                )
            }
            previousProjection = write.previousCanonicalModel
        } else {
            guard write.previousModel == nil else {
                throw PolymorphicProjectionError.missingProjection(
                    entity: modelType.persistableType,
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
                memberTypeName: modelType.persistableType
            ),
            logicalTypeName: projection.group.identifier,
            transaction: transaction
        )
    }

    func remove(
        _ model: any Persistable,
        transaction: any TransactionAccess
    ) async throws {
        let modelType = type(of: model)
        guard let entityRuntime = container.runtimeConfiguration.entityRuntimes
            .registration(named: modelType.persistableType) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: modelType.persistableType
            )
        }
        guard let membership = modelType.polymorphicMembership,
              requiresProjection(
                modelType: modelType,
                membership: membership
              ) else {
            return
        }

        let projection = try await resolveProjection(
            membership: membership,
            transaction: transaction
        )
        let compositeID = try PolymorphicIdentifierKey.tuple(
            for: modelType,
            identifier: try PersistableIdentifierKeyCodec.tuple(for: model)
        )
        let key = projection.items.pack(compositeID)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: projection.blobs
        )
        guard let bytes = try await storage.read(for: key) else {
            throw PolymorphicProjectionError.missingProjection(
                entity: modelType.persistableType,
                group: projection.group.identifier
            )
        }
        let projectedModel = try DataAccess.deserializePersistedModel(
            bytes,
            expectedEntity: modelType.persistableType
        )
        try await projection.indexes.updateIndexesUntyped(
            runtime: entityRuntime,
            oldModel: projectedModel,
            newModel: nil,
            id: compositeID,
            descriptors: container.schema.polymorphicIndexDescriptors(
                identifier: projection.group.identifier,
                memberTypeName: modelType.persistableType
            ),
            logicalTypeName: projection.group.identifier,
            transaction: transaction
        )
        try await storage.delete(for: key)
    }

    private func requiresProjection(
        modelType: any Persistable.Type,
        membership: PolymorphicMembership
    ) -> Bool {
        modelType.directoryPathComponents
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
            container.indexConfigurations[index.name] ?? []
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
                indexSubspace: subspace.subspace(SubspaceKey.indexes),
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
