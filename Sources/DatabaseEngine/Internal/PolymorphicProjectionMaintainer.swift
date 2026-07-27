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
        guard let polymorphicType = modelType as? any Polymorphable.Type,
              requiresProjection(
                modelType: modelType,
                polymorphicType: polymorphicType
              ) else {
            return
        }

        let projection = try await resolveProjection(
            polymorphicType: polymorphicType,
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
        let previousProjection: (any Persistable)?
        if let bytes = try await storage.read(for: key) {
            guard write.previousModel != nil else {
                throw PolymorphicProjectionError.unexpectedProjection(
                    entity: modelType.persistableType,
                    group: projection.group.identifier
                )
            }
            previousProjection = try DataAccess.deserializeAny(
                bytes,
                as: modelType
            )
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
            oldModel: previousProjection,
            newModel: write.model,
            id: compositeID,
            descriptors: container.schema.polymorphicIndexDescriptors(
                identifier: projection.group.identifier,
                memberType: modelType
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
        guard let polymorphicType = modelType as? any Polymorphable.Type,
              requiresProjection(
                modelType: modelType,
                polymorphicType: polymorphicType
              ) else {
            return
        }

        let projection = try await resolveProjection(
            polymorphicType: polymorphicType,
            transaction: transaction
        )
        let compositeID = try PolymorphicIdentifierKey.tuple(
            for: modelType,
            identifier: try model.persistableIdentifierTuple()
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
        let projectedModel = try DataAccess.deserializeAny(
            bytes,
            as: modelType
        )
        try await projection.indexes.updateIndexesUntyped(
            oldModel: projectedModel,
            newModel: nil as (any Persistable)?,
            id: compositeID,
            descriptors: container.schema.polymorphicIndexDescriptors(
                identifier: projection.group.identifier,
                memberType: modelType
            ),
            logicalTypeName: projection.group.identifier,
            transaction: transaction
        )
        try await storage.delete(for: key)
    }

    private func requiresProjection(
        modelType: any Persistable.Type,
        polymorphicType: any Polymorphable.Type
    ) -> Bool {
        modelType.directoryPathComponents
            != polymorphicType.polymorphicDirectoryPathComponents
    }

    private func resolveProjection(
        polymorphicType: any Polymorphable.Type,
        transaction: any TransactionAccess
    ) async throws -> Projection {
        let group = try container.polymorphicGroup(
            identifier: polymorphicType.polymorphableType
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
                maintainerProviders: container.runtimeConfiguration
                    .indexMaintainerProviders,
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
