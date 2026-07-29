import DatabaseKit
import StorageKit

extension Persistable {
    public static func buildEntityIndex(
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        indexLifecycleStore: IndexLifecycleStore,
        batchSize: Int,
        configurations: [any IndexRuntimeConfiguration]
    ) async throws {
        let indexSubspace = storeSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(index.subspaceKey)
        let maintainer = try createIndexMaintainer(
            for: index,
            indexSubspace: indexSubspace,
            providers: container.runtimeConfiguration.indexMaintainerProviders,
            configurations: configurations
        )
        let uniquenessMaintainer: (any IndexUniquenessMaintainer<Self>)?
        if index.isUnique {
            uniquenessMaintainer = try container.runtimeConfiguration
                .indexMaintainerProviders.makeIndexUniquenessMaintainer(
                index: index,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: configurations
            )
        } else {
            uniquenessMaintainer = nil
        }
        let indexer = try OnlineIndexer<Self>(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Self.persistableType,
            index: index,
            indexMaintainer: maintainer,
            uniquenessMaintainer: uniquenessMaintainer,
            indexLifecycleStore: indexLifecycleStore,
            batchSize: batchSize
        )
        try await indexer.buildIndex(clearFirst: false)
    }

    private static func createIndexMaintainer(
        for index: Index,
        indexSubspace: Subspace,
        providers: IndexMaintainerProviderRegistry,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Self> {
        try providers.makeIndexMaintainer(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: configurations
        )
    }
}
