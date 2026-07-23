import Core
import StorageKit

extension Persistable {
    public static func buildEntityIndex(
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        indexLifecycleStore: IndexLifecycleStore,
        batchSize: Int,
        configurations: [any IndexConfiguration]
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
        let indexer = OnlineIndexer<Self>(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Self.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: indexLifecycleStore,
            batchSize: batchSize
        )
        try await indexer.buildIndex(clearFirst: false)
    }

    private static func createIndexMaintainer(
        for index: Index,
        indexSubspace: Subspace,
        providers: IndexMaintainerProviderRegistry,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Self> {
        try providers.makeIndexMaintainer(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            configurations: configurations
        )
    }
}
