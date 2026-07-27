// EntityIndexBuilder.swift
// DatabaseEngine - Entity-specific index building support
//
// Provides runtime type dispatch for OnlineIndexer instantiation.

import StorageKit
import DatabaseKit

/// Helper namespace for runtime index building
///
/// Opens the compiled `Persistable` metatype stored by `Schema.Entity`, then
/// creates the matching generic `OnlineIndexer` without process-global state.
public struct EntityIndexBuilder {
    private typealias Builder = @Sendable (
        _ container: DBContainer,
        _ storeSubspace: Subspace,
        _ index: Index,
        _ indexLifecycleStore: IndexLifecycleStore,
        _ batchSize: Int,
        _ configurations: [any IndexRuntimeConfiguration]
    ) async throws -> Void

    public static func buildIndex(
        for persistableType: any Persistable.Type,
        container: DBContainer,
        storeSubspace: Subspace,
        index: Index,
        indexLifecycleStore: IndexLifecycleStore,
        batchSize: Int = 100,
        configurations: [any IndexRuntimeConfiguration] = []
    ) async throws {
        func makeBuilder<T: Persistable>(_: T.Type) -> Builder {
            { container, storeSubspace, index, lifecycleStore, batchSize, configurations in
                try await T.buildEntityIndex(
                    container: container,
                    storeSubspace: storeSubspace,
                    index: index,
                    indexLifecycleStore: lifecycleStore,
                    batchSize: batchSize,
                    configurations: configurations
                )
            }
        }

        let builder = _openExistential(persistableType, do: makeBuilder)
        try await builder(
            container,
            storeSubspace,
            index,
            indexLifecycleStore,
            batchSize,
            configurations
        )
    }
}
