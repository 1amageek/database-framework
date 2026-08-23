// EntityIndexBuilder.swift
// DatabaseEngine - Entity-specific index building support
//
// Provides runtime type dispatch for OnlineIndexer instantiation.

import DatabaseKit
import StorageKit

/// Helper namespace for runtime index building
///
/// Dispatches through the statically bound entity runtime without reopening a
/// model metatype during database execution.
package struct EntityIndexBuilder {
    package static func buildIndex(
        for runtime: EntityRuntimeRegistration,
        transactionAuthority: IndexBuildTransactionAuthority,
        container: DBContainer,
        storeSubspace: Subspace,
        index: ResolvedIndex,
        indexLifecycleStore: IndexLifecycleStore,
        batchSize: Int = 100,
        configurations: [any IndexRuntimeConfiguration] = []
    ) async throws {
        try await runtime.buildIndex(
            transactionAuthority: transactionAuthority,
            container: container,
            storeSubspace: storeSubspace,
            index: index,
            lifecycleStore: indexLifecycleStore,
            batchSize: batchSize,
            configurations: configurations
        )
    }
}
