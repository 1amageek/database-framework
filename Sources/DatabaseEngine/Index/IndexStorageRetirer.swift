import DatabaseKit
import StorageKit

/// Owns the physical layout cleared when an index generation is retired.
package enum IndexStorageRetirer {
    package static func retire(
        indexName: String,
        selection: DatabaseIndexStorageRetirement,
        storeSubspace: Subspace,
        transaction: any TransactionAccess
    ) throws {
        switch selection {
        case .allGenerations:
            try clearRange(
                storeSubspace
                    .subspace(SubspaceKey.indexes)
                    .subspace(indexName),
                transaction: transaction
            )
            try clearRange(
                storeSubspace.subspace("state").subspace(indexName),
                transaction: transaction
            )
            try clearRange(
                storeSubspace
                    .subspace(SubspaceKey.metadata)
                    .subspace("_violations")
                    .subspace(indexName),
                transaction: transaction
            )
            try clearRange(
                storeSubspace
                    .subspace(SubspaceKey.metadata)
                    .subspace("index-rebuild")
                    .subspace(indexName),
                transaction: transaction
            )

        case .physicalGeneration(
            let definitionFingerprint,
            let layoutFingerprint
        ):
            try clearRange(
                storeSubspace
                    .subspace(SubspaceKey.indexes)
                    .subspace(indexName)
                    .subspace(definitionFingerprint.bytes)
                    .subspace(layoutFingerprint),
                transaction: transaction
            )
            try transaction.clear(
                key: storeSubspace
                    .subspace("state")
                    .subspace(indexName)
                    .pack(
                        Tuple(
                            definitionFingerprint.bytes,
                            layoutFingerprint
                        )
                    )
            )
            try clearRange(
                storeSubspace
                    .subspace(SubspaceKey.metadata)
                    .subspace("_violations")
                    .subspace(indexName)
                    .subspace(definitionFingerprint.bytes)
                    .subspace(layoutFingerprint),
                transaction: transaction
            )
            try transaction.clear(
                key: storeSubspace
                    .subspace(SubspaceKey.metadata)
                    .subspace("index-rebuild")
                    .pack(
                        Tuple(
                            indexName,
                            definitionFingerprint.bytes,
                            layoutFingerprint
                        )
                    )
            )
        }
    }

    private static func clearRange(
        _ subspace: Subspace,
        transaction: any TransactionAccess
    ) throws {
        let range = subspace.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)
    }
}
