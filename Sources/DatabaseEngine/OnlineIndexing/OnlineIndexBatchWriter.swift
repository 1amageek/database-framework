import DatabaseKit
import StorageKit

/// Writes one online-build batch while preserving uniqueness semantics.
enum OnlineIndexBatchWriter {
    static func write<Item: Persistable>(
        _ entries: [(item: Item, id: Tuple)],
        index: Index,
        maintainer: any IndexMaintainer<Item>,
        violationTracker: UniquenessViolationTracker,
        transaction: any TransactionAccess
    ) async throws {
        guard index.isUnique else {
            try await maintainer.scanItems(entries, transaction: transaction)
            return
        }

        for entry in entries {
            try await IndexUniquenessConstraint.enforce(
                index: index,
                item: entry.item,
                id: entry.id,
                state: .writeOnly,
                maintainer: maintainer,
                violationTracker: violationTracker,
                transaction: transaction
            )
            try await maintainer.scanItem(
                entry.item,
                id: entry.id,
                transaction: transaction
            )
        }
    }
}
