import DatabaseKit
import StorageKit

/// Enforces the logical uniqueness contract shared by mutations and index builds.
enum IndexUniquenessConstraint {
    static func enforce<Item: PersistedEntityValue>(
        index: Index,
        item: Item,
        id: Tuple,
        state: IndexState,
        maintainer: any IndexUniquenessMaintainer<Item>,
        violationTracker: UniquenessViolationTracker,
        transaction: any TransactionAccess
    ) async throws {
        let conflicts = try await maintainer.uniquenessConflicts(
            for: item,
            id: id,
            transaction: transaction
        )
        for conflict in conflicts {
            switch state {
            case .readable:
                throw UniquenessViolationError(
                    indexName: index.name,
                    persistableType: item.persistedEntityName,
                    conflictingValues: conflict.conflictingValues,
                    existingPrimaryKey: conflict.existingPrimaryKey,
                    newPrimaryKey: id
                )
            case .writeOnly:
                try await violationTracker.recordViolation(
                    indexName: index.name,
                    persistableType: item.persistedEntityName,
                    valueKey: conflict.valueKey,
                    conflictingValues: conflict.conflictingValues,
                    existingPrimaryKey: conflict.existingPrimaryKey,
                    newPrimaryKey: id,
                    transaction: transaction
                )
            case .disabled:
                return
            }
        }
    }
}
