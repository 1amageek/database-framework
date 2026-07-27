import DatabaseKit
import StorageKit

/// Physical conflict lookup implemented only by uniqueness-capable indexes.
public protocol IndexUniquenessMaintainer<Item>: IndexMaintainer
where Item: Persistable {
    func uniquenessConflicts(
        for item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws -> [IndexUniquenessConflict]
}
