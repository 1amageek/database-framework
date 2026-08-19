#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Stores lifecycle-job deletion intent independently from Base-local data.
package struct DatabaseBaseDeletionStore: Sendable {
    private let records: Subspace

    package init(root: Subspace, collection: String) {
        self.records = root
            .subspace("catalog")
            .subspace("base-deletions")
            .subspace(collection)
    }

    package func load(
        _ id: Base.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseDeletionRecord? {
        guard let value = try await transaction.getValue(
            for: key(id),
            snapshot: false
        ) else {
            return nil
        }
        do {
            let record = try StorageFrameCodec.decode(
                DatabaseBaseDeletionRecord.self,
                from: value
            )
            guard record.baseID == id else {
                throw DatabaseBaseCatalogError.corruptedRecord(id)
            }
            return record
        } catch let error as DatabaseBaseCatalogError {
            throw error
        } catch {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
    }

    package func insert(
        _ record: DatabaseBaseDeletionRecord,
        transaction: any TransactionAccess
    ) async throws {
        if let current = try await load(
            record.baseID,
            transaction: transaction
        ) {
            guard current == record else {
                throw DatabaseBaseCatalogError.baseDeletionClaimed(
                    record.baseID
                )
            }
            return
        }
        try transaction.setValue(
            StorageFrameCodec.encode(record),
            for: key(record.baseID)
        )
    }

    package func remove(
        _ id: Base.ID,
        owner: ByteString,
        transaction: any TransactionAccess
    ) async throws {
        guard let current = try await load(id, transaction: transaction) else {
            return
        }
        guard current.owner == owner else {
            throw DatabaseBaseCatalogError.baseDeletionClaimed(id)
        }
        try transaction.clear(key: key(id))
    }

    private func key(_ id: Base.ID) -> ByteString {
        records.pack(Tuple(id.value))
    }
}

#endif
