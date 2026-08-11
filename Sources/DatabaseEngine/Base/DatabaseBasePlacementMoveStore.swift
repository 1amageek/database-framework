#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Stores the immutable placement descriptor before external-domain work starts.
package struct DatabaseBasePlacementMoveStore: Sendable {
    private let records: Subspace

    package init(controlDomain: DatabaseStorageDomainRuntime) {
        self.records = controlDomain.root
            .subspace("catalog")
            .subspace("base-placement-moves")
            .subspace("records")
    }

    package func load(
        _ id: Base.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBasePlacementMoveRecord? {
        guard let value = try await transaction.getValue(
            for: key(id),
            snapshot: false
        ) else {
            return nil
        }
        do {
            let record = try StorageFrameCodec.decode(
                DatabaseBasePlacementMoveRecord.self,
                from: value
            )
            guard record.descriptor.baseID == id else {
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
        _ record: DatabaseBasePlacementMoveRecord,
        transaction: any TransactionAccess
    ) async throws {
        if let current = try await load(
            record.descriptor.baseID,
            transaction: transaction
        ) {
            guard current == record else {
                throw DatabaseBaseCatalogError.placementDestinationClaimed(
                    record.descriptor.baseID
                )
            }
            return
        }
        try transaction.setValue(
            StorageFrameCodec.encode(record),
            for: key(record.descriptor.baseID)
        )
    }

    package func replacePrepared(
        _ record: DatabaseBasePlacementMoveRecord,
        transaction: any TransactionAccess
    ) async throws {
        guard let current = try await load(
            record.descriptor.baseID,
            transaction: transaction
        ), current.owner == record.owner,
        Self.sameIntent(current.descriptor, record.descriptor) else {
            throw DatabaseBaseCatalogError.corruptedRecord(
                record.descriptor.baseID
            )
        }
        try transaction.setValue(
            StorageFrameCodec.encode(record),
            for: key(record.descriptor.baseID)
        )
    }

    package func remove(
        _ id: Base.ID,
        owner: DatabaseTypes.ByteString,
        transaction: any TransactionAccess
    ) async throws {
        guard let current = try await load(id, transaction: transaction) else {
            return
        }
        guard current.owner == owner else {
            throw DatabaseBaseCatalogError.placementDestinationClaimed(id)
        }
        try transaction.clear(key: key(id))
    }

    private func key(_ id: Base.ID) -> DatabaseTypes.ByteString {
        records.pack(Tuple(id.value))
    }

    private static func sameIntent(
        _ lhs: DatabaseBasePlacementMoveDescriptor,
        _ rhs: DatabaseBasePlacementMoveDescriptor
    ) -> Bool {
        return lhs.baseID == rhs.baseID
            && lhs.sourcePlacementID == rhs.sourcePlacementID
            && lhs.sourceDomainID == rhs.sourceDomainID
            && lhs.sourceNamespacePath == rhs.sourceNamespacePath
            && lhs.sourcePlacementGeneration == rhs.sourcePlacementGeneration
            && lhs.movingRevision == rhs.movingRevision
            && lhs.destinationPlacementID == rhs.destinationPlacementID
            && lhs.destinationDomainID == rhs.destinationDomainID
            && lhs.destinationNamespacePath == rhs.destinationNamespacePath
            && lhs.destinationPlacementGeneration
                == rhs.destinationPlacementGeneration
    }
}

#endif
