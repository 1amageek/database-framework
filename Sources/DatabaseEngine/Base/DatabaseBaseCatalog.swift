#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Durable control-domain source of truth for Base placement and lifecycle.
package struct DatabaseBaseCatalog: Sendable {
    private static let maximumRecordCount = 65_536

    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock
    private let records: Subspace

    package init(
        controlDomain: DatabaseStorageDomainRuntime,
        clock: any StorageMonotonicClock
    ) {
        self.transactionExecutor = controlDomain.transactionExecutor
        self.clock = clock
        let root = controlDomain.systemRoot
            .subspace("catalog")
            .subspace("bases")
        self.records = root.subspace("records")
    }

    package func loadAll() async throws -> [DatabaseBaseRecord] {
        try await transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: clock
        ) { transaction in
            try await loadAll(transaction: transaction)
        }
    }

    package func load(
        _ id: Base.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseRecord? {
        guard let bytes = try await transaction.getValue(
            for: recordKey(id),
            snapshot: false
        ) else {
            return nil
        }
        do {
            let record = try StorageFrameCodec.decode(
                DatabaseBaseRecord.self,
                from: bytes
            )
            guard record.id == id else {
                throw DatabaseBaseCatalogError.corruptedRecord(id)
            }
            return record
        } catch let error as DatabaseBaseCatalogError {
            throw error
        } catch {
            throw DatabaseBaseCatalogError.corruptedRecord(id)
        }
    }

    package func insertProvisioning(
        id: Base.ID,
        placement: DatabaseStoragePlacement,
        domain: DatabaseStorageDomainRuntime,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseRecord {
        guard expectedRevision == 0 else {
            throw DatabaseBaseCatalogError.revisionConflict(
                expected: expectedRevision,
                actual: 0
            )
        }
        if let existing = try await load(id, transaction: transaction) {
            guard existing.revision == expectedRevision else {
                throw DatabaseBaseCatalogError.revisionConflict(
                    expected: expectedRevision,
                    actual: existing.revision
                )
            }
            if existing.lifecycle == .tombstone {
                throw DatabaseBaseCatalogError.baseIdentifierRetired(id)
            }
            throw DatabaseBaseCatalogError.baseAlreadyExists(id)
        }
        let resultingRevision: UInt64 = 1
        let record = DatabaseBaseRecord(
            id: id,
            placementID: placement.id,
            domainID: domain.id,
            placementGeneration: 1,
            revision: resultingRevision,
            lifecycle: .provisioning
        )
        try write(record, transaction: transaction)
        return record
    }

    package func replace(
        _ record: DatabaseBaseRecord,
        expectedRecordRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseRecord {
        guard let current = try await load(record.id, transaction: transaction)
        else {
            throw DatabaseBaseCatalogError.baseNotFound(record.id)
        }
        guard current.revision == expectedRecordRevision else {
            throw DatabaseBaseCatalogError.revisionConflict(
                expected: expectedRecordRevision,
                actual: current.revision
            )
        }
        let expectedNextRevision = try increment(current.revision)
        guard record.revision == expectedNextRevision else {
            throw DatabaseBaseCatalogError.revisionConflict(
                expected: expectedNextRevision,
                actual: record.revision
            )
        }
        try write(record, transaction: transaction)
        return record
    }

    package func loadAll(
        transaction: any TransactionAccess
    ) async throws -> [DatabaseBaseRecord] {
        let range = records.range()
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: Self.maximumRecordCount + 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        guard rows.count <= Self.maximumRecordCount else {
            throw DatabaseBaseCatalogError.catalogTooLarge(
                maximum: Self.maximumRecordCount
            )
        }
        var result: [DatabaseBaseRecord] = []
        result.reserveCapacity(rows.count)
        for (_, bytes) in rows {
            do {
                result.append(
                    try StorageFrameCodec.decode(
                        DatabaseBaseRecord.self,
                        from: bytes
                    )
                )
            } catch {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
        }
        return result.sorted { $0.id < $1.id }
    }

    private func recordKey(_ id: Base.ID) -> ByteString {
        records.pack(Tuple(id.value))
    }

    private func write(
        _ record: DatabaseBaseRecord,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(
            StorageFrameCodec.encode(record),
            for: recordKey(record.id)
        )
    }

    private func increment(_ revision: UInt64) throws -> UInt64 {
        let (result, overflow) = revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseBaseCatalogError.corruptedRecord(nil)
        }
        return result
    }
}

#endif
