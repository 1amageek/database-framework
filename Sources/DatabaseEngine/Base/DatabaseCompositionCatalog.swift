import DatabaseKit
import StorageKit

/// Durable control-domain source of truth for named Base Compositions.
package struct DatabaseCompositionCatalog: Sendable {
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
        self.records = controlDomain.root
            .subspace("catalog")
            .subspace("compositions")
            .subspace("records")
    }

    package func loadAll() async throws -> [DatabaseCompositionRecord] {
        try await transactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: clock
        ) { transaction in
            try await loadAll(transaction: transaction)
        }
    }

    package func loadAll(
        transaction: any TransactionAccess
    ) async throws -> [DatabaseCompositionRecord] {
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
            throw DatabaseCompositionCatalogError.catalogTooLarge(
                maximum: Self.maximumRecordCount
            )
        }
        var result: [DatabaseCompositionRecord] = []
        result.reserveCapacity(rows.count)
        for (_, bytes) in rows {
            do {
                result.append(
                    try StorageFrameCodec.decode(
                        DatabaseCompositionRecord.self,
                        from: bytes
                    )
                )
            } catch {
                throw DatabaseCompositionCatalogError.corruptedRecord(nil)
            }
        }
        return result.sorted { $0.composition.id < $1.composition.id }
    }

    package func load(
        _ id: Base.Composition.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord? {
        guard let bytes = try await transaction.getValue(
            for: recordKey(id),
            snapshot: false
        ) else {
            return nil
        }
        do {
            let record = try StorageFrameCodec.decode(
                DatabaseCompositionRecord.self,
                from: bytes
            )
            guard record.composition.id == id else {
                throw DatabaseCompositionCatalogError.corruptedRecord(id)
            }
            return record
        } catch let error as DatabaseCompositionCatalogError {
            throw error
        } catch {
            throw DatabaseCompositionCatalogError.corruptedRecord(id)
        }
    }

    package func create(
        _ composition: Base.Composition,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord {
        guard expectedRevision == 0 else {
            throw DatabaseCompositionCatalogError.revisionConflict(
                expected: expectedRevision,
                actual: 0
            )
        }
        if let existing = try await load(
            composition.id,
            transaction: transaction
        ) {
            throw DatabaseCompositionCatalogError.compositionAlreadyExists(
                existing.composition.id
            )
        }
        let record = DatabaseCompositionRecord(
            composition: composition,
            revision: 1,
            generation: 1
        )
        try write(record, transaction: transaction)
        return record
    }

    package func replace(
        id: Base.Composition.ID,
        bases: [Base.ID],
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord {
        guard let current = try await load(id, transaction: transaction) else {
            throw DatabaseCompositionCatalogError.compositionNotFound(id)
        }
        guard current.revision == expectedRevision else {
            throw DatabaseCompositionCatalogError.revisionConflict(
                expected: expectedRevision,
                actual: current.revision
            )
        }
        let composition: Base.Composition
        do {
            composition = try Base.Composition(id: id, bases: bases)
        } catch {
            throw DatabaseCompositionCatalogError.corruptedRecord(id)
        }
        let record = DatabaseCompositionRecord(
            composition: composition,
            revision: try increment(current.revision, id: id),
            generation: try increment(current.generation, id: id)
        )
        try write(record, transaction: transaction)
        return record
    }

    package func delete(
        _ id: Base.Composition.ID,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> (revision: UInt64, generation: UInt64) {
        guard let current = try await load(id, transaction: transaction) else {
            throw DatabaseCompositionCatalogError.compositionNotFound(id)
        }
        guard current.revision == expectedRevision else {
            throw DatabaseCompositionCatalogError.revisionConflict(
                expected: expectedRevision,
                actual: current.revision
            )
        }
        let revision = try increment(current.revision, id: id)
        let generation = try increment(current.generation, id: id)
        try transaction.clear(key: recordKey(id))
        return (revision, generation)
    }

    package func contains(
        baseID: Base.ID,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try await loadAll(transaction: transaction).contains {
            $0.composition.bases.contains(baseID)
        }
    }

    private func recordKey(_ id: Base.Composition.ID) -> ByteString {
        records.pack(Tuple(id.value))
    }

    private func write(
        _ record: DatabaseCompositionRecord,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(
            StorageFrameCodec.encode(record),
            for: recordKey(record.composition.id)
        )
    }

    private func increment(
        _ value: UInt64,
        id: Base.Composition.ID
    ) throws -> UInt64 {
        let (next, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseCompositionCatalogError.corruptedRecord(id)
        }
        return next
    }
}
