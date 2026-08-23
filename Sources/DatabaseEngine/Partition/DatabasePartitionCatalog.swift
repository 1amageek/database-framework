import DatabaseKit
import DatabaseTypes
import StorageKit

package struct DatabasePartitionCatalog: Sendable {
    private static let maximumPageSize = 256

    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock
    private let entries: Subspace
    private let storageLimits: StorageFrameLimits

    package init(
        engine: any StorageEngine,
        root: Subspace,
        clock: any StorageMonotonicClock,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: engine)
        self.clock = clock
        self.entries = root
            .subspace("database-framework")
            .subspace("partition-catalog")
            .subspace("entries")
        self.storageLimits = storageLimits
    }

    /// Opens the pre-Base partition catalog whose namespace root was resolved
    /// through the backend namespace service. This exists only for the explicit
    /// layout-v1 migration path. Standard execution uses the single database
    /// root; `MultiBase` execution uses the operation-bound Base root.
    package init(
        legacyEngine engine: any StorageEngine,
        resolvedCatalogRoot: Subspace,
        clock: any StorageMonotonicClock,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: engine)
        self.clock = clock
        self.entries = resolvedCatalogRoot.subspace("entries")
        self.storageLimits = storageLimits
    }

    package func register(
        entity: String,
        partitions: FieldObject
    ) async throws {
        try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: clock
        ) { transaction in
            try await register(
                entity: entity,
                partitions: partitions,
                transaction: transaction
            )
        }
    }

    package func register(
        entity: String,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws {
        try validate(entity: entity, partitions: partitions)
        let entry = DatabasePartitionCatalogEntry(
            entity: entity,
            partitions: partitions
        )
        let bytes = try StorageFrameCodec.encode(
            entry,
            limits: storageLimits
        )
        let key = entryKey(entity: entity, encodedEntry: bytes)
        let storageBytes = bytes

        if let existing = try await transaction.getValue(
            for: key,
            snapshot: false
        ) {
            guard existing == storageBytes else {
                throw DatabasePartitionCatalogError.digestCollision
            }
            return
        }
        try transaction.setValue(storageBytes, for: key)
    }

    package func contains(
        entity: String,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> Bool {
        try validate(entity: entity, partitions: partitions)
        let entry = DatabasePartitionCatalogEntry(
            entity: entity,
            partitions: partitions
        )
        let bytes = try StorageFrameCodec.encode(
            entry,
            limits: storageLimits
        )
        let key = entryKey(entity: entity, encodedEntry: bytes)
        guard let existing = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return false
        }
        guard existing == bytes else {
            throw DatabasePartitionCatalogError.digestCollision
        }
        return true
    }

    package func page(
        entity: String? = nil,
        continuation: ByteString? = nil,
        limit: Int
    ) async throws -> DatabasePartitionCatalogPage {
        guard limit > 0, limit <= Self.maximumPageSize else {
            throw DatabasePartitionCatalogError.invalidPageLimit(
                actual: limit,
                maximum: Self.maximumPageSize
            )
        }
        if let entity, entity.isEmpty {
            throw DatabasePartitionCatalogError.invalidEntity(entity)
        }

        let scanSpace: Subspace
        if let entity {
            scanSpace = entries.subspace(entity)
        } else {
            scanSpace = entries
        }
        let range = scanSpace.range()
        let begin: KeySelector
        if let continuation {
            let decoded: DatabasePartitionCatalogContinuation
            do {
                decoded = try StorageFrameCodec.decode(
                    DatabasePartitionCatalogContinuation.self,
                    from: continuation,
                    limits: storageLimits
                )
            } catch {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            let lastKey = decoded.lastKey
            guard decoded.entity == entity,
                  scanSpace.contains(lastKey) else {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            begin = .firstGreaterThan(lastKey)
        } else {
            begin = .firstGreaterOrEqual(range.begin)
        }

        return try await transactionExecutor.withTransaction(
            configuration: .batch,
            clock: clock
        ) { transaction in
            try await page(
                entity: entity,
                begin: begin,
                rangeEnd: range.end,
                limit: limit,
                snapshot: true,
                transaction: transaction
            )
        }
    }

    /// Reads a partition page in a caller-owned transaction.
    package func page(
        entity: String,
        continuation: ByteString?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> DatabasePartitionCatalogPage {
        guard limit > 0, limit <= Self.maximumPageSize else {
            throw DatabasePartitionCatalogError.invalidPageLimit(
                actual: limit,
                maximum: Self.maximumPageSize
            )
        }
        guard !entity.isEmpty else {
            throw DatabasePartitionCatalogError.invalidEntity(entity)
        }
        let scanSpace = entries.subspace(entity)
        let range = scanSpace.range()
        let begin: KeySelector
        if let continuation {
            let decoded: DatabasePartitionCatalogContinuation
            do {
                decoded = try StorageFrameCodec.decode(
                    DatabasePartitionCatalogContinuation.self,
                    from: continuation,
                    limits: storageLimits
                )
            } catch {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            guard decoded.entity == entity,
                  scanSpace.contains(decoded.lastKey) else {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            begin = .firstGreaterThan(decoded.lastKey)
        } else {
            begin = .firstGreaterOrEqual(range.begin)
        }
        return try await page(
            entity: entity,
            begin: begin,
            rangeEnd: range.end,
            limit: limit,
            snapshot: false,
            transaction: transaction
        )
    }

    package func containsEntries(
        entity: String,
        transaction: any TransactionReadAccess
    ) async throws -> Bool {
        let page = try await page(
            entity: entity,
            continuation: nil,
            limit: 1,
            transaction: transaction
        )
        return !page.entries.isEmpty
    }

    private func page(
        entity: String?,
        begin: KeySelector,
        rangeEnd: ByteString,
        limit: Int,
        snapshot: Bool,
        transaction: any TransactionReadAccess
    ) async throws -> DatabasePartitionCatalogPage {
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: begin,
            to: .firstGreaterOrEqual(rangeEnd),
            limit: limit + 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let visibleRows = rows.prefix(limit)
        var decodedEntries: [DatabasePartitionCatalogItem] = []
        decodedEntries.reserveCapacity(visibleRows.count)
        for row in visibleRows {
            let entry = try decodeEntry(key: row.0, bytes: row.1)
            decodedEntries.append(
                DatabasePartitionCatalogItem(
                    entity: entry.entity,
                    partitions: entry.partitions
                )
            )
        }
        let next: ByteString?
        if rows.count > limit, let lastKey = visibleRows.last?.0 {
            next = try StorageFrameCodec.encode(
                DatabasePartitionCatalogContinuation(
                    entity: entity,
                    lastKey: lastKey
                ),
                limits: storageLimits
            )
        } else {
            next = nil
        }
        return DatabasePartitionCatalogPage(
            entries: decodedEntries,
            continuation: next
        )
    }

    private func decodeEntry(
        key: ByteString,
        bytes: ByteString
    ) throws -> DatabasePartitionCatalogEntry {
        let entry: DatabasePartitionCatalogEntry
        do {
            entry = try StorageFrameCodec.decode(
                DatabasePartitionCatalogEntry.self,
                from: bytes,
                limits: storageLimits
            )
        } catch {
            throw DatabasePartitionCatalogError.corruptedEntry
        }
        try validate(entity: entry.entity, partitions: entry.partitions)
        guard entryKey(entity: entry.entity, encodedEntry: bytes) == key else {
            throw DatabasePartitionCatalogError.corruptedEntry
        }
        return entry
    }

    private func validate(
        entity: String,
        partitions: FieldObject
    ) throws {
        guard !entity.isEmpty else {
            throw DatabasePartitionCatalogError.invalidEntity(entity)
        }
        guard !partitions.isEmpty else {
            throw DatabasePartitionCatalogError.invalidPartitions(
                entity: entity,
                reason: "a dynamic partition must contain at least one field"
            )
        }
        for partition in partitions.fields {
            guard !partition.key.isEmpty,
                  partition.value != .null else {
                throw DatabasePartitionCatalogError.invalidPartitions(
                    entity: entity,
                    reason: "partition fields must have non-empty names and non-null values"
                )
            }
        }
    }

    private func entryKey(
        entity: String,
        encodedEntry: ByteString
    ) -> ByteString {
        var hasher = SHA256Accumulator()
        hasher.update(encodedEntry)
        let digest = hasher.finalize()
        let storageDigest = ByteString.copying(count: digest.count) { destination in
            digest.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
        }
        return entries.subspace(entity).pack(Tuple(storageDigest))
    }
}
