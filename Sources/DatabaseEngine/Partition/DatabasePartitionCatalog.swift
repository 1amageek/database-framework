import DatabaseDigest
import DatabaseValue
import DatabaseWire
import StorageKit

package struct DatabasePartitionCatalog: Sendable {
    private static let maximumPageSize = 256

    private let engine: any StorageEngine
    private let entries: Subspace
    private let wireLimits: DatabaseWireLimits

    package init(
        engine: any StorageEngine,
        wireLimits: DatabaseWireLimits = .default
    ) async throws {
        let root = try await engine.createOrOpenDirectory(
            path: ["database-framework", "partition-catalog"]
        )
        self.engine = engine
        self.entries = root.subspace("entries")
        self.wireLimits = wireLimits
    }

    package func register(
        entity: String,
        partitions: [DatabaseObjectField]
    ) async throws {
        try await engine.withTransaction(configuration: .batch) { transaction in
            try await register(
                entity: entity,
                partitions: partitions,
                transaction: transaction
            )
        }
    }

    package func register(
        entity: String,
        partitions: [DatabaseObjectField],
        transaction: any Transaction
    ) async throws {
        try validate(entity: entity, partitions: partitions)
        let entry = DatabasePartitionCatalogEntry(
            entity: entity,
            partitions: partitions
        )
        let bytes = try DatabaseEnvelopeCodec.encode(entry, limits: wireLimits)
        let key = entryKey(entity: entity, encodedEntry: bytes)
        let storageBytes = Bytes(retaining: bytes)

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

    package func page(
        entity: String? = nil,
        continuation: DatabaseBytes? = nil,
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
                decoded = try DatabaseEnvelopeCodec.decode(
                    DatabasePartitionCatalogContinuation.self,
                    from: continuation,
                    limits: wireLimits
                )
            } catch {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            let lastKey = Bytes(retaining: decoded.lastKey)
            guard decoded.entity == entity,
                  scanSpace.contains(lastKey) else {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            begin = .firstGreaterThan(lastKey)
        } else {
            begin = .firstGreaterOrEqual(range.begin)
        }

        return try await engine.withTransaction(configuration: .batch) { transaction in
            let rows = try await transaction.collectRange(
                from: begin,
                to: .firstGreaterOrEqual(range.end),
                limit: limit + 1,
                snapshot: true,
                streamingMode: .iterator
            )
            let visibleRows = rows.prefix(limit)
            var decodedEntries: [DatabasePartitionCatalogEntry] = []
            decodedEntries.reserveCapacity(visibleRows.count)
            for row in visibleRows {
                decodedEntries.append(
                    try decodeEntry(
                        key: row.0,
                        bytes: DatabaseBytes(retaining: row.1)
                    )
                )
            }

            let next: DatabaseBytes?
            if rows.count > limit, let lastKey = visibleRows.last?.0 {
                next = try DatabaseEnvelopeCodec.encode(
                    DatabasePartitionCatalogContinuation(
                        entity: entity,
                        lastKey: DatabaseBytes(retaining: lastKey)
                    ),
                    limits: wireLimits
                )
            } else {
                next = nil
            }
            return DatabasePartitionCatalogPage(
                entries: decodedEntries,
                continuation: next
            )
        }
    }

    private func decodeEntry(
        key: Bytes,
        bytes: DatabaseBytes
    ) throws -> DatabasePartitionCatalogEntry {
        do {
            let entry = try DatabaseEnvelopeCodec.decode(
                DatabasePartitionCatalogEntry.self,
                from: bytes,
                limits: wireLimits
            )
            try validate(entity: entry.entity, partitions: entry.partitions)
            guard entryKey(entity: entry.entity, encodedEntry: bytes) == key else {
                throw DatabasePartitionCatalogError.corruptedEntry
            }
            return entry
        } catch let error as DatabasePartitionCatalogError {
            throw error
        } catch {
            throw DatabasePartitionCatalogError.corruptedEntry
        }
    }

    private func validate(
        entity: String,
        partitions: [DatabaseObjectField]
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
        var names = Set<String>()
        var numbers = Set<UInt32>()
        for partition in partitions {
            guard partition.number > 0,
                  !partition.name.isEmpty,
                  partition.value != .null,
                  names.insert(partition.name).inserted,
                  numbers.insert(partition.number).inserted else {
                throw DatabasePartitionCatalogError.invalidPartitions(
                    entity: entity,
                    reason: "partition fields must be non-null and have unique names and numbers"
                )
            }
        }
    }

    private func entryKey(
        entity: String,
        encodedEntry: DatabaseBytes
    ) -> Bytes {
        var hasher = SHA256Accumulator()
        hasher.update(encodedEntry)
        let digest = hasher.finalize()
        let storageDigest = Bytes.copying(count: digest.count) { destination in
            digest.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
        }
        return entries.subspace(entity).pack(Tuple(storageDigest))
    }
}
