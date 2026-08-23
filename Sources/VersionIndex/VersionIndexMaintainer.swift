// VersionIndexMaintainer.swift
// VersionIndexLayer - Index maintainer for version history
//
// Maintains version history using FDB versionstamps for global ordering.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

private enum VersionValueLayout {
    static let timestampByteCount = MemoryLayout<Int64>.size
        + MemoryLayout<UInt32>.size
}

/// Maintainer for version history indexes
///
/// **Functionality**:
/// - Store item snapshots at each version
/// - Use FDB versionstamps for globally monotonic ordering
/// - Automatic cleanup based on retention strategy
/// - Point-in-time queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][primaryKey][versionstamp(10 bytes)]
/// Value: [timestamp(8 bytes)][item data]
/// ```
///
/// **Design**:
/// - Key uses FDB versionstamp for global ordering (assigned by FDB at commit)
/// - Value stores timestamp for time-based retention (keepForDuration)
/// - This hybrid approach provides both global ordering and time-based queries
///
/// **Examples**:
/// ```swift
/// // Version history
/// Key: [I]/Document_version/[uuid]/[versionstamp1] = [timestamp1][snapshot1]
/// Key: [I]/Document_version/[uuid]/[versionstamp2] = [timestamp2][snapshot2]
/// ```
public struct VersionIndexMaintainer<Item: PersistedEntityValue>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: ResolvedIndex

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Version history retention strategy
    private let strategy: VersionHistoryStrategy

    /// Absolute time is injected by the owning database container so version
    /// retention and persisted timestamps are deterministic across platforms.
    private let wallClock: any WallClock

    // MARK: - Initialization

    public init(
        index: ResolvedIndex,
        strategy: VersionHistoryStrategy,
        subspace: Subspace,
        idExpression: KeyExpression,
        wallClock: any WallClock
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.strategy = strategy
        self.wallClock = wallClock
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
    ///
    /// Uses FDB's SET_VERSIONSTAMPED_KEY for globally ordered version keys.
    ///
    /// **Important**: Retention strategy is applied BEFORE storing the new version
    /// because FDB does not allow reading versionstamped keys in the same transaction.
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Apply retention strategy FIRST (before setVersionstampedKey)
        // FDB constraint: cannot read keys written with setVersionstampedKey in same transaction
        if let item = newItem ?? oldItem {
            try await applyRetentionStrategy(item: item, transaction: transaction)
        }

        // Now store the new version
        if let newItem = newItem {
            try await storeVersion(item: newItem, transaction: transaction)
        } else if let oldItem = oldItem {
            // For deletes, store a deletion marker
            try await storeDeletionMarker(item: oldItem, transaction: transaction)
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        try await storeVersion(item: item, id: id, transaction: transaction)
    }

    /// Version index keys embed a versionstamp assigned at commit time, so the
    /// expected key set cannot be computed from the item alone. Returning an
    /// empty set would mark every item as intentionally unindexed (sparse), so
    /// this reports the missing verification capability explicitly.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        throw IndexVerificationError.expectedKeysUnsupported
    }

    // MARK: - Query Methods

    /// Get version history for an item
    ///
    /// - Parameters:
    ///   - primaryKey: Primary key values
    ///   - limit: Maximum number of versions to return (optional)
    ///   - transaction: FDB transaction
    /// - Returns: Array of (version, timestamp, data) tuples, sorted by version (newest first)
    public func getVersionHistory(
        primaryKey: [any TupleElement],
        limit: Int? = nil,
        transaction: any TransactionReadAccess
    ) async throws -> [(version: Version, data: ByteString)] {
        try await VersionIndexReader(subspace: subspace).history(
            primaryKey: primaryKey,
            limit: limit,
            transaction: transaction
        )
    }

    /// Get latest version of an item
    public func getLatestVersion(
        primaryKey: [any TupleElement],
        transaction: any TransactionReadAccess
    ) async throws -> ByteString? {
        try await VersionIndexReader(subspace: subspace).latest(
            primaryKey: primaryKey,
            transaction: transaction
        )
    }

    // MARK: - Private Methods

    /// Store a new version snapshot using FDB versionstamp
    private func storeVersion(
        item: Item,
        id: Tuple? = nil,
        transaction: any TransactionAccess
    ) async throws {
        // Extract primary key using protocol method
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        // Serialize item
        let itemData = try DataAccess.serialize(item)

        // Build key: [subspace][primaryKey]
        let keyPrefix = subspace.pack(primaryKeyTuple)
        let versionPosition = keyPrefix.count

        // Validate position fits in UInt32 range
        guard versionPosition <= Int(UInt32.max) else {
            throw VersionIndexError.versionKeyTooLong(
                byteCount: versionPosition
            )
        }

        let key = versionstampedKey(
            prefix: keyPrefix,
            versionPosition: UInt32(versionPosition)
        )

        // Build value: [timestamp(8 bytes)][item data]
        let timestamp = wallClock.now
        let value = versionValue(timestamp: timestamp, itemData: itemData)

        // Use atomicOp with setVersionstampedKey
        // FDB will replace 10 bytes at versionPosition with actual versionstamp
        try transaction.atomicOp(key: key, param: value, mutationType: .setVersionstampedKey)
    }

    /// Store a deletion marker using FDB versionstamp
    private func storeDeletionMarker(
        item: Item,
        transaction: any TransactionAccess
    ) async throws {
        let primaryKeyTuple = try resolveItemId(for: item, providedId: nil)

        // Build key: [subspace][primaryKey]
        let keyPrefix = subspace.pack(primaryKeyTuple)
        let versionPosition = keyPrefix.count

        guard versionPosition <= Int(UInt32.max) else {
            throw VersionIndexError.versionKeyTooLong(
                byteCount: versionPosition
            )
        }

        let key = versionstampedKey(
            prefix: keyPrefix,
            versionPosition: UInt32(versionPosition)
        )

        // Value: [timestamp(8 bytes)] only (empty item data = deletion marker)
        let value = versionValue(timestamp: wallClock.now, itemData: [])

        try transaction.atomicOp(key: key, param: value, mutationType: .setVersionstampedKey)
    }

    private func versionstampedKey(
        prefix: ByteString,
        versionPosition: UInt32
    ) -> ByteString {
        let suffixByteCount = 10 + MemoryLayout<UInt32>.size
        return ByteString.copying(
            count: prefix.count + suffixByteCount
        ) { destination in
            prefix.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[..<source.count]
                ).copyMemory(from: source)
            }
            UnsafeMutableRawBufferPointer(
                rebasing: destination[prefix.count..<(prefix.count + 10)]
            ).initializeMemory(as: UInt8.self, repeating: 0xFF)
            var littleEndianPosition = versionPosition.littleEndian
            withUnsafeBytes(of: &littleEndianPosition) { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[(prefix.count + 10)...]
                ).copyMemory(from: source)
            }
        }
    }

    private func versionValue(
        timestamp: Timestamp,
        itemData: ByteString
    ) -> ByteString {
        let secondsEnd = MemoryLayout<Int64>.size
        let timestampEnd = VersionValueLayout.timestampByteCount
        return ByteString.copying(
            count: timestampEnd + itemData.count
        ) { destination in
            var seconds = timestamp.secondsSinceUnixEpoch.littleEndian
            withUnsafeBytes(of: &seconds) { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[..<secondsEnd]
                ).copyMemory(from: source)
            }
            var nanoseconds = timestamp.nanoseconds.littleEndian
            withUnsafeBytes(of: &nanoseconds) { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[secondsEnd..<timestampEnd]
                ).copyMemory(from: source)
            }
            itemData.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[
                        timestampEnd...
                    ]
                ).copyMemory(from: source)
            }
        }
    }

    /// Get primary key subspace for an item
    private func getPrimaryKeySubspace(for item: Item) throws -> (subspace: Subspace, beginKey: ByteString, endKey: ByteString) {
        let primaryKeyTuple = try resolveItemId(for: item, providedId: nil)
        let beginKey = subspace.pack(primaryKeyTuple)
        let endKey = beginKey.appending(0xFF)
        return (Subspace(prefix: beginKey), beginKey, endKey)
    }

    /// Apply retention strategy to clean up old versions
    private func applyRetentionStrategy(
        item: Item,
        transaction: any TransactionAccess
    ) async throws {
        switch strategy {
        case .keepAll:
            return

        case .keepLast(let count):
            try await applyKeepLastStrategy(item: item, count: count, transaction: transaction)

        case .keepForDuration(let duration):
            try await applyKeepForDurationStrategy(item: item, duration: duration, transaction: transaction)
        }
    }

    /// Apply keepLast(N) retention strategy
    ///
    /// Note: This is called BEFORE adding the new version, so we keep `count - 1` existing versions
    /// to make room for the new one.
    private func applyKeepLastStrategy(
        item: Item,
        count: Int,
        transaction: any TransactionAccess
    ) async throws {
        let (_, beginKey, endKey) = try getPrimaryKeySubspace(for: item)

        var versionKeys: [ByteString] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )

        for (key, _) in sequence {
            versionKeys.append(key)
        }

        // Keep count - 1 existing versions (new version will be added after this)
        let keepCount = max(0, count - 1)
        if versionKeys.count > keepCount {
            let keysToDelete = versionKeys.dropLast(keepCount)
            for keyToDelete in keysToDelete {
                try transaction.clear(key: keyToDelete)
            }
        }
    }

    /// Apply keepForDuration retention strategy
    private func applyKeepForDurationStrategy(
        item: Item,
        duration: TimeSpan,
        transaction: any TransactionAccess
    ) async throws {
        let (_, beginKey, endKey) = try getPrimaryKeySubspace(for: item)
        let cutoffTime = try subtract(duration, from: wallClock.now)

        var versionsToDelete: [ByteString] = []
        var totalCount = 0

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )

        for (key, value) in sequence {
            totalCount += 1

            let timestamp = try decodeTimestamp(from: value)

            if timestamp < cutoffTime {
                versionsToDelete.append(key)
            }
        }

        // Always keep at least one version
        if versionsToDelete.count == totalCount && totalCount > 0 {
            versionsToDelete.removeLast()
        }

        for keyToDelete in versionsToDelete {
            try transaction.clear(key: keyToDelete)
        }
    }

    private func decodeTimestamp(
        from value: ByteString
    ) throws(VersionIndexError) -> Timestamp {
        guard value.count >= VersionValueLayout.timestampByteCount else {
            throw .malformedVersionValue(byteCount: value.count)
        }
        let secondsBits: UInt64
        let nanoseconds: UInt32
        let secondsEnd = MemoryLayout<Int64>.size
        let timestampEnd = VersionValueLayout.timestampByteCount
        do {
            secondsBits = try ByteConversion.bytesToUInt64(
                value[..<secondsEnd]
            )
            nanoseconds = value[secondsEnd..<timestampEnd].withUnsafeBytes {
                UInt32(littleEndian: $0.loadUnaligned(as: UInt32.self))
            }
        } catch {
            throw .malformedVersionValue(byteCount: value.count)
        }
        do {
            return try Timestamp(
                secondsSinceUnixEpoch: Int64(bitPattern: secondsBits),
                nanoseconds: nanoseconds
            )
        } catch {
            throw .invalidTimestamp(error)
        }
    }

    private func subtract(
        _ duration: TimeSpan,
        from timestamp: Timestamp
    ) throws(VersionIndexError) -> Timestamp {
        let secondsDifference = timestamp.secondsSinceUnixEpoch
            .subtractingReportingOverflow(duration.seconds)
        guard !secondsDifference.overflow else {
            throw .timestampArithmeticOverflow
        }
        var seconds = secondsDifference.partialValue
        var nanoseconds = Int64(timestamp.nanoseconds)
            - Int64(duration.nanoseconds)
        if nanoseconds < 0 {
            let borrowed = seconds.subtractingReportingOverflow(1)
            guard !borrowed.overflow else {
                throw .timestampArithmeticOverflow
            }
            seconds = borrowed.partialValue
            nanoseconds += 1_000_000_000
        }
        do {
            return try Timestamp(
                secondsSinceUnixEpoch: seconds,
                nanoseconds: UInt32(nanoseconds)
            )
        } catch {
            throw .invalidTimestamp(error)
        }
    }
}
