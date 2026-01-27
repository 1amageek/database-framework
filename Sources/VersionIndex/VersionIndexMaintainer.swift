// VersionIndexMaintainer.swift
// VersionIndexLayer - Index maintainer for version history
//
// Maintains version history using FDB versionstamps for global ordering.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
public struct VersionIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Version history retention strategy
    private let strategy: VersionHistoryStrategy

    // MARK: - Initialization

    public init(
        index: Index,
        strategy: VersionHistoryStrategy,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.strategy = strategy
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
        transaction: any TransactionProtocol
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
        transaction: any TransactionProtocol
    ) async throws {
        try await storeVersion(item: item, id: id, transaction: transaction)
    }

    /// Version indexes don't have deterministic keys (versionstamp is set at commit)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return []
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
        transaction: any TransactionProtocol
    ) async throws -> [(version: Version, data: [UInt8])] {
        let pkTuple = Tuple(primaryKey)
        let beginKey = subspace.pack(pkTuple)
        let endKey = beginKey + [0xFF]

        var versions: [(Version, [UInt8])] = []

        if let limit = limit {
            // Reverse scan: fetch newest N versions directly.
            // FDB stores versionstamps in ascending order (oldest first).
            // Reverse scan returns newest first, so limit correctly returns newest N.
            let result = try await transaction.getRangeNative(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                limit: limit,
                targetBytes: 0,
                streamingMode: .wantAll,
                iteration: 1,
                reverse: true,
                snapshot: true
            )

            for (key, value) in result.records {
                guard key.count >= 10 else { continue }
                let versionBytes = Array(key.suffix(10))
                let version = Version(bytes: versionBytes)

                let data: [UInt8]
                if value.count > 8 {
                    data = Array(value.dropFirst(8))
                } else {
                    data = []
                }

                versions.append((version, data))
            }
            // Already in descending order (newest first) from reverse scan
        } else {
            // No limit: forward scan all versions, then reverse
            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, value) in sequence {
                guard key.count >= 10 else { continue }
                let versionBytes = Array(key.suffix(10))
                let version = Version(bytes: versionBytes)

                let data: [UInt8]
                if value.count > 8 {
                    data = Array(value.dropFirst(8))
                } else {
                    data = []
                }

                versions.append((version, data))
            }
            versions.reverse()
        }

        return versions
    }

    /// Get latest version of an item
    public func getLatestVersion(
        primaryKey: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> [UInt8]? {
        let pkTuple = Tuple(primaryKey)
        let beginKey = subspace.pack(pkTuple)
        let endKey = beginKey + [0xFF]

        // Use lastLessThan to get the most recent version efficiently
        let lastSelector = FDB.KeySelector.lastLessThan(endKey)

        guard let lastKey = try await transaction.getKey(selector: lastSelector, snapshot: true) else {
            return nil
        }

        // Verify the key is in our range
        guard lastKey.starts(with: beginKey) else {
            return nil
        }

        // Get the value
        guard let value = try await transaction.getValue(for: lastKey, snapshot: true) else {
            return nil
        }

        // Return item data (skip first 8 bytes which is timestamp)
        if value.count > 8 {
            return Array(value.dropFirst(8))
        }
        return []
    }

    // MARK: - Private Methods

    /// Store a new version snapshot using FDB versionstamp
    private func storeVersion(
        item: Item,
        id: Tuple? = nil,
        transaction: any TransactionProtocol
    ) async throws {
        // Extract primary key using protocol method
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        // Serialize item
        let itemData = try DataAccess.serialize(item)

        // Build key: [subspace][primaryKey]
        var key = subspace.pack(primaryKeyTuple)
        let versionPosition = key.count

        // Validate position fits in UInt32 range
        guard versionPosition <= Int(UInt32.max) - 10 else {
            throw IndexError.invalidConfiguration("Version key too long")
        }

        // Append 10-byte versionstamp placeholder (0xFF)
        key.append(contentsOf: [UInt8](repeating: 0xFF, count: 10))

        // Append 4-byte position (little-endian) as required by FDB
        let position32 = UInt32(versionPosition)
        let positionBytes = withUnsafeBytes(of: position32.littleEndian) { Array($0) }
        key.append(contentsOf: positionBytes)

        // Build value: [timestamp(8 bytes)][item data]
        let timestamp = Date().timeIntervalSince1970
        var value = withUnsafeBytes(of: timestamp.bitPattern) { Array($0) }
        value.append(contentsOf: itemData)

        // Use atomicOp with setVersionstampedKey
        // FDB will replace 10 bytes at versionPosition with actual versionstamp
        transaction.atomicOp(key: key, param: value, mutationType: .setVersionstampedKey)
    }

    /// Store a deletion marker using FDB versionstamp
    private func storeDeletionMarker(
        item: Item,
        transaction: any TransactionProtocol
    ) async throws {
        let primaryKeyTuple = try resolveItemId(for: item, providedId: nil)

        // Build key: [subspace][primaryKey]
        var key = subspace.pack(primaryKeyTuple)
        let versionPosition = key.count

        guard versionPosition <= Int(UInt32.max) - 10 else {
            throw IndexError.invalidConfiguration("Version key too long")
        }

        // Append 10-byte versionstamp placeholder
        key.append(contentsOf: [UInt8](repeating: 0xFF, count: 10))

        // Append 4-byte position (little-endian)
        let position32 = UInt32(versionPosition)
        let positionBytes = withUnsafeBytes(of: position32.littleEndian) { Array($0) }
        key.append(contentsOf: positionBytes)

        // Value: [timestamp(8 bytes)] only (empty item data = deletion marker)
        let timestamp = Date().timeIntervalSince1970
        let value = withUnsafeBytes(of: timestamp.bitPattern) { Array($0) }

        transaction.atomicOp(key: key, param: value, mutationType: .setVersionstampedKey)
    }

    /// Get primary key subspace for an item
    private func getPrimaryKeySubspace(for item: Item) throws -> (subspace: Subspace, beginKey: [UInt8], endKey: [UInt8]) {
        let primaryKeyTuple = try resolveItemId(for: item, providedId: nil)
        let beginKey = subspace.pack(primaryKeyTuple)
        let endKey = beginKey + [0xFF]
        return (Subspace(prefix: beginKey), beginKey, endKey)
    }

    /// Apply retention strategy to clean up old versions
    private func applyRetentionStrategy(
        item: Item,
        transaction: any TransactionProtocol
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
        transaction: any TransactionProtocol
    ) async throws {
        let (_, beginKey, endKey) = try getPrimaryKeySubspace(for: item)

        var versionKeys: [[UInt8]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: false
        )

        for try await (key, _) in sequence {
            versionKeys.append(key)
        }

        // Keep count - 1 existing versions (new version will be added after this)
        let keepCount = max(0, count - 1)
        if versionKeys.count > keepCount {
            let keysToDelete = versionKeys.dropLast(keepCount)
            for keyToDelete in keysToDelete {
                transaction.clear(key: keyToDelete)
            }
        }
    }

    /// Apply keepForDuration retention strategy
    private func applyKeepForDurationStrategy(
        item: Item,
        duration: TimeInterval,
        transaction: any TransactionProtocol
    ) async throws {
        let (_, beginKey, endKey) = try getPrimaryKeySubspace(for: item)
        let cutoffTime = Date().timeIntervalSince1970 - duration

        var versionsToDelete: [[UInt8]] = []
        var totalCount = 0

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: false
        )

        for try await (key, value) in sequence {
            totalCount += 1

            // Extract timestamp from value (first 8 bytes)
            guard value.count >= 8 else { continue }
            let bitPattern = value.prefix(8).withUnsafeBytes { $0.load(as: UInt64.self) }
            let timestamp = TimeInterval(bitPattern: bitPattern)

            if timestamp < cutoffTime {
                versionsToDelete.append(key)
            }
        }

        // Always keep at least one version
        if versionsToDelete.count == totalCount && totalCount > 0 {
            versionsToDelete.removeLast()
        }

        for keyToDelete in versionsToDelete {
            transaction.clear(key: keyToDelete)
        }
    }
}
