// CountUpdatesIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT_UPDATES aggregation
//
// Tracks the number of times each entity has been updated.
// Reference: FDB Record Layer COUNT_UPDATES index type

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for COUNT_UPDATES indexes
///
/// **Functionality**:
/// - Track update counts per entity
/// - Checked transactional increment on updates
/// - Query by update frequency
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][primaryKey]
/// Value: Int64 (update count, 8 bytes little-endian)
/// ```
///
/// **Behavior**:
/// - Insert: Set count to 0 (first version, no updates yet)
/// - Update: Increment count by 1
/// - Delete: Remove the count entry
public struct CountUpdatesIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private var maximumScanEntries: Int { 100_000 }

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        let oldKey = try oldItem.map { try packAndValidate(DataAccess.extractId(from: $0, using: idExpression)) }
        let newKey = try newItem.map { try packAndValidate(DataAccess.extractId(from: $0, using: idExpression)) }

        switch (oldKey, newKey) {
        case (nil, let key?):
            // Insert: Initialize count to 0
            try transaction.setValue(ByteConversion.int64ToBytes(0), for: key)

        case (let key?, nil):
            // Delete: Remove count entry
            try transaction.clear(key: key)

        case (let oldKey?, let newKey?) where oldKey == newKey:
            // Same ID: checked read/replace preserves overflow semantics on
            // every storage backend instead of relying on wrapping atomics.
            guard let stored = try await transaction.getValue(for: oldKey) else {
                throw AggregationIndexError.invalidStructure(
                    "COUNT_UPDATES entry is missing for an existing entity"
                )
            }
            let current = try ByteConversion.bytesToInt64(stored)
            guard current >= 0 else {
                throw AggregationStorageError.negativeCount(current)
            }
            let (updated, overflow) = current.addingReportingOverflow(1)
            guard !overflow else {
                throw AggregationStorageError.integerOverflow
            }
            try transaction.setValue(
                ByteConversion.int64ToBytes(updated),
                for: oldKey
            )

        case (let oldKey?, let newKey?):
            // ID changed (unusual): Remove old, initialize new
            try transaction.clear(key: oldKey)
            try transaction.setValue(ByteConversion.int64ToBytes(0), for: newKey)

        case (nil, nil):
            break
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        let key = try packAndValidate(id)
        try transaction.setValue(ByteConversion.int64ToBytes(0), for: key)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        [try packAndValidate(id)]
    }

    // MARK: - Query Methods

    /// Get the update count for a specific entity
    public func getUpdateCount(
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws -> Int64? {
        let key = try packAndValidate(id)
        guard let bytes = try await transaction.getValue(for: key) else {
            return nil
        }
        let count = try ByteConversion.bytesToInt64(bytes)
        guard count >= 0 else {
            throw AggregationStorageError.negativeCount(count)
        }
        return count
    }

    /// Get all update counts
    public func getAllUpdateCounts(
        transaction: any TransactionAccess
    ) async throws -> [(id: Tuple, count: Int64)] {
        try await scanUpdateCounts(
            minimumCount: nil,
            transaction: transaction
        )
    }

    /// Get entities with update count above threshold
    public func getFrequentlyUpdated(
        threshold: Int64,
        transaction: any TransactionAccess
    ) async throws -> [(id: Tuple, count: Int64)] {
        try await scanUpdateCounts(
            minimumCount: threshold,
            transaction: transaction
        )
    }

    private func scanUpdateCounts(
        minimumCount: Int64?,
        transaction: any TransactionAccess
    ) async throws -> [(id: Tuple, count: Int64)] {
        let range = subspace.range()
        var results: [(id: Tuple, count: Int64)] = []
        var scannedEntries = 0
        var scannedBytes = 0

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maximumScanEntries + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedEntries += 1
            guard scannedEntries <= maximumScanEntries else {
                throw AggregationStorageError.scanLimitExceeded(
                    maximumScanEntries
                )
            }
            scannedBytes = try checkedAggregationScannedBytes(
                scannedBytes,
                adding: key.count + value.count
            )

            let count = try ByteConversion.bytesToInt64(value)
            guard count >= 0 else {
                throw AggregationStorageError.negativeCount(count)
            }
            if let minimumCount, count < minimumCount {
                return
            }
            results.append((id: try subspace.unpack(key), count: count))
        }
        return results
    }
}
