// CountUpdatesIndexMaintainer.swift
// AggregationIndex - Index maintainer for COUNT_UPDATES aggregation
//
// Tracks the number of times each record has been updated.
// Reference: FDB Record Layer COUNT_UPDATES index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for COUNT_UPDATES indexes
///
/// **Functionality**:
/// - Track update counts per record
/// - Atomic increment on updates
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
        transaction: any TransactionProtocol
    ) async throws {
        let oldKey = try oldItem.map { try packAndValidate(DataAccess.extractId(from: $0, using: idExpression)) }
        let newKey = try newItem.map { try packAndValidate(DataAccess.extractId(from: $0, using: idExpression)) }

        switch (oldKey, newKey) {
        case (nil, let key?):
            // Insert: Initialize count to 0
            transaction.setValue(ByteConversion.int64ToBytes(0), for: key)

        case (let key?, nil):
            // Delete: Remove count entry
            transaction.clear(key: key)

        case (let oldKey?, let newKey?) where oldKey == newKey:
            // Same ID: Increment count
            let increment = ByteConversion.int64ToBytes(1)
            transaction.atomicOp(key: oldKey, param: increment, mutationType: .add)

        case (let oldKey?, let newKey?):
            // ID changed (unusual): Remove old, initialize new
            transaction.clear(key: oldKey)
            transaction.setValue(ByteConversion.int64ToBytes(0), for: newKey)

        case (nil, nil):
            break
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let key = try packAndValidate(id)
        transaction.setValue(ByteConversion.int64ToBytes(0), for: key)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        [try packAndValidate(id)]
    }

    // MARK: - Query Methods

    /// Get the update count for a specific record
    public func getUpdateCount(
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> Int64? {
        let key = try packAndValidate(id)
        guard let bytes = try await transaction.getValue(for: key) else {
            return nil
        }
        return ByteConversion.bytesToInt64(bytes)
    }

    /// Get all update counts
    public func getAllUpdateCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(id: Tuple, count: Int64)] {
        let range = subspace.range()
        var results: [(id: Tuple, count: Int64)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }
            let idTuple = try subspace.unpack(key)
            let count = ByteConversion.bytesToInt64(value)
            results.append((id: idTuple, count: count))
        }

        return results
    }

    /// Get records with update count above threshold
    public func getFrequentlyUpdated(
        threshold: Int64,
        transaction: any TransactionProtocol
    ) async throws -> [(id: Tuple, count: Int64)] {
        let allCounts = try await getAllUpdateCounts(transaction: transaction)
        return allCounts.filter { $0.count >= threshold }
    }
}
