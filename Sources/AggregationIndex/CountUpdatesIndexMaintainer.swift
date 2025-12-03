// CountUpdatesIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for COUNT_UPDATES aggregation
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
/// **Examples**:
/// ```swift
/// Key: [I]/Document_updates_id/["doc-123"] = 5
/// Key: [I]/Document_updates_id/["doc-456"] = 12
/// ```
///
/// **Behavior**:
/// - Insert: Set count to 0 (first version, no updates yet)
/// - Update: Increment count by 1
/// - Delete: Remove the count entry
public struct CountUpdatesIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // MARK: - Initialization

    /// Initialize count updates index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
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

    /// Update index when item changes
    ///
    /// **Cases**:
    /// - Insert (oldItem=nil, newItem=some): Set count to 0
    /// - Delete (oldItem=some, newItem=nil): Remove count entry
    /// - Update (both some): Increment count by 1
    ///
    /// - Parameters:
    ///   - oldItem: Previous item (nil for insert)
    ///   - newItem: New item (nil for delete)
    ///   - transaction: FDB transaction
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Extract IDs
        let oldId: Tuple?
        if let oldItem = oldItem {
            oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
        } else {
            oldId = nil
        }

        let newId: Tuple?
        if let newItem = newItem {
            newId = try DataAccess.extractId(from: newItem, using: idExpression)
        } else {
            newId = nil
        }

        switch (oldId, newId) {
        case (nil, let id?):
            // Insert: Initialize count to 0
            let key = try packAndValidate(id)
            let zeroBytes = ByteConversion.int64ToBytes(0)
            transaction.setValue(zeroBytes, for: key)

        case (let id?, nil):
            // Delete: Remove count entry
            let key = try packAndValidate(id)
            transaction.clear(key: key)

        case (let oldId?, let newId?):
            // Update
            let oldKey = try packAndValidate(oldId)
            let newKey = try packAndValidate(newId)

            if oldKey == newKey {
                // Same ID: Increment count
                let increment = ByteConversion.int64ToBytes(1)
                transaction.atomicOp(key: oldKey, param: increment, mutationType: .add)
            } else {
                // ID changed (unusual): Remove old, initialize new
                transaction.clear(key: oldKey)
                let zeroBytes = ByteConversion.int64ToBytes(0)
                transaction.setValue(zeroBytes, for: newKey)
            }

        case (nil, nil):
            // Nothing to do
            break
        }
    }

    /// Build index entries for an item during batch indexing
    ///
    /// - Parameters:
    ///   - item: Item to index
    ///   - id: The item's unique identifier
    ///   - transaction: FDB transaction
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let key = try packAndValidate(id)
        // During batch build, initialize count to 0
        let zeroBytes = ByteConversion.int64ToBytes(0)
        transaction.setValue(zeroBytes, for: key)
    }

    /// Compute expected index keys for an item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return [try packAndValidate(id)]
    }

    // MARK: - Query Methods

    /// Get the update count for a specific record
    ///
    /// - Parameters:
    ///   - id: The record's primary key
    ///   - transaction: The transaction to use
    /// - Returns: The update count (0 if record exists but never updated, nil if not found)
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
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (id, updateCount) tuples
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
    ///
    /// - Parameters:
    ///   - threshold: Minimum update count
    ///   - transaction: The transaction to use
    /// - Returns: Array of (id, updateCount) tuples
    public func getFrequentlyUpdated(
        threshold: Int64,
        transaction: any TransactionProtocol
    ) async throws -> [(id: Tuple, count: Int64)] {
        let allCounts = try await getAllUpdateCounts(transaction: transaction)
        return allCounts.filter { $0.count >= threshold }
    }
}
