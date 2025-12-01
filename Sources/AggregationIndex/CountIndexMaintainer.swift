// CountIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for COUNT aggregation
//
// Maintains counts using atomic FDB operations for thread-safe updates.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for COUNT aggregation indexes
///
/// **Functionality**:
/// - Maintain counts of items grouped by field values
/// - Atomic increment/decrement operations
/// - Efficient GROUP BY COUNT queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Int64 (8 bytes little-endian)
/// ```
///
/// **Examples**:
/// ```swift
/// // Single grouping field
/// Key: [I]/User_city/["Tokyo"] = 150
/// Key: [I]/User_city/["London"] = 230
///
/// // Multiple grouping fields
/// Key: [I]/Order_status_type/["pending"]/["normal"] = 45
/// Key: [I]/Order_status_type/["shipped"]/["express"] = 12
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = CountIndexMaintainer<User>(
///     index: cityCountIndex,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct CountIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // MARK: - Initialization

    /// Initialize count index maintainer
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
    /// - Insert (oldItem=nil, newItem=some): Increment count
    /// - Delete (oldItem=some, newItem=nil): Decrement count
    /// - Update (both some, same group): No change
    /// - Update (both some, different group): Decrement old, increment new
    ///
    /// **Atomic Operations**: Uses FDB.MutationType.add (no read-modify-write)
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
        // Extract grouping values for old and new items
        let oldGrouping: [any TupleElement]?
        if let oldItem = oldItem {
            oldGrouping = try DataAccess.evaluateIndexFields(
                from: oldItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } else {
            oldGrouping = nil
        }

        let newGrouping: [any TupleElement]?
        if let newItem = newItem {
            newGrouping = try DataAccess.evaluateIndexFields(
                from: newItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } else {
            newGrouping = nil
        }

        // Compare groupings if both exist (update case)
        if let old = oldGrouping, let new = newGrouping {
            let oldKey = subspace.pack(Tuple(old))
            let newKey = subspace.pack(Tuple(new))

            if oldKey == newKey {
                // Same group, count unchanged - no operation needed
                return
            } else {
                // Different groups: decrement old, increment new
                let decrement = int64ToBytes(-1)
                transaction.atomicOp(key: oldKey, param: decrement, mutationType: .add)

                let increment = int64ToBytes(1)
                transaction.atomicOp(key: newKey, param: increment, mutationType: .add)
            }
        } else if let new = newGrouping {
            // Insert: increment new group
            let newKey = subspace.pack(Tuple(new))
            let increment = int64ToBytes(1)
            transaction.atomicOp(key: newKey, param: increment, mutationType: .add)
        } else if let old = oldGrouping {
            // Delete: decrement old group
            let oldKey = subspace.pack(Tuple(old))
            let decrement = int64ToBytes(-1)
            transaction.atomicOp(key: oldKey, param: decrement, mutationType: .add)
        }
        // else: both nil, nothing to do
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
        let groupingValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )
        let groupingTuple = Tuple(groupingValues)
        let countKey = subspace.pack(groupingTuple)

        // Increment count
        let increment = int64ToBytes(1)
        transaction.atomicOp(key: countKey, param: increment, mutationType: .add)
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the count key that should be affected by this item.
    /// Note: For COUNT indexes, we return the grouping key, not the full key with value.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let groupingValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )
        let groupingTuple = Tuple(groupingValues)
        return [subspace.pack(groupingTuple)]
    }

    // MARK: - Query Methods

    /// Get the count for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The count (0 if no entries)
    public func getCount(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let groupingTuple = Tuple(groupingValues)
        let countKey = subspace.pack(groupingTuple)

        guard let bytes = try await transaction.getValue(for: countKey) else {
            return 0
        }

        return bytesToInt64(bytes)
    }

    /// Get all counts in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, count) tuples
    public func getAllCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], count: Int64)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let count = bytesToInt64(value)

            results.append((grouping: elements, count: count))
        }

        return results
    }

    // MARK: - Private Helpers

    /// Convert Int64 to little-endian bytes for atomic operations
    private func int64ToBytes(_ value: Int64) -> [UInt8] {
        return withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert little-endian bytes to Int64
    private func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        guard bytes.count == 8 else {
            return 0
        }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }
}
