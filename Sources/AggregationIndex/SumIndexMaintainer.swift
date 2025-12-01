// SumIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for SUM aggregation
//
// Maintains sums using atomic FDB operations for thread-safe updates.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for SUM aggregation indexes
///
/// **Functionality**:
/// - Maintain sums of numeric values grouped by field values
/// - Atomic add/subtract operations
/// - Efficient GROUP BY SUM queries
/// - Supports both Int64 and Double values
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Double (8 bytes IEEE 754) or Int64 (8 bytes little-endian)
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., sum_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to sum
///
/// **Examples**:
/// ```swift
/// // Sum of sales amount by category
/// Key: [I]/Sale_category_amount/["electronics"] = 350000.0
/// Key: [I]/Sale_category_amount/["clothing"] = 120000.0
///
/// // Sum of order totals by (status, payment_method)
/// Key: [I]/Order_status_payment_total/["shipped"]/["credit_card"] = 5000000.0
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = SumIndexMaintainer<Sale>(
///     index: categorySumIndex,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct SumIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // MARK: - Initialization

    /// Initialize sum index maintainer
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
    /// **Process**:
    /// - Insert (oldItem=nil): Add new sum value
    /// - Delete (newItem=nil): Subtract old sum value
    /// - Update (same group): Apply delta only (1 atomic op instead of 2)
    /// - Update (different group): Subtract from old group, add to new group
    ///
    /// **Atomic Operations**: Uses FDB atomic add with fixed-point Int64
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
        // Extract grouping and sum values
        let oldData: (groupingKey: FDB.Bytes, sumValue: Double)?
        if let oldItem = oldItem {
            let values = try DataAccess.evaluateIndexFields(
                from: oldItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractNumericValue(values.last!)
                let groupingKey = subspace.pack(Tuple(groupingValues))
                oldData = (groupingKey, sumValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingKey: FDB.Bytes, sumValue: Double)?
        if let newItem = newItem {
            let values = try DataAccess.evaluateIndexFields(
                from: newItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let sumValue = try extractNumericValue(values.last!)
                let groupingKey = subspace.pack(Tuple(groupingValues))
                newData = (groupingKey, sumValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        // Apply updates based on case
        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingKey == new.groupingKey:
            // Same group: apply delta only (1 atomic op)
            let delta = new.sumValue - old.sumValue
            if delta != 0 {
                try await addToSum(key: new.groupingKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            // Different groups: subtract from old, add to new (2 atomic ops)
            try await addToSum(key: old.groupingKey, value: -old.sumValue, transaction: transaction)
            try await addToSum(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (nil, .some(new)):
            // Insert: add to new group
            try await addToSum(key: new.groupingKey, value: new.sumValue, transaction: transaction)

        case let (.some(old), nil):
            // Delete: subtract from old group
            try await addToSum(key: old.groupingKey, value: -old.sumValue, transaction: transaction)

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
        let values = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        guard values.count >= 2 else {
            throw IndexError.invalidConfiguration(
                "Sum index requires at least 2 fields: [grouping_fields..., sum_field]"
            )
        }

        let groupingValues = Array(values.dropLast())
        let sumValue = try extractNumericValue(values.last!)

        let groupingTuple = Tuple(groupingValues)
        let sumKey = subspace.pack(groupingTuple)

        // Add to sum
        try await addToSum(key: sumKey, value: sumValue, transaction: transaction)
    }

    /// Compute expected index keys for an item (for scrubber verification)
    ///
    /// Returns the sum key that should be affected by this item.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let values = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        guard values.count >= 2 else {
            return []
        }

        let groupingValues = Array(values.dropLast())
        let groupingTuple = Tuple(groupingValues)
        return [subspace.pack(groupingTuple)]
    }

    // MARK: - Query Methods

    /// Get the sum for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The sum (0.0 if no entries)
    public func getSum(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double {
        let groupingTuple = Tuple(groupingValues)
        let sumKey = subspace.pack(groupingTuple)

        guard let bytes = try await transaction.getValue(for: sumKey) else {
            return 0.0
        }

        return scaledBytesToDouble(bytes)
    }

    /// Get all sums in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSums(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double)] {
        let range = subspace.range()
        var results: [(grouping: [any TupleElement], sum: Double)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())
            let sum = scaledBytesToDouble(value)

            results.append((grouping: elements, sum: sum))
        }

        return results
    }

    // MARK: - Private Helpers

    /// Scale factor for fixed-point representation
    /// 6 decimal places provides precision for most financial calculations
    /// Range: ±9,223,372,036,854.775807 (Int64.max / scale)
    private static var scaleFactor: Double { 1_000_000.0 }

    /// Add value to sum using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation to prevent lost updates
    /// under concurrent transactions.
    ///
    /// **Implementation**: Converts Double to fixed-point Int64 (6 decimal places)
    /// to enable atomic integer addition.
    ///
    /// **Precision**: 6 decimal places (e.g., 123456.789012)
    /// **Range**: ±9,223,372,036,854.775807
    private func addToSum(
        key: FDB.Bytes,
        value: Double,
        transaction: any TransactionProtocol
    ) async throws {
        // Convert to fixed-point Int64 for atomic operation
        let scaledValue = Int64(value * Self.scaleFactor)
        let bytes = withUnsafeBytes(of: scaledValue.littleEndian) { Array($0) }

        // Use atomic add - no read required, concurrent-safe
        transaction.atomicOp(key: key, param: bytes, mutationType: .add)
    }

    /// Extract numeric value from tuple element as Double
    ///
    /// **Supported Types**:
    /// - `Double` → stored as-is
    /// - `Float` → cast to Double
    /// - `Int64` → cast to Double
    /// - `Int` → cast to Double
    /// - `Int32` → cast to Double
    ///
    /// - Parameter element: Tuple element to extract
    /// - Returns: Double value
    /// - Throws: IndexError if element is not a supported numeric type
    private func extractNumericValue(_ element: any TupleElement) throws -> Double {
        if let double = element as? Double {
            return double
        } else if let float = element as? Float {
            return Double(float)
        } else if let int64 = element as? Int64 {
            return Double(int64)
        } else if let int = element as? Int {
            return Double(int)
        } else if let int32 = element as? Int32 {
            return Double(int32)
        } else if let uint64 = element as? UInt64 {
            return Double(uint64)
        } else {
            throw IndexError.invalidConfiguration(
                "SUM index supports numeric types: Double, Float, Int64, Int, Int32, UInt64. " +
                "Got: \(type(of: element))."
            )
        }
    }

    /// Convert fixed-point Int64 bytes back to Double
    ///
    /// Reads an Int64 in little-endian format and converts to Double
    /// by dividing by scale factor.
    private func scaledBytesToDouble(_ bytes: [UInt8]) -> Double {
        guard bytes.count == 8 else {
            return 0.0
        }
        let scaledValue = bytes.withUnsafeBytes { $0.load(as: Int64.self).littleEndian }
        return Double(scaledValue) / Self.scaleFactor
    }
}
