// AverageIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for AVERAGE aggregation
//
// Maintains averages by storing sum and count separately using atomic operations.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for AVERAGE aggregation indexes
///
/// **Functionality**:
/// - Maintain average values grouped by field values
/// - Store sum and count separately for exact average calculation
/// - Atomic add/subtract operations for both sum and count
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1]...[\"sum\"]
/// Value: Double (8 bytes IEEE 754)
///
/// Key: [indexSubspace][groupValue1]...[\"count\"]
/// Value: Int64 (8 bytes little-endian)
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., averaged_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to average
///
/// **Examples**:
/// ```swift
/// // Average rating by product
/// Key: [I]/Review_productID_rating/[123]/["sum"] = 2250.0
/// Key: [I]/Review_productID_rating/[123]/["count"] = 5
/// // Average: 2250 / 5 = 450 (4.5 stars)
///
/// // Average price by (category, brand)
/// Key: [I]/Product_category_brand_price/["electronics"]/["Apple"]/["sum"] = 5000000.0
/// Key: [I]/Product_category_brand_price/["electronics"]/["Apple"]/["count"] = 50
/// // Average: 5000000 / 50 = 100000 ($1000.00)
/// ```
public struct AverageIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
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

    /// Update index when item changes
    ///
    /// **Process**:
    /// - Insert (oldItem=nil): Add to sum and increment count
    /// - Delete (newItem=nil): Subtract from sum and decrement count
    /// - Update (same group): Apply delta to sum only (count unchanged, 1 atomic op)
    /// - Update (different group): Full subtract/add for both groups
    ///
    /// **Atomic Operations**: Uses FDB.MutationType.add for both sum and count
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
        // Extract grouping tuple and value
        let oldData: (groupingTuple: Tuple, avgValue: Double)?
        if let oldItem = oldItem {
            let values = try DataAccess.evaluateIndexFields(
                from: oldItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractNumericValue(values.last!)
                oldData = (Tuple(groupingValues), avgValue)
            } else {
                oldData = nil
            }
        } else {
            oldData = nil
        }

        let newData: (groupingTuple: Tuple, avgValue: Double)?
        if let newItem = newItem {
            let values = try DataAccess.evaluateIndexFields(
                from: newItem,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
            if values.count >= 2 {
                let groupingValues = Array(values.dropLast())
                let avgValue = try extractNumericValue(values.last!)
                newData = (Tuple(groupingValues), avgValue)
            } else {
                newData = nil
            }
        } else {
            newData = nil
        }

        // Apply updates based on case
        switch (oldData, newData) {
        case let (.some(old), .some(new)) where old.groupingTuple.pack() == new.groupingTuple.pack():
            // Same group: apply delta to sum only (count unchanged)
            let delta = new.avgValue - old.avgValue
            if delta != 0 {
                let sumKey = buildSumKey(groupingTuple: new.groupingTuple)
                try await addToSum(key: sumKey, value: delta, transaction: transaction)
            }

        case let (.some(old), .some(new)):
            // Different groups: full subtract from old, full add to new
            let oldSumKey = buildSumKey(groupingTuple: old.groupingTuple)
            let oldCountKey = buildCountKey(groupingTuple: old.groupingTuple)
            try await addToSum(key: oldSumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: oldCountKey, param: int64ToBytes(-1), mutationType: .add)

            let newSumKey = buildSumKey(groupingTuple: new.groupingTuple)
            let newCountKey = buildCountKey(groupingTuple: new.groupingTuple)
            try await addToSum(key: newSumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: newCountKey, param: int64ToBytes(1), mutationType: .add)

        case let (nil, .some(new)):
            // Insert: add to sum and increment count
            let sumKey = buildSumKey(groupingTuple: new.groupingTuple)
            let countKey = buildCountKey(groupingTuple: new.groupingTuple)
            try await addToSum(key: sumKey, value: new.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: int64ToBytes(1), mutationType: .add)

        case let (.some(old), nil):
            // Delete: subtract from sum and decrement count
            let sumKey = buildSumKey(groupingTuple: old.groupingTuple)
            let countKey = buildCountKey(groupingTuple: old.groupingTuple)
            try await addToSum(key: sumKey, value: -old.avgValue, transaction: transaction)
            transaction.atomicOp(key: countKey, param: int64ToBytes(-1), mutationType: .add)

        case (nil, nil):
            // Nothing to do
            break
        }
    }

    /// Scan item during batch indexing
    ///
    /// - Parameters:
    ///   - item: Item to index
    ///   - id: Primary key tuple
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
                "Average index requires at least 2 fields: [grouping_fields..., averaged_field]"
            )
        }

        let groupingValues = Array(values.dropLast())
        let avgValue = try extractNumericValue(values.last!)

        let groupingTuple = Tuple(groupingValues)
        let sumKey = buildSumKey(groupingTuple: groupingTuple)
        let countKey = buildCountKey(groupingTuple: groupingTuple)

        // Add to sum
        try await addToSum(key: sumKey, value: avgValue, transaction: transaction)

        // Increment count atomically
        let countBytes = int64ToBytes(1)
        transaction.atomicOp(key: countKey, param: countBytes, mutationType: .add)
    }

    /// Compute expected index keys for an item (for scrubber verification)
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

        return [
            buildSumKey(groupingTuple: groupingTuple),
            buildCountKey(groupingTuple: groupingTuple)
        ]
    }

    // MARK: - Query Methods

    /// Get the average for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: Tuple of (sum, count, average)
    /// - Throws: IndexError if no values found
    public func getAverage(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> (sum: Double, count: Int64, average: Double) {
        let groupingTuple = Tuple(groupingValues)
        let sumKey = buildSumKey(groupingTuple: groupingTuple)
        let countKey = buildCountKey(groupingTuple: groupingTuple)

        let sum: Double
        let count: Int64

        if let sumBytes = try await transaction.getValue(for: sumKey) {
            sum = scaledBytesToDouble(sumBytes)
        } else {
            sum = 0.0
        }

        if let countBytes = try await transaction.getValue(for: countKey) {
            count = bytesToInt64(countBytes)
        } else {
            count = 0
        }

        guard count > 0 else {
            throw IndexError.noData("No values found for AVERAGE aggregate")
        }

        let average = sum / Double(count)
        return (sum: sum, count: count, average: average)
    }

    /// Get all averages in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum, count, average) tuples
    public func getAllAverages(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], sum: Double, count: Int64, average: Double)] {
        // Collect all sum keys
        let range = subspace.range()
        var sumData: [String: (grouping: [any TupleElement], sum: Double)] = [:]
        var countData: [String: Int64] = [:]

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard subspace.contains(key) else { break }

            let keyTuple = try subspace.unpack(key)
            let elements = try Tuple.unpack(from: keyTuple.pack())

            guard elements.count >= 1,
                  let marker = elements.last as? String else {
                continue
            }

            let grouping = Array(elements.dropLast())
            // Use Tuple.pack() for stable binary encoding (not String(describing:) which is type-dependent)
            let groupingKey = Data(Tuple(grouping).pack()).base64EncodedString()

            if marker == "sum" {
                sumData[groupingKey] = (grouping: grouping, sum: scaledBytesToDouble(value))
            } else if marker == "count" {
                countData[groupingKey] = bytesToInt64(value)
            }
        }

        // Combine sum and count data
        var results: [(grouping: [any TupleElement], sum: Double, count: Int64, average: Double)] = []

        for (groupingKey, sumInfo) in sumData {
            let count = countData[groupingKey] ?? 0
            guard count > 0 else { continue }

            let average = sumInfo.sum / Double(count)
            results.append((grouping: sumInfo.grouping, sum: sumInfo.sum, count: count, average: average))
        }

        return results
    }

    // MARK: - Private Helpers

    private func buildSumKey(groupingTuple: Tuple) -> FDB.Bytes {
        var elements: [any TupleElement] = []
        for i in 0..<groupingTuple.count {
            if let element = groupingTuple[i] {
                elements.append(element)
            }
        }
        elements.append("sum")
        return subspace.pack(Tuple(elements))
    }

    private func buildCountKey(groupingTuple: Tuple) -> FDB.Bytes {
        var elements: [any TupleElement] = []
        for i in 0..<groupingTuple.count {
            if let element = groupingTuple[i] {
                elements.append(element)
            }
        }
        elements.append("count")
        return subspace.pack(Tuple(elements))
    }

    /// Scale factor for fixed-point representation (same as SumIndexMaintainer)
    private static var scaleFactor: Double { 1_000_000.0 }

    /// Add value to sum using atomic operation
    ///
    /// **Atomicity**: Uses FDB's `.add` atomic operation to prevent lost updates
    /// under concurrent transactions.
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

    /// Convert fixed-point Int64 bytes back to Double
    private func scaledBytesToDouble(_ bytes: [UInt8]) -> Double {
        guard bytes.count == 8 else {
            return 0.0
        }
        let scaledValue = bytes.withUnsafeBytes { $0.load(as: Int64.self).littleEndian }
        return Double(scaledValue) / Self.scaleFactor
    }

    /// Extract numeric value from tuple element as Double
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
                "AVERAGE index supports numeric types: Double, Float, Int64, Int, Int32, UInt64. " +
                "Got: \(type(of: element))."
            )
        }
    }

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
