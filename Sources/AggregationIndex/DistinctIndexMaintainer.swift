// DistinctIndexMaintainer.swift
// AggregationIndex - Index maintainer for DISTINCT aggregation using HyperLogLog++
//
// Maintains approximate cardinality (distinct count) using HyperLogLog++.
// Note: This is add-only - deletions do NOT decrease the cardinality.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

extension DistinctIndexKind: IndexKindMaintainable {
    /// Create a DistinctIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return DistinctIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            precision: precision
        )
    }
}

// MARK: - AggregationIndexKindProtocol Conformance

extension DistinctIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "distinct" }

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - DistinctIndexMaintainer

/// Maintainer for DISTINCT aggregation indexes using HyperLogLog++
///
/// **Functionality**:
/// - Maintain approximate distinct counts grouped by field values
/// - Uses HyperLogLog++ for O(1) cardinality estimation
/// - ~0.81% standard error (fixed precision=14, 16384 registers)
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Serialized HyperLogLog (JSON, ~16KB)
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., distinct_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to count distinct
///
/// **Important Limitations**:
/// - Add-only: Deleting an item does NOT decrease the cardinality
/// - Approximate: Results are estimates with ~0.81% error
/// - Memory: ~16KB per group (fixed precision=14)
///
/// **Note**: The precision parameter in DistinctIndexKind is accepted for API
/// compatibility but Core.HyperLogLog uses fixed precision=14. The actual
/// error rate is always ~0.81%.
public struct DistinctIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer, GroupingKeySupport {
    // MARK: - Constants

    /// Fixed precision for Core.HyperLogLog (2^14 = 16384 registers)
    /// This is the actual precision used by HyperLogLog, regardless of user-specified value
    /// Note: Computed property because static stored properties are not supported in generic types
    private var fixedPrecision: Int { 14 }

    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// User-specified precision (stored for API compatibility, but not used)
    /// Core.HyperLogLog uses fixed precision=14
    @available(*, deprecated, message: "Core.HyperLogLog uses fixed precision=14")
    private let precision: Int

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        precision: Int
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.precision = precision
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // For INSERT: add value to HLL
        // For DELETE: no-op (HLL is add-only, cannot remove values)
        // For UPDATE: add new value (old value remains in HLL)

        if let newItem = newItem {
            try await addValueToHLL(item: newItem, transaction: transaction)
        }

        // Note: We intentionally do NOT handle deletion
        // HyperLogLog is an add-only data structure
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        try await addValueToHLL(item: item, transaction: transaction)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Sparse index: if any field value is nil, no index entry
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }
        guard allValues.count >= 1 else { return [] }

        let groupingValues = allValues.count > 1 ? Array(allValues.dropLast()) : []
        return [try buildGroupingKey(groupingValues)]
    }

    // MARK: - Query Methods

    /// Get the estimated distinct count for a specific grouping
    ///
    /// - Parameters:
    ///   - groupingValues: Values for grouping fields
    ///   - transaction: Transaction to use
    /// - Returns: Tuple of (estimatedCount, errorRate)
    public func getDistinctCount(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> (estimated: Int64, errorRate: Double) {
        let key = try buildGroupingKey(groupingValues)

        guard let data = try await transaction.getValue(for: key) else {
            return (estimated: 0, errorRate: 0)
        }

        let hll = try JSONDecoder().decode(Core.HyperLogLog.self, from: Data(data))
        let estimated = hll.cardinality()

        // Standard error for HyperLogLog++ with precision p: 1.04 / sqrt(2^p)
        // Uses fixed precision=14 (Core.HyperLogLog constant)
        let errorRate = 1.04 / sqrt(Double(1 << fixedPrecision))

        return (estimated: estimated, errorRate: errorRate)
    }

    /// Get all distinct counts in this index
    ///
    /// - Parameter transaction: Transaction to use
    /// - Returns: Array of (grouping values, estimated count, error rate)
    public func getAllDistinctCounts(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], estimated: Int64, errorRate: Double)] {
        let range = subspace.range()

        var results: [(grouping: [any TupleElement], estimated: Int64, errorRate: Double)] = []
        // Standard error for HyperLogLog++ with fixed precision=14
        let errorRate = 1.04 / sqrt(Double(1 << fixedPrecision))

        for try await (key, value) in transaction.getRange(from: range.begin, to: range.end) {
            // Extract grouping values from key by unpacking the subspace
            let keyTuple = try subspace.unpack(key)
            var groupingValues: [any TupleElement] = []
            for i in 0..<keyTuple.count {
                if let element = keyTuple[i] {
                    groupingValues.append(element)
                }
            }

            // Note: JSONDecoder.decode throws on corrupted data (fail-fast behavior)
            // This differs from PercentileIndexMaintainer which skips corrupted entries
            let hll = try JSONDecoder().decode(Core.HyperLogLog.self, from: Data(value))
            results.append((grouping: groupingValues, estimated: hll.cardinality(), errorRate: errorRate))
        }

        return results
    }

    // MARK: - Private Methods

    /// Add a value to the HyperLogLog for the item's group
    private func addValueToHLL(item: Item, transaction: any TransactionProtocol) async throws {
        // Sparse index: if any field value is nil, skip indexing
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return
        }

        guard allValues.count >= 1 else { return }

        // Split: [grouping..., value]
        let groupingValues = allValues.count > 1 ? Array(allValues.dropLast()) : []
        let valueElement = allValues.last!

        let key = try buildGroupingKey(groupingValues)

        // Read existing HLL or create new one
        // Note: Core.HyperLogLog has fixed precision of 14 (16384 registers)
        var hll: Core.HyperLogLog
        if let existingData = try await transaction.getValue(for: key) {
            hll = try JSONDecoder().decode(Core.HyperLogLog.self, from: Data(existingData))
        } else {
            hll = Core.HyperLogLog()
        }

        // Add value to HLL based on its TupleElement type
        // HyperLogLog.add() requires FieldValue
        if let stringValue = valueElement as? String {
            hll.add(FieldValue.string(stringValue))
        } else if let int64Value = valueElement as? Int64 {
            hll.add(FieldValue.int64(int64Value))
        } else if let doubleValue = valueElement as? Double {
            hll.add(FieldValue.double(doubleValue))
        } else {
            // For other types, convert to string representation
            hll.add(FieldValue.string(String(describing: valueElement)))
        }

        // Write updated HLL back
        let encodedData = try JSONEncoder().encode(hll)
        transaction.setValue([UInt8](encodedData), for: key)
    }
}

// MARK: - DistinctIndexError

/// Errors specific to DISTINCT index operations
public enum DistinctIndexError: Error, CustomStringConvertible {
    /// Invalid value for grouping field
    case invalidGroupingValue(fieldName: String)

    /// Invalid value for distinct field
    case invalidDistinctValue(fieldName: String)

    public var description: String {
        switch self {
        case .invalidGroupingValue(let fieldName):
            return "Invalid grouping value for field: \(fieldName)"
        case .invalidDistinctValue(let fieldName):
            return "Invalid distinct value for field: \(fieldName)"
        }
    }
}
