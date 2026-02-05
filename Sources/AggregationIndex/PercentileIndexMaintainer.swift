// PercentileIndexMaintainer.swift
// AggregationIndex - Index maintainer for PERCENTILE aggregation using t-digest
//
// Maintains streaming quantile estimates using t-digest.
// Note: This is add-only - deletions do NOT update the percentiles.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

extension PercentileIndexKind: IndexKindMaintainable {
    /// Create a PercentileIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return PercentileIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            compression: compression
        )
    }
}

// MARK: - AggregationIndexKindProtocol Conformance

extension PercentileIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "percentile" }

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - PercentileIndexMaintainer

/// Maintainer for PERCENTILE aggregation indexes using t-digest
///
/// **Functionality**:
/// - Maintain streaming quantile estimates grouped by field values
/// - Uses t-digest for accurate extreme percentile estimation
/// - High accuracy at p99, p99.9, etc.
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...
/// Value: Serialized TDigest (binary, ~10KB for compression=100)
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., percentile_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to track percentiles
///
/// **Important Limitations**:
/// - Add-only: Deleting an item does NOT update the percentiles
/// - Approximate: Results are estimates
/// - Memory: ~10KB per group (compression=100)
public struct PercentileIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer, GroupingKeySupport {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// t-digest compression parameter
    private let compression: Double

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        compression: Double
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.compression = compression
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // For INSERT: add value to t-digest
        // For DELETE: no-op (t-digest is add-only, cannot remove values)
        // For UPDATE: add new value (old value remains in t-digest)

        if let newItem = newItem {
            try await addValueToTDigest(item: newItem, transaction: transaction)
        }

        // Note: We intentionally do NOT handle deletion
        // t-digest is an add-only data structure
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        try await addValueToTDigest(item: item, transaction: transaction)
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

    /// Get the estimated value at a specific percentile for a grouping
    ///
    /// - Parameters:
    ///   - percentile: Percentile to query (0.0 to 1.0, e.g., 0.99 for p99)
    ///   - groupingValues: Values for grouping fields
    ///   - transaction: Transaction to use
    /// - Returns: Estimated value at the percentile, or nil if no data
    public func getPercentile(
        percentile: Double,
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double? {
        let key = try buildGroupingKey(groupingValues)

        guard let data = try await transaction.getValue(for: key) else {
            return nil
        }

        guard var digest = TDigest.decode(from: Data(data)) else {
            throw PercentileIndexError.corruptedData
        }

        return digest.quantile(percentile)
    }

    /// Get multiple percentiles efficiently for a grouping
    ///
    /// - Parameters:
    ///   - percentiles: Array of percentiles to query (0.0 to 1.0)
    ///   - groupingValues: Values for grouping fields
    ///   - transaction: Transaction to use
    /// - Returns: Dictionary mapping each percentile to its estimated value
    public func getPercentiles(
        percentiles: [Double],
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> [Double: Double] {
        let key = try buildGroupingKey(groupingValues)

        guard let data = try await transaction.getValue(for: key) else {
            return [:]
        }

        guard var digest = TDigest.decode(from: Data(data)) else {
            throw PercentileIndexError.corruptedData
        }

        return digest.quantiles(percentiles)
    }

    /// Get the estimated CDF (cumulative distribution function) value for a given value
    ///
    /// - Parameters:
    ///   - value: The value to find the percentile of
    ///   - groupingValues: Values for grouping fields
    ///   - transaction: Transaction to use
    /// - Returns: Estimated percentile (0.0 to 1.0) for the given value
    public func getCDF(
        value: Double,
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Double? {
        let key = try buildGroupingKey(groupingValues)

        guard let data = try await transaction.getValue(for: key) else {
            return nil
        }

        guard var digest = TDigest.decode(from: Data(data)) else {
            throw PercentileIndexError.corruptedData
        }

        return digest.cdf(value)
    }

    /// Get statistics for a grouping
    ///
    /// - Parameters:
    ///   - groupingValues: Values for grouping fields
    ///   - transaction: Transaction to use
    /// - Returns: Tuple of (count, min, max, median) or nil if no data
    public func getStatistics(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> (count: Int64, min: Double, max: Double, median: Double)? {
        let key = try buildGroupingKey(groupingValues)

        guard let data = try await transaction.getValue(for: key) else {
            return nil
        }

        guard var digest = TDigest.decode(from: Data(data)) else {
            throw PercentileIndexError.corruptedData
        }

        return (
            count: digest.count,
            min: digest.min,
            max: digest.max,
            median: digest.quantile(0.5)
        )
    }

    /// Get all percentile data in this index
    ///
    /// - Parameters:
    ///   - percentiles: Array of percentiles to compute for each group
    ///   - transaction: Transaction to use
    /// - Returns: Array of (grouping values, percentile values)
    public func getAllPercentiles(
        percentiles: [Double],
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], values: [Double: Double])] {
        let range = subspace.range()

        var results: [(grouping: [any TupleElement], values: [Double: Double])] = []

        for try await (key, value) in transaction.getRange(from: range.begin, to: range.end) {
            // Extract grouping values from key by unpacking the subspace
            let keyTuple = try subspace.unpack(key)
            var groupingValues: [any TupleElement] = []
            for i in 0..<keyTuple.count {
                if let element = keyTuple[i] {
                    groupingValues.append(element)
                }
            }

            guard var digest = TDigest.decode(from: Data(value)) else {
                // Log warning and skip corrupted entry (consistent with batch operation behavior)
                // Note: For single queries, we throw corruptedData error.
                // For batch operations, we skip to allow partial results.
                continue
            }

            let percentileValues = digest.quantiles(percentiles)
            results.append((grouping: groupingValues, values: percentileValues))
        }

        return results
    }

    // MARK: - Private Methods

    /// Add a value to the t-digest for the item's group
    private func addValueToTDigest(item: Item, transaction: any TransactionProtocol) async throws {
        // Sparse index: if any field value is nil, skip indexing
        let allValues: [any TupleElement]
        do {
            allValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return
        }

        guard let valueElement = allValues.last else { return }

        // Split: [grouping..., value]
        let groupingValues = allValues.count > 1 ? Array(allValues.dropLast()) : []

        // Extract numeric value
        guard let numericValue = try? TypeConversion.double(from: valueElement) else {
            return  // Skip if value is not numeric
        }

        let key = try buildGroupingKey(groupingValues)

        // Read existing t-digest or create new one
        var digest: TDigest
        if let existingData = try await transaction.getValue(for: key) {
            if let decoded = TDigest.decode(from: Data(existingData)) {
                digest = decoded
            } else {
                digest = TDigest(compression: compression)
            }
        } else {
            digest = TDigest(compression: compression)
        }

        // Add value to t-digest
        digest.add(numericValue)

        // Write updated t-digest back
        let encodedData = digest.encode()
        transaction.setValue([UInt8](encodedData), for: key)
    }

}

// MARK: - PercentileIndexError

/// Errors specific to PERCENTILE index operations
public enum PercentileIndexError: Error, CustomStringConvertible {
    /// Invalid value for grouping field
    case invalidGroupingValue(fieldName: String)

    /// Invalid numeric value for percentile field
    case invalidNumericValue(fieldName: String)

    /// Corrupted t-digest data
    case corruptedData

    public var description: String {
        switch self {
        case .invalidGroupingValue(let fieldName):
            return "Invalid grouping value for field: \(fieldName)"
        case .invalidNumericValue(let fieldName):
            return "Invalid numeric value for field: \(fieldName)"
        case .corruptedData:
            return "Corrupted t-digest data in index"
        }
    }
}
