// SumIndexMaintainer.swift
// AggregationIndex - Index maintainer for SUM aggregation
//
// Maintains sums and membership counts transactionally.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for SUM aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves numeric type at compile time
/// - Signed integer sums are stored as Int64.
/// - Unsigned integer sums are stored as UInt64.
/// - Floating-point sums use a persistent two-component Neumaier accumulator.
///
/// **Functionality**:
/// - Maintain sums of numeric values grouped by field values
/// - Checked add/remove/replace operations in one storage transaction
/// - Efficient GROUP BY SUM queries
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1][groupValue2]...["sum"]
/// Value: scalar storage selected by the declared value type
///
/// Key: [indexSubspace][groupValue1][groupValue2]...["count"]
/// Value: positive Int64 membership count
/// ```
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., sum_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to sum
public struct SumIndexMaintainer<Item: Persistable, Value: IndexNumericValue>: NumericAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var numericStorageKind: AggregationNumericStorageKind {
        get throws {
            try NumericValueExtractor.storageKind(Value.self)
        }
    }

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
        let oldData = try extractAggregationData(from: oldItem)
        let newData = try extractAggregationData(from: newItem)

        try await applyDelta(oldData: oldData, newData: newData, transaction: transaction)
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        guard let data = try extractAggregationData(from: item) else {
            return
        }

        try await mutateNumericAggregate(
            sumKey: data.sumKey,
            countKey: data.countKey,
            removing: nil,
            adding: data.numericValue,
            transaction: transaction
        )
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// A null aggregate value returns an empty array. Nullable grouping values
    /// remain canonical group-key components.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        guard let data = try extractAggregationData(from: item) else {
            return []
        }
        return [data.sumKey, data.countKey]
    }

    // MARK: - Query Methods

    /// Get the canonical typed sum for a specific grouping.
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The exact sum, or `nil` when the group has no indexed values.
    public func getSum(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> FieldValue? {
        let storedGrouping = try FieldValue.toTupleElements(groupingValues)
        let sumKey = try buildSumKey(storedGroupingElements: storedGrouping)
        let countKey = try buildCountKey(storedGroupingElements: storedGrouping)
        let sumBytes = try await transaction.getValue(
            for: sumKey,
            snapshot: true
        )
        let countBytes = try await transaction.getValue(
            for: countKey,
            snapshot: true
        )
        guard sumBytes != nil || countBytes != nil else {
            return nil
        }
        guard let sumBytes, let countBytes else {
            throw AggregationIndexError.invalidStructure(
                "Sum index requires both sum and count entries"
            )
        }
        let count = try readInt64Value(countBytes)
        guard count > 0 else {
            throw AggregationIndexError.invalidStructure("Sum index count must be positive")
        }
        return try readStoredNumericValue(sumBytes).fieldValue
    }

    /// Get a lossless Double view of the sum for a specific grouping.
    public func getSumAsDouble(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> Double? {
        guard let sum = try await getSum(
            groupingValues: groupingValues,
            transaction: transaction
        ) else {
            return nil
        }
        return try exactDouble(from: sum)
    }

    /// Maximum number of keys to scan for safety (prevents DoS on large indexes)
    private var maxScanGroups: Int { 100_000 }

    /// Get all sums in this index
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of (groupingValues, sum) tuples
    public func getAllSumsAsDouble(
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], sum: Double)] {
        let exactResults = try await getAllSums(transaction: transaction)
        var results: [(grouping: [FieldValue], sum: Double)] = []
        results.reserveCapacity(exactResults.count)
        for result in exactResults {
            results.append((
                grouping: result.grouping,
                sum: try exactDouble(from: result.sum)
            ))
        }
        return results
    }

    public func getAllSums(
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], sum: FieldValue)] {
        var groupingByIdentity: [Bytes: [any TupleElement]] = [:]
        var sums: [Bytes: FieldValue] = [:]
        var counts: [Bytes: Int64] = [:]
        let range = subspace.range()
        var scannedEntries = 0
        var scannedBytes = 0

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: (maxScanGroups * 2) + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedEntries += 1
            guard scannedEntries <= maxScanGroups * 2 else {
                throw AggregationStorageError.scanLimitExceeded(
                    maxScanGroups
                )
            }
            scannedBytes = try checkedAggregationScannedBytes(
                scannedBytes,
                adding: key.count + value.count
            )

            let decodedKey = try decodeAggregationStorageKey(key, in: subspace)
            let identity = decodedKey.groupingIdentity
            guard decodedKey.groupingElements.count
                    == index.rootExpression.columnCount - 1 else {
                throw AggregationIndexError.invalidStructure(
                    "Sum index key has an invalid grouping field count"
                )
            }
            if groupingByIdentity[identity] == nil {
                guard groupingByIdentity.count < maxScanGroups else {
                    throw AggregationStorageError.scanLimitExceeded(
                        maxScanGroups
                    )
                }
                groupingByIdentity[identity] = decodedKey.groupingElements
            }
            switch decodedKey.marker {
            case "sum":
                sums[identity] = try readStoredNumericValue(value).fieldValue
            case "count":
                let count = try readInt64Value(value)
                guard count > 0 else {
                    throw AggregationIndexError.invalidStructure("Sum index count must be positive")
                }
                counts[identity] = count
            default:
                throw AggregationIndexError.invalidStructure(
                    "Sum index key has an unknown value marker: \(decodedKey.marker)"
                )
            }
        }

        var results: [(grouping: [FieldValue], sum: FieldValue)] = []
        results.reserveCapacity(sums.count)
        for (identity, sum) in sums {
            guard let storedGrouping = groupingByIdentity[identity], counts[identity] != nil else {
                throw AggregationIndexError.invalidStructure("Sum index value is missing its count")
            }
            results.append((
                grouping: try AggregationGroupingValueDecoder.decode(
                    storedGrouping
                ),
                sum: sum
            ))
        }
        for identity in counts.keys where sums[identity] == nil {
            throw AggregationIndexError.invalidStructure("Sum index count is missing its value")
        }
        return results
    }

    // MARK: - Private Helpers

    private struct AggregationData {
        let sumKey: Bytes
        let countKey: Bytes
        let numericValue: AggregationNumericValue
    }

    /// Extract aggregation data from an item
    ///
    /// A null aggregate value contributes nothing. Null grouping fields remain
    /// canonical group values and are never treated as sparse-index exclusions.
    private func extractAggregationData(from item: Item?) throws -> AggregationData? {
        guard let item = item else { return nil }
        guard let fields = try AggregationFieldExtractor.contribution(
            from: item,
            index: index
        ) else {
            return nil
        }
        let numericValue = try NumericValueExtractor.extractNumeric(
            from: fields.value,
            as: Value.self
        )

        return AggregationData(
            sumKey: try buildSumKey(storedGroupingElements: fields.grouping),
            countKey: try buildCountKey(storedGroupingElements: fields.grouping),
            numericValue: numericValue
        )
    }

    private func applyDelta(
        oldData: AggregationData?,
        newData: AggregationData?,
        transaction: any TransactionAccess
    ) async throws {
        switch (oldData, newData) {
        case let (.some(old), .some(new))
            where old.sumKey == new.sumKey
                && old.numericValue == new.numericValue:
            break

        case let (.some(old), .some(new))
            where old.sumKey == new.sumKey:
            try await mutateNumericAggregate(
                sumKey: new.sumKey,
                countKey: new.countKey,
                removing: old.numericValue,
                adding: new.numericValue,
                transaction: transaction
            )

        case let (.some(old), .some(new)):
            try await remove(old, transaction: transaction)
            try await insert(new, transaction: transaction)

        case let (nil, .some(new)):
            try await insert(new, transaction: transaction)

        case let (.some(old), nil):
            try await remove(old, transaction: transaction)

        case (nil, nil):
            break
        }
    }

    private func insert(
        _ data: AggregationData,
        transaction: any TransactionAccess
    ) async throws {
        try await mutateNumericAggregate(
            sumKey: data.sumKey,
            countKey: data.countKey,
            removing: nil,
            adding: data.numericValue,
            transaction: transaction
        )
    }

    private func remove(
        _ data: AggregationData,
        transaction: any TransactionAccess
    ) async throws {
        try await mutateNumericAggregate(
            sumKey: data.sumKey,
            countKey: data.countKey,
            removing: data.numericValue,
            adding: nil,
            transaction: transaction
        )
    }

    private func buildSumKey<Elements: Collection>(
        storedGroupingElements: Elements
    ) throws -> Bytes where Elements.Element == any TupleElement {
        try validateGroupingCount(storedGroupingElements.count)
        let key = subspace.pack(
            elements: storedGroupingElements,
            appending: "sum"
        )
        try validateKeySize(key)
        return key
    }

    private func buildCountKey<Elements: Collection>(
        storedGroupingElements: Elements
    ) throws -> Bytes where Elements.Element == any TupleElement {
        try validateGroupingCount(storedGroupingElements.count)
        let key = subspace.pack(
            elements: storedGroupingElements,
            appending: "count"
        )
        try validateKeySize(key)
        return key
    }

    private func validateGroupingCount(_ count: Int) throws {
        guard index.rootExpression.columnCount >= 1,
              count == index.rootExpression.columnCount - 1 else {
            throw AggregationIndexError.invalidArgument(
                "Grouping value count does not match sum index '\(index.name)'"
            )
        }
    }
}
