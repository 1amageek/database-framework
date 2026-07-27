// AverageIndexMaintainer.swift
// AggregationIndex - Index maintainer for AVERAGE aggregation
//
// Maintains averages by storing an exact typed sum and count separately.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintainer for AVERAGE aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves numeric type at compile time
/// - Signed integer sums are stored as Int128.
/// - Unsigned integer sums are stored as UInt128.
/// - Floating-point sums use a persistent two-component Neumaier accumulator.
///
/// **Functionality**:
/// - Maintain average values grouped by field values
/// - Store sum and count separately for exact average calculation
/// - Checked sum and count mutations in one storage transaction
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1]...["sum"]
/// Value: scalar storage selected by the declared value type
///
/// Key: [indexSubspace][groupValue1]...["count"]
/// Value: Int64 (8 bytes little-endian)
/// ```
public struct AverageIndexMaintainer<Item: Persistable, Value: IndexNumericValue>: NumericAggregationMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    public var numericStorageKind: AggregationNumericStorageKind {
        get throws {
            try NumericValueExtractor.storageKind(Value.self)
        }
    }

    public var storesWideIntegerAccumulator: Bool { true }

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

    /// Get the canonical typed average for a specific grouping.
    public func getAverage(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> (count: Int64, average: FieldValue) {
        let storedGrouping = try FieldValue.toTupleElements(groupingValues)
        let sumKey = try buildSumKey(storedGroupingElements: storedGrouping)
        let countKey = try buildCountKey(storedGroupingElements: storedGrouping)

        let sumBytes = try await transaction.getValue(for: sumKey)
        let countBytes = try await transaction.getValue(for: countKey)
        guard sumBytes != nil || countBytes != nil else {
            throw AggregationIndexError.noData("No values found for AVERAGE aggregate")
        }
        guard let sumBytes, let countBytes else {
            throw AggregationIndexError.invalidStructure(
                "Average index requires both sum and count entries"
            )
        }
        let sum = try readStoredNumericAccumulator(sumBytes)
        let count = try readInt64Value(countBytes)
        guard count > 0 else {
            throw AggregationIndexError.invalidStructure("Average index count must be positive")
        }
        return (
            count: count,
            average: try exactAverage(sum: sum, count: count)
        )
    }

    /// Get a lossless Double view of the average for a specific grouping.
    public func getAverageAsDouble(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> (count: Int64, average: Double) {
        let exact = try await getAverage(
            groupingValues: groupingValues,
            transaction: transaction
        )
        return (
            count: exact.count,
            average: try exactDouble(from: exact.average)
        )
    }

    /// Maximum number of aggregate groups to scan for safety.
    private var maxScanGroups: Int { 100_000 }

    /// Get all averages in this index
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    public func getAllAveragesAsDouble(
        transaction: any TransactionAccess
    ) async throws -> [(
        grouping: [FieldValue],
        count: Int64,
        average: Double
    )] {
        let exactResults = try await getAllAverages(transaction: transaction)
        var results: [(
            grouping: [FieldValue],
            count: Int64,
            average: Double
        )] = []
        results.reserveCapacity(exactResults.count)
        for result in exactResults {
            results.append((
                grouping: result.grouping,
                count: result.count,
                average: try exactDouble(from: result.average)
            ))
        }
        return results
    }

    public func getAllAverages(
        transaction: any TransactionAccess
    ) async throws -> [(
        grouping: [FieldValue],
        count: Int64,
        average: FieldValue
    )] {
        var sumData: [Bytes: (
            grouping: [any TupleElement],
            sum: AggregationNumericAccumulatorValue
        )] = [:]
        var countData: [Bytes: Int64] = [:]

        let range = subspace.range()
        var scannedEntries = 0
        var scannedGroups = 0
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
            let grouping = decodedKey.groupingElements
            let groupingKey = decodedKey.groupingIdentity
            guard grouping.count == index.rootExpression.columnCount - 1 else {
                throw AggregationIndexError.invalidStructure(
                    "Average index key has an invalid grouping field count"
                )
            }

            if sumData[groupingKey] == nil,
               countData[groupingKey] == nil {
                scannedGroups += 1
                guard scannedGroups <= maxScanGroups else {
                    throw AggregationStorageError.scanLimitExceeded(
                        maxScanGroups
                    )
                }
            }

            if decodedKey.marker == "sum" {
                sumData[groupingKey] = (
                    grouping: grouping,
                    sum: try readStoredNumericAccumulator(value)
                )
            } else if decodedKey.marker == "count" {
                let count = try readInt64Value(value)
                guard count > 0 else {
                    throw AggregationIndexError.invalidStructure(
                        "Average index count must be positive"
                    )
                }
                countData[groupingKey] = count
            } else {
                throw AggregationIndexError.invalidStructure(
                    "Average index key has an unknown value marker: \(decodedKey.marker)"
                )
            }
        }

        var results: [(
            grouping: [FieldValue],
            count: Int64,
            average: FieldValue
        )] = []
        results.reserveCapacity(sumData.count)

        for (groupingKey, sumInfo) in sumData {
            guard let count = countData[groupingKey] else {
                throw AggregationIndexError.invalidStructure(
                    "Average index sum is missing a positive count"
                )
            }
            results.append((
                grouping: try AggregationGroupingValueDecoder.decode(
                    sumInfo.grouping
                ),
                count: count,
                average: try exactAverage(sum: sumInfo.sum, count: count)
            ))
        }
        for groupingKey in countData.keys where sumData[groupingKey] == nil {
            throw AggregationIndexError.invalidStructure(
                "Average index count is missing its sum"
            )
        }

        return results
    }

    // MARK: - Private Helpers

    private func exactAverage(
        sum: AggregationNumericAccumulatorValue,
        count: Int64
    ) throws -> FieldValue {
        switch sum {
        case .signedInteger(let total):
            return try CanonicalAggregationReducer.average(
                signedTotal: total,
                count: count,
                field: index.name
            )
        case .unsignedInteger(let total):
            return try CanonicalAggregationReducer.average(
                unsignedTotal: total,
                count: count,
                field: index.name
            )
        case .floatingPoint(let total):
            return try CanonicalAggregationReducer.average(
                floatingPointTotal: total,
                count: count,
                field: index.name
            )
        }
    }

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
                "Grouping value count does not match average index '\(index.name)'"
            )
        }
    }
}
