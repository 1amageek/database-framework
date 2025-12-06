// MinMaxIndexMaintainer.swift
// AggregationIndex - Index maintainer for MIN/MAX aggregation
//
// Maintains min/max values using tuple ordering for efficient range scans.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for MIN aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves the value type at compile time
/// - Result type is `Value` (not forced to Int64)
///
/// **Functionality**:
/// - Maintain minimum values grouped by field values
/// - Uses FDB tuple ordering (values stored in keys)
/// - Efficient O(1) min queries via range scan
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1]...[minValue][primaryKey]
/// Value: '' (empty)
/// ```
public struct MinIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
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
        if let oldItem = oldItem {
            let oldKey = try buildIndexKey(for: oldItem)
            transaction.clear(key: oldKey)
        }

        if let newItem = newItem {
            let newKey = try buildIndexKey(for: newItem)
            transaction.setValue([], for: newKey)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let indexKey = try buildIndexKey(for: item, id: id)
        transaction.setValue([], for: indexKey)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        [try buildIndexKey(for: item, id: id)]
    }

    // MARK: - Query Methods

    /// Get the minimum value for a specific grouping
    public func getMin(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Value {
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedGroupingCount else {
            throw IndexError.invalidArgument(
                "Grouping values count (\(groupingValues.count)) does not match " +
                "expected count (\(expectedGroupingCount)) for index '\(index.name)'"
            )
        }

        let groupingTuple = Tuple(groupingValues)
        let groupingBytes = groupingTuple.pack()
        let groupingSubspace = Subspace(prefix: subspace.prefix + groupingBytes)
        let range = groupingSubspace.range()

        let selector = FDB.KeySelector.firstGreaterOrEqual(range.begin)
        guard let firstKey = try await transaction.getKey(selector: selector, snapshot: true) else {
            throw IndexError.noData("No values found for MIN aggregate")
        }

        guard groupingSubspace.contains(firstKey) else {
            throw IndexError.noData("No values found for MIN aggregate in range")
        }

        let dataTuple = try groupingSubspace.unpack(firstKey)
        let dataElements = try Tuple.unpack(from: dataTuple.pack())
        guard !dataElements.isEmpty else {
            throw IndexError.invalidStructure("Invalid MIN index key structure")
        }

        return try ComparableValueExtractor.extract(from: dataElements[0], as: Value.self)
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        return try packAndValidate(Tuple(allValues))
    }
}

/// Maintainer for MAX aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves the value type at compile time
/// - Result type is `Value` (not forced to Int64)
///
/// **Functionality**:
/// - Maintain maximum values grouped by field values
/// - Uses FDB tuple ordering (values stored in keys)
/// - Efficient O(1) max queries via reverse range scan
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][groupValue1]...[maxValue][primaryKey]
/// Value: '' (empty)
/// ```
public struct MaxIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
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
        if let oldItem = oldItem {
            let oldKey = try buildIndexKey(for: oldItem)
            transaction.clear(key: oldKey)
        }

        if let newItem = newItem {
            let newKey = try buildIndexKey(for: newItem)
            transaction.setValue([], for: newKey)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let indexKey = try buildIndexKey(for: item, id: id)
        transaction.setValue([], for: indexKey)
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        [try buildIndexKey(for: item, id: id)]
    }

    // MARK: - Query Methods

    /// Get the maximum value for a specific grouping
    public func getMax(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Value {
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedGroupingCount else {
            throw IndexError.invalidArgument(
                "Grouping values count (\(groupingValues.count)) does not match " +
                "expected count (\(expectedGroupingCount)) for index '\(index.name)'"
            )
        }

        let groupingTuple = Tuple(groupingValues)
        let groupingBytes = groupingTuple.pack()
        let groupingSubspace = Subspace(prefix: subspace.prefix + groupingBytes)
        let range = groupingSubspace.range()

        let selector = FDB.KeySelector.lastLessThan(range.end)
        guard let lastKey = try await transaction.getKey(selector: selector, snapshot: true) else {
            throw IndexError.noData("No values found for MAX aggregate")
        }

        guard groupingSubspace.contains(lastKey) else {
            throw IndexError.noData("No values found for MAX aggregate in range")
        }

        let dataTuple = try groupingSubspace.unpack(lastKey)
        let dataElements = try Tuple.unpack(from: dataTuple.pack())
        guard !dataElements.isEmpty else {
            throw IndexError.invalidStructure("Invalid MAX index key structure")
        }

        return try ComparableValueExtractor.extract(from: dataElements[0], as: Value.self)
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        return try packAndValidate(Tuple(allValues))
    }
}
