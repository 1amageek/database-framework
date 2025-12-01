// MinMaxIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for MIN/MAX aggregation
//
// Maintains min/max values using tuple ordering for efficient range scans.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for MIN aggregation indexes
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
///
/// **Expression Structure**:
/// The index expression must produce: [grouping_fields..., min_field]
/// - All fields except the last are grouping keys
/// - The last field is the value to minimize
///
/// **Examples**:
/// ```swift
/// // Minimum age by city
/// Key: [I]/User_city_age_min/["Tokyo"]/[18]/[123] = ''
/// Key: [I]/User_city_age_min/["Tokyo"]/[25]/[456] = ''
/// // Query: Scan from ["Tokyo"] → first key = minimum
///
/// // Minimum price by (category, brand)
/// Key: [I]/Product_category_brand_price_min/["electronics"]/["Apple"]/[99900]/[789] = ''
/// ```
public struct MinIndexMaintainer<Item: Persistable>: IndexMaintainer {
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

    /// Compute expected index keys for an item (for scrubber verification)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return [try buildIndexKey(for: item, id: id)]
    }

    // MARK: - Query Methods

    /// Get the minimum value for a specific grouping
    ///
    /// **Process**:
    /// 1. Create subspace with grouping prefix
    /// 2. Scan first key in range (tuple ordering ensures it's minimum)
    /// 3. Extract value from key
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The minimum value
    /// - Throws: IndexError if no values found
    public func getMin(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
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

        return try extractNumericValue(dataElements[0])
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        var allValues: [any TupleElement] = indexedValues

        // Append primary key elements
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allValues.append(element)
            }
        }

        let tuple = Tuple(allValues)
        return subspace.pack(tuple)
    }

    private func extractNumericValue(_ element: any TupleElement) throws -> Int64 {
        if let int64 = element as? Int64 {
            return int64
        } else if let int = element as? Int {
            return Int64(int)
        } else if let int32 = element as? Int32 {
            return Int64(int32)
        } else if let double = element as? Double {
            return Int64(double)
        } else if let float = element as? Float {
            return Int64(float)
        } else {
            throw IndexError.invalidConfiguration(
                "Aggregate value must be numeric, got: \(type(of: element))"
            )
        }
    }
}

/// Maintainer for MAX aggregation indexes
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
///
/// **Examples**:
/// ```swift
/// // Maximum age by city
/// Key: [I]/User_city_age_max/["Tokyo"]/[65]/[789] = ''
/// Key: [I]/User_city_age_max/["Tokyo"]/[42]/[456] = ''
/// // Query: Scan from end of ["Tokyo"] range → last key = maximum
/// ```
public struct MaxIndexMaintainer<Item: Persistable>: IndexMaintainer {
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

    /// Compute expected index keys for an item (for scrubber verification)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return [try buildIndexKey(for: item, id: id)]
    }

    // MARK: - Query Methods

    /// Get the maximum value for a specific grouping
    ///
    /// **Process**:
    /// 1. Create subspace with grouping prefix
    /// 2. Scan last key in range (tuple ordering ensures it's maximum)
    /// 3. Extract value from key
    ///
    /// - Parameters:
    ///   - groupingValues: The grouping key values
    ///   - transaction: The transaction to use
    /// - Returns: The maximum value
    /// - Throws: IndexError if no values found
    public func getMax(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
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

        return try extractNumericValue(dataElements[0])
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        var allValues: [any TupleElement] = indexedValues

        // Append primary key elements
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allValues.append(element)
            }
        }

        let tuple = Tuple(allValues)
        return subspace.pack(tuple)
    }

    private func extractNumericValue(_ element: any TupleElement) throws -> Int64 {
        if let int64 = element as? Int64 {
            return int64
        } else if let int = element as? Int {
            return Int64(int)
        } else if let int32 = element as? Int32 {
            return Int64(int32)
        } else if let double = element as? Double {
            return Int64(double)
        } else if let float = element as? Float {
            return Int64(float)
        } else {
            throw IndexError.invalidConfiguration(
                "Aggregate value must be numeric, got: \(type(of: element))"
            )
        }
    }
}
