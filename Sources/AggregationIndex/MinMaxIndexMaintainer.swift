// MinMaxIndexMaintainer.swift
// AggregationIndexLayer - Index maintainer for MIN/MAX aggregation
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
public struct MinIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
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
    /// - Returns: The minimum value (type-safe, preserves Value type)
    /// - Throws: IndexError if no values found
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

        return try extractValue(dataElements[0])
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)

        // Extract primary key
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues

        // Append primary key elements
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        return try packAndValidate(Tuple(allValues))
    }

    /// Extract value from tuple element (type-safe)
    private func extractValue(_ element: any TupleElement) throws -> Value {
        // Try to cast directly to Value type
        // FDB stores Int as Int64, Float as Double in Tuple layer
        switch Value.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int64, got \(type(of: element))")
            }
            return value as! Value

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int (as Int64), got \(type(of: element))")
            }
            return Int(value) as! Value

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return Int32(value) as! Value

        case is Double.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Double, got \(type(of: element))")
            }
            return value as! Value

        case is Float.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Float (as Double), got \(type(of: element))")
            }
            return Float(value) as! Value

        case is String.Type:
            guard let value = element as? String else {
                throw IndexError.invalidConfiguration("Expected String, got \(type(of: element))")
            }
            return value as! Value

        default:
            // Fallback: try direct cast
            guard let value = element as? Value else {
                throw IndexError.invalidConfiguration(
                    "Cannot convert \(type(of: element)) to \(Value.self)"
                )
            }
            return value
        }
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
///
/// **Examples**:
/// ```swift
/// // Maximum age by city
/// Key: [I]/User_city_age_max/["Tokyo"]/[65]/[789] = ''
/// Key: [I]/User_city_age_max/["Tokyo"]/[42]/[456] = ''
/// // Query: Scan from end of ["Tokyo"] range → last key = maximum
/// ```
public struct MaxIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
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
    /// - Returns: The maximum value (type-safe, preserves Value type)
    /// - Throws: IndexError if no values found
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

        return try extractValue(dataElements[0])
    }

    // MARK: - Private Methods

    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)

        // Extract primary key
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues

        // Append primary key elements
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        return try packAndValidate(Tuple(allValues))
    }

    /// Extract value from tuple element (type-safe)
    private func extractValue(_ element: any TupleElement) throws -> Value {
        // Try to cast directly to Value type
        // FDB stores Int as Int64, Float as Double in Tuple layer
        switch Value.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int64, got \(type(of: element))")
            }
            return value as! Value

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int (as Int64), got \(type(of: element))")
            }
            return Int(value) as! Value

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return Int32(value) as! Value

        case is Double.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Double, got \(type(of: element))")
            }
            return value as! Value

        case is Float.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Float (as Double), got \(type(of: element))")
            }
            return Float(value) as! Value

        case is String.Type:
            guard let value = element as? String else {
                throw IndexError.invalidConfiguration("Expected String, got \(type(of: element))")
            }
            return value as! Value

        default:
            // Fallback: try direct cast
            guard let value = element as? Value else {
                throw IndexError.invalidConfiguration(
                    "Cannot convert \(type(of: element)) to \(Value.self)"
                )
            }
            return value
        }
    }
}
