// MinMaxIndexMaintainer.swift
// AggregationIndex - Index maintainer for MIN/MAX aggregation
//
// 2-layer architecture for efficient batch queries while maintaining deletion accuracy.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Subspace Layers

/// Subspace layers for MIN/MAX indexes
///
/// **Layer 1 (Individual)**: Stores all individual values for accurate recomputation
/// **Layer 2 (Aggregated)**: Caches aggregated min/max values for efficient batch queries
private struct MinMaxSubspaces: Sendable {
    let individual: Subspace
    let aggregated: Subspace

    init(base: Subspace) {
        self.individual = base.subspace(Int64(0))
        self.aggregated = base.subspace(Int64(1))
    }
}

// MARK: - MIN Index Maintainer

/// Maintainer for MIN aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves the value type at compile time
/// - Result type is `Value` (not forced to Int64)
///
/// **2-Layer Architecture**:
/// - Layer 1 (Individual): `[indexSubspace][0]/[groupValue1]...[minValue][primaryKey]`
///   - Stores all individual values (for accurate recomputation on deletion)
///   - Uses FDB tuple ordering for automatic sorting
/// - Layer 2 (Aggregated): `[indexSubspace][1]/[groupValue1]...` → `Tuple(minValue, primaryKey)`
///   - Caches aggregated MIN value (for O(1) single-group queries and O(G) batch queries)
///   - Updated automatically when items are inserted/deleted
///
/// **Functionality**:
/// - Maintain minimum values grouped by field values
/// - Efficient O(1) min queries (Layer 2 direct read)
/// - Efficient O(G) batch queries (Layer 2 scan), where G = number of groups
/// - Accurate recomputation on deletion (Layer 1 provides all values)
///
/// **Performance**:
/// - `getMin(groupingValues:)`: O(1) - Layer 2 direct read
/// - `getAllMins()`: O(G) - Layer 2 range scan
/// - Insert/Update: O(log N) + O(log M) - Layer 1 write + Layer 2 update
/// - Delete: O(log N) + O(log M) - Layer 1 clear + Layer 2 recomputation
public struct MinIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let layers: MinMaxSubspaces

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.layers = MinMaxSubspaces(base: subspace)
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // 1. Layer 1: Update individual values
        if let oldItem = oldItem {
            do {
                let oldKey = try buildIndividualKey(for: oldItem)
                transaction.clear(key: oldKey)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil value was not indexed
            }
        }

        if let newItem = newItem {
            do {
                let newKey = try buildIndividualKey(for: newItem)
                let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
                transaction.setValue(value, for: newKey)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil value is not indexed
            }
        }

        // 2. Layer 2: Update aggregates for affected groups
        var affectedGroups: [[any TupleElement]] = []
        if let oldGrouping = try? extractGrouping(from: oldItem) {
            affectedGroups.append(oldGrouping)
        }
        if let newGrouping = try? extractGrouping(from: newItem) {
            // Only add if different from old grouping
            if affectedGroups.isEmpty || !areGroupingsEqual(affectedGroups[0], newGrouping) {
                affectedGroups.append(newGrouping)
            }
        }

        for groupingValues in affectedGroups {
            try await updateAggregateForGroup(
                groupingValues: groupingValues,
                transaction: transaction
            )
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Sparse index: if value field is nil, skip indexing
        do {
            // Layer 1: Store individual value
            let indexKey = try buildIndividualKey(for: item, id: id)
            let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
            transaction.setValue(value, for: indexKey)

            // Layer 2: Update aggregate for this group
            if let groupingValues = try? extractGrouping(from: item) {
                try await updateAggregateForGroup(
                    groupingValues: groupingValues,
                    transaction: transaction
                )
            }
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil value is not indexed
        }
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// If the value field is nil, returns an empty array.
    ///
    /// **Note**: Returns only Layer 1 keys (Layer 2 is internal cache)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        do {
            return [try buildIndividualKey(for: item, id: id)]
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }
    }

    // MARK: - Query Methods

    /// Get the minimum value for a specific grouping
    ///
    /// **Performance**: O(1) - Direct read from Layer 2
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

        // Layer 2: Direct read (O(1))
        let aggregateKey = layers.aggregated.pack(Tuple(groupingValues))
        guard let valueData = try await transaction.getValue(for: aggregateKey, snapshot: true) else {
            throw IndexError.noData("No MIN value found for group")
        }

        let tuple = try Tuple.unpack(from: valueData)
        guard tuple.count >= 2 else {
            throw IndexError.invalidStructure("Invalid MIN aggregate structure")
        }

        return try TupleDecoder.decode(tuple[0], as: Value.self)
    }

    /// Get all minimum values across all groups
    ///
    /// **Performance**: O(G) where G = number of groups
    ///
    /// **Returns**: Array of tuples containing:
    /// - `grouping`: Grouping field values
    /// - `min`: Minimum value for the group
    /// - `itemId`: Primary key of the item with minimum value
    public func getAllMins(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], min: Value, itemId: Tuple)] {
        var results: [(grouping: [any TupleElement], min: Value, itemId: Tuple)] = []

        // Layer 2: Scan only aggregated values (O(G))
        let range = layers.aggregated.range()
        let kvs = transaction.getRange(
            begin: range.begin,
            end: range.end,
            snapshot: true
        )

        for try await (key, value) in kvs {
            // Extract grouping values from key
            let groupingTuple = try layers.aggregated.unpack(key)
            let groupingElements = (0..<groupingTuple.count).compactMap { groupingTuple[$0] }

            // Extract MIN value and itemId from value
            // Value structure: [value, id_element1, id_element2, ...]
            let valueTuple = try Tuple.unpack(from: value)
            guard valueTuple.count >= 2 else { continue }

            let valueElements = (0..<valueTuple.count).compactMap { valueTuple[$0] }
            guard valueElements.count >= 2 else { continue }

            let minValue = try TupleDecoder.decode(valueElements[0], as: Value.self)
            // Primary key is all elements after the first
            let idElements = Array(valueElements.dropFirst())
            let itemId = Tuple(idElements)

            results.append((
                grouping: groupingElements,
                min: minValue,
                itemId: itemId
            ))
        }

        return results
    }

    // MARK: - Private Methods

    /// Build Layer 1 key (individual value storage)
    private func buildIndividualKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        // Use Layer 1 subspace
        return try packAndValidate(Tuple(allValues), in: layers.individual)
    }

    /// Extract grouping values from an item
    ///
    /// **Field structure**: [grouping_fields..., value_field]
    /// - All fields except the last are grouping keys
    /// - The last field is the value to aggregate
    private func extractGrouping(from item: Item?) throws -> [any TupleElement]? {
        guard let item = item else { return nil }
        let allValues = try evaluateIndexFields(from: item)
        // Last field is the value, everything before is grouping
        guard allValues.count >= 2 else { return nil }
        return Array(allValues.dropLast())
    }

    /// Update Layer 2 aggregate for a specific group
    ///
    /// **Algorithm**:
    /// 1. Scan Layer 1 to find the first key (MIN value)
    /// 2. If found, update Layer 2 with (minValue, itemId)
    /// 3. If not found (group is empty), clear Layer 2 entry
    private func updateAggregateForGroup(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws {
        let groupingTuple = Tuple(groupingValues)
        let groupingBytes = groupingTuple.pack()

        // Find MIN value from Layer 1
        let individualGroupSpace = Subspace(
            prefix: layers.individual.prefix + groupingBytes
        )
        let range = individualGroupSpace.range()
        let selector = FDB.KeySelector.firstGreaterOrEqual(range.begin)

        guard let minKey = try await transaction.getKey(selector: selector, snapshot: true),
              individualGroupSpace.contains(minKey) else {
            // Group is empty → Clear Layer 2
            let aggregateKey = layers.aggregated.pack(groupingTuple)
            transaction.clear(key: aggregateKey)
            return
        }

        // Extract MIN value and itemId from Layer 1 key
        // Key structure: [value][id_element1][id_element2]...
        let dataTuple = try individualGroupSpace.unpack(minKey)
        guard dataTuple.count >= 2 else {
            throw IndexError.invalidStructure("Invalid Layer 1 key structure: expected at least [value, id]")
        }

        // First element is the value
        guard let minValue = dataTuple[0] else {
            throw IndexError.invalidStructure("Missing value in Layer 1 key")
        }

        // Remaining elements are the primary key components
        let idElements = (1..<dataTuple.count).compactMap { dataTuple[$0] }
        guard !idElements.isEmpty else {
            throw IndexError.invalidStructure("Missing primary key in Layer 1 key")
        }

        // Update Layer 2
        // Store as flat tuple: [value, id_element1, id_element2, ...]
        let aggregateKey = layers.aggregated.pack(groupingTuple)
        var aggregateElements: [any TupleElement] = [minValue]
        aggregateElements.append(contentsOf: idElements)
        let aggregateValue = Tuple(aggregateElements).pack()
        transaction.setValue(aggregateValue, for: aggregateKey)
    }
}

// MARK: - MAX Index Maintainer

/// Maintainer for MAX aggregation indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Value` type parameter preserves the value type at compile time
/// - Result type is `Value` (not forced to Int64)
///
/// **2-Layer Architecture**:
/// - Layer 1 (Individual): `[indexSubspace][0]/[groupValue1]...[maxValue][primaryKey]`
///   - Stores all individual values (for accurate recomputation on deletion)
///   - Uses FDB tuple ordering for automatic sorting
/// - Layer 2 (Aggregated): `[indexSubspace][1]/[groupValue1]...` → `Tuple(maxValue, primaryKey)`
///   - Caches aggregated MAX value (for O(1) single-group queries and O(G) batch queries)
///   - Updated automatically when items are inserted/deleted
///
/// **Functionality**:
/// - Maintain maximum values grouped by field values
/// - Efficient O(1) max queries (Layer 2 direct read)
/// - Efficient O(G) batch queries (Layer 2 scan), where G = number of groups
/// - Accurate recomputation on deletion (Layer 1 provides all values)
///
/// **Performance**:
/// - `getMax(groupingValues:)`: O(1) - Layer 2 direct read
/// - `getAllMaxs()`: O(G) - Layer 2 range scan
/// - Insert/Update: O(log N) + O(log M) - Layer 1 write + Layer 2 update
/// - Delete: O(log N) + O(log M) - Layer 1 clear + Layer 2 recomputation
public struct MaxIndexMaintainer<Item: Persistable, Value: Comparable & Codable & Sendable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let layers: MinMaxSubspaces

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.layers = MinMaxSubspaces(base: subspace)
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // 1. Layer 1: Update individual values
        if let oldItem = oldItem {
            do {
                let oldKey = try buildIndividualKey(for: oldItem)
                transaction.clear(key: oldKey)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil value was not indexed
            }
        }

        if let newItem = newItem {
            do {
                let newKey = try buildIndividualKey(for: newItem)
                let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
                transaction.setValue(value, for: newKey)
            } catch DataAccessError.nilValueCannotBeIndexed {
                // Sparse index: nil value is not indexed
            }
        }

        // 2. Layer 2: Update aggregates for affected groups
        var affectedGroups: [[any TupleElement]] = []
        if let oldGrouping = try? extractGrouping(from: oldItem) {
            affectedGroups.append(oldGrouping)
        }
        if let newGrouping = try? extractGrouping(from: newItem) {
            // Only add if different from old grouping
            if affectedGroups.isEmpty || !areGroupingsEqual(affectedGroups[0], newGrouping) {
                affectedGroups.append(newGrouping)
            }
        }

        for groupingValues in affectedGroups {
            try await updateAggregateForGroup(
                groupingValues: groupingValues,
                transaction: transaction
            )
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Sparse index: if value field is nil, skip indexing
        do {
            // Layer 1: Store individual value
            let indexKey = try buildIndividualKey(for: item, id: id)
            let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
            transaction.setValue(value, for: indexKey)

            // Layer 2: Update aggregate for this group
            if let groupingValues = try? extractGrouping(from: item) {
                try await updateAggregateForGroup(
                    groupingValues: groupingValues,
                    transaction: transaction
                )
            }
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil value is not indexed
        }
    }

    /// Compute expected index keys for this item
    ///
    /// **Sparse index behavior**:
    /// If the value field is nil, returns an empty array.
    ///
    /// **Note**: Returns only Layer 1 keys (Layer 2 is internal cache)
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        do {
            return [try buildIndividualKey(for: item, id: id)]
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }
    }

    // MARK: - Query Methods

    /// Get the maximum value for a specific grouping
    ///
    /// **Performance**: O(1) - Direct read from Layer 2
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

        // Layer 2: Direct read (O(1))
        let aggregateKey = layers.aggregated.pack(Tuple(groupingValues))
        guard let valueData = try await transaction.getValue(for: aggregateKey, snapshot: true) else {
            throw IndexError.noData("No MAX value found for group")
        }

        let tuple = try Tuple.unpack(from: valueData)
        guard tuple.count >= 2 else {
            throw IndexError.invalidStructure("Invalid MAX aggregate structure")
        }

        return try TupleDecoder.decode(tuple[0], as: Value.self)
    }

    /// Get all maximum values across all groups
    ///
    /// **Performance**: O(G) where G = number of groups
    ///
    /// **Returns**: Array of tuples containing:
    /// - `grouping`: Grouping field values
    /// - `max`: Maximum value for the group
    /// - `itemId`: Primary key of the item with maximum value
    public func getAllMaxs(
        transaction: any TransactionProtocol
    ) async throws -> [(grouping: [any TupleElement], max: Value, itemId: Tuple)] {
        var results: [(grouping: [any TupleElement], max: Value, itemId: Tuple)] = []

        // Layer 2: Scan only aggregated values (O(G))
        let range = layers.aggregated.range()
        let kvs = try await transaction.getRange(
            begin: range.begin,
            end: range.end,
            snapshot: true
        )

        for try await (key, value) in kvs {
            // Extract grouping values from key
            let groupingTuple = try layers.aggregated.unpack(key)
            let groupingElements = (0..<groupingTuple.count).compactMap { groupingTuple[$0] }

            // Extract MAX value and itemId from value
            // Value structure: [value, id_element1, id_element2, ...]
            let valueTuple = try Tuple.unpack(from: value)
            guard valueTuple.count >= 2 else { continue }

            let valueElements = (0..<valueTuple.count).compactMap { valueTuple[$0] }
            guard valueElements.count >= 2 else { continue }

            let maxValue = try TupleDecoder.decode(valueElements[0], as: Value.self)
            // Primary key is all elements after the first
            let idElements = Array(valueElements.dropFirst())
            let itemId = Tuple(idElements)

            results.append((
                grouping: groupingElements,
                max: maxValue,
                itemId: itemId
            ))
        }

        return results
    }

    // MARK: - Private Methods

    /// Build Layer 1 key (individual value storage)
    private func buildIndividualKey(for item: Item, id: Tuple? = nil) throws -> FDB.Bytes {
        let indexedValues = try evaluateIndexFields(from: item)
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = indexedValues
        allValues.append(contentsOf: extractIdElements(from: primaryKeyTuple))

        // Use Layer 1 subspace
        return try packAndValidate(Tuple(allValues), in: layers.individual)
    }

    /// Extract grouping values from an item
    ///
    /// **Field structure**: [grouping_fields..., value_field]
    /// - All fields except the last are grouping keys
    /// - The last field is the value to aggregate
    private func extractGrouping(from item: Item?) throws -> [any TupleElement]? {
        guard let item = item else { return nil }
        let allValues = try evaluateIndexFields(from: item)
        // Last field is the value, everything before is grouping
        guard allValues.count >= 2 else { return nil }
        return Array(allValues.dropLast())
    }

    /// Update Layer 2 aggregate for a specific group
    ///
    /// **Algorithm**:
    /// 1. Scan Layer 1 to find the last key (MAX value)
    /// 2. If found, update Layer 2 with (maxValue, itemId)
    /// 3. If not found (group is empty), clear Layer 2 entry
    private func updateAggregateForGroup(
        groupingValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws {
        let groupingTuple = Tuple(groupingValues)
        let groupingBytes = groupingTuple.pack()

        // Find MAX value from Layer 1
        let individualGroupSpace = Subspace(
            prefix: layers.individual.prefix + groupingBytes
        )
        let range = individualGroupSpace.range()
        let selector = FDB.KeySelector.lastLessThan(range.end)

        guard let maxKey = try await transaction.getKey(selector: selector, snapshot: true),
              individualGroupSpace.contains(maxKey) else {
            // Group is empty → Clear Layer 2
            let aggregateKey = layers.aggregated.pack(groupingTuple)
            transaction.clear(key: aggregateKey)
            return
        }

        // Extract MAX value and itemId from Layer 1 key
        // Key structure: [value][id_element1][id_element2]...
        let dataTuple = try individualGroupSpace.unpack(maxKey)
        guard dataTuple.count >= 2 else {
            throw IndexError.invalidStructure("Invalid Layer 1 key structure: expected at least [value, id]")
        }

        // First element is the value
        guard let maxValue = dataTuple[0] else {
            throw IndexError.invalidStructure("Missing value in Layer 1 key")
        }

        // Remaining elements are the primary key components
        let idElements = (1..<dataTuple.count).compactMap { dataTuple[$0] }
        guard !idElements.isEmpty else {
            throw IndexError.invalidStructure("Missing primary key in Layer 1 key")
        }

        // Update Layer 2
        // Store as flat tuple: [value, id_element1, id_element2, ...]
        let aggregateKey = layers.aggregated.pack(groupingTuple)
        var aggregateElements: [any TupleElement] = [maxValue]
        aggregateElements.append(contentsOf: idElements)
        let aggregateValue = Tuple(aggregateElements).pack()
        transaction.setValue(aggregateValue, for: aggregateKey)
    }
}

// MARK: - Helper Functions

/// Compare two grouping arrays for equality
///
/// Since `[any TupleElement]` cannot conform to `Equatable`, we compare element by element using their packed Tuple representations.
private func areGroupingsEqual(_ lhs: [any TupleElement], _ rhs: [any TupleElement]) -> Bool {
    guard lhs.count == rhs.count else { return false }
    for (left, right) in zip(lhs, rhs) {
        // Use Tuple.pack() for accurate comparison
        if Tuple([left]).pack() != Tuple([right]).pack() {
            return false
        }
    }
    return true
}
