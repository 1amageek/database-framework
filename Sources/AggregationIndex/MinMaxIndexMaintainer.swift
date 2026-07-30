// MinMaxIndexMaintainer.swift
// AggregationIndex - Index maintainer for MIN/MAX aggregation
//
// 2-layer architecture for efficient batch queries while maintaining deletion accuracy.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

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
public struct MinIndexMaintainer<Item: Persistable, Value: IndexComparableValue>: SubspaceIndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let layers: MinMaxSubspaces

    private var maximumScanGroups: Int { 100_000 }

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
        transaction: any TransactionAccess
    ) async throws {
        // Extract both sides before mutating Layer 1.
        let oldContribution = try oldItem.flatMap {
            try contribution(for: $0)
        }
        let newContribution = try newItem.flatMap {
            try contribution(for: $0)
        }
        let keyChanged = oldContribution?.individualKey
            != newContribution?.individualKey

        if keyChanged, let oldContribution {
            try transaction.clear(key: oldContribution.individualKey)
        }
        if let newContribution,
           keyChanged
            || oldContribution?.storedValue != newContribution.storedValue {
            try transaction.setValue(
                newContribution.storedValue,
                for: newContribution.individualKey
            )
        }

        var affectedGroups: [[any TupleElement]] = []
        if keyChanged, let oldContribution {
            affectedGroups.append(oldContribution.grouping)
        }
        if keyChanged, let newContribution {
            if affectedGroups.isEmpty
                || !areGroupingsEqual(
                    affectedGroups[0],
                    newContribution.grouping
                ) {
                affectedGroups.append(newContribution.grouping)
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
        transaction: any TransactionAccess
    ) async throws {
        guard let contribution = try contribution(for: item, id: id) else {
            return
        }
        try transaction.setValue(
            contribution.storedValue,
            for: contribution.individualKey
        )
        try await updateAggregateForGroup(
            groupingValues: contribution.grouping,
            transaction: transaction
        )
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
    ) async throws -> [ByteString] {
        guard let contribution = try contribution(for: item, id: id) else {
            return []
        }
        return [contribution.individualKey]
    }

    // MARK: - Query Methods

    /// Get the minimum value for a specific grouping
    ///
    /// **Performance**: O(1) - Direct read from Layer 2
    public func getMin(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> Value {
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedGroupingCount else {
            throw AggregationIndexError.invalidArgument(
                "Grouping values count (\(groupingValues.count)) does not match " +
                "expected count (\(expectedGroupingCount)) for index '\(index.name)'"
            )
        }

        // Layer 2: Direct read (O(1))
        let storedGrouping = try FieldValue.toTupleElements(groupingValues)
        let aggregateKey = layers.aggregated.pack(elements: storedGrouping)
        try validateKeySize(aggregateKey)
        guard let valueData = try await transaction.getValue(for: aggregateKey, snapshot: true) else {
            throw AggregationIndexError.noData("No MIN value found for group")
        }
        return try decodeAggregateValue(valueData).value
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
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], min: Value, itemId: Tuple)] {
        var results: [(grouping: [FieldValue], min: Value, itemId: Tuple)] = []
        guard index.rootExpression.columnCount >= 1 else {
            throw AggregationIndexError.invalidStructure(
                "MIN index must contain a value field"
            )
        }
        let groupingFieldCount = index.rootExpression.columnCount - 1

        // The empty grouping tuple packs to the aggregated subspace prefix
        // itself, which is intentionally outside Subspace.range(). Read the
        // single global aggregate directly.
        if groupingFieldCount == 0 {
            let aggregateKey = layers.aggregated.pack(elements: [])
            try validateKeySize(aggregateKey)
            guard let value = try await transaction.getValue(
                for: aggregateKey,
                snapshot: true
            ) else {
                return []
            }
            let aggregate = try decodeAggregateValue(value)
            return [(grouping: [], min: aggregate.value, itemId: aggregate.itemId)]
        }

        // Layer 2: Scan only aggregated values (O(G))
        let range = layers.aggregated.range()
        var scannedGroups = 0
        var scannedBytes = 0
        try await TransactionRangeIteration.forEach(
            in: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maximumScanGroups + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedGroups += 1
            guard scannedGroups <= maximumScanGroups else {
                throw AggregationStorageError.scanLimitExceeded(
                    maximumScanGroups
                )
            }
            scannedBytes = try checkedAggregationScannedBytes(
                scannedBytes,
                adding: key.count + value.count
            )

            // Extract grouping values from key
            let groupingTuple = try layers.aggregated.unpack(key)
            let groupingElements = try groupingTuple.elements()
            guard groupingElements.count == groupingFieldCount else {
                throw AggregationIndexError.invalidStructure(
                    "MIN aggregate key has an invalid grouping field count"
                )
            }

            // Extract MIN value and itemId from value
            // Value structure: [value, id_element1, id_element2, ...]
            let aggregate = try decodeAggregateValue(value)

            results.append((
                grouping: try AggregationGroupingValueDecoder.decode(
                    groupingElements
                ),
                min: aggregate.value,
                itemId: aggregate.itemId
            ))
        }

        return results
    }

    // MARK: - Private Methods

    private struct Contribution {
        let grouping: [any TupleElement]
        let individualKey: ByteString
        let storedValue: ByteString
    }

    private func decodeAggregateValue(
        _ bytes: ByteString
    ) throws -> (value: Value, itemId: Tuple) {
        var cursor = TupleCursor(bytes: bytes)
        let minimum = try cursor.requireNext()
        let idStart = cursor.consumedByteCount
        guard !cursor.isAtEnd else {
            throw AggregationIndexError.invalidStructure(
                "Invalid minimum aggregate value: expected [value, id]"
            )
        }
        return (
            value: try decodeStoredAggregationValue(
                minimum,
                as: Value.self,
                index: index
            ),
            itemId: try Tuple(packed: bytes[idStart..<bytes.count])
        )
    }

    /// Builds one sparse contribution while retaining null grouping values.
    private func contribution(
        for item: Item,
        id: Tuple? = nil
    ) throws -> Contribution? {
        guard let fields = try AggregationFieldExtractor.contribution(
            from: item,
            index: index
        ) else {
            return nil
        }
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = []
        allValues.reserveCapacity(
            fields.grouping.count + 1 + primaryKeyTuple.count
        )
        allValues.append(contentsOf: fields.grouping)
        allValues.append(fields.value)
        for index in 0..<primaryKeyTuple.count {
            allValues.append(try primaryKeyTuple.element(at: index))
        }

        let individualKey = try packAndValidate(
            elements: allValues,
            in: layers.individual
        )
        let storedValue = try CoveringValueBuilder.build(
            for: item,
            index: index
        )
        try validateValueSize(storedValue)
        return Contribution(
            grouping: fields.grouping,
            individualKey: individualKey,
            storedValue: storedValue
        )
    }

    /// Update Layer 2 aggregate for a specific group
    ///
    /// **Algorithm**:
    /// 1. Scan Layer 1 to find the first key (MIN value)
    /// 2. If found, update Layer 2 with (minValue, itemId)
    /// 3. If not found (group is empty), clear Layer 2 entry
    private func updateAggregateForGroup(
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws {
        // Find MIN value from Layer 1
        let individualGroupSpace = Subspace(
            prefix: layers.individual.pack(elements: groupingValues)
        )
        let range = individualGroupSpace.range()
        let selector = KeySelector.firstGreaterOrEqual(range.begin)

        guard let minKey = try await transaction.getKey(selector: selector, snapshot: true),
              individualGroupSpace.contains(minKey) else {
            // Group is empty → Clear Layer 2
            let aggregateKey = layers.aggregated.pack(
                elements: groupingValues
            )
            try validateKeySize(aggregateKey)
            try transaction.clear(key: aggregateKey)
            return
        }

        // Extract MIN value and itemId from Layer 1 key
        // Key structure: [value][id_element1][id_element2]...
        let aggregateValue = minKey[
            individualGroupSpace.prefix.count..<minKey.count
        ]
        var cursor = TupleCursor(bytes: aggregateValue)
        let minimum = try cursor.requireNext()
        _ = try decodeStoredAggregationValue(
            minimum,
            as: Value.self,
            index: index
        )
        guard !cursor.isAtEnd else {
            throw AggregationIndexError.invalidStructure("Invalid Layer 1 key structure: expected at least [value, id]")
        }
        while !cursor.isAtEnd {
            _ = try cursor.requireNext()
        }

        // Update Layer 2
        // Store as flat tuple: [value, id_element1, id_element2, ...]
        let aggregateKey = layers.aggregated.pack(elements: groupingValues)
        try validateKeySize(aggregateKey)
        try transaction.setValue(aggregateValue, for: aggregateKey)
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
public struct MaxIndexMaintainer<Item: Persistable, Value: IndexComparableValue>: SubspaceIndexMaintainer {
    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let layers: MinMaxSubspaces

    private var maximumScanGroups: Int { 100_000 }

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
        transaction: any TransactionAccess
    ) async throws {
        let oldContribution = try oldItem.flatMap {
            try contribution(for: $0)
        }
        let newContribution = try newItem.flatMap {
            try contribution(for: $0)
        }
        let keyChanged = oldContribution?.individualKey
            != newContribution?.individualKey

        if keyChanged, let oldContribution {
            try transaction.clear(key: oldContribution.individualKey)
        }
        if let newContribution,
           keyChanged
            || oldContribution?.storedValue != newContribution.storedValue {
            try transaction.setValue(
                newContribution.storedValue,
                for: newContribution.individualKey
            )
        }

        var affectedGroups: [[any TupleElement]] = []
        if keyChanged, let oldContribution {
            affectedGroups.append(oldContribution.grouping)
        }
        if keyChanged, let newContribution {
            if affectedGroups.isEmpty
                || !areGroupingsEqual(
                    affectedGroups[0],
                    newContribution.grouping
                ) {
                affectedGroups.append(newContribution.grouping)
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
        transaction: any TransactionAccess
    ) async throws {
        guard let contribution = try contribution(for: item, id: id) else {
            return
        }
        try transaction.setValue(
            contribution.storedValue,
            for: contribution.individualKey
        )
        try await updateAggregateForGroup(
            groupingValues: contribution.grouping,
            transaction: transaction
        )
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
    ) async throws -> [ByteString] {
        guard let contribution = try contribution(for: item, id: id) else {
            return []
        }
        return [contribution.individualKey]
    }

    // MARK: - Query Methods

    /// Get the maximum value for a specific grouping
    ///
    /// **Performance**: O(1) - Direct read from Layer 2
    public func getMax(
        groupingValues: [FieldValue],
        transaction: any TransactionAccess
    ) async throws -> Value {
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedGroupingCount else {
            throw AggregationIndexError.invalidArgument(
                "Grouping values count (\(groupingValues.count)) does not match " +
                "expected count (\(expectedGroupingCount)) for index '\(index.name)'"
            )
        }

        // Layer 2: Direct read (O(1))
        let storedGrouping = try FieldValue.toTupleElements(groupingValues)
        let aggregateKey = layers.aggregated.pack(elements: storedGrouping)
        try validateKeySize(aggregateKey)
        guard let valueData = try await transaction.getValue(for: aggregateKey, snapshot: true) else {
            throw AggregationIndexError.noData("No MAX value found for group")
        }
        return try decodeAggregateValue(valueData).value
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
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], max: Value, itemId: Tuple)] {
        var results: [(grouping: [FieldValue], max: Value, itemId: Tuple)] = []
        guard index.rootExpression.columnCount >= 1 else {
            throw AggregationIndexError.invalidStructure(
                "MAX index must contain a value field"
            )
        }
        let groupingFieldCount = index.rootExpression.columnCount - 1

        // The empty grouping tuple packs to the aggregated subspace prefix
        // itself, which is intentionally outside Subspace.range(). Read the
        // single global aggregate directly.
        if groupingFieldCount == 0 {
            let aggregateKey = layers.aggregated.pack(elements: [])
            try validateKeySize(aggregateKey)
            guard let value = try await transaction.getValue(
                for: aggregateKey,
                snapshot: true
            ) else {
                return []
            }
            let aggregate = try decodeAggregateValue(value)
            return [(grouping: [], max: aggregate.value, itemId: aggregate.itemId)]
        }

        // Layer 2: Scan only aggregated values (O(G))
        let range = layers.aggregated.range()
        var scannedGroups = 0
        var scannedBytes = 0
        try await TransactionRangeIteration.forEach(
            in: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maximumScanGroups + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedGroups += 1
            guard scannedGroups <= maximumScanGroups else {
                throw AggregationStorageError.scanLimitExceeded(
                    maximumScanGroups
                )
            }
            scannedBytes = try checkedAggregationScannedBytes(
                scannedBytes,
                adding: key.count + value.count
            )

            // Extract grouping values from key
            let groupingTuple = try layers.aggregated.unpack(key)
            let groupingElements = try groupingTuple.elements()
            guard groupingElements.count == groupingFieldCount else {
                throw AggregationIndexError.invalidStructure(
                    "MAX aggregate key has an invalid grouping field count"
                )
            }

            // Extract MAX value and itemId from value
            // Value structure: [value, id_element1, id_element2, ...]
            let aggregate = try decodeAggregateValue(value)

            results.append((
                grouping: try AggregationGroupingValueDecoder.decode(
                    groupingElements
                ),
                max: aggregate.value,
                itemId: aggregate.itemId
            ))
        }

        return results
    }

    // MARK: - Private Methods

    private struct Contribution {
        let grouping: [any TupleElement]
        let individualKey: ByteString
        let storedValue: ByteString
    }

    private func decodeAggregateValue(
        _ bytes: ByteString
    ) throws -> (value: Value, itemId: Tuple) {
        var cursor = TupleCursor(bytes: bytes)
        let maximum = try cursor.requireNext()
        let idStart = cursor.consumedByteCount
        guard !cursor.isAtEnd else {
            throw AggregationIndexError.invalidStructure(
                "Invalid maximum aggregate value: expected [value, id]"
            )
        }
        return (
            value: try decodeStoredAggregationValue(
                maximum,
                as: Value.self,
                index: index
            ),
            itemId: try Tuple(packed: bytes[idStart..<bytes.count])
        )
    }

    /// Builds one sparse contribution while retaining null grouping values.
    private func contribution(
        for item: Item,
        id: Tuple? = nil
    ) throws -> Contribution? {
        guard let fields = try AggregationFieldExtractor.contribution(
            from: item,
            index: index
        ) else {
            return nil
        }
        let primaryKeyTuple = try resolveItemId(for: item, providedId: id)

        var allValues: [any TupleElement] = []
        allValues.reserveCapacity(
            fields.grouping.count + 1 + primaryKeyTuple.count
        )
        allValues.append(contentsOf: fields.grouping)
        allValues.append(fields.value)
        for index in 0..<primaryKeyTuple.count {
            allValues.append(try primaryKeyTuple.element(at: index))
        }

        let individualKey = try packAndValidate(
            elements: allValues,
            in: layers.individual
        )
        let storedValue = try CoveringValueBuilder.build(
            for: item,
            index: index
        )
        try validateValueSize(storedValue)
        return Contribution(
            grouping: fields.grouping,
            individualKey: individualKey,
            storedValue: storedValue
        )
    }

    /// Update Layer 2 aggregate for a specific group
    ///
    /// **Algorithm**:
    /// 1. Scan Layer 1 to find the last key (MAX value)
    /// 2. If found, update Layer 2 with (maxValue, itemId)
    /// 3. If not found (group is empty), clear Layer 2 entry
    private func updateAggregateForGroup(
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws {
        // Find MAX value from Layer 1
        let individualGroupSpace = Subspace(
            prefix: layers.individual.pack(elements: groupingValues)
        )
        let range = individualGroupSpace.range()
        let selector = KeySelector.lastLessThan(range.end)

        guard let maxKey = try await transaction.getKey(selector: selector, snapshot: true),
              individualGroupSpace.contains(maxKey) else {
            // Group is empty → Clear Layer 2
            let aggregateKey = layers.aggregated.pack(
                elements: groupingValues
            )
            try validateKeySize(aggregateKey)
            try transaction.clear(key: aggregateKey)
            return
        }

        // Extract MAX value and itemId from Layer 1 key
        // Key structure: [value][id_element1][id_element2]...
        let aggregateValue = maxKey[
            individualGroupSpace.prefix.count..<maxKey.count
        ]
        var cursor = TupleCursor(bytes: aggregateValue)
        let maximum = try cursor.requireNext()
        _ = try decodeStoredAggregationValue(
            maximum,
            as: Value.self,
            index: index
        )
        guard !cursor.isAtEnd else {
            throw AggregationIndexError.invalidStructure("Invalid Layer 1 key structure: expected at least [value, id]")
        }
        while !cursor.isAtEnd {
            _ = try cursor.requireNext()
        }

        // Update Layer 2
        // Store as flat tuple: [value, id_element1, id_element2, ...]
        let aggregateKey = layers.aggregated.pack(elements: groupingValues)
        try validateKeySize(aggregateKey)
        try transaction.setValue(aggregateValue, for: aggregateKey)
    }
}

// MARK: - Helper Functions

private func decodeStoredAggregationValue<Value: IndexComparableValue>(
    _ element: any TupleElement,
    as type: Value.Type,
    index: Index
) throws -> Value {
    guard let fieldName = index.kind.fieldNames.last else {
        throw AggregationIndexError.invalidStructure(
            "Aggregation index '\(index.name)' has no value field"
        )
    }
    return try Value.decodeFieldValue(
        FieldValue(tupleElement: element),
        field: fieldName
    )
}

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
