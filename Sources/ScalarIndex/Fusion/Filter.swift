// Filter.swift
// ScalarIndex - Scalar filter query for Fusion
//
// This file is part of ScalarIndex module, not DatabaseEngine.
// DatabaseEngine does not know about ScalarIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - FilterError

/// Errors that can occur during filter execution
public enum FilterError: Error, Sendable {
    /// Type conversion produced no elements
    case emptyTupleConversion

    /// Value type cannot be compared in range queries
    case incomparableType(actualType: String)

    /// Numeric conversion failed during comparison
    case numericConversionFailed(from: String, to: String)
}

// MARK: - Filter

/// Scalar filter query for Fusion
///
/// Filters items based on scalar field values using index.
/// All matching items receive a score of 1.0.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Filter(\.category, equals: "electronics")
///     Search(\.description).terms(["wireless"])
/// }
/// .execute()
/// ```
public struct Filter<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var predicate: FilterPredicate

    private enum FilterPredicate: Sendable {
        case equals(any Sendable & Hashable)
        case `in`([any Sendable & Hashable])
        case range(min: (any Sendable)?, max: (any Sendable)?, minInclusive: Bool, maxInclusive: Bool)
        case custom(@Sendable (T) -> Bool)
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Filter for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Filter(\.category, equals: "electronics")
    ///     Search(\.description).terms(["wireless"])
    /// }
    /// ```
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V?>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Filter for set membership
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        in values: [V]
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .in(values)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Range)

    /// Create a Filter for range comparison
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        range: ClosedRange<V>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: range.lowerBound, max: range.upperBound, minInclusive: true, maxInclusive: true)
        self.queryContext = context
    }

    /// Create a Filter for half-open range
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        range: Range<V>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: range.lowerBound, max: range.upperBound, minInclusive: true, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for greater than
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        greaterThan value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: value, max: nil, minInclusive: false, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        greaterThanOrEqual value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: value, max: nil, minInclusive: true, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for less than
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        lessThan value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: nil, max: value, minInclusive: false, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for less than or equal
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        lessThanOrEqual value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: nil, max: value, minInclusive: false, maxInclusive: true)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Custom)

    /// Create a Filter with custom predicate
    public init(_ predicate: @escaping @Sendable (T) -> Bool) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.fieldName = ""
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Filter for equality comparison with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Filter for set membership with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .in(values)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Range)

    /// Create a Filter for range comparison with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        range: ClosedRange<V>,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: range.lowerBound, max: range.upperBound, minInclusive: true, maxInclusive: true)
        self.queryContext = context
    }

    /// Create a Filter for half-open range with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        range: Range<V>,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: range.lowerBound, max: range.upperBound, minInclusive: true, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for greater than with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        greaterThan value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: value, max: nil, minInclusive: false, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        greaterThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: value, max: nil, minInclusive: true, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for less than with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        lessThan value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: nil, max: value, minInclusive: false, maxInclusive: false)
        self.queryContext = context
    }

    /// Create a Filter for less than or equal with explicit context
    public init<V: Sendable & Comparable>(
        _ keyPath: KeyPath<T, V>,
        lessThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .range(min: nil, max: value, minInclusive: false, maxInclusive: true)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Custom)

    /// Create a Filter with custom predicate and explicit context
    public init(_ predicate: @escaping @Sendable (T) -> Bool, context: IndexQueryContext) {
        self.fieldName = ""
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Find the index descriptor that can efficiently answer this query
    ///
    /// For scalar indexes, only the leftmost field can be used for efficient
    /// equality/range queries. This follows B-tree index semantics:
    ///
    /// - Composite index `[a, b, c]` has key structure: `[a値][b値][c値][primaryKey]`
    /// - Efficient queries: `a` alone, `(a, b)`, or `(a, b, c)` (left-to-right)
    /// - Inefficient queries: `b` alone, `c` alone (requires full index scan)
    ///
    /// **Reference**: "Database System Concepts" (Silberschatz) - Chapter 14.3
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == ScalarIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName - MUST be the FIRST (leftmost) field
            guard let kind = descriptor.kind as? ScalarIndexKind<T> else {
                return false
            }
            // CRITICAL: Only match if fieldName is the FIRST field in the index
            // This ensures efficient B-tree index usage (left-prefix rule)
            return kind.fieldNames.first == fieldName
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        var results: [T]

        switch predicate {
        case .equals(let value):
            results = try await executeEqualitySearch(value: value)

        case .in(let values):
            // Union of equality searches
            var allResults: [T] = []
            var seen: Set<String> = []
            for value in values {
                let matches = try await executeEqualitySearch(value: value)
                for item in matches {
                    let id = "\(item.id)"
                    if !seen.contains(id) {
                        seen.insert(id)
                        allResults.append(item)
                    }
                }
            }
            results = allResults

        case .range(let min, let max, let minInclusive, let maxInclusive):
            results = try await executeRangeSearch(
                min: min,
                max: max,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive
            )

        case .custom(let predicate):
            // For custom predicates, we need candidates or fetch all
            if let candidateIds = candidates {
                let items = try await queryContext.fetchItemsByStringIds(type: T.self, ids: Array(candidateIds))
                results = items.filter(predicate)
            } else {
                // This is expensive - should be avoided in practice
                results = try await queryContext.fetchAllItems(type: T.self).filter(predicate)
            }
        }

        // Filter to candidates if provided
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.id)") }
        }

        // All matching items get score 1.0 (pass/fail filter)
        return results.map { ScoredResult(item: $0, score: 1.0) }
    }

    // MARK: - Scalar Index Reading

    /// Index structure:
    /// - Key: `[indexSubspace][fieldValue][primaryKey]`
    /// - Value: empty

    /// Execute equality search using scalar index
    private func executeEqualitySearch(value: any Sendable & Hashable) async throws -> [T] {
        guard let descriptor = findIndexDescriptor() else {
            // Fallback to full scan with filter
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            let targetFieldValue = TypeConversion.toFieldValue(value)
            return allItems.filter { item in
                guard let fieldValue = item[dynamicMember: fieldName] else { return false }
                let itemFieldValue = TypeConversion.toFieldValue(fieldValue)
                return itemFieldValue == targetFieldValue
            }
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            try await self.searchScalarEquals(
                value: value,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
    }

    /// Execute range search using scalar index
    private func executeRangeSearch(
        min: (any Sendable)?,
        max: (any Sendable)?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) async throws -> [T] {
        guard let descriptor = findIndexDescriptor() else {
            // Fallback to full scan with filter
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            return allItems.filter { item in
                guard let fieldValue = item[dynamicMember: fieldName] else { return false }
                return matchesRange(fieldValue, min: min, max: max, minInclusive: minInclusive, maxInclusive: maxInclusive)
            }
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            try await self.searchScalarRange(
                min: min,
                max: max,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
    }

    /// Search scalar index for equality
    private func searchScalarEquals(
        value: any Sendable & Hashable,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        let tupleValue = try TupleEncoder.encode(value)
        let valueSubspace = indexSubspace.subspace(tupleValue)
        let (begin, end) = valueSubspace.range()

        var results: [Tuple] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard valueSubspace.contains(key) else { break }

            guard let keyTuple = try? valueSubspace.unpack(key) else {
                continue
            }
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }
            results.append(Tuple(elements))
        }

        return results
    }

    /// Search scalar index for range
    private func searchScalarRange(
        min: (any Sendable)?,
        max: (any Sendable)?,
        minInclusive: Bool,
        maxInclusive: Bool,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        // Build range selectors
        let beginKey: [UInt8]
        let endKey: [UInt8]

        if let minValue = min {
            let minTuple = try TupleEncoder.encode(minValue)
            let packed = indexSubspace.pack(Tuple(minTuple))
            if minInclusive {
                beginKey = packed
            } else {
                beginKey = incrementKey(packed)
            }
        } else {
            beginKey = indexSubspace.prefix
        }

        if let maxValue = max {
            let maxTuple = try TupleEncoder.encode(maxValue)
            let packed = indexSubspace.pack(Tuple(maxTuple))
            if maxInclusive {
                endKey = incrementKey(packed)
            } else {
                endKey = packed
            }
        } else {
            endKey = incrementKey(indexSubspace.prefix)
        }

        var results: [Tuple] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard indexSubspace.contains(key) else { break }

            guard let keyTuple = try? indexSubspace.unpack(key) else {
                continue
            }

            // Key structure: [fieldValue][primaryKey]
            // We need to extract the primary key (last element(s))
            guard keyTuple.count >= 2 else { continue }

            // Assume single primary key element for now
            var pkElements: [any TupleElement] = []
            for i in 1..<keyTuple.count {
                if let elem = keyTuple[i] {
                    pkElements.append(elem)
                }
            }

            results.append(Tuple(pkElements))
        }

        return results
    }

    // MARK: - Helpers

    /// Check if a value matches the range predicate using type-aware comparison
    ///
    /// Compares values using their natural ordering, not string representation.
    /// This fixes the issue where string comparison produces incorrect ordering
    /// for numeric values (e.g., "9" > "10" in string order, but 9 < 10 numerically).
    ///
    /// **Comparison Rules**:
    /// - Numeric types (Int, Int64, Double): Mathematical comparison
    ///   - Mixed Int/Double comparisons use Double for precision
    /// - String: Lexicographic comparison
    /// - UUID: String representation comparison (valid because UUID format is fixed-length)
    /// - Date: Converted to Double for comparison
    /// - Unsupported types: Returns false (cannot compare)
    ///
    /// Uses `TypeConversion` for unified type conversion.
    private func matchesRange(
        _ value: Any,
        min: (any Sendable)?,
        max: (any Sendable)?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) -> Bool {
        // Strategy: Use Double comparison for mixed numeric types
        // This handles Int vs Double comparisons correctly

        // 1. Try Double comparison first (handles all numeric types including mixed Int/Double)
        if let doubleValue = TypeConversion.asDouble(value) {
            let minDouble = min.flatMap { TypeConversion.asDouble($0) }
            let maxDouble = max.flatMap { TypeConversion.asDouble($0) }

            // Verify bounds can be converted (if provided)
            let minOk = min == nil || minDouble != nil
            let maxOk = max == nil || maxDouble != nil

            if minOk && maxOk {
                return compareNumericRange(
                    doubleValue,
                    min: minDouble,
                    max: maxDouble,
                    minInclusive: minInclusive,
                    maxInclusive: maxInclusive
                )
            }
        }

        // 2. Try pure Int64 comparison (for non-numeric bounds like String)
        if let int64Value = TypeConversion.asInt64(value) {
            let minInt = min.flatMap { TypeConversion.asInt64($0) }
            let maxInt = max.flatMap { TypeConversion.asInt64($0) }

            let minOk = min == nil || minInt != nil
            let maxOk = max == nil || maxInt != nil

            if minOk && maxOk {
                return compareNumericRange(
                    int64Value,
                    min: minInt,
                    max: maxInt,
                    minInclusive: minInclusive,
                    maxInclusive: maxInclusive
                )
            }
        }

        // 3. Try String comparison
        if let stringValue = TypeConversion.asString(value) {
            let minStr = min.flatMap { TypeConversion.asString($0) }
            let maxStr = max.flatMap { TypeConversion.asString($0) }

            let minOk = min == nil || minStr != nil
            let maxOk = max == nil || maxStr != nil

            if minOk && maxOk {
                return compareStringRange(
                    stringValue,
                    min: minStr,
                    max: maxStr,
                    minInclusive: minInclusive,
                    maxInclusive: maxInclusive
                )
            }
        }

        // Unsupported type or incompatible bounds - cannot compare
        return false
    }

    /// Compare numeric values in a range
    private func compareNumericRange<Value: Comparable>(
        _ value: Value,
        min: Value?,
        max: Value?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) -> Bool {
        if let minVal = min {
            if minInclusive {
                if value < minVal { return false }
            } else {
                if value <= minVal { return false }
            }
        }

        if let maxVal = max {
            if maxInclusive {
                if value > maxVal { return false }
            } else {
                if value >= maxVal { return false }
            }
        }

        return true
    }

    /// Compare string values in a range
    private func compareStringRange(
        _ value: String,
        min: String?,
        max: String?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) -> Bool {
        if let minVal = min {
            if minInclusive {
                if value < minVal { return false }
            } else {
                if value <= minVal { return false }
            }
        }

        if let maxVal = max {
            if maxInclusive {
                if value > maxVal { return false }
            } else {
                if value >= maxVal { return false }
            }
        }

        return true
    }

    /// Increment the last byte of a key (for range end)
    private func incrementKey(_ key: [UInt8]) -> [UInt8] {
        var result = key
        if result.isEmpty {
            result.append(0x00)
        } else {
            var i = result.count - 1
            while i >= 0 {
                if result[i] < 0xFF {
                    result[i] += 1
                    return result
                }
                i -= 1
            }
            result.append(0x00)
        }
        return result
    }
}
