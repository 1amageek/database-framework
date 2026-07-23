// Filter.swift
// ScalarIndex - Scalar filter query for Fusion
//
// This file is part of ScalarIndex module, not DatabaseEngine.
// DatabaseEngine does not know about ScalarIndexKind.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit

// MARK: - FilterError

/// Errors that can occur during filter execution
public enum FilterError: Error, Sendable, Equatable {
    case incomparableValues(
        fieldName: String,
        valueType: String,
        boundType: String
    )
    case unorderedFloatingPoint(fieldName: String)
    case malformedIndexEntry(
        fieldName: String,
        indexedFieldCount: Int,
        elementCount: Int
    )
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
            // CRITICAL: Only match if fieldName is the FIRST field in the index
            // This ensures efficient B-tree index usage (left-prefix rule)
            return descriptor.fieldNames.first == fieldName
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        var results: [T]

        switch predicate {
        case .equals(let value):
            results = try await executeEqualitySearch(value: value)

        case .in(let values):
            // Union of equality searches
            var allResults: [T] = []
            var seen: Set<T.ID> = []
            for value in values {
                let matches = try await executeEqualitySearch(value: value)
                for item in matches {
                    let id = item.id
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
            if let candidateIDs = candidates {
                let items = try await queryContext.fetchItems(
                    identifiers: Array(candidateIDs),
                    type: T.self
                )
                results = items.filter(predicate)
            } else {
                // This is expensive - should be avoided in practice
                results = try await queryContext.fetchAllItems(type: T.self).filter(predicate)
            }
        }

        // Filter to candidates if provided
        if let candidateIDs = candidates {
            results = results.filter { candidateIDs.contains($0.id) }
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
        let targetFieldValue = try ScalarRangeValueMatcher.fieldValue(
            from: value,
            fieldName: fieldName
        )
        guard let descriptor = findIndexDescriptor() else {
            // A full scan is valid when no scalar index is configured, but value
            // conversion remains exact and can fail.
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            var matches: [T] = []
            matches.reserveCapacity(allItems.count)
            for item in allItems {
                guard let fieldValue = item[dynamicMember: fieldName] else {
                    continue
                }
                let itemFieldValue = try ScalarRangeValueMatcher.fieldValue(
                    from: fieldValue,
                    fieldName: fieldName
                )
                if itemFieldValue == targetFieldValue {
                    matches.append(item)
                }
            }
            return matches
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            try await self.searchScalarEquals(
                value: targetFieldValue,
                indexedFieldCount: descriptor.fieldNames.count,
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
        let minimum = try min.map {
            try ScalarRangeValueMatcher.fieldValue(
                from: $0,
                fieldName: fieldName
            )
        }
        let maximum = try max.map {
            try ScalarRangeValueMatcher.fieldValue(
                from: $0,
                fieldName: fieldName
            )
        }

        guard let descriptor = findIndexDescriptor() else {
            // Fallback to full scan with filter
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            var matches: [T] = []
            matches.reserveCapacity(allItems.count)
            for item in allItems {
                guard let rawValue = item[dynamicMember: fieldName] else {
                    continue
                }
                let value = try ScalarRangeValueMatcher.fieldValue(
                    from: rawValue,
                    fieldName: fieldName
                )
                if try ScalarRangeValueMatcher.matches(
                    value,
                    minimum: minimum,
                    maximum: maximum,
                    minimumInclusive: minInclusive,
                    maximumInclusive: maxInclusive,
                    fieldName: fieldName
                ) {
                    matches.append(item)
                }
            }
            return matches
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            try await self.searchScalarRange(
                min: minimum,
                max: maximum,
                minInclusive: minInclusive,
                maxInclusive: maxInclusive,
                indexedFieldCount: descriptor.fieldNames.count,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
    }

    /// Search scalar index for equality
    private func searchScalarEquals(
        value: FieldValue,
        indexedFieldCount: Int,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let tupleValue = try TupleEncoder.encode(value)
        let valueSubspace = indexSubspace.subspace(tupleValue)
        let (begin, end) = valueSubspace.range()

        var results: [Tuple] = []

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for (key, _) in sequence {
            guard valueSubspace.contains(key) else { break }

            let keyTuple = try valueSubspace.unpack(key)
            results.append(
                try ScalarFilterIndexEntryDecoder.primaryKey(
                    from: keyTuple,
                    remainingIndexedFieldCount: indexedFieldCount - 1,
                    fieldName: fieldName
                )
            )
        }

        return results
    }

    /// Search scalar index for range
    private func searchScalarRange(
        min: FieldValue?,
        max: FieldValue?,
        minInclusive: Bool,
        maxInclusive: Bool,
        indexedFieldCount: Int,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        // Build range selectors
        let beginKey: Bytes
        let endKey: Bytes

        if let min {
            let minTuple = try TupleEncoder.encode(min)
            let packed = indexSubspace.pack(Tuple(minTuple))
            if minInclusive {
                beginKey = packed
            } else {
                beginKey = incrementKey(packed)
            }
        } else {
            beginKey = indexSubspace.prefix
        }

        if let max {
            let maxTuple = try TupleEncoder.encode(max)
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

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for (key, _) in sequence {
            guard indexSubspace.contains(key) else { break }

            let keyTuple = try indexSubspace.unpack(key)

            results.append(
                try ScalarFilterIndexEntryDecoder.primaryKey(
                    from: keyTuple,
                    remainingIndexedFieldCount: indexedFieldCount,
                    fieldName: fieldName
                )
            )
        }

        return results
    }

    /// Increment the last byte of a key (for range end)
    private func incrementKey(_ key: Bytes) -> Bytes {
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
