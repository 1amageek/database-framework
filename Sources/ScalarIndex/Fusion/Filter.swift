// Filter.swift
// ScalarIndex - Scalar filter query for Fusion
//
import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

// MARK: - FilterError

/// Errors that can occur during filter execution
public enum FilterError: Error, Sendable, Equatable {
    case missingFieldSelection
    case incomparableValues(
        fieldName: String,
        valueType: String,
        boundType: String
    )
    case unorderedFloatingPoint(fieldName: String)
    case missingPersistedField(
        entity: String,
        field: FieldIdentity
    )
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
///     Filter(#field(\Product.category), equals: "electronics")
///     Search(\.description).terms(["wireless"])
/// }
/// .execute()
/// ```
public struct Filter<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let field: FieldIdentity?
    private let predicate: FilterPredicate

    private enum FilterPredicate: Sendable {
        case equals(FieldValue)
        case `in`([FieldValue])
        case range(
            min: FieldValue?,
            max: FieldValue?,
            minInclusive: Bool,
            maxInclusive: Bool
        )
        case custom(@Sendable (T) -> Bool)
    }

    private var fieldName: String {
        field?.name ?? ""
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Filter for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Filter(#field(\Product.category), equals: "electronics")
    ///     Search(\.description).terms(["wireless"])
    /// }
    /// ```
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V?>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Filter for set membership
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        in values: [V]
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .in(values.map(\.fieldValue))
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Range)

    /// Create a Filter for range comparison
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: ClosedRange<V>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: true
        )
        self.queryContext = context
    }

    /// Create a Filter for half-open range
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: Range<V>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThan value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThanOrEqual value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThan value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than or equal
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThanOrEqual value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: true
        )
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Custom)

    /// Create a Filter with custom predicate
    public init(_ predicate: @escaping @Sendable (T) -> Bool) {
        guard let context = FusionContext.current else {
            fatalError("Filter must be used within context.fuse { } block")
        }
        self.field = nil
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Filter for equality comparison with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Filter for set membership with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .in(values.map(\.fieldValue))
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Range)

    /// Create a Filter for range comparison with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: ClosedRange<V>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: true
        )
        self.queryContext = context
    }

    /// Create a Filter for half-open range with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: Range<V>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThan value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThan value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than or equal with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: true
        )
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Custom)

    /// Create a Filter with custom predicate and explicit context
    public init(_ predicate: @escaping @Sendable (T) -> Bool, context: IndexQueryContext) {
        self.field = nil
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Find the index descriptor that can efficiently answer this query
    ///
    /// For scalar indexes, only the leftmost field can be used for efficient
    /// equality/range queries. This follows B-tree index semantics:
    ///
    /// - Composite index `[a, b, c]` has key structure: `[a][b][c][primaryKey]`
    /// - Efficient queries: `a` alone, `(a, b)`, or `(a, b, c)` (left-to-right)
    /// - Inefficient queries: `b` alone, `c` alone (requires full index scan)
    ///
    /// **Reference**: "Database System Concepts" (Silberschatz) - Chapter 14.3
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        try T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kind.identifier == "scalar" else {
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
    private func executeEqualitySearch(value: FieldValue) async throws -> [T] {
        guard let descriptor = try findIndexDescriptor() else {
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            var matches: [T] = []
            matches.reserveCapacity(allItems.count)
            for item in allItems {
                if try persistedFieldValue(in: item) == value {
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
                value: value,
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
        min: FieldValue?,
        max: FieldValue?,
        minInclusive: Bool,
        maxInclusive: Bool
    ) async throws -> [T] {
        guard let descriptor = try findIndexDescriptor() else {
            // Fallback to full scan with filter
            let allItems = try await queryContext.fetchAllItems(type: T.self)
            var matches: [T] = []
            matches.reserveCapacity(allItems.count)
            for item in allItems {
                let value = try persistedFieldValue(in: item)
                if try ScalarRangeValueMatcher.matches(
                    value,
                    minimum: min,
                    maximum: max,
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
                min: min,
                max: max,
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

    private func persistedFieldValue(in item: T) throws -> FieldValue {
        guard let field else {
            throw FilterError.missingFieldSelection
        }
        guard let value = try item.persistedFieldValue(for: field) else {
            throw FilterError.missingPersistedField(
                entity: T.persistableType,
                field: field
            )
        }
        return value
    }

    /// Search scalar index for equality
    private func searchScalarEquals(
        value: FieldValue,
        indexedFieldCount: Int,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let tupleValue = try value.toTupleElement()
        let valueSubspace = indexSubspace.subspace(tupleValue)
        let (begin, end) = valueSubspace.range()

        var results: [Tuple] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
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
        let beginKey: ByteString
        let endKey: ByteString

        if let min {
            let minTuple = try min.toTupleElement()
            let packed = indexSubspace.pack(Tuple(minTuple))
            if minInclusive {
                beginKey = packed
            } else {
                beginKey = try strinc(packed)
            }
        } else {
            beginKey = indexSubspace.prefix
        }

        if let max {
            let maxTuple = try max.toTupleElement()
            let packed = indexSubspace.pack(Tuple(maxTuple))
            if maxInclusive {
                endKey = try strinc(packed)
            } else {
                endKey = packed
            }
        } else {
            endKey = try strinc(indexSubspace.prefix)
        }

        var results: [Tuple] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
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

}
