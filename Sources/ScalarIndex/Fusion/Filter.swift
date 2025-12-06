// Filter.swift
// ScalarIndex - Scalar filter query for Fusion
//
// This file is part of ScalarIndex module, not DatabaseEngine.
// DatabaseEngine does not know about ScalarIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Scalar filter query for Fusion
///
/// Filters items based on scalar field values using index.
/// All matching items receive a score of 1.0.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Filter(\.category, equals: "electronics", context: context.indexQueryContext)
///     Search(\.description, context: context.indexQueryContext).terms(["wireless"])
/// }
/// .execute()
/// ```
public struct Filter<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var predicate: FilterPredicate

    private enum FilterPredicate: @unchecked Sendable {
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

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == ScalarIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? ScalarIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        var results: [T]

        switch predicate {
        case .equals(let value):
            // Try to find scalar index
            if let descriptor = findIndexDescriptor() {
                results = try await queryContext.executeScalarIndexSearch(
                    type: T.self,
                    indexName: descriptor.name,
                    fieldName: fieldName,
                    value: value
                )
            } else {
                // Fallback: use generic scalar index name format
                let indexName = "\(T.persistableType)_\(fieldName)"
                results = try await queryContext.executeScalarIndexSearch(
                    type: T.self,
                    indexName: indexName,
                    fieldName: fieldName,
                    value: value
                )
            }

        case .in(let values):
            // Union of equality searches
            var allResults: [T] = []
            let indexName = findIndexDescriptor()?.name ?? "\(T.persistableType)_\(fieldName)"
            for value in values {
                let matches = try await queryContext.executeScalarIndexSearch(
                    type: T.self,
                    indexName: indexName,
                    fieldName: fieldName,
                    value: value
                )
                allResults.append(contentsOf: matches)
            }
            // Deduplicate
            var seen: Set<String> = []
            results = allResults.filter { item in
                let id = "\(item.id)"
                if seen.contains(id) { return false }
                seen.insert(id)
                return true
            }

        case .range(let min, let max, let minInclusive, let maxInclusive):
            // Range scan using scalar index
            let indexName = findIndexDescriptor()?.name ?? "\(T.persistableType)_\(fieldName)"
            results = try await queryContext.executeScalarRangeSearch(
                type: T.self,
                indexName: indexName,
                fieldName: fieldName,
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
}
