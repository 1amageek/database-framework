// IndexQueryContext+Filter.swift
// ScalarIndex - Factory methods for Filter query

import Foundation
import Core
import DatabaseEngine

extension IndexQueryContext {
    /// Create a Filter query for equality comparison
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Product.self) {
    ///     context.indexQueryContext.filter(Product.self, \.category, equals: "electronics")
    ///     context.indexQueryContext.search(Product.self, \.description).terms(["wireless"])
    /// }
    /// .execute()
    /// ```
    public func filter<T: Persistable, V: Sendable & Hashable & Equatable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> Filter<T> {
        Filter(keyPath, equals: value, context: self)
    }

    /// Create a Filter query for set membership
    public func filter<T: Persistable, V: Sendable & Hashable & Equatable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        in values: [V]
    ) -> Filter<T> {
        Filter(keyPath, in: values, context: self)
    }

    /// Create a Filter query for range comparison (closed range)
    public func filter<T: Persistable, V: Sendable & Comparable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        range: ClosedRange<V>
    ) -> Filter<T> {
        Filter(keyPath, range: range, context: self)
    }

    /// Create a Filter query for range comparison (half-open range)
    public func filter<T: Persistable, V: Sendable & Comparable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        range: Range<V>
    ) -> Filter<T> {
        Filter(keyPath, range: range, context: self)
    }

    /// Create a Filter query with greater than comparison
    public func filter<T: Persistable, V: Sendable & Comparable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        greaterThan value: V
    ) -> Filter<T> {
        Filter(keyPath, greaterThan: value, context: self)
    }

    /// Create a Filter query with less than comparison
    public func filter<T: Persistable, V: Sendable & Comparable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        lessThan value: V
    ) -> Filter<T> {
        Filter(keyPath, lessThan: value, context: self)
    }
}
