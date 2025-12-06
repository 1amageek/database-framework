// IndexQueryContext+Rank.swift
// RankIndex - Factory method for Rank query

import Foundation
import Core
import DatabaseEngine

extension IndexQueryContext {
    /// Create a Rank query for numeric field ranking
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Product.self) {
    ///     context.indexQueryContext.search(Product.self, \.description).terms(["coffee"])
    ///     context.indexQueryContext.rank(Product.self, \.rating).order(.descending)
    /// }
    /// .execute()
    /// ```
    public func rank<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, Int>
    ) -> Rank<T> {
        Rank(keyPath, context: self)
    }

    /// Create a Rank query for Int64 field
    public func rank<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, Int64>
    ) -> Rank<T> {
        Rank(keyPath, context: self)
    }

    /// Create a Rank query for Double field
    public func rank<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, Double>
    ) -> Rank<T> {
        Rank(keyPath, context: self)
    }

    /// Create a Rank query for Float field
    public func rank<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, Float>
    ) -> Rank<T> {
        Rank(keyPath, context: self)
    }
}
