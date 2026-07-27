// IndexQueryContext+Rank.swift
// RankIndex - Factory method for Rank query

import DatabaseKit
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
    public func rank<T: Persistable, Value: RankNumericValue>(
        _ type: T.Type,
        _ field: Field<T, Value>
    ) -> Rank<T> {
        Rank(field, context: self)
    }

    public func rank<T: Persistable, Value: RankNumericValue>(
        _ type: T.Type,
        _ field: Field<T, Value?>
    ) -> Rank<T> {
        Rank(field, context: self)
    }
}
