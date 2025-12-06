// IndexQueryContext+Search.swift
// FullTextIndex - Factory method for Search query

import Foundation
import Core
import DatabaseEngine

extension IndexQueryContext {
    /// Create a Search query for full-text search
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Article.self) {
    ///     context.indexQueryContext.search(Article.self, \.content)
    ///         .terms(["swift", "concurrency"])
    /// }
    /// .execute()
    /// ```
    public func search<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, String>
    ) -> Search<T> {
        Search(keyPath, context: self)
    }

    /// Create a Search query for optional text field
    public func search<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, String?>
    ) -> Search<T> {
        Search(keyPath, context: self)
    }
}
