// IndexQueryContext+Similar.swift
// VectorIndex - Factory method for Similar query

import Foundation
import Core
import DatabaseEngine

extension IndexQueryContext {
    /// Create a Similar query for vector search
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Product.self) {
    ///     context.indexQueryContext.similar(Product.self, \.embedding, dimensions: 384)
    ///         .nearest(to: queryVector, k: 100)
    /// }
    /// .execute()
    /// ```
    public func similar<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, [Float]>,
        dimensions: Int
    ) -> Similar<T> {
        Similar(keyPath, dimensions: dimensions, context: self)
    }

    /// Create a Similar query for optional vector field
    public func similar<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, [Float]?>,
        dimensions: Int
    ) -> Similar<T> {
        Similar(keyPath, dimensions: dimensions, context: self)
    }
}
