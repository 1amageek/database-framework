// FusionContext.swift
// DatabaseEngine - TaskLocal context for FusionQuery initialization

import Foundation
import Core

/// TaskLocal storage for IndexQueryContext during fusion query building
///
/// This enables FusionQuery implementations to access the context without
/// explicit parameter passing, enabling clean ResultBuilder syntax:
///
/// ```swift
/// context.fuse(Product.self) {
///     Search(\.description).terms(["coffee"])
///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
/// }
/// ```
///
/// **Thread Safety**: Uses TaskLocal which is safe for concurrent access.
/// Each task/thread gets its own copy of the context.
///
/// **Usage in FusionQuery implementations**:
/// ```swift
/// public struct Search<T: Persistable>: FusionQuery {
///     public init(_ keyPath: KeyPath<T, String>) {
///         guard let context = FusionContext.current else {
///             fatalError("Search must be used within context.fuse { } block")
///         }
///         self.queryContext = context
///         // ...
///     }
/// }
/// ```
public enum FusionContext {
    /// TaskLocal storage for the current IndexQueryContext
    @TaskLocal public static var current: IndexQueryContext?

    /// Execute a closure with the given context available via `FusionContext.current`
    ///
    /// - Parameters:
    ///   - context: The IndexQueryContext to make available
    ///   - operation: The closure to execute
    /// - Returns: The result of the closure
    public static func withContext<T>(
        _ context: IndexQueryContext,
        operation: () throws -> T
    ) rethrows -> T {
        try $current.withValue(context, operation: operation)
    }
}
