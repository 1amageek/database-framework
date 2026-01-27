// BitmapQuery.swift
// BitmapIndex - Query extension for bitmap indexes
//
// Provides FDBContext extension and query builder for set operations.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Bitmap Entry Point

/// Entry point for bitmap queries
///
/// **Usage**:
/// ```swift
/// import BitmapIndex
///
/// // Find all active users
/// let activeUsers = try await context.bitmap(User.self)
///     .field(\.status)
///     .equals("active")
///     .execute()
///
/// // Find users with status "active" OR "pending"
/// let users = try await context.bitmap(User.self)
///     .field(\.status)
///     .in(["active", "pending"])
///     .execute()
///
/// // Count active users
/// let count = try await context.bitmap(User.self)
///     .field(\.status)
///     .equals("active")
///     .count()
/// ```
public struct BitmapEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the bitmap index field
    ///
    /// - Parameter keyPath: KeyPath to the indexed field
    /// - Returns: Bitmap query builder
    public func field<V>(_ keyPath: KeyPath<T, V>) -> BitmapQueryBuilder<T> {
        BitmapQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}

// MARK: - Bitmap Query Builder

/// Builder for bitmap index queries
///
/// Supports efficient set operations on low-cardinality fields.
public struct BitmapQueryBuilder<T: Persistable>: Sendable {
    // MARK: - Types

    /// Query operation type
    public enum Operation: Sendable {
        case equals(any TupleElement & Sendable)
        case `in`([any TupleElement & Sendable])
        case and([[any TupleElement & Sendable]])
    }

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var operation: Operation?
    private var limitCount: Int?

    // MARK: - Initialization

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    // MARK: - Query Methods

    /// Match a single value
    ///
    /// - Parameter value: The value to match
    /// - Returns: Updated query builder
    public func equals(_ value: some TupleElement & Sendable) -> Self {
        var copy = self
        copy.operation = .equals(value)
        return copy
    }

    /// Match any of the given values (OR)
    ///
    /// - Parameter values: Values to match
    /// - Returns: Updated query builder
    public func `in`(_ values: [some TupleElement & Sendable]) -> Self {
        var copy = self
        copy.operation = .in(values)
        return copy
    }

    /// Match all of the given values (AND)
    ///
    /// This is useful for multi-field bitmap indexes.
    ///
    /// - Parameter valueSets: Array of value arrays to AND together
    /// - Returns: Updated query builder
    public func all(_ valueSets: [[some TupleElement & Sendable]]) -> Self {
        var copy = self
        copy.operation = .and(valueSets)
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute the query and return matching items
    ///
    /// - Returns: Array of matching items
    public func execute() async throws -> [T] {
        let primaryKeys: [Tuple] = try await withResolvedBitmap { bitmap, maintainer, transaction in
            var resultBitmap = bitmap
            if let limit = self.limitCount {
                let array = bitmap.toArray()
                if array.count > limit {
                    resultBitmap = RoaringBitmap()
                    for id in array.prefix(limit) {
                        resultBitmap.add(id)
                    }
                }
            }
            return try await maintainer.getPrimaryKeys(from: resultBitmap, transaction: transaction)
        }
        return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
    }

    /// Get the count of matching items
    ///
    /// More efficient than execute() when only count is needed.
    ///
    /// - Returns: Number of matching items
    public func count() async throws -> Int {
        try await withResolvedBitmap { bitmap, _, _ in
            bitmap.cardinality
        }
    }

    /// Get the bitmap directly (for advanced operations)
    ///
    /// - Returns: RoaringBitmap of matching record IDs
    public func getBitmap() async throws -> RoaringBitmap {
        try await withResolvedBitmap { bitmap, _, _ in
            bitmap
        }
    }

    // MARK: - Private Methods

    /// Resolve bitmap using the configured operation, then pass to body.
    ///
    /// Centralizes maintainer creation and operation dispatch shared by
    /// `execute()`, `count()`, and `getBitmap()`.
    private func withResolvedBitmap<R: Sendable>(
        _ body: @escaping @Sendable (RoaringBitmap, BitmapIndexMaintainer<T>, any TransactionProtocol) async throws -> R
    ) async throws -> R {
        guard let op = operation else {
            throw BitmapQueryError.noOperation
        }

        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            let maintainer = BitmapIndexMaintainer<T>(
                index: Index(
                    name: indexName,
                    kind: BitmapIndexKind<T>(fieldNames: [self.fieldName]),
                    rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                    keyPaths: []
                ),
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            let bitmap: RoaringBitmap
            switch op {
            case .equals(let value):
                bitmap = try await maintainer.getBitmap(for: [value], transaction: transaction)

            case .in(let values):
                let valueSets = values.map { [$0] as [any TupleElement] }
                bitmap = try await maintainer.orQuery(values: valueSets, transaction: transaction)

            case .and(let valueSets):
                let converted = valueSets.map { $0 as [any TupleElement] }
                bitmap = try await maintainer.andQuery(values: converted, transaction: transaction)
            }

            return try await body(bitmap, maintainer, transaction)
        }
    }

    private func buildIndexName() -> String {
        "\(T.persistableType)_bitmap_\(fieldName)"
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a bitmap index query
    ///
    /// This method is available when you import `BitmapIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import BitmapIndex
    ///
    /// // Find all active users
    /// let activeUsers = try await context.bitmap(User.self)
    ///     .field(\.status)
    ///     .equals("active")
    ///     .execute()
    ///
    /// // Count active users (more efficient)
    /// let count = try await context.bitmap(User.self)
    ///     .field(\.status)
    ///     .equals("active")
    ///     .count()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the bitmap query
    public func bitmap<T: Persistable>(_ type: T.Type) -> BitmapEntryPoint<T> {
        BitmapEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Bitmap Query Error

/// Errors for bitmap query operations
public enum BitmapQueryError: Error, CustomStringConvertible {
    /// No operation specified
    case noOperation

    /// Index not found
    case indexNotFound(String)

    public var description: String {
        switch self {
        case .noOperation:
            return "No bitmap query operation specified. Use .equals() or .in() to specify a query."
        case .indexNotFound(let name):
            return "Bitmap index not found: \(name)"
        }
    }
}
