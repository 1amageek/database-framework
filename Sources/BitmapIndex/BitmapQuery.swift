// BitmapQuery.swift
// BitmapIndex - Query extension for bitmap indexes
//
// Provides DatabaseContext extension and query builder for set operations.

import DatabaseEngine
import DatabaseKit
import StorageKit

// MARK: - Bitmap Entry Point

/// Entry point for bitmap queries
///
/// **Usage**:
/// ```swift
/// import BitmapIndex
///
/// // Find all active users
/// let activeUsers = try await context.bitmap(User.self)
///     .field(User.fields.status)
///     .equals("active")
///     .execute()
///
/// // Find users with status "active" OR "pending"
/// let users = try await context.bitmap(User.self)
///     .field(User.fields.status)
///     .in(["active", "pending"])
///     .execute()
///
/// // Count active users
/// let count = try await context.bitmap(User.self)
///     .field(User.fields.status)
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
    /// - Parameter field: Compiled identity of the indexed field
    /// - Returns: Bitmap query builder
    public func field<Value: FieldValueRepresentable>(
        _ field: Field<T, Value>
    ) -> BitmapQueryBuilder<T, Value> {
        BitmapQueryBuilder(
            queryContext: queryContext,
            fieldName: field.name
        )
    }
}

// MARK: - Bitmap Query Builder

/// Builder for bitmap index queries
///
/// Supports efficient set operations on low-cardinality fields.
public struct BitmapQueryBuilder<
    T: Persistable,
    Value: FieldValueRepresentable
>: Sendable {
    // MARK: - Types

    /// Query operation type
    public enum Operation: Sendable {
        case equals(FieldValue)
        case `in`([FieldValue])
        case and([[FieldValue]])
    }

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let selectedIndexName: String?
    private var operation: Operation?
    private var limitCount: UInt64?

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        fieldName: String,
        selectedIndexName: String? = nil
    ) {
        self.queryContext = queryContext
        self.fieldName = fieldName
        self.selectedIndexName = selectedIndexName
    }

    // MARK: - Query Methods

    /// Match a single value
    ///
    /// - Parameter value: The value to match
    /// - Returns: Updated query builder
    public func equals(_ value: Value) -> Self {
        var copy = self
        copy.operation = .equals(value.fieldValue)
        return copy
    }

    /// Match any of the given values (OR)
    ///
    /// - Parameter values: Values to match
    /// - Returns: Updated query builder
    public func `in`(_ values: [Value]) -> Self {
        var copy = self
        copy.operation = .in(values.map(\.fieldValue))
        return copy
    }

    /// Match all of the given values (AND)
    ///
    /// This is useful for multi-field bitmap indexes.
    ///
    /// - Parameter valueSets: Array of value arrays to AND together
    /// - Returns: Updated query builder
    public func all(_ valueSets: [[Value]]) -> Self {
        var copy = self
        copy.operation = .and(
            valueSets.map { $0.map(\.fieldValue) }
        )
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: Updated query builder
    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute the query and return matching items
    ///
    /// - Returns: Array of matching items
    public func execute() async throws -> [T] {
        let response = try await queryContext.context.query(
            try toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            try QueryRowCodec.decode(row, as: T.self)
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> [T] {
        let indexName = try resolveIndexName()
        return try await withResolvedBitmap(
            configuration: configuration,
            authorization: IndexReadAuthorization(
                limit: try limitCount.map {
                    guard let value = Int(exactly: $0) else {
                        throw BitmapQueryError.invalidLimit($0)
                    }
                    return value
                },
                offset: nil,
                orderBy: nil
            ),
            missing: { [] }
        ) { bitmap, maintainer, transaction in
            var resultBitmap = bitmap
            if let limit = self.limitCount {
                let array = bitmap.toArray()
                if UInt64(array.count) > limit {
                    resultBitmap = RoaringBitmap()
                    for id in array.prefix(Int(limit)) {
                        resultBitmap.add(id)
                    }
                }
            }
            let primaryKeys = try await maintainer.primaryKeys(
                for: resultBitmap,
                transaction: transaction
            )
            let items = try await self.queryContext.fetchItemsPreservingOrder(
                ids: primaryKeys,
                type: T.self
            )
            var results: [T] = []
            results.reserveCapacity(items.count)
            for (primaryKey, item) in zip(primaryKeys, items) {
                guard let item else {
                    throw BitmapQueryError.indexedItemMissing(
                        index: indexName,
                        primaryKey: primaryKey.pack()
                    )
                }
                results.append(item)
            }
            return results
        }
    }

    /// Get the count of matching items
    ///
    /// More efficient than execute() when only count is needed.
    ///
    /// - Returns: Number of matching items
    public func count() async throws -> Int {
        try await countDirect(configuration: .readOnly)
    }

    internal func countDirect(
        configuration: TransactionConfiguration = .readOnly
    ) async throws -> Int {
        return try await withResolvedBitmap(
            configuration: configuration,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            ),
            missing: { 0 }
        ) { bitmap, _, _ in
            bitmap.cardinality
        }
    }

    /// Get the bitmap directly (for advanced operations)
    ///
    /// - Returns: RoaringBitmap of matching entity IDs
    public func getBitmap() async throws -> RoaringBitmap {
        try await getBitmapDirect(configuration: .readOnly)
    }

    internal func getBitmapDirect(
        configuration: TransactionConfiguration = .readOnly
    ) async throws -> RoaringBitmap {
        return try await withResolvedBitmap(
            configuration: configuration,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            ),
            missing: { RoaringBitmap() }
        ) { bitmap, _, _ in
            bitmap
        }
    }

    // MARK: - Private Methods

    /// Resolve bitmap using the configured operation, then pass to body.
    ///
    /// Centralizes maintainer creation and operation dispatch shared by
    /// `execute()`, `count()`, and `getBitmap()`.
    private func withResolvedBitmap<R: Sendable>(
        configuration: TransactionConfiguration,
        authorization: IndexReadAuthorization,
        missing: @Sendable @escaping () -> R,
        _ body: @escaping @Sendable (
            RoaringBitmap,
            BitmapIndexReader,
            any TransactionReadAccess
        ) async throws -> R
    ) async throws -> R {
        guard let op = operation else {
            throw BitmapQueryError.noOperation
        }

        let indexName = try resolveIndexName()
        guard let descriptor = queryContext.indexDescriptors(
            for: T.self
        ).first(where: { $0.name == indexName }) else {
            throw BitmapQueryError.indexNotFound(indexName)
        }
        guard descriptor.fieldNames == [fieldName] else {
            throw BitmapQueryError.invalidIndex(indexName)
        }
        guard descriptor.type == .bitmap else { throw BitmapQueryError.invalidIndex(descriptor.name) }
        return try await queryContext.withReadableIndex(
            named: indexName,
            indexType: .bitmap,
            for: T.self,
            authorization: authorization,
            configuration: configuration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return missing()
            }
            let reader = BitmapIndexReader(
                subspace: readableIndex.subspace
            )

            let bitmap: RoaringBitmap
            switch op {
            case .equals(let value):
                bitmap = try await reader.bitmap(
                    for: [try value.toTupleElement()],
                    transaction: transaction
                )

            case .in(let values):
                let valueSets = try values.map {
                    [try $0.toTupleElement()] as [any TupleElement]
                }
                bitmap = try await reader.union(of: valueSets, transaction: transaction)

            case .and(let valueSets):
                let converted = try valueSets.map {
                    try FieldValue.toTupleElements($0)
                }
                bitmap = try await reader.intersection(of: converted, transaction: transaction)
            }

            return try await body(bitmap, reader, transaction)
        }
    }

    private func resolveIndexName() throws -> String {
        let matches = queryContext.indexDescriptors(for: T.self).filter {
            $0.type == .bitmap
                && $0.fieldNames == [fieldName]
        }
        if let selectedIndexName {
            guard matches.contains(where: {
                $0.name == selectedIndexName
            }) else {
                throw BitmapQueryError.indexNotFound(selectedIndexName)
            }
            return selectedIndexName
        }
        guard let match = matches.first else {
            throw BitmapQueryError.indexNotFound(
                "\(T.persistableType).\(fieldName)"
            )
        }
        guard matches.count == 1 else {
            throw BitmapQueryError.ambiguousIndexes(
                entity: T.persistableType,
                field: fieldName
            )
        }
        return match.name
    }

    internal func toSelectQuery() throws -> SelectQuery {
        guard let operation else {
            throw BitmapQueryError.noOperation
        }

        var parameters: [String: FieldValue] = [
            BitmapReadParameter.fieldName: .string(fieldName)
        ]
        if let limitCount {
            parameters[BitmapReadParameter.limit] = .uint64(limitCount)
        }

        switch operation {
        case .equals(let value):
            parameters[BitmapReadParameter.operation] = .string(BitmapReadParameter.equalsOperation)
            parameters[BitmapReadParameter.values] = .array([value])
        case .in(let values):
            parameters[BitmapReadParameter.operation] = .string(BitmapReadParameter.inOperation)
            parameters[BitmapReadParameter.values] = .array(values)
        case .and(let valueSets):
            parameters[BitmapReadParameter.operation] = .string(BitmapReadParameter.andOperation)
            parameters[BitmapReadParameter.valueSets] = .array(
                valueSets.map { valueSet in
                    .array(valueSet)
                }
            )
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: try resolveIndexName(),
                    indexType: .bitmap,
                    parameters: parameters
                )
            ),
            limit: limitCount
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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
    ///     .field(User.fields.status)
    ///     .equals("active")
    ///     .execute()
    ///
    /// // Count active users (more efficient)
    /// let count = try await context.bitmap(User.self)
    ///     .field(User.fields.status)
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

    /// The resolved index definition does not match the requested bitmap field.
    case invalidIndex(String)

    /// More than one bitmap index targets the requested field.
    case ambiguousIndexes(entity: String, field: String)

    /// The requested query limit cannot be represented by this runtime.
    case invalidLimit(UInt64)

    /// An index entry references an entity that is not present in storage.
    case indexedItemMissing(index: String, primaryKey: ByteString)

    public var description: String {
        switch self {
        case .noOperation:
            return "No bitmap query operation specified. Use .equals() or .in() to specify a query."
        case .indexNotFound(let name):
            return "Bitmap index not found: \(name)"
        case .invalidIndex(let name):
            return "Bitmap index definition is invalid: \(name)"
        case .ambiguousIndexes(let entity, let field):
            return "Multiple bitmap indexes target \(entity).\(field)"
        case .invalidLimit(let limit):
            return "Bitmap query limit exceeds the runtime range: \(limit)"
        case .indexedItemMissing(let index, let primaryKey):
            return "Bitmap index '\(index)' references missing item \(primaryKey)"
        }
    }
}
