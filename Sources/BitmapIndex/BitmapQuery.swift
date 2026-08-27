// BitmapQuery.swift
// BitmapIndex - Query extension for bitmap indexes
//
// Provides DatabaseContext extension and query builder for set operations.

@_spi(DatabaseExecution) import DatabaseEngine
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
    public func field<Value>(_ field: Field<T, Value>) -> BitmapQueryBuilder<T> {
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
    public func equals(_ value: some TupleElement & Sendable) -> Self {
        var copy = self
        copy.operation = .equals(value)
        return copy
    }

    internal func equalsAny(_ value: any TupleElement & Sendable) -> Self {
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

    internal func inAny(_ values: [any TupleElement & Sendable]) -> Self {
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

    internal func allAny(_ valueSets: [[any TupleElement & Sendable]]) -> Self {
        var copy = self
        copy.operation = .and(valueSets)
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
        return try await queryContext.withReadOperation {
            let indexName = try self.resolveIndexName()
            guard let entity = self.queryContext.schema.entitiesByName[
                T.persistableType
            ] else {
                throw BitmapQueryError.indexNotFound(indexName)
            }
            let selectQuery = try self.toSelectQuery()
            let policy = try self.queryContext.context.readPolicy()
            let authorization = try policy.authorizeRead(
                    listRequirements: [
                        try DatabaseReadPolicy.listRequirement(
                            entityName: entity.name,
                            selectQuery: selectQuery
                        )
                    ],
                    fields: .make(
                        query: selectQuery,
                        schema: policy.schema
                    )
                )
            return try await self.withResolvedBitmap(
                configuration: configuration,
                authorization: authorization,
                missing: { [] }
            ) { bitmap, reader, session, workMeter in
                let retainedPrimaryKeys = try await reader.primaryKeys(
                    for: bitmap,
                    transaction: session.transaction.storageTransaction,
                    limit: self.limitCount.flatMap { Int(exactly: $0) },
                    workMeter: workMeter
                )
                let items = try await session
                    .fetchRetainedPersistedModelsPreservingOrder(
                        entity: entity,
                        primaryKeys: retainedPrimaryKeys,
                        partitions: FieldObject(),
                        snapshot: true
                    )
                var results: [T] = []
                results.reserveCapacity(items.count)
                for position in 0..<items.count {
                    guard let retained = items[position] else {
                        var primaryKey = ByteString()
                        retainedPrimaryKeys.withRetainedPrimaryKey(
                            at: position
                        ) { key in
                            primaryKey = key.pack()
                        }
                        throw BitmapQueryError.indexedItemMissing(
                            index: indexName,
                            primaryKey: primaryKey
                        )
                    }
                    try retained.withModel { model in
                        results.append(try model.decode(as: T.self))
                    }
                }
                return results
            }
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
            missing: { 0 }
        ) { bitmap, _, _, _ in
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
            missing: { RoaringBitmap() }
        ) { bitmap, _, _, _ in
            bitmap.promoteToOutput()
        }
    }

    // MARK: - Private Methods

    /// Resolve bitmap using the configured operation, then pass to body.
    ///
    /// Centralizes maintainer creation and operation dispatch shared by
    /// `execute()`, `count()`, and `getBitmap()`.
    private func withResolvedBitmap<R: Sendable>(
        configuration: TransactionConfiguration,
        authorization: DatabaseReadAuthorization? = nil,
        missing: @Sendable @escaping () -> R,
        _ body: @escaping @Sendable (
            consuming BitmapReadOwner,
            BitmapIndexReader,
            DatabaseReadSession,
            DatabaseWorkMeter
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
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: queryContext.context.container.monotonicClock
        )
        return try await queryContext.withSession(
            configuration: configuration,
            workMeter: workMeter
        ) { session in
            let admittedSession: DatabaseReadSession
            if let authorization {
                admittedSession = try session.authorizedSession(authorization)
            } else {
                admittedSession = session
            }
            let transaction = admittedSession.transaction
            let readableIndex = try await queryContext.readableIndex(
                named: indexName,
                indexType: .bitmap,
                for: T.self,
                transaction: transaction
            )
            guard let readableIndex else {
                return missing()
            }
            let reader = BitmapIndexReader(
                subspace: readableIndex.subspace
            )

            let bitmap: BitmapReadOwner
            switch op {
            case .equals(let value):
                bitmap = try await reader.bitmap(
                    for: [value],
                    transaction: transaction.storageTransaction,
                    workMeter: workMeter
                )

            case .in(let values):
                let valueSets = values.map { [$0] as [any TupleElement] }
                bitmap = try await reader.union(
                    of: valueSets,
                    transaction: transaction.storageTransaction,
                    workMeter: workMeter
                )

            case .and(let valueSets):
                let converted = valueSets.map { $0 as [any TupleElement] }
                bitmap = try await reader.intersection(
                    of: converted,
                    transaction: transaction.storageTransaction,
                    workMeter: workMeter
                )
            }

            return try await body(
                consume bitmap,
                reader,
                admittedSession,
                workMeter
            )
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
            parameters[BitmapReadParameter.values] = .array([
                try DatabaseEngine.CanonicalTupleElementCodec.encode(value)
            ])
        case .in(let values):
            parameters[BitmapReadParameter.operation] = .string(BitmapReadParameter.inOperation)
            parameters[BitmapReadParameter.values] = .array(
                try values.map { try DatabaseEngine.CanonicalTupleElementCodec.encode($0) }
            )
        case .and(let valueSets):
            parameters[BitmapReadParameter.operation] = .string(BitmapReadParameter.andOperation)
            parameters[BitmapReadParameter.valueSets] = .array(
                try valueSets.map { valueSet in
                    .array(try valueSet.map { try DatabaseEngine.CanonicalTupleElementCodec.encode($0) })
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
        case .indexedItemMissing(let index, let primaryKey):
            return "Bitmap index '\(index)' references missing item \(primaryKey)"
        }
    }
}
