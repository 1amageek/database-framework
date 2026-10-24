import DatabaseKit
import DatabaseTypes

// MARK: - Query

/// Type-safe query builder for fetching Persistable models
///
/// **Usage**:
/// ```swift
/// // Fluent API
/// let users = try await context.fetch(User.self)
///     .where(#field(\User.isActive) == true)
///     .where(#field(\User.age) > 18)
///     .orderBy(#field(\User.name))
///     .limit(10)
///     .execute()
///
/// // Simple fetch all
/// let allUsers = try await context.fetch(User.self).execute()
///
/// // First result
/// let user = try await context.fetch(User.self)
///     .where(#field(\User.email) == "alice@example.com")
///     .first()
///
/// // Count
/// let count = try await context.fetch(User.self)
///     .where(#field(\User.isActive) == true)
///     .count()
/// ```
public struct Query<T: Persistable>: Sendable {
    /// Filter predicates (combined with AND)
    public var predicates: [Predicate<T>]

    /// Sort descriptors
    public var sortDescriptors: [SortDescriptor<T>]

    /// Maximum number of results
    public var fetchLimit: Int?

    /// Number of results to skip
    public var fetchOffset: Int?

    /// Partition binding for dynamic directories
    ///
    /// Required for types with `Field(\.keyPath)` in their `#Directory` declaration.
    /// Set via `.partition()` fluent API method.
    public var partitionBinding: DirectoryPath<T>?

    /// Cache policy for this query
    ///
    /// Controls whether the query uses cached read versions for performance optimization.
    /// Default is `.server` (strict consistency).
    ///
    /// - `.server`: Always fetch latest data from server (strict consistency)
    /// - `.cached`: Use cached read version (5 seconds staleness)
    /// - `.stale(N)`: Allow stale data up to N seconds
    public var cachePolicy: CachePolicy = .server

    /// Forced index hint set by callers that already decided which named index
    /// must serve this query (e.g. a canonical `SelectQuery` carrying an
    /// `accessPath`). When non-nil, the fetch path restricts index selection to
    /// the named index and raises an error if the index cannot serve the
    /// pushed-down predicate — silent fallback to a full scan is forbidden.
    public var forcedIndex: IndexHint?

    /// Request-scoped execution accounting used by canonical server reads.
    /// Native typed queries leave this unset unless a higher-level runtime
    /// explicitly supplies a bounded execution context.
    var executionWorkMeter: DatabaseWorkMeter?

    /// Storage-key position used only by the canonical stable-snapshot pager.
    /// The value is the packed entity identifier, not an externally visible
    /// continuation token.
    var executionStartAfterIdentifier: ByteString?
    var executionStorageOffset: Int
    var executionWindowIsPushed: Bool

    /// Initialize an empty query
    public init() {
        self.predicates = []
        self.sortDescriptors = []
        self.fetchLimit = nil
        self.fetchOffset = nil
        self.partitionBinding = nil
        self.cachePolicy = .server
        self.forcedIndex = nil
        self.executionWorkMeter = nil
        self.executionStartAfterIdentifier = nil
        self.executionStorageOffset = 0
        self.executionWindowIsPushed = false
    }

    // MARK: - Fluent API

    /// Add a filter predicate
    public func `where`(_ predicate: Predicate<T>) -> Query<T> {
        var copy = self
        copy.predicates.append(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>
    ) -> Query<T> {
        var copy = self
        copy.sortDescriptors.append(
            SortDescriptor(field: field, order: .ascending)
        )
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>,
        _ order: SortOrder
    ) -> Query<T> {
        var copy = self
        copy.sortDescriptors.append(
            SortDescriptor(field: field, order: order)
        )
        return copy
    }

    /// Set maximum number of results
    public func limit(_ count: Int) -> Query<T> {
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    /// Set number of results to skip
    public func offset(_ count: Int) -> Query<T> {
        var copy = self
        copy.fetchOffset = count
        return copy
    }

    /// Set cache policy for this query
    ///
    /// Controls whether the query uses cached read versions for performance optimization.
    ///
    /// - Parameter policy: The cache policy to use
    /// - Returns: A new Query with the cache policy set
    ///
    /// **Usage**:
    /// ```swift
    /// // Fetch latest data (strict consistency)
    /// let users = try await context.fetch(User.self)
    ///     .cachePolicy(.server)
    ///     .execute()
    ///
    /// // Allow 30-second stale data
    /// let products = try await context.fetch(Product.self)
    ///     .cachePolicy(.stale(.seconds(30)))
    ///     .execute()
    /// ```
    public func cachePolicy(_ policy: CachePolicy) -> Query<T> {
        var copy = self
        copy.cachePolicy = policy
        return copy
    }

    // MARK: - Partition

    /// Bind a partition field value for dynamic directory resolution
    ///
    /// Required for types with `Field(\.keyPath)` in their `#Directory` declaration.
    /// The partition value is used to resolve the correct directory subspace.
    ///
    /// **Usage**:
    /// ```swift
    /// @Persistable
    /// struct Order {
    ///     #Directory<Order>("tenants", Field(\.tenantID), "orders")
    ///     var tenantID: String
    /// }
    ///
    /// let orders = try await context.fetch(Order.self)
    ///     .partition(#field(\Order.tenantID), equals: "tenant_123")
    ///     .where(#field(\Order.status) == "open")
    ///     .execute()
    /// ```
    ///
    /// - Parameters:
    ///   - field: The compiled partition field
    ///   - value: The value for directory resolution
    /// - Returns: A new Query with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V
    ) -> Query<T> {
        var copy = self
        var binding = copy.partitionBinding ?? DirectoryPath<T>()
        binding.set(field, to: value)
        copy.partitionBinding = binding

        // Also add as a where clause for filtering (defense in depth)
        // This ensures data integrity even if wrong partition is somehow accessed
        // Only add if not already present for this keyPath
        let alreadyHasPredicate = copy.predicates.contains { predicate in
            if case .comparison(let comparison) = predicate {
                return comparison.field == field.identity
            }
            return false
        }
        if !alreadyHasPredicate {
            copy.predicates.append(
                .comparison(
                    FieldComparison(
                        field: field,
                        op: .equal,
                        value: value
                    )
                )
            )
        }
        return copy
    }
}

// MARK: - IndexHint

/// Restricts index selection during a fetch to a specific named index.
///
/// Set on `Query<T>.forcedIndex` when a higher layer (typically the canonical
/// `SelectQuery` planner) has already chosen the index to use. The fetch path
/// honors the hint strictly: if the named index does not exist or cannot serve
/// the pushed-down predicate, it raises an error rather than silently falling
/// back to a full table scan.
public struct IndexHint: Sendable, Equatable, Hashable {
    /// The name of the index registered on the target `Persistable` type.
    public let indexName: String

    public init(indexName: String) {
        self.indexName = indexName
    }
}

// MARK: - Model Predicate Evaluation

extension FieldComparison {
    // MARK: - Evaluation

    /// Evaluate this comparison through the generated model adapter.
    public func evaluate(
        on model: borrowing T
    ) throws(QueryEvaluationError) -> Bool {
        let encoded: FieldValue?
        do {
            encoded = try model.persistedFieldValue(for: field)
        } catch let error {
            throw .fieldEncoding(error)
        }
        guard let modelFieldValue = encoded else {
            throw .missingField(
                entity: T.persistableType,
                field: field
            )
        }
        return evaluate(modelFieldValue)
    }

    private func evaluate(_ modelFieldValue: FieldValue) -> Bool {
        switch op {
        case .isNil:
            return modelFieldValue.isNull
        case .isNotNil:
            return !modelFieldValue.isNull
        default:
            break
        }

        if modelFieldValue.isNull { return false }

        switch op {
        case .equal:
            return modelFieldValue.isEqual(to: value)
        case .notEqual:
            return !modelFieldValue.isEqual(to: value)
        case .lessThan:
            return modelFieldValue.isLessThan(value)
        case .lessThanOrEqual:
            return modelFieldValue.isLessThan(value) || modelFieldValue.isEqual(to: value)
        case .greaterThan:
            return value.isLessThan(modelFieldValue)
        case .greaterThanOrEqual:
            return value.isLessThan(modelFieldValue) || modelFieldValue.isEqual(to: value)
        case .contains:
            if let str = modelFieldValue.stringValue,
               let substr = value.stringValue {
                return DatabaseStringSearch.contains(substr, in: str)
            }
            return false
        case .hasPrefix:
            if let str = modelFieldValue.stringValue,
               let prefix = value.stringValue {
                return str.hasPrefix(prefix)
            }
            return false
        case .hasSuffix:
            if let str = modelFieldValue.stringValue,
               let suffix = value.stringValue {
                return str.hasSuffix(suffix)
            }
            return false
        case .in, .notIn:
            if let arrayValues = value.arrayValue {
                let contains = arrayValues.contains {
                    modelFieldValue.isEqual(to: $0)
                }
                return op == .in ? contains : !contains
            }
            return false
        case .isNil, .isNotNil:
            return false
        }
    }

}

// MARK: - SortDescriptor

/// Describes how to sort query results
public struct SortDescriptor<T: Persistable>: Sendable {
    /// The exact compiled schema field used for ordering.
    public let field: FieldIdentity

    /// The canonical field name used by planners and
    public var fieldName: String {
        field.name
    }

    /// Sort order
    public let order: SortOrder

    /// Create a sort descriptor from a compile-time typed field.
    public init<V: Comparable & Sendable>(
        field: Field<T, V>,
        order: SortOrder = .ascending
    ) {
        self.field = field.identity
        self.order = order
    }

    /// Create a sort descriptor from an identity validated by QueryIR binding.
    package init(field: FieldIdentity, order: SortOrder) {
        self.field = field
        self.order = order
    }

    // MARK: - Comparison

    /// Compare two models with sort direction applied.
    public func orderedComparison(
        _ lhs: borrowing T,
        _ rhs: borrowing T
    ) throws(QueryEvaluationError) -> QueryComparison {
        let left = try fieldValue(from: lhs)
        let right = try fieldValue(from: rhs)
        let rawResult: QueryComparison
        do {
            rawResult = try FieldValueComparator.compare(left, right)
        } catch {
            throw .incomparableValues(
                entity: T.persistableType,
                field: field,
                left: left,
                right: right
            )
        }
        switch order {
        case .ascending:
            return rawResult
        case .descending:
            switch rawResult {
            case .lessThan: return .greaterThan
            case .greaterThan: return .lessThan
            case .equal: return .equal
            }
        }
    }

    private func fieldValue(
        from model: borrowing T
    ) throws(QueryEvaluationError) -> FieldValue {
        let encoded: FieldValue?
        do {
            encoded = try model.persistedFieldValue(for: field)
        } catch let error {
            throw .fieldEncoding(error)
        }
        guard let value = encoded else {
            throw .missingField(
                entity: T.persistableType,
                field: field
            )
        }
        return value
    }
}

/// Sort order
public enum SortOrder: String, Sendable, Hashable {
    case ascending
    case descending
}

// MARK: - QueryExecutor

/// Executor for fluent query API
///
/// **Usage**:
/// ```swift
/// let users = try await context.fetch(User.self)
///     .where(#field(\User.isActive) == true)
///     .where(#field(\User.age) > 18)
///     .orderBy(#field(\User.name))
///     .limit(10)
///     .execute()
/// ```
public struct QueryExecutor<T: Persistable>: Sendable {
    package let context: DatabaseContext
    package var query: Query<T>

    /// Initialize with context and query
    public init(context: DatabaseContext, query: Query<T>) {
        self.context = context
        self.query = query
    }

    /// Add a filter predicate
    public func `where`(_ predicate: Predicate<T>) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(field)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>,
        _ order: SortOrder
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(field, order)
        return copy
    }

    /// Set maximum number of results
    public func limit(_ count: Int) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    /// Set number of results to skip
    public func offset(_ count: Int) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.offset(count)
        return copy
    }

    /// Set cache policy for this query
    ///
    /// Controls whether the query uses cached read versions for performance optimization.
    ///
    /// - Parameter policy: The cache policy to use
    /// - Returns: A new QueryExecutor with the cache policy set
    ///
    /// **Usage**:
    /// ```swift
    /// // Fetch latest data (strict consistency)
    /// let users = try await context.fetch(User.self)
    ///     .cachePolicy(.server)
    ///     .execute()
    ///
    /// // Allow 30-second stale data
    /// let products = try await context.fetch(Product.self)
    ///     .cachePolicy(.stale(.seconds(30)))
    ///     .execute()
    /// ```
    public func cachePolicy(_ policy: CachePolicy) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.cachePolicy(policy)
        return copy
    }

    // MARK: - Partition

    /// Bind a partition field value for dynamic directory resolution
    ///
    /// Required for types with `Field(\.keyPath)` in their `#Directory` declaration.
    /// The partition value is used to resolve the correct directory subspace.
    ///
    /// **Usage**:
    /// ```swift
    /// @Persistable
    /// struct Order {
    ///     #Directory<Order>("tenants", Field(\.tenantID), "orders")
    ///     var tenantID: String
    /// }
    ///
    /// let orders = try await context.fetch(Order.self)
    ///     .partition(#field(\Order.tenantID), equals: "tenant_123")
    ///     .where(#field(\Order.status) == "open")
    ///     .execute()
    /// ```
    ///
    /// - Parameters:
    ///   - field: The compiled partition field
    ///   - value: The value for directory resolution
    /// - Returns: A new QueryExecutor with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.partition(field, equals: value)
        return copy
    }

    // MARK: - Execute

    /// Execute the query and return results
    ///
    /// Security: LIST operation is evaluated by DataStore internally.
    public func execute() async throws -> [T] {
        return try await context.fetch(query)
    }

    /// Execute the query and return count
    ///
    /// Security: LIST operation is evaluated by DataStore internally.
    public func count() async throws -> Int {
        return try await context.fetchCount(query)
    }

    /// Execute the query and return first result
    public func first() async throws -> T? {
        try await limit(1).execute().first
    }

    /// Resolve the physical storage path that execution would currently use.
    ///
    /// The operation is asynchronous because index lifecycle state is part of
    /// the decision. The returned plan does not claim unmeasured cost or row
    /// estimates.
    public func executionPlan() async throws -> QueryAccessPlan {
        try await context.executionPlan(for: query)
    }
}
