import Core
import Relationship

// MARK: - Query

/// Type-safe query builder for fetching Persistable models
///
/// **Usage**:
/// ```swift
/// // Fluent API
/// let users = try await context.fetch(User.self)
///     .where(\.isActive == true)
///     .where(\.age > 18)
///     .orderBy(\.name)
///     .limit(10)
///     .execute()
///
/// // Simple fetch all
/// let allUsers = try await context.fetch(User.self).execute()
///
/// // First result
/// let user = try await context.fetch(User.self)
///     .where(\.email == "alice@example.com")
///     .first()
///
/// // Count
/// let count = try await context.fetch(User.self)
///     .where(\.isActive == true)
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

    /// Initialize an empty query
    public init() {
        self.predicates = []
        self.sortDescriptors = []
        self.fetchLimit = nil
        self.fetchOffset = nil
    }

    // MARK: - Fluent API

    /// Add a filter predicate
    public func `where`(_ predicate: Predicate<T>) -> Query<T> {
        var copy = self
        copy.predicates.append(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>) -> Query<T> {
        var copy = self
        copy.sortDescriptors.append(SortDescriptor(keyPath: keyPath, order: .ascending))
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, _ order: SortOrder) -> Query<T> {
        var copy = self
        copy.sortDescriptors.append(SortDescriptor(keyPath: keyPath, order: order))
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
}

// MARK: - Predicate

/// Type-safe predicate for filtering models
///
/// Use operator overloads on KeyPaths to create predicates:
/// ```swift
/// \.email == "alice@example.com"
/// \.age > 18
/// \.name != nil
/// \.status.in(["active", "pending"])
/// ```
public indirect enum Predicate<T: Persistable>: Sendable {
    /// Field comparison with a value
    case comparison(FieldComparison<T>)

    /// All predicates must match (AND)
    case and([Predicate<T>])

    /// Any predicate must match (OR)
    case or([Predicate<T>])

    /// Negate the predicate (NOT)
    case not(Predicate<T>)

    /// Always true
    case `true`

    /// Always false
    case `false`

    // MARK: - Logical Operators

    /// Combine predicates with AND
    public static func && (lhs: Predicate<T>, rhs: Predicate<T>) -> Predicate<T> {
        switch (lhs, rhs) {
        case (.and(let left), .and(let right)):
            return .and(left + right)
        case (.and(let left), _):
            return .and(left + [rhs])
        case (_, .and(let right)):
            return .and([lhs] + right)
        default:
            return .and([lhs, rhs])
        }
    }

    /// Combine predicates with OR
    public static func || (lhs: Predicate<T>, rhs: Predicate<T>) -> Predicate<T> {
        switch (lhs, rhs) {
        case (.or(let left), .or(let right)):
            return .or(left + right)
        case (.or(let left), _):
            return .or(left + [rhs])
        case (_, .or(let right)):
            return .or([lhs] + right)
        default:
            return .or([lhs, rhs])
        }
    }

    /// Negate a predicate
    public static prefix func ! (predicate: Predicate<T>) -> Predicate<T> {
        .not(predicate)
    }
}

// MARK: - FieldComparison

/// Represents a comparison of a field value
///
/// Uses `FieldValue` from Core for type-safe value representation that maps directly
/// to FoundationDB's TupleElement types.
public struct FieldComparison<T: Persistable>: @unchecked Sendable, Hashable {
    /// The field's KeyPath (type-erased)
    public let keyPath: AnyKeyPath

    /// The comparison operator
    public let op: ComparisonOperator

    /// The value to compare against (type-safe)
    public let value: FieldValue

    /// Create a field comparison with FieldValue
    public init(keyPath: AnyKeyPath, op: ComparisonOperator, value: FieldValue) {
        self.keyPath = keyPath
        self.op = op
        self.value = value
    }

    /// Create a field comparison from FieldValueConvertible
    public init<V: FieldValueConvertible>(keyPath: KeyPath<T, V>, op: ComparisonOperator, value: V) {
        self.keyPath = keyPath
        self.op = op
        self.value = value.toFieldValue()
    }

    /// Create a nil comparison (isNil/isNotNil)
    public init<V>(keyPath: KeyPath<T, V?>, op: ComparisonOperator) {
        self.keyPath = keyPath
        self.op = op
        self.value = .null
    }

    /// Create an IN comparison
    public init<V: FieldValueConvertible>(keyPath: KeyPath<T, V>, values: [V]) {
        self.keyPath = keyPath
        self.op = .in
        self.value = .array(values.map { $0.toFieldValue() })
    }

    /// Get the field name using Persistable's fieldName method
    public var fieldName: String {
        T.fieldName(for: keyPath)
    }

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        // Use keyPath directly (AnyKeyPath is Hashable)
        // This ensures different properties produce different hashes
        hasher.combine(keyPath)
        hasher.combine(op)
        hasher.combine(value)
    }

    public static func == (lhs: FieldComparison<T>, rhs: FieldComparison<T>) -> Bool {
        lhs.keyPath == rhs.keyPath && lhs.op == rhs.op && lhs.value == rhs.value
    }
}

// MARK: - ComparisonOperator

/// Comparison operators for predicates
public enum ComparisonOperator: String, Sendable, Hashable {
    case equal = "=="
    case notEqual = "!="
    case lessThan = "<"
    case lessThanOrEqual = "<="
    case greaterThan = ">"
    case greaterThanOrEqual = ">="
    case contains = "contains"
    case hasPrefix = "hasPrefix"
    case hasSuffix = "hasSuffix"
    case `in` = "in"
    case isNil = "isNil"
    case isNotNil = "isNotNil"
}


// MARK: - SortDescriptor

/// Describes how to sort query results
public struct SortDescriptor<T: Persistable>: @unchecked Sendable {
    /// The field's KeyPath (type-erased)
    public let keyPath: AnyKeyPath

    /// Sort order
    public let order: SortOrder

    /// Create a sort descriptor
    public init<V: Comparable & Sendable>(keyPath: KeyPath<T, V>, order: SortOrder = .ascending) {
        self.keyPath = keyPath
        self.order = order
    }

    /// Get the field name using Persistable's fieldName method
    public var fieldName: String {
        T.fieldName(for: keyPath)
    }
}

/// Sort order
public enum SortOrder: String, Sendable {
    case ascending
    case descending
}

// MARK: - KeyPath Operators (FieldValueConvertible)

/// Equal comparison
public func == <T: Persistable, V: Equatable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .equal, value: rhs))
}

/// Not equal comparison
public func != <T: Persistable, V: Equatable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .notEqual, value: rhs))
}

/// Less than comparison
public func < <T: Persistable, V: Comparable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .lessThan, value: rhs))
}

/// Less than or equal comparison
public func <= <T: Persistable, V: Comparable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .lessThanOrEqual, value: rhs))
}

/// Greater than comparison
public func > <T: Persistable, V: Comparable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .greaterThan, value: rhs))
}

/// Greater than or equal comparison
public func >= <T: Persistable, V: Comparable & FieldValueConvertible>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .greaterThanOrEqual, value: rhs))
}

// MARK: - Optional KeyPath Operators

/// Check if optional field is nil
public func == <T: Persistable, V>(
    lhs: KeyPath<T, V?>,
    rhs: V?.Type
) -> Predicate<T> where V? == Optional<V> {
    .comparison(FieldComparison(keyPath: lhs, op: .isNil))
}

/// Check if optional field is not nil
public func != <T: Persistable, V>(
    lhs: KeyPath<T, V?>,
    rhs: V?.Type
) -> Predicate<T> where V? == Optional<V> {
    .comparison(FieldComparison(keyPath: lhs, op: .isNotNil))
}

// MARK: - String Predicate Extensions

extension KeyPath where Root: Persistable, Value == String {
    /// Check if string contains substring
    public func contains(_ substring: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self, op: .contains, value: substring))
    }

    /// Check if string starts with prefix
    public func hasPrefix(_ prefix: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self, op: .hasPrefix, value: prefix))
    }

    /// Check if string ends with suffix
    public func hasSuffix(_ suffix: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self, op: .hasSuffix, value: suffix))
    }
}

// MARK: - Optional String Predicate Extensions

extension KeyPath where Root: Persistable, Value == String? {
    /// Check if optional string contains substring
    public func contains(_ substring: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self as AnyKeyPath, op: .contains, value: .string(substring)))
    }

    /// Check if optional string starts with prefix
    public func hasPrefix(_ prefix: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self as AnyKeyPath, op: .hasPrefix, value: .string(prefix)))
    }

    /// Check if optional string ends with suffix
    public func hasSuffix(_ suffix: String) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self as AnyKeyPath, op: .hasSuffix, value: .string(suffix)))
    }
}

// MARK: - IN Predicate Extension

extension KeyPath where Root: Persistable, Value: Equatable & FieldValueConvertible {
    /// Check if value is in array
    public func `in`(_ values: [Value]) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self, values: values))
    }
}

// MARK: - QueryExecutor

/// Executor for fluent query API with Snapshot support
///
/// All queries return `Snapshot<T>` for uniform API. Use `joining()` to load relationships.
///
/// **Usage**:
/// ```swift
/// // Basic query - returns Snapshot<User>[]
/// let users = try await context.fetch(User.self)
///     .where(\.isActive == true)
///     .where(\.age > 18)
///     .orderBy(\.name)
///     .limit(10)
///     .execute()
///
/// // With relationship loading
/// let orders = try await context.fetch(Order.self)
///     .joining(\.customerID)  // Load customer for each order
///     .execute()
///
/// for order in orders {
///     // Access via ref() since Snapshot extensions can't be macro-generated
///     let customer = order.ref(Customer.self, \.customerID)
///     print(customer?.name)
/// }
/// ```
public struct QueryExecutor<T: Persistable>: Sendable {
    private let context: FDBContext
    private var query: Query<T>

    /// KeyPaths of FK fields to join (load related items)
    ///
    /// Note: `nonisolated(unsafe)` is used because AnyKeyPath is not Sendable,
    /// but this is safe since the array is only modified during query building
    /// (before execution) and each call returns a new QueryExecutor copy.
    private nonisolated(unsafe) var joiningKeyPaths: [AnyKeyPath]

    /// Initialize with context and query
    public init(context: FDBContext, query: Query<T>) {
        self.context = context
        self.query = query
        self.joiningKeyPaths = []
    }

    /// Add a filter predicate
    public func `where`(_ predicate: Predicate<T>) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(keyPath)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, _ order: SortOrder) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(keyPath, order)
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

    // MARK: - Joining (Relationship Loading)

    /// Join a to-one relationship (optional FK field)
    ///
    /// When executed, this will batch-load the related items for all results.
    ///
    /// **Usage**:
    /// ```swift
    /// let orders = try await context.fetch(Order.self)
    ///     .joining(\.customerID)
    ///     .execute()
    ///
    /// for order in orders {
    ///     let customer = order.ref(Customer.self, \.customerID)
    /// }
    /// ```
    ///
    /// - Parameter keyPath: KeyPath to the optional FK field
    /// - Returns: QueryExecutor with the join added
    public func joining(_ keyPath: KeyPath<T, String?>) -> QueryExecutor<T> {
        var copy = self
        copy.joiningKeyPaths.append(keyPath)
        return copy
    }

    /// Join a to-one relationship (required FK field)
    ///
    /// - Parameter keyPath: KeyPath to the required FK field
    /// - Returns: QueryExecutor with the join added
    public func joining(_ keyPath: KeyPath<T, String>) -> QueryExecutor<T> {
        var copy = self
        copy.joiningKeyPaths.append(keyPath)
        return copy
    }

    /// Join a to-many relationship (FK array field)
    ///
    /// When executed, this will batch-load all related items.
    ///
    /// **Usage**:
    /// ```swift
    /// let customers = try await context.fetch(Customer.self)
    ///     .joining(\.orderIDs)
    ///     .execute()
    ///
    /// for customer in customers {
    ///     let orders = customer.refs(Order.self, \.orderIDs)
    /// }
    /// ```
    ///
    /// - Parameter keyPath: KeyPath to the FK array field
    /// - Returns: QueryExecutor with the join added
    public func joining(_ keyPath: KeyPath<T, [String]>) -> QueryExecutor<T> {
        var copy = self
        copy.joiningKeyPaths.append(keyPath)
        return copy
    }

    // MARK: - Execute

    /// Execute the query and return Snapshot results
    ///
    /// Returns `[Snapshot<T>]` which wraps items and optionally loaded relationships.
    /// Use `ref()` or `refs()` methods on Snapshot to access loaded relationships.
    public func execute() async throws -> [Snapshot<T>] {
        let items = try await context.fetch(query)

        // If no joins, return simple snapshots
        if joiningKeyPaths.isEmpty {
            return items.map { Snapshot(item: $0) }
        }

        // Build snapshots with loaded relationships
        return try await buildSnapshots(items: items)
    }

    /// Execute the query and return raw items (no Snapshot wrapper)
    ///
    /// Use this for backward compatibility or when you don't need relationship loading.
    public func executeRaw() async throws -> [T] {
        try await context.fetch(query)
    }

    /// Execute the query and return count
    public func count() async throws -> Int {
        try await context.fetchCount(query)
    }

    /// Execute the query and return first Snapshot result
    public func first() async throws -> Snapshot<T>? {
        try await limit(1).execute().first
    }

    /// Execute the query and return first raw item
    public func firstRaw() async throws -> T? {
        try await limit(1).executeRaw().first
    }

    // MARK: - Private Helpers

    /// Build Snapshots with loaded relationships
    private func buildSnapshots(items: [T]) async throws -> [Snapshot<T>] {
        // Find relationship descriptors for the joining keyPaths
        let descriptors = T.relationshipDescriptors
        var relationshipsToLoad: [(keyPath: AnyKeyPath, descriptor: RelationshipDescriptor)] = []

        for keyPath in joiningKeyPaths {
            let fieldName = T.fieldName(for: keyPath)
            if let descriptor = descriptors.first(where: { $0.propertyName == fieldName }) {
                relationshipsToLoad.append((keyPath, descriptor))
            }
        }

        // Collect all FK values to batch load
        var fkValuesToLoad: [String: Set<String>] = [:]  // relatedTypeName -> Set of IDs

        for item in items {
            for (keyPath, descriptor) in relationshipsToLoad {
                if descriptor.isToMany {
                    // To-many: collect all IDs from the array
                    if let typedKeyPath = keyPath as? KeyPath<T, [String]> {
                        let ids = item[keyPath: typedKeyPath]
                        var idSet = fkValuesToLoad[descriptor.relatedTypeName] ?? []
                        ids.forEach { idSet.insert($0) }
                        fkValuesToLoad[descriptor.relatedTypeName] = idSet
                    }
                } else {
                    // To-one: collect the single ID if present
                    if let typedKeyPath = keyPath as? KeyPath<T, String?> {
                        if let id = item[keyPath: typedKeyPath] {
                            var idSet = fkValuesToLoad[descriptor.relatedTypeName] ?? []
                            idSet.insert(id)
                            fkValuesToLoad[descriptor.relatedTypeName] = idSet
                        }
                    } else if let typedKeyPath = keyPath as? KeyPath<T, String> {
                        let id = item[keyPath: typedKeyPath]
                        var idSet = fkValuesToLoad[descriptor.relatedTypeName] ?? []
                        idSet.insert(id)
                        fkValuesToLoad[descriptor.relatedTypeName] = idSet
                    }
                }
            }
        }

        // Batch load related items by type
        // Note: This is a simplified implementation. A full implementation would
        // use the Schema to resolve types and batch load efficiently.
        // For now, we load items via context for each unique ID.
        var loadedItems: [String: [String: any Persistable]] = [:]  // typeName -> (id -> item)

        for (typeName, ids) in fkValuesToLoad {
            var itemsById: [String: any Persistable] = [:]
            for id in ids {
                // Load item using the relationship descriptor's type name
                // This requires knowing the type at runtime - simplified for now
                if let item = try await context.loadItemByTypeName(typeName, id: id) {
                    itemsById[id] = item
                }
            }
            loadedItems[typeName] = itemsById
        }

        // Build Snapshots with loaded relations
        var snapshots: [Snapshot<T>] = []

        for item in items {
            var relations: [AnyKeyPath: any Sendable] = [:]

            for (keyPath, descriptor) in relationshipsToLoad {
                guard let itemsById = loadedItems[descriptor.relatedTypeName] else {
                    continue
                }

                if descriptor.isToMany {
                    // To-many: load array of related items
                    if let typedKeyPath = keyPath as? KeyPath<T, [String]> {
                        let ids = item[keyPath: typedKeyPath]
                        let relatedItems = ids.compactMap { itemsById[$0] }
                        relations[keyPath] = relatedItems
                    }
                } else {
                    // To-one: load single related item
                    if let typedKeyPath = keyPath as? KeyPath<T, String?> {
                        if let id = item[keyPath: typedKeyPath], let related = itemsById[id] {
                            relations[keyPath] = related
                        }
                    } else if let typedKeyPath = keyPath as? KeyPath<T, String> {
                        let id = item[keyPath: typedKeyPath]
                        if let related = itemsById[id] {
                            relations[keyPath] = related
                        }
                    }
                }
            }

            snapshots.append(Snapshot(item: item, relations: relations))
        }

        return snapshots
    }

    // MARK: - Query Planner Integration

    /// Explain the query plan without executing
    ///
    /// Returns a human-readable explanation of how the query would be executed,
    /// including estimated costs, index usage, and execution tree.
    ///
    /// **Usage**:
    /// ```swift
    /// let explanation = try context.fetch(User.self)
    ///     .where(\.age > 18)
    ///     .orderBy(\.name)
    ///     .explain()
    /// print(explanation)
    /// ```
    ///
    /// - Parameter hints: Optional query hints to influence planning
    /// - Returns: Human-readable plan explanation
    public func explain(hints: QueryHints = .default) throws -> PlanExplanation {
        let planner = QueryPlanner<T>(
            indexes: T.indexDescriptors,
            statistics: DefaultStatisticsProvider(),
            costModel: .default
        )
        let plan: QueryPlan<T> = try planner.plan(query: query, hints: hints)
        return PlanExplanation(plan: plan)
    }

    /// Explain the query plan with custom statistics
    ///
    /// Use this method when you have collected statistics for more accurate cost estimation.
    ///
    /// - Parameters:
    ///   - statistics: Statistics provider with collected data
    ///   - hints: Optional query hints to influence planning
    /// - Returns: Human-readable plan explanation
    public func explain(
        with statistics: StatisticsProvider,
        hints: QueryHints = .default
    ) throws -> PlanExplanation {
        let planner = QueryPlanner<T>(
            indexes: T.indexDescriptors,
            statistics: statistics,
            costModel: .default
        )
        let plan: QueryPlan<T> = try planner.plan(query: query, hints: hints)
        return PlanExplanation(plan: plan)
    }

    /// Get the raw query plan for advanced usage
    ///
    /// - Parameter hints: Optional query hints to influence planning
    /// - Returns: The compiled query plan
    public func plan(hints: QueryHints = .default) throws -> QueryPlan<T> {
        let planner = QueryPlanner<T>(
            indexes: T.indexDescriptors,
            statistics: DefaultStatisticsProvider(),
            costModel: .default
        )
        return try planner.plan(query: query, hints: hints)
    }
}
