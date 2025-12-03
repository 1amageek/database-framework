import Core

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
public struct FieldComparison<T: Persistable>: @unchecked Sendable {
    /// The field's KeyPath (type-erased)
    public let keyPath: AnyKeyPath

    /// The comparison operator
    public let op: ComparisonOperator

    /// The value to compare against (type-erased)
    public let value: any Sendable

    /// Create a field comparison
    public init<V: Sendable>(keyPath: KeyPath<T, V>, op: ComparisonOperator, value: V) {
        self.keyPath = keyPath
        self.op = op
        self.value = value
    }

    /// Create a nil comparison
    ///
    /// For `isNil`/`isNotNil` checks, the value field stores `NullValue.shared`
    /// as a sentinel indicating null check operation.
    public init<V: Sendable>(keyPath: KeyPath<T, V?>, op: ComparisonOperator) {
        self.keyPath = keyPath
        self.op = op
        // Store NullValue sentinel for nil checks
        self.value = NullValue.instance
    }

    /// Create an IN comparison
    public init<V: Sendable>(keyPath: KeyPath<T, V>, values: [V]) {
        self.keyPath = keyPath
        self.op = .in
        self.value = values
    }

    /// Get the field name using Persistable's fieldName method
    public var fieldName: String {
        T.fieldName(for: keyPath)
    }
}

// MARK: - ComparisonOperator

/// Comparison operators for predicates
public enum ComparisonOperator: String, Sendable {
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

// MARK: - KeyPath Operators

/// Equal comparison
public func == <T: Persistable, V: Equatable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .equal, value: rhs))
}

/// Not equal comparison
public func != <T: Persistable, V: Equatable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .notEqual, value: rhs))
}

/// Less than comparison
public func < <T: Persistable, V: Comparable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .lessThan, value: rhs))
}

/// Less than or equal comparison
public func <= <T: Persistable, V: Comparable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .lessThanOrEqual, value: rhs))
}

/// Greater than comparison
public func > <T: Persistable, V: Comparable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .greaterThan, value: rhs))
}

/// Greater than or equal comparison
public func >= <T: Persistable, V: Comparable & Sendable>(
    lhs: KeyPath<T, V>,
    rhs: V
) -> Predicate<T> {
    .comparison(FieldComparison(keyPath: lhs, op: .greaterThanOrEqual, value: rhs))
}

// MARK: - Optional KeyPath Operators

/// Check if optional field is nil
public func == <T: Persistable, V: Sendable>(
    lhs: KeyPath<T, V?>,
    rhs: V?.Type
) -> Predicate<T> where V? == Optional<V> {
    .comparison(FieldComparison(keyPath: lhs, op: .isNil))
}

/// Check if optional field is not nil
public func != <T: Persistable, V: Sendable>(
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

// MARK: - IN Predicate Extension

extension KeyPath where Root: Persistable, Value: Equatable & Sendable {
    /// Check if value is in array
    public func `in`(_ values: [Value]) -> Predicate<Root> {
        .comparison(FieldComparison(keyPath: self, values: values))
    }
}

// MARK: - QueryExecutor

/// Executor for fluent query API
///
/// **Usage**:
/// ```swift
/// let users = try await context.fetch(User.self)
///     .where(\.isActive == true)
///     .where(\.age > 18)
///     .orderBy(\.name)
///     .limit(10)
///     .execute()
/// ```
public struct QueryExecutor<T: Persistable>: Sendable {
    private let context: FDBContext
    private var query: Query<T>

    /// Initialize with context and query
    public init(context: FDBContext, query: Query<T>) {
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

    /// Execute the query and return results
    public func execute() async throws -> [T] {
        try await context.fetch(query)
    }

    /// Execute the query and return count
    public func count() async throws -> Int {
        try await context.fetchCount(query)
    }

    /// Execute the query and return first result
    public func first() async throws -> T? {
        try await limit(1).execute().first
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
