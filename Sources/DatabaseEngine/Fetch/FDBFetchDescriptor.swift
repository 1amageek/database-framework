#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue

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
        _ keyPath: KeyPath<T, V> & Sendable
    ) -> Query<T> {
        var copy = self
        copy.sortDescriptors.append(SortDescriptor(keyPath: keyPath, order: .ascending))
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ keyPath: KeyPath<T, V> & Sendable,
        _ order: SortOrder
    ) -> Query<T> {
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
    ///     .cachePolicy(.stale(30))
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
    ///     .partition(\.tenantID, equals: "tenant_123")
    ///     .where(\.status == "open")
    ///     .execute()
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: The partition field's keyPath
    ///   - value: The value for directory resolution
    /// - Returns: A new Query with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V> & Sendable,
        equals value: V
    ) -> Query<T> {
        var copy = self
        var binding = copy.partitionBinding ?? DirectoryPath<T>()
        binding.set(keyPath, to: value)
        copy.partitionBinding = binding

        // Also add as a where clause for filtering (defense in depth)
        // This ensures data integrity even if wrong partition is somehow accessed
        // Only add if not already present for this keyPath
        let fieldName = T.fieldName(for: keyPath)
        let alreadyHasPredicate = copy.predicates.contains { predicate in
            if case .comparison(let comparison) = predicate {
                return comparison.fieldName == fieldName
            }
            return false
        }
        if !alreadyHasPredicate {
            copy.predicates.append(.comparison(FieldComparison(
                keyPath: keyPath, op: .equal, value: value,
                evaluate: { $0[keyPath: keyPath] == value }
            )))
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

// MARK: - Predicate

/// Type-safe predicate for filtering models
///
/// Use operator overloads on KeyPaths to create predicates:
/// ```swift
/// \.email == "alice@example.com"
/// \.age > 18
/// \.name != nil
/// Predicate<T>.matchesAny(of: ["active", "pending"], at: \.status)
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
///
/// **Zero-copy evaluation**: When constructed with typed KeyPaths (via operator overloads),
/// captures a `@Sendable` closure that evaluates the comparison without any allocation.
/// Falls back to `FieldReader` when the closure is unavailable (e.g., after type erasure
/// in QueryRewriter/DNFConverter).
public struct FieldComparison<T: Persistable>: Sendable, Hashable {
    /// The canonical field name used by planning and type-erased evaluation.
    public let fieldName: String

    /// The comparison operator
    public let op: ComparisonOperator

    /// The value to compare against (type-safe)
    public let value: FieldValue

    /// Zero-copy evaluation closure, captured at construction time
    /// when typed KeyPath and value are available.
    /// Excluded from Hashable identity (field name + operator + value suffice).
    private let _evaluate: (@Sendable (T) -> Bool)?

    /// Typed field access retained across predicate rewrites.
    private let _readValue: (@Sendable (T) -> FieldValue)?

    /// Create a field comparison with a canonical field name.
    ///
    /// Used by QueryRewriter/DNFConverter after type erasure.
    /// Optionally accepts an evaluate closure for zero-copy evaluation.
    public init(
        fieldName: String,
        op: ComparisonOperator,
        value: FieldValue,
        evaluate: (@Sendable (T) -> Bool)? = nil
    ) {
        self.fieldName = fieldName
        self.op = op
        self.value = value
        self._evaluate = evaluate
        self._readValue = nil
    }

    /// Create a field comparison from FieldValueConvertible
    ///
    /// Optionally accepts an evaluate closure for zero-copy evaluation.
    /// The operator overloads pass a typed closure; direct callers may omit it.
    public init<V: FieldValueConvertible>(
        keyPath: KeyPath<T, V> & Sendable,
        op: ComparisonOperator,
        value: V,
        evaluate: (@Sendable (T) -> Bool)? = nil
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.op = op
        self.value = value.toFieldValue()
        self._evaluate = evaluate
        self._readValue = { model in
            model[keyPath: keyPath].toFieldValue()
        }
    }

    /// Create a comparison for a non-optional field and a canonical comparison value.
    public init<V: Sendable>(
        keyPath: KeyPath<T, V> & Sendable,
        op: ComparisonOperator,
        value: FieldValue,
        evaluate: (@Sendable (T) -> Bool)? = nil
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.op = op
        self.value = value
        self._evaluate = evaluate
        self._readValue = { model in
            FieldValue(model[keyPath: keyPath]) ?? .null
        }
    }

    /// Create a comparison for an optional field and a canonical comparison value.
    public init<V: Sendable>(
        keyPath: KeyPath<T, V?> & Sendable,
        op: ComparisonOperator,
        value: FieldValue,
        evaluate: (@Sendable (T) -> Bool)? = nil
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.op = op
        self.value = value
        self._evaluate = evaluate
        self._readValue = { model in
            guard let fieldValue = model[keyPath: keyPath] else {
                return .null
            }
            return FieldValue(fieldValue) ?? .null
        }
    }

    /// Create a nil comparison (isNil/isNotNil)
    ///
    /// Captures a zero-copy closure for nil checks.
    public init<V: Sendable>(
        keyPath: KeyPath<T, V?> & Sendable,
        op: ComparisonOperator
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.op = op
        self.value = .null
        self._readValue = { model in
            model[keyPath: keyPath] == nil ? .null : .bool(true)
        }
        switch op {
        case .isNil:
            self._evaluate = { model in model[keyPath: keyPath] == nil }
        case .isNotNil:
            self._evaluate = { model in model[keyPath: keyPath] != nil }
        default:
            self._evaluate = nil
        }
    }

    /// Create an IN comparison
    ///
    /// Optionally accepts an evaluate closure for zero-copy evaluation.
    public init<V: FieldValueConvertible>(
        keyPath: KeyPath<T, V> & Sendable,
        values: [V],
        evaluate: (@Sendable (T) -> Bool)? = nil
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.op = .in
        self.value = .array(values.map { $0.toFieldValue() })
        self._evaluate = evaluate
        self._readValue = { model in
            model[keyPath: keyPath].toFieldValue()
        }
    }

    /// Rebuild a comparison while retaining its typed field accessor.
    func replacing(
        op: ComparisonOperator,
        value: FieldValue? = nil
    ) -> FieldComparison<T> {
        FieldComparison(
            fieldName: fieldName,
            op: op,
            value: value ?? self.value,
            readValue: _readValue
        )
    }

    private init(
        fieldName: String,
        op: ComparisonOperator,
        value: FieldValue,
        readValue: (@Sendable (T) -> FieldValue)?
    ) {
        self.fieldName = fieldName
        self.op = op
        self.value = value
        self._evaluate = nil
        self._readValue = readValue
    }

    // MARK: - Evaluation

    /// Evaluate this comparison against a model
    ///
    /// Fast path: uses the captured typed closure (zero allocation per evaluation).
    /// Slow path: falls back to FieldReader + FieldValue comparison.
    public func evaluate(on model: T) -> Bool {
        if let eval = _evaluate {
            return eval(model)
        }
        return evaluateViaFieldReader(model)
    }

    /// Fallback evaluation using FieldReader (non-throwing field access)
    ///
    /// Handles all comparison operators including string operations.
    /// Used when typed closure is unavailable (type-erased paths from QueryRewriter/DNFConverter).
    private func evaluateViaFieldReader(_ model: T) -> Bool {
        let fieldValue = _readValue?(model)
            ?? FieldReader.readFieldValue(from: model, fieldName: fieldName)
        return evaluateFieldValue(fieldValue)
    }

    /// Core comparison logic for raw values
    ///
    /// Converts raw value to FieldValue and evaluates comparison operator.
    /// Handles all comparison operators including string operations.
    private func evaluateFieldValue(_ modelFieldValue: FieldValue) -> Bool {
        // Handle nil check operators first
        // NOTE: Cannot use `raw == nil` because PartialKeyPath path returns
        // Optional<V>.none boxed in Any (.some(Any(Optional.none))), not nil.
        // Must use FieldValue conversion which correctly maps boxed nil to .null.
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
                return DatabaseText.contains(substr, in: str)
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

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        // Evaluation closures are excluded from semantic identity.
        hasher.combine(fieldName)
        hasher.combine(op)
        hasher.combine(value)
    }

    public static func == (lhs: FieldComparison<T>, rhs: FieldComparison<T>) -> Bool {
        lhs.fieldName == rhs.fieldName
            && lhs.op == rhs.op
            && lhs.value == rhs.value
    }
}

// MARK: - SortDescriptor

/// Describes how to sort query results
///
/// **Zero-copy comparison**: When constructed with a typed KeyPath, captures
/// a `@Sendable` comparator closure. Falls back to `FieldReader` + `FieldValue.compare`
/// when the closure is unavailable.
public struct SortDescriptor<T: Persistable>: Sendable {
    /// The canonical field name used by planning and type-erased comparison.
    public let fieldName: String

    /// Sort order
    public let order: SortOrder

    /// Zero-copy comparison closure, captured at construction time.
    /// Returns raw comparison (ascending order); caller applies sort direction.
    private let _compare: (@Sendable (T, T) -> ComparisonResult)?

    /// Create a sort descriptor with zero-copy comparison
    public init<V: Comparable & Sendable>(
        keyPath: KeyPath<T, V> & Sendable,
        order: SortOrder = .ascending
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.order = order
        self._compare = { lhs, rhs in
            let l = lhs[keyPath: keyPath]
            let r = rhs[keyPath: keyPath]
            if l < r { return .orderedAscending }
            if l > r { return .orderedDescending }
            return .orderedSame
        }
    }

    /// Create a sort descriptor from a QueryIR field name.
    ///
    /// Used when the sort key originates from a canonical/erased source and a typed
    /// KeyPath is unavailable. Comparison falls back to FieldReader, reading strictly
    /// by `fieldName`.
    init(fieldName: String, order: SortOrder) {
        self.fieldName = fieldName
        self.order = order
        self._compare = nil
    }

    // MARK: - Comparison

    /// Compare two models with sort direction applied
    ///
    /// Returns `.orderedAscending` when lhs should come before rhs,
    /// `.orderedDescending` when rhs should come before lhs,
    /// `.orderedSame` when equal (move to next sort descriptor).
    public func orderedComparison(_ lhs: T, _ rhs: T) -> ComparisonResult {
        let rawResult: ComparisonResult
        if let cmp = _compare {
            rawResult = cmp(lhs, rhs)
        } else {
            rawResult = compareViaFieldReader(lhs, rhs)
        }
        switch order {
        case .ascending:
            return rawResult
        case .descending:
            switch rawResult {
            case .orderedAscending: return .orderedDescending
            case .orderedDescending: return .orderedAscending
            case .orderedSame: return .orderedSame
            }
        }
    }

    /// Fallback comparison using FieldReader + FieldValue
    ///
    /// Handles null ordering: null sorts first in raw comparison
    /// (ascending = null first, descending = null last after flip).
    private func compareViaFieldReader(_ lhs: T, _ rhs: T) -> ComparisonResult {
        let lhsField: FieldValue
        let rhsField: FieldValue
        lhsField = FieldReader.readFieldValue(from: lhs, fieldName: fieldName)
        rhsField = FieldReader.readFieldValue(from: rhs, fieldName: fieldName)

        // Null sorts first in raw (ascending) comparison
        if case .null = lhsField, case .null = rhsField { return .orderedSame }
        if case .null = lhsField { return .orderedAscending }
        if case .null = rhsField { return .orderedDescending }

        return lhsField.compare(to: rhsField) ?? .orderedSame
    }
}

/// Sort order
public enum SortOrder: String, Sendable, Hashable {
    case ascending
    case descending
}

// MARK: - KeyPath Operators (FieldValueConvertible)

/// Equal comparison (zero-copy closure captured)
public func == <T: Persistable, V: Equatable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .equal, value: rhs,
        evaluate: { $0[keyPath: lhs] == rhs }
    ))
}

/// Not equal comparison (zero-copy closure captured)
public func != <T: Persistable, V: Equatable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .notEqual, value: rhs,
        evaluate: { $0[keyPath: lhs] != rhs }
    ))
}

/// Less than comparison (zero-copy closure captured)
public func < <T: Persistable, V: Comparable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .lessThan, value: rhs,
        evaluate: { $0[keyPath: lhs] < rhs }
    ))
}

/// Less than or equal comparison (zero-copy closure captured)
public func <= <T: Persistable, V: Comparable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .lessThanOrEqual, value: rhs,
        evaluate: { $0[keyPath: lhs] <= rhs }
    ))
}

/// Greater than comparison (zero-copy closure captured)
public func > <T: Persistable, V: Comparable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .greaterThan, value: rhs,
        evaluate: { $0[keyPath: lhs] > rhs }
    ))
}

/// Greater than or equal comparison (zero-copy closure captured)
public func >= <T: Persistable, V: Comparable & FieldValueConvertible & Sendable>(
    lhs: KeyPath<T, V> & Sendable,
    rhs: V
) -> Predicate<T> {
    return .comparison(FieldComparison(
        keyPath: lhs, op: .greaterThanOrEqual, value: rhs,
        evaluate: { $0[keyPath: lhs] >= rhs }
    ))
}

// MARK: - Optional KeyPath Operators

/// Check if optional field is nil (zero-copy closure captured in init)
public func == <T: Persistable, V: Sendable>(
    lhs: KeyPath<T, V?> & Sendable,
    rhs: V?.Type
) -> Predicate<T> where V? == Optional<V> {
    .comparison(FieldComparison(keyPath: lhs, op: .isNil))
}

/// Check if optional field is not nil (zero-copy closure captured in init)
public func != <T: Persistable, V: Sendable>(
    lhs: KeyPath<T, V?> & Sendable,
    rhs: V?.Type
) -> Predicate<T> where V? == Optional<V> {
    .comparison(FieldComparison(keyPath: lhs, op: .isNotNil))
}

// MARK: - Typed Predicate Factories

extension Predicate {
    /// Check whether a string field contains a substring.
    public static func contains(
        _ substring: String,
        in keyPath: KeyPath<T, String> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .contains, value: substring,
            evaluate: { DatabaseText.contains(substring, in: $0[keyPath: keyPath]) }
        ))
    }

    /// Check whether a string field starts with a prefix.
    public static func hasPrefix(
        _ prefix: String,
        in keyPath: KeyPath<T, String> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .hasPrefix, value: prefix,
            evaluate: { $0[keyPath: keyPath].hasPrefix(prefix) }
        ))
    }

    /// Check whether a string field ends with a suffix.
    public static func hasSuffix(
        _ suffix: String,
        in keyPath: KeyPath<T, String> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .hasSuffix, value: suffix,
            evaluate: { $0[keyPath: keyPath].hasSuffix(suffix) }
        ))
    }

    /// Check whether an optional string field contains a substring.
    public static func contains(
        _ substring: String,
        in keyPath: KeyPath<T, String?> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .contains, value: .string(substring),
            evaluate: { value in
                guard let field = value[keyPath: keyPath] else { return false }
                return DatabaseText.contains(substring, in: field)
            }
        ))
    }

    /// Check whether an optional string field starts with a prefix.
    public static func hasPrefix(
        _ prefix: String,
        in keyPath: KeyPath<T, String?> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .hasPrefix, value: .string(prefix),
            evaluate: { $0[keyPath: keyPath]?.hasPrefix(prefix) ?? false }
        ))
    }

    /// Check whether an optional string field ends with a suffix.
    public static func hasSuffix(
        _ suffix: String,
        in keyPath: KeyPath<T, String?> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, op: .hasSuffix, value: .string(suffix),
            evaluate: { $0[keyPath: keyPath]?.hasSuffix(suffix) ?? false }
        ))
    }

    /// Check whether a field matches any value in the supplied collection.
    public static func matchesAny<Value: Equatable & FieldValueConvertible & Sendable>(
        of values: [Value],
        at keyPath: KeyPath<T, Value> & Sendable
    ) -> Predicate<T> {
        return .comparison(FieldComparison(
            keyPath: keyPath, values: values,
            evaluate: { model in values.contains(model[keyPath: keyPath]) }
        ))
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
    package let context: FDBContext
    package var query: Query<T>

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
    public func orderBy<V: Comparable & Sendable>(
        _ keyPath: KeyPath<T, V> & Sendable
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(keyPath)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ keyPath: KeyPath<T, V> & Sendable,
        _ order: SortOrder
    ) -> QueryExecutor<T> {
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
    ///     .cachePolicy(.stale(30))
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
    ///     .partition(\.tenantID, equals: "tenant_123")
    ///     .where(\.status == "open")
    ///     .execute()
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: The partition field's keyPath
    ///   - value: The value for directory resolution
    /// - Returns: A new QueryExecutor with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V> & Sendable,
        equals value: V
    ) -> QueryExecutor<T> {
        var copy = self
        copy.query = query.partition(keyPath, equals: value)
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
            statistics: HeuristicStatisticsProvider(),
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
            statistics: HeuristicStatisticsProvider(),
            costModel: .default
        )
        return try planner.plan(query: query, hints: hints)
    }
}
