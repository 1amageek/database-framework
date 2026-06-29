// PreparedPlan.swift
// QueryPlanner - Prepared query plans with parameter binding support

import Foundation
import Core
import Synchronization

/// A prepared query plan that can be reused with different parameter values
///
/// **Usage**:
/// ```swift
/// // Prepare once
/// let prepared = try planner.prepare(query: usersByAge)
///
/// // Execute many times with different parameters
/// let users25 = try await prepared.execute(with: ["age": 25])
/// let users30 = try await prepared.execute(with: ["age": 30])
/// ```
///
/// **Benefits**:
/// - Plan compilation is done once
/// - Parameter binding is efficient (no re-planning)
/// - Suitable for frequently executed queries with varying parameters
public struct PreparedPlan<T: Persistable>: @unchecked Sendable {
    /// Unique identifier
    public let id: UUID

    /// Query fingerprint (for cache lookup)
    public let fingerprint: QueryFingerprint

    /// The compiled plan template
    public let planTemplate: QueryPlan<T>

    /// Parameter bindings in the plan
    public let parameterBindings: [ParameterBinding]

    /// Timestamp when this plan was created
    public let createdAt: Date

    /// Create a prepared plan
    public init(
        id: UUID = UUID(),
        fingerprint: QueryFingerprint,
        planTemplate: QueryPlan<T>,
        parameterBindings: [ParameterBinding],
        createdAt: Date = Date()
    ) {
        self.id = id
        self.fingerprint = fingerprint
        self.planTemplate = planTemplate
        self.parameterBindings = parameterBindings
        self.createdAt = createdAt
    }

    /// Check if the plan has any parameters
    public var hasParameters: Bool {
        !parameterBindings.isEmpty
    }

    /// Get all parameter names
    public var parameterNames: [String] {
        parameterBindings.map { $0.name }
    }
}

// MARK: - Parameter Binding

/// Represents a parameter binding in a prepared plan
public struct ParameterBinding: Sendable, Hashable {
    /// Parameter name (e.g., "age", "userId")
    public let name: String

    /// Field this parameter binds to
    public let fieldName: String

    /// Expected type for validation
    public let expectedType: ParameterType

    /// Position in the query (for ordered parameters)
    public let position: Int

    public init(
        name: String,
        fieldName: String,
        expectedType: ParameterType,
        position: Int
    ) {
        self.name = name
        self.fieldName = fieldName
        self.expectedType = expectedType
        self.position = position
    }
}

/// Parameter types for validation
public indirect enum ParameterType: Sendable, Hashable {
    case string
    case int
    case int64
    case double
    case bool
    case date
    case data
    case array(element: ParameterType)
    case any

    /// Check if a value matches this type
    public func matches(_ value: Any) -> Bool {
        switch self {
        case .string: return value is String
        case .int: return value is Int
        case .int64: return value is Int64
        case .double: return value is Double
        case .bool: return value is Bool
        case .date: return value is Date
        case .data: return value is Data
        case .array: return value is [Any]
        case .any: return true
        }
    }
}

// MARK: - Query Fingerprint

/// A fingerprint that uniquely identifies a query structure (ignoring literal values)
///
/// Two queries with the same fingerprint can share a cached plan:
/// - `WHERE age = 25` and `WHERE age = 30` → same fingerprint
/// - `WHERE age = 25` and `WHERE name = "John"` → different fingerprints
public struct QueryFingerprint: Sendable, Hashable {
    /// Type name being queried
    public let typeName: String

    /// Field conditions structure (without values)
    public let conditionStructure: String

    /// Sort structure
    public let sortStructure: String

    /// Limit/offset presence
    public let hasLimit: Bool
    public let hasOffset: Bool

    public init(
        typeName: String,
        conditionStructure: String,
        sortStructure: String,
        hasLimit: Bool,
        hasOffset: Bool
    ) {
        self.typeName = typeName
        self.conditionStructure = conditionStructure
        self.sortStructure = sortStructure
        self.hasLimit = hasLimit
        self.hasOffset = hasOffset
    }
}

// MARK: - Query Fingerprint Builder

/// Builds fingerprints from queries
public struct QueryFingerprintBuilder<T: Persistable> {

    public init() {}

    /// Build a fingerprint from a query
    public func build(from query: Query<T>) -> QueryFingerprint {
        let conditionStructure = buildConditionStructure(from: query.predicates)
        let sortStructure = buildSortStructure(from: query.sortDescriptors)

        return QueryFingerprint(
            typeName: String(describing: T.self),
            conditionStructure: conditionStructure,
            sortStructure: sortStructure,
            hasLimit: query.fetchLimit != nil,
            hasOffset: query.fetchOffset != nil
        )
    }

    /// Build condition structure string
    private func buildConditionStructure(from predicates: [Predicate<T>]) -> String {
        predicates.map { predicateStructure($0) }.joined(separator: "&")
    }

    /// Get structural representation of a predicate (without values)
    private func predicateStructure(_ predicate: Predicate<T>) -> String {
        switch predicate {
        case .comparison(let comparison):
            return "\(comparison.fieldName):\(comparison.op)"
        case .and(let inner):
            return "AND(" + inner.map { predicateStructure($0) }.joined(separator: ",") + ")"
        case .or(let inner):
            return "OR(" + inner.map { predicateStructure($0) }.joined(separator: ",") + ")"
        case .not(let inner):
            return "NOT(" + predicateStructure(inner) + ")"
        case .true:
            return "TRUE"
        case .false:
            return "FALSE"
        }
    }

    /// Build sort structure string
    private func buildSortStructure(from sortDescriptors: [SortDescriptor<T>]) -> String {
        sortDescriptors.map { "\($0.fieldName):\($0.order)" }.joined(separator: ",")
    }
}

// MARK: - Plan Cache

/// Cache for prepared query plans
///
/// Thread-safe cache that stores prepared plans indexed by query fingerprint.
/// Supports LRU eviction and TTL-based expiration.
public final class PlanCache: Sendable {

    /// Cache entry with metadata
    private struct CacheEntry<T: Persistable>: @unchecked Sendable {
        let plan: PreparedPlan<T>
        let lastAccessTime: Date
        let accessCount: Int
    }

    /// Internal state protected by Mutex
    private struct State: @unchecked Sendable {
        var entries: [QueryFingerprint: any Sendable] = [:]
        var accessOrder: [QueryFingerprint] = []
        var hitCount: Int = 0
        var missCount: Int = 0
    }

    private let state: Mutex<State>

    /// Maximum number of cached plans
    public let maxSize: Int

    /// Time-to-live for cached plans (nil = no expiration)
    public let ttl: TimeInterval?

    public init(maxSize: Int = 1000, ttl: TimeInterval? = 3600) {
        self.maxSize = maxSize
        self.ttl = ttl
        self.state = Mutex(State())
    }

    /// Get a cached plan
    public func get<T: Persistable>(
        fingerprint: QueryFingerprint,
        type: T.Type
    ) -> PreparedPlan<T>? {
        return state.withLock { state in
            guard let anyEntry = state.entries[fingerprint] else {
                state.missCount += 1
                return nil
            }

            guard let entry = anyEntry as? CacheEntry<T> else {
                state.missCount += 1
                return nil
            }

            // Check TTL
            if let ttl = ttl {
                if Date().timeIntervalSince(entry.plan.createdAt) > ttl {
                    state.entries.removeValue(forKey: fingerprint)
                    state.accessOrder.removeAll { $0 == fingerprint }
                    state.missCount += 1
                    return nil
                }
            }

            // Update access order (move to end)
            state.accessOrder.removeAll { $0 == fingerprint }
            state.accessOrder.append(fingerprint)
            state.hitCount += 1

            // Update entry with new access metadata
            let updatedEntry = CacheEntry<T>(
                plan: entry.plan,
                lastAccessTime: Date(),
                accessCount: entry.accessCount + 1
            )
            state.entries[fingerprint] = updatedEntry

            return entry.plan
        }
    }

    /// Store a plan in the cache
    public func put<T: Persistable>(_ plan: PreparedPlan<T>) {
        state.withLock { state in
            // Evict if at capacity
            while state.entries.count >= maxSize, let oldest = state.accessOrder.first {
                state.entries.removeValue(forKey: oldest)
                state.accessOrder.removeFirst()
            }

            let entry = CacheEntry<T>(
                plan: plan,
                lastAccessTime: Date(),
                accessCount: 1
            )
            state.entries[plan.fingerprint] = entry
            state.accessOrder.append(plan.fingerprint)
        }
    }

    /// Remove a specific plan from the cache
    public func remove(fingerprint: QueryFingerprint) {
        state.withLock { state in
            state.entries.removeValue(forKey: fingerprint)
            state.accessOrder.removeAll { $0 == fingerprint }
        }
    }

    /// Clear all cached plans
    public func clear() {
        state.withLock { state in
            state.entries.removeAll()
            state.accessOrder.removeAll()
        }
    }

    /// Invalidate plans for a specific type
    public func invalidate(typeName: String) {
        state.withLock { state in
            let toRemove = state.entries.keys.filter { $0.typeName == typeName }
            for key in toRemove {
                state.entries.removeValue(forKey: key)
                state.accessOrder.removeAll { $0 == key }
            }
        }
    }

    /// Get cache statistics
    public var statistics: CacheStatistics {
        state.withLock { state in
            CacheStatistics(
                size: state.entries.count,
                hitCount: state.hitCount,
                missCount: state.missCount,
                hitRate: state.hitCount + state.missCount > 0
                    ? Double(state.hitCount) / Double(state.hitCount + state.missCount)
                    : 0.0
            )
        }
    }
}

/// Cache statistics
public struct CacheStatistics: Sendable {
    public let size: Int
    public let hitCount: Int
    public let missCount: Int
    public let hitRate: Double
}

// MARK: - QueryPlanner Extension

extension QueryPlanner {
    /// Prepare a query for repeated execution
    ///
    /// **Example**:
    /// ```swift
    /// let query = Query<User>()
    ///     .filter(\.age, .greaterThan, Parameter("minAge"))
    /// let prepared = try planner.prepare(query)
    /// ```
    public func prepare(query: Query<T>, cache: PlanCache? = nil) throws -> PreparedPlan<T> {
        let fingerprintBuilder = QueryFingerprintBuilder<T>()
        let fingerprint = fingerprintBuilder.build(from: query)

        // Check cache first
        if let cache = cache, let cached: PreparedPlan<T> = cache.get(fingerprint: fingerprint, type: T.self) {
            return cached
        }

        // Plan the query
        let plan = try plan(query: query)

        // Extract parameter bindings
        let bindings = extractParameterBindings(from: query)

        let prepared = PreparedPlan<T>(
            fingerprint: fingerprint,
            planTemplate: plan,
            parameterBindings: bindings
        )

        // Cache the prepared plan
        cache?.put(prepared)

        return prepared
    }

    /// Extract parameter bindings from a query
    private func extractParameterBindings(from query: Query<T>) -> [ParameterBinding] {
        var bindings: [ParameterBinding] = []
        var position = 0

        for predicate in query.predicates {
            extractBindings(from: predicate, bindings: &bindings, position: &position)
        }

        return bindings
    }

    /// Recursively extract bindings from a predicate
    private func extractBindings(
        from predicate: Predicate<T>,
        bindings: inout [ParameterBinding],
        position: inout Int
    ) {
        switch predicate {
        case .comparison:
            // NOTE: Parameter placeholder support requires adding a .placeholder case to FieldValue
            // For now, prepared plans work with constant values only
            break

        case .and(let inner), .or(let inner):
            for pred in inner {
                extractBindings(from: pred, bindings: &bindings, position: &position)
            }

        case .not(let inner):
            extractBindings(from: inner, bindings: &bindings, position: &position)

        case .true, .false:
            break
        }
    }

    /// Infer parameter type from comparison
    private func inferType(from comparison: FieldComparison<T>) -> ParameterType {
        // Default to any - in production, use reflection on keyPath type
        .any
    }
}

// MARK: - Parameter Placeholder

/// A placeholder for a parameter value in a prepared query
public struct ParameterPlaceholder: Sendable {
    public let name: String

    public init(_ name: String) {
        self.name = name
    }
}

// MARK: - Parameter Binding Errors

/// Errors during parameter binding
public enum ParameterBindingError: Error, Sendable {
    case missingParameter(name: String)
    case typeMismatch(parameter: String, expected: ParameterType, actualType: String)
    case invalidParameterCount(expected: Int, actual: Int)
}

extension ParameterBindingError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .missingParameter(let name):
            return "Missing required parameter: '\(name)'"
        case .typeMismatch(let param, let expected, let actualType):
            return "Type mismatch for parameter '\(param)': expected \(expected), got \(actualType)"
        case .invalidParameterCount(let expected, let actual):
            return "Invalid parameter count: expected \(expected), got \(actual)"
        }
    }
}

// MARK: - Plan Validator

/// Validates that a cached plan is still valid
public struct PlanValidator<T: Persistable>: Sendable {

    private let availableIndexes: [IndexDescriptor]

    public init(availableIndexes: [IndexDescriptor]) {
        self.availableIndexes = availableIndexes
    }

    /// Check if a plan is still valid
    ///
    /// A plan becomes invalid if:
    /// - An index it uses has been dropped
    /// - Schema has changed
    /// - Statistics have significantly changed (optional)
    public func isValid(_ plan: PreparedPlan<T>) -> Bool {
        // Check that all used indexes still exist
        for usedIndex in plan.planTemplate.usedIndexes {
            if !availableIndexes.contains(where: { $0.name == usedIndex.name }) {
                return false
            }
        }

        return true
    }
}
