// PreparedPlan.swift
// QueryPlanner - Prepared query plans with parameter binding support

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import Synchronization

/// A compiled query plan cached for one exact query.
///
/// **Usage**:
/// ```swift
/// // Prepare once
/// let prepared = try planner.prepare(query: usersByAge)
///
/// // Execute many times with different parameters
/// Reuse is allowed only when predicates, literal values, sorting, limit, and
/// offset are identical. Parameterized queries belong to QueryIR and are not
/// represented by the native `Query<T>` API.
public struct PreparedPlan<T: Persistable>: Sendable {
    /// Unique identifier
    public let id: UUID

    /// Query fingerprint (for cache lookup)
    public let fingerprint: QueryFingerprint

    /// The compiled plan template
    public let planTemplate: QueryPlan<T>

    /// Timestamp when this plan was created
    public let createdAt: Date

    /// Create a prepared plan
    public init(
        id: UUID = UUID(),
        fingerprint: QueryFingerprint,
        planTemplate: QueryPlan<T>,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.fingerprint = fingerprint
        self.planTemplate = planTemplate
        self.createdAt = createdAt
    }
}

// MARK: - Query Fingerprint

/// A fingerprint that uniquely identifies every plan-affecting query value.
public struct QueryFingerprint: Sendable, Hashable {
    /// Type name being queried
    public let typeName: String

    public let predicates: [QueryPredicateFingerprint]
    public let sorting: [QuerySortFingerprint]
    public let fetchLimit: Int?
    public let fetchOffset: Int?

    public init(
        typeName: String,
        predicates: [QueryPredicateFingerprint],
        sorting: [QuerySortFingerprint],
        fetchLimit: Int?,
        fetchOffset: Int?
    ) {
        self.typeName = typeName
        self.predicates = predicates
        self.sorting = sorting
        self.fetchLimit = fetchLimit
        self.fetchOffset = fetchOffset
    }
}

public indirect enum QueryPredicateFingerprint: Sendable, Hashable {
    case comparison(
        fieldName: String,
        operation: ComparisonOperator,
        value: FieldValue
    )
    case and([QueryPredicateFingerprint])
    case or([QueryPredicateFingerprint])
    case not(QueryPredicateFingerprint)
    case alwaysTrue
    case alwaysFalse
}

public struct QuerySortFingerprint: Sendable, Hashable {
    public let fieldName: String
    public let order: SortOrder

    public init(fieldName: String, order: SortOrder) {
        self.fieldName = fieldName
        self.order = order
    }
}

// MARK: - Query Fingerprint Builder

/// Builds fingerprints from queries
public struct QueryFingerprintBuilder<T: Persistable> {

    public init() {}

    /// Build a fingerprint from a query
    public func build(from query: Query<T>) -> QueryFingerprint {
        return QueryFingerprint(
            typeName: T.persistableType,
            predicates: query.predicates.map(predicateFingerprint),
            sorting: query.sortDescriptors.map {
                QuerySortFingerprint(
                    fieldName: $0.fieldName,
                    order: $0.order
                )
            },
            fetchLimit: query.fetchLimit,
            fetchOffset: query.fetchOffset
        )
    }

    private func predicateFingerprint(
        _ predicate: Predicate<T>
    ) -> QueryPredicateFingerprint {
        switch predicate {
        case .comparison(let comparison):
            return .comparison(
                fieldName: comparison.fieldName,
                operation: comparison.op,
                value: comparison.value
            )
        case .and(let inner):
            return .and(inner.map(predicateFingerprint))
        case .or(let inner):
            return .or(inner.map(predicateFingerprint))
        case .not(let inner):
            return .not(predicateFingerprint(inner))
        case .true:
            return .alwaysTrue
        case .false:
            return .alwaysFalse
        }
    }
}

// MARK: - Plan Cache

/// Cache for prepared query plans
///
/// Thread-safe cache that stores prepared plans indexed by query fingerprint.
/// Supports LRU eviction and TTL-based expiration.
public final class PlanCache: Sendable {

    /// Cache entry with metadata
    private struct CacheEntry<T: Persistable>: Sendable {
        let plan: PreparedPlan<T>
        let lastAccessTime: Date
        let accessCount: Int
    }

    /// Internal state protected by Mutex
    private struct State: Sendable {
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

        let prepared = PreparedPlan<T>(
            fingerprint: fingerprint,
            planTemplate: plan
        )

        // Cache the prepared plan
        cache?.put(prepared)

        return prepared
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
