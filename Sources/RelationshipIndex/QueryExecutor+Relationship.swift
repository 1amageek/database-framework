// QueryExecutor+Relationship.swift
// RelationshipIndex - Relationship loading extension for QueryExecutor
//
// Provides joining() and executeWithRelations() for relationship queries.

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

// MARK: - RelationshipQueryExecutor

/// Executor for queries with relationship loading
///
/// Created by calling `joining()` on a `QueryExecutor`.
/// Use `execute()` to return `[Snapshot<T>]` with loaded relationships.
///
/// **Usage**:
/// ```swift
/// let orders = try await context.fetch(Order.self)
///     .joining(\.customerID, as: Customer.self)
///     .execute()
///
/// for order in orders {
///     let customer = order.ref(Customer.self, \.customerID)
///     print(customer?.name)
/// }
/// ```
public struct RelationshipQueryExecutor<T: Persistable>: Sendable {
    private let context: FDBContext
    internal var query: Query<T>

    /// Relationship joins to load
    ///
    /// Note: `nonisolated(unsafe)` is used because AnyKeyPath is not Sendable,
    /// but this is safe since the array is only modified during query building
    /// (before execution) and each call returns a new executor copy.
    private nonisolated(unsafe) var joins: [RelationshipJoin]

    /// Initialize with context and query
    public init(context: FDBContext, query: Query<T>) {
        self.context = context
        self.query = query
        self.joins = []
    }

    // MARK: - Fluent API (Delegating to Query)

    /// Add a filter predicate
    public func `where`(_ predicate: DatabaseEngine.Predicate<T>) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(keyPath)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, _ order: DatabaseEngine.SortOrder) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.query = query.orderBy(keyPath, order)
        return copy
    }

    /// Set maximum number of results
    public func limit(_ count: Int) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    /// Set number of results to skip
    public func offset(_ count: Int) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.query = query.offset(count)
        return copy
    }

    // MARK: - Joining

    /// Join a to-one relationship (optional FK field)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional FK field
    ///   - relatedType: The type of the related item
    /// - Returns: Executor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, String?>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.joins.append(RelationshipJoin(
            keyPath: keyPath,
            relatedTypeName: R.persistableType,
            isToMany: false
        ))
        return copy
    }

    /// Join a to-one relationship (required FK field)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the required FK field
    ///   - relatedType: The type of the related item
    /// - Returns: Executor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, String>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.joins.append(RelationshipJoin(
            keyPath: keyPath,
            relatedTypeName: R.persistableType,
            isToMany: false
        ))
        return copy
    }

    /// Join a to-many relationship (FK array field)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the FK array field
    ///   - relatedType: The type of the related items
    /// - Returns: Executor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, [String]>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        var copy = self
        copy.joins.append(RelationshipJoin(
            keyPath: keyPath,
            relatedTypeName: R.persistableType,
            isToMany: true
        ))
        return copy
    }

    // MARK: - Execute

    /// Execute the query and return Snapshot results with loaded relationships
    public func execute() async throws -> [Snapshot<T>] {
        let executor = QueryExecutor(context: context, query: query)
        let items = try await executor.execute()
        return try await buildSnapshots(items: items)
    }

    /// Execute the query and return count
    public func count() async throws -> Int {
        let executor = QueryExecutor(context: context, query: query)
        return try await executor.count()
    }

    /// Execute the query and return first Snapshot result
    public func first() async throws -> Snapshot<T>? {
        try await limit(1).execute().first
    }

    // MARK: - Private Helpers

    /// Build Snapshots with loaded relationships
    private func buildSnapshots(items: [T]) async throws -> [Snapshot<T>] {
        guard !joins.isEmpty else {
            return items.map { Snapshot(item: $0) }
        }

        // Collect all FK values to batch load
        var fkValuesToLoad: [String: Set<String>] = [:]  // relatedTypeName -> Set of IDs

        for item in items {
            for join in joins {
                if join.isToMany {
                    if let typedKeyPath = join.keyPath as? KeyPath<T, [String]> {
                        let ids = item[keyPath: typedKeyPath]
                        var idSet = fkValuesToLoad[join.relatedTypeName] ?? []
                        ids.forEach { idSet.insert($0) }
                        fkValuesToLoad[join.relatedTypeName] = idSet
                    }
                } else {
                    if let typedKeyPath = join.keyPath as? KeyPath<T, String?> {
                        if let id = item[keyPath: typedKeyPath] {
                            var idSet = fkValuesToLoad[join.relatedTypeName] ?? []
                            idSet.insert(id)
                            fkValuesToLoad[join.relatedTypeName] = idSet
                        }
                    } else if let typedKeyPath = join.keyPath as? KeyPath<T, String> {
                        let id = item[keyPath: typedKeyPath]
                        var idSet = fkValuesToLoad[join.relatedTypeName] ?? []
                        idSet.insert(id)
                        fkValuesToLoad[join.relatedTypeName] = idSet
                    }
                }
            }
        }

        // Batch load related items by type
        var loadedItems: [String: [String: any Persistable]] = [:]  // typeName -> (id -> item)

        for (typeName, ids) in fkValuesToLoad {
            var itemsById: [String: any Persistable] = [:]
            for id in ids {
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

            for join in joins {
                guard let itemsById = loadedItems[join.relatedTypeName] else {
                    continue
                }

                if join.isToMany {
                    if let typedKeyPath = join.keyPath as? KeyPath<T, [String]> {
                        let ids = item[keyPath: typedKeyPath]
                        let relatedItems = ids.compactMap { itemsById[$0] }
                        relations[join.keyPath] = relatedItems
                    }
                } else {
                    if let typedKeyPath = join.keyPath as? KeyPath<T, String?> {
                        if let id = item[keyPath: typedKeyPath], let related = itemsById[id] {
                            relations[join.keyPath] = related
                        }
                    } else if let typedKeyPath = join.keyPath as? KeyPath<T, String> {
                        let id = item[keyPath: typedKeyPath]
                        if let related = itemsById[id] {
                            relations[join.keyPath] = related
                        }
                    }
                }
            }

            snapshots.append(Snapshot(item: item, relations: relations))
        }

        return snapshots
    }
}

// MARK: - RelationshipJoin

/// Describes a relationship to join
private struct RelationshipJoin: @unchecked Sendable {
    let keyPath: AnyKeyPath
    let relatedTypeName: String
    let isToMany: Bool
}

// MARK: - QueryExecutor Extension

extension QueryExecutor {
    /// Join a to-one relationship (optional FK field)
    ///
    /// Converts this QueryExecutor into a RelationshipQueryExecutor that
    /// will load the related items when executed.
    ///
    /// **Usage**:
    /// ```swift
    /// let orders = try await context.fetch(Order.self)
    ///     .joining(\.customerID, as: Customer.self)
    ///     .execute()
    ///
    /// for order in orders {
    ///     let customer = order.ref(Customer.self, \.customerID)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional FK field
    ///   - relatedType: The type of the related item
    /// - Returns: RelationshipQueryExecutor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, String?>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath, as: relatedType)
    }

    /// Join a to-one relationship (required FK field)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the required FK field
    ///   - relatedType: The type of the related item
    /// - Returns: RelationshipQueryExecutor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, String>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath, as: relatedType)
    }

    /// Join a to-many relationship (FK array field)
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the FK array field
    ///   - relatedType: The type of the related items
    /// - Returns: RelationshipQueryExecutor with the join added
    public func joining<R: Persistable>(
        _ keyPath: KeyPath<T, [String]>,
        as relatedType: R.Type
    ) -> RelationshipQueryExecutor<T> {
        RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath, as: relatedType)
    }
}
