// QueryBridge.swift
// DatabaseEngine - Bridge between Query<T> and QueryIR.SelectQuery

import Foundation
import Core
import QueryIR

// MARK: - Query<T> → SelectQuery (Forward: always succeeds)

extension Query {
    /// Convert a type-safe Query to a QueryIR SelectQuery.
    ///
    /// Maps all Query components to their QueryIR equivalents:
    /// - predicates → filter Expression (AND-combined)
    /// - sortDescriptors → orderBy [SortKey]
    /// - fetchLimit → limit
    /// - fetchOffset → offset
    /// - source → .table(TableRef(name: typeName))
    ///
    /// The resulting SelectQuery is serializable and can be used for
    /// plan explanation, caching keys, or cross-module query execution.
    public func toSelectQuery() -> QueryIR.SelectQuery {
        let typeName = String(describing: T.self)

        // Build filter from predicates
        let filter: QueryIR.Expression? = {
            guard !predicates.isEmpty else { return nil }
            let expressions = predicates.map { $0.toExpression() }
            return expressions.reduceExpressions(with: { .and($0, $1) })
        }()

        // Build orderBy from sortDescriptors
        let orderBy: [QueryIR.SortKey]? = {
            guard !sortDescriptors.isEmpty else { return nil }
            return sortDescriptors.map { $0.toSortKey() }
        }()

        return QueryIR.SelectQuery(
            projection: .all,
            source: .table(QueryIR.TableRef(table: typeName)),
            filter: filter,
            orderBy: orderBy,
            limit: fetchLimit,
            offset: fetchOffset
        )
    }
}

// MARK: - SortDescriptor<T> → SortKey

extension SortDescriptor {
    /// Convert a SortDescriptor to a QueryIR SortKey.
    public func toSortKey() -> QueryIR.SortKey {
        QueryIR.SortKey(
            .column(QueryIR.ColumnRef(column: fieldName)),
            direction: order.toSortDirection
        )
    }
}

// MARK: - SelectQuery → Query<T> (Reverse: partial)

extension QueryIR.SelectQuery {
    /// Attempt to convert a QueryIR SelectQuery back to a type-safe Query.
    ///
    /// Returns `nil` when the SelectQuery cannot be represented as a Query<T>:
    /// - Source is not a single table matching T's type name
    /// - Filter contains unsupported expression patterns
    /// - GroupBy, having, subqueries, or SPARQL-specific features are used
    /// - ORDER BY is present (field name → KeyPath resolution is not available)
    /// - Column names don't match any field in the target type
    ///
    /// Successfully converted queries use `FieldReader`-based evaluation
    /// via `dynamicMember` subscript. They do NOT have zero-copy KeyPath closures.
    ///
    /// - Note: ORDER BY conversion requires `keyPath(for: String)` which is not
    ///   available on `Persistable`. Queries with sort keys return nil.
    ///   Use the forward direction (`Query.toSelectQuery()`) for serialization.
    public func toQuery<T: Persistable>(for type: T.Type) -> Query<T>? {
        // Source must be a single table
        guard case .table(let tableRef) = source else { return nil }
        let expectedName = String(describing: T.self)
        guard tableRef.table == expectedName else { return nil }

        // Must not use advanced features
        guard groupBy == nil, having == nil, subqueries == nil else { return nil }

        // ORDER BY requires KeyPath resolution which is not available
        guard orderBy == nil || orderBy!.isEmpty else { return nil }

        var query = Query<T>()

        // Convert filter
        if let filterExpr = filter {
            guard let predicate: Predicate<T> = filterExpr.toPredicate(for: type) else {
                return nil
            }
            query.predicates = [predicate]
        }

        query.fetchLimit = limit
        query.fetchOffset = offset

        return query
    }
}
