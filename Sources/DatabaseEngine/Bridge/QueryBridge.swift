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
            accessPath: nil,
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
