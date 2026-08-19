// QueryConversion.swift
// DatabaseEngine - Conversion between Query<T> and SelectQuery

import DatabaseKit
import DatabaseTypes

// MARK: - Query<T> → SelectQuery

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
    public func toSelectQuery() throws(QueryConversionError) -> SelectQuery {
        // Build filter from predicates
        let filter: Expression?
        if predicates.isEmpty {
            filter = nil
        } else {
            var expressions: [Expression] = []
            expressions.reserveCapacity(predicates.count)
            for predicate in predicates {
                do {
                    expressions.append(try predicate.toExpression())
                } catch let error {
                    throw .literal(error)
                }
            }
            filter = expressions.reduceExpressions(with: { .and($0, $1) })
        }

        // Build orderBy from sortDescriptors
        let orderBy: [SortKey]? = {
            guard !sortDescriptors.isEmpty else { return nil }
            return sortDescriptors.map { $0.toSortKey() }
        }()

        let limit: UInt64?
        if let fetchLimit {
            guard let value = UInt64(exactly: fetchLimit) else {
                throw .negativeLimit(fetchLimit)
            }
            limit = value
        } else {
            limit = nil
        }
        let offset: UInt64?
        if let fetchOffset {
            guard let value = UInt64(exactly: fetchOffset) else {
                throw .negativeOffset(fetchOffset)
            }
            offset = value
        } else {
            offset = nil
        }

        let partitions: FieldObject
        if let partitionBinding {
            do {
                partitions = try partitionBinding.canonicalPartitions()
            } catch {
                throw .directory(error)
            }
        } else {
            partitions = FieldObject()
        }

        let accessPath = forcedIndex.map {
            AccessPath.index(
                IndexScanSource(
                    indexName: $0.indexName,
                    indexType: .ordered
                )
            )
        }

        return SelectQuery(
            projection: .all,
            source: .table(
                TableRef(
                    table: T.persistableType,
                    partitions: partitions
                )
            ),
            accessPath: accessPath,
            filter: filter,
            orderBy: orderBy,
            limit: limit,
            offset: offset
        )
    }
}

// MARK: - SortDescriptor<T> → SortKey

extension SortDescriptor {
    /// Convert a SortDescriptor to a QueryIR SortKey.
    public func toSortKey() -> SortKey {
        SortKey(
            .column(ColumnRef(column: fieldName)),
            direction: order.toSortDirection
        )
    }
}
