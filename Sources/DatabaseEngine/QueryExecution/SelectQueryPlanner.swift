// SelectQueryPlanner.swift
// DatabaseEngine - Translate a SelectQuery into a typed Query<T>
// while tracking which clauses were pushed down so the caller can skip
// redundant residual evaluation.

import DatabaseKit
import DatabaseTypes

/// Result of planning a SelectQuery against a concrete Persistable type.
///
/// `typedQuery` carries the pushed-down predicates, sort descriptors, and
/// limit/offset. `residualFilter` and `residualOrderBy` carry the conjuncts
/// and sort keys that could not be pushed and must be evaluated in-memory
/// over the fetched rows. `limitPushed` / `offsetPushed` tell the caller
/// whether to strip the clauses from the pagination input.
struct SelectQueryPushdownPlan<T: Persistable>: Sendable {
    var typedQuery: Query<T>
    /// Residual filter that must be applied after the fetch.
    /// `nil` means the filter was fully pushed (or absent).
    var residualFilter: Expression?
    /// Residual sort keys that must be applied after the fetch.
    /// `nil` means the orderBy was fully pushed (or absent).
    var residualOrderBy: [SortKey]?
    /// Whether `selectQuery.limit` was pushed into `typedQuery.fetchLimit`.
    var limitPushed: Bool
    /// Whether `selectQuery.offset` was pushed into `typedQuery.fetchOffset`.
    var offsetPushed: Bool
}

/// Translates a `SelectQuery` targeting a single `.table` source into
/// a `Query<T>` against the concrete Persistable type.
///
/// The planner pushes work into the typed fetch path so that
/// `DatabaseDataStore.fetchInternalWithTransaction` can engage index selection
/// and range scans. Anything not convertible is left for residual evaluation
/// by the caller.
///
/// Step 2 scope:
///   - Filter: partial AND pushdown. Each top-level conjunct is attempted
///     independently; convertible conjuncts are added to `predicates` and the
///     rest are combined back into a residual `Expression`.
///   - OrderBy: full-or-nothing pushdown via column-only sort keys. Pushed
///     sort descriptors use FieldReader-based comparison (no KeyPath required).
///   - Limit/Offset: pushed only when it is provably safe — no residual filter,
///     no orderBy clause at all, and no external pagination state (continuation /
///     pageSize). Pushing LIMIT together with a pushed sort is currently unsafe
///     because the typed fetch path truncates at the storage layer before
///     applying sortDescriptors.
///
/// Step 3 scope:
///   - accessPath: if `selectQuery.accessPath == .index(IndexScanSource)` and the
///     scan targets a scalar index, the named index is validated against the
///     target type's descriptors and projected onto `query.forcedIndex`. Missing
///     indexes or non-scalar kinds raise `CanonicalReadError` — silent fallback
///     to a full scan is forbidden because the caller explicitly requested the
///     index.
enum SelectQueryPlanner {
    static func plan<T: Persistable>(
        _ selectQuery: SelectQuery,
        as type: T.Type,
        indexDescriptors: [IndexDescriptor],
        options: ReadExecutionContext
    ) throws -> SelectQueryPushdownPlan<T> {
        var query = Query<T>()

        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSource(
                "Typed table planning requires a table source"
            )
        }
        if let binding = try CanonicalPartitionBinding.makeBinding(
            for: T.self,
            partitions: tableRef.partitions
        ) {
            query.partitionBinding = binding
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        query.cachePolicy = execution.cachePolicy
        query.executionWorkMeter = options.workMeter

        // Filter: partial AND pushdown.
        var residualFilter: Expression? = nil
        if let filter = selectQuery.filter {
            let split = try filter.splitAnd(for: T.self)
            query.predicates.append(contentsOf: split.pushed)
            if !split.residual.isEmpty {
                residualFilter = combineAnd(split.residual)
            }
        }

        // OrderBy: full-or-nothing pushdown.
        // Pushed sort requires every sort key to be a plain column reference
        // that names a field on T, with no NULLS FIRST/LAST qualifier (typed
        // SortDescriptor does not model null ordering).
        var residualOrderBy: [SortKey]? = nil
        if let orderBy = selectQuery.orderBy, !orderBy.isEmpty {
            if let descriptors = sortDescriptors(from: orderBy, for: T.self) {
                query.sortDescriptors = descriptors
            } else {
                residualOrderBy = orderBy
            }
        }

        // Limit/Offset: push only when it is safe — no residual filter, no
        // ORDER BY clause at all, and no external pagination state. See the
        // type-level doc comment for why pushing with a pushed sort is unsafe.
        var limitPushed = false
        var offsetPushed = false
        let noResidualFilter = residualFilter == nil
        let noOrderBy = selectQuery.orderBy?.isEmpty ?? true
        let resolvedPageSize = try options.resolvePageSize()
        let noExternalPagination = options.continuation == nil
            && resolvedPageSize == nil
        if noResidualFilter && noOrderBy && noExternalPagination {
            if let limit = selectQuery.limit {
                guard let limit = Int(exactly: limit) else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Query limit exceeds the platform integer range"
                    )
                }
                query.fetchLimit = limit
                limitPushed = true
            }
            if let offset = selectQuery.offset {
                guard let offset = Int(exactly: offset) else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Query offset exceeds the platform integer range"
                    )
                }
                query.fetchOffset = offset
                offsetPushed = true
            }
        }

        // accessPath: honor an explicit index hint when the caller already chose
        // the index. Validation happens here so downstream fetch code can trust
        // the hint's existence. Applicability against the predicate is checked
        // by the fetch path, which has the Sendable IndexableCondition.
        if let accessPath = selectQuery.accessPath {
            try applyAccessPath(
                accessPath,
                to: &query,
                for: T.self,
                indexDescriptors: indexDescriptors
            )
        }

        return SelectQueryPushdownPlan(
            typedQuery: query,
            residualFilter: residualFilter,
            residualOrderBy: residualOrderBy,
            limitPushed: limitPushed,
            offsetPushed: offsetPushed
        )
    }

    /// Apply a canonical `AccessPath` to the typed query.
    ///
    /// Only scalar-index access paths are currently honored for single-table
    /// queries; other kinds raise `CanonicalReadError.unsupportedAccessPath`
    /// because routing them belongs to a different executor (polymorphic/fusion).
    private static func applyAccessPath<T: Persistable>(
        _ accessPath: AccessPath,
        to query: inout Query<T>,
        for type: T.Type,
        indexDescriptors: [IndexDescriptor]
    ) throws {
        switch accessPath {
        case .index(let indexScan):
            guard indexScan.kindIdentifier == "scalar" else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "accessPath with kind '\(indexScan.kindIdentifier)' is not supported for single-table queries"
                )
            }
            guard indexDescriptors.contains(
                where: { $0.name == indexScan.indexName }
            ) else {
                throw CanonicalReadError.indexHintNotFound(
                    "Forced index '\(indexScan.indexName)' not found on type '\(T.persistableType)'"
                )
            }
            query.forcedIndex = IndexHint(indexName: indexScan.indexName)

        case .fusion:
            throw CanonicalReadError.unsupportedAccessPath(
                "Fusion access paths are not supported for single-table queries"
            )
        }
    }

    /// Left-fold a non-empty array of conjuncts into a single AND expression.
    private static func combineAnd(_ expressions: [Expression]) -> Expression {
        guard let first = expressions.first else { return .literal(.bool(true)) }
        return expressions.dropFirst().reduce(first) { .and($0, $1) }
    }

    /// Translate QueryIR sort keys to typed `SortDescriptor<T>` entries.
    /// Returns `nil` if any sort key cannot be represented as a pushed
    /// descriptor (non-column expression, unknown column, or NULLS ordering).
    private static func sortDescriptors<T: Persistable>(
        from sortKeys: [SortKey], for type: T.Type
    ) -> [SortDescriptor<T>]? {
        var descriptors: [SortDescriptor<T>] = []
        descriptors.reserveCapacity(sortKeys.count)
        for sortKey in sortKeys {
            // Typed SortDescriptor has no null-ordering model — if the query
            // asked for NULLS FIRST/LAST explicitly, leave the clause residual
            // so the canonical layer can honor it.
            if sortKey.nulls != nil { return nil }
            guard case .column(let column) = sortKey.expression else { return nil }
            guard let schema = T.fieldSchemas.first(where: {
                $0.name == column.column && $0.fieldNumber > 0
            }) else {
                return nil
            }
            let order: SortOrder = sortKey.direction == .ascending ? .ascending : .descending
            descriptors.append(
                SortDescriptor<T>(
                    field: FieldIdentity(
                        name: schema.name,
                        number: schema.fieldNumber
                    ),
                    order: order
                )
            )
        }
        return descriptors
    }
}
