// SelectQueryPlanner.swift
// DatabaseEngine - Translate a QueryIR.SelectQuery into a typed Query<T>
// while tracking which clauses were pushed down so the caller can skip
// redundant residual evaluation.

import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

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
    var residualFilter: QueryIR.Expression?
    /// Residual sort keys that must be applied after the fetch.
    /// `nil` means the orderBy was fully pushed (or absent).
    var residualOrderBy: [SortKey]?
    /// Whether `selectQuery.limit` was pushed into `typedQuery.fetchLimit`.
    var limitPushed: Bool
    /// Whether `selectQuery.offset` was pushed into `typedQuery.fetchOffset`.
    var offsetPushed: Bool
}

// MARK: - ExecutionPlan derivation

extension SelectQueryPushdownPlan {
    /// Derive an `ExecutionPlan<T>` from this pushdown plan.
    ///
    /// - If the caller forced an index via `accessPath`, the derivation emits an
    ///   `IndexAccessPlan` with the forced index name. Equality bindings are
    ///   lifted from top-level AND conjuncts on `typedQuery.predicates`; the
    ///   executor still decides whether each binding matches a leading field of
    ///   the chosen index.
    /// - Otherwise the derivation emits a `FullScanPlan` carrying the residual
    ///   work (filter / orderBy / limit / offset).
    ///
    /// Limit and offset are reported on the plan only when they were actually
    /// pushed; a non-pushed LIMIT/OFFSET remains the caller's responsibility
    /// (canonical pagination layer).
    var executionPlan: ExecutionPlan<T> {
        let limit = limitPushed ? typedQuery.fetchLimit : nil
        let offset = offsetPushed ? typedQuery.fetchOffset : nil

        if let forced = typedQuery.forcedIndex {
            let bindings = equalityBindings(from: typedQuery.predicates)
            return .indexAccess(IndexAccessPlan<T>(
                indexName: forced.indexName,
                bindings: bindings,
                range: nil,
                direction: .forward,
                residualFilter: residualFilter,
                residualOrderBy: residualOrderBy,
                limit: limit,
                offset: offset
            ))
        }

        return .fullScan(FullScanPlan<T>(
            residualFilter: residualFilter,
            residualOrderBy: residualOrderBy,
            limit: limit,
            offset: offset
        ))
    }

    /// Extract equality bindings from top-level AND predicates.
    ///
    /// Only `FieldComparison` with `.equal` is considered; everything else
    /// remains a residual predicate the executor must evaluate.
    private func equalityBindings(from predicates: [Predicate<T>]) -> [KeyFieldBinding] {
        var bindings: [KeyFieldBinding] = []
        for predicate in predicates {
            bindings.append(contentsOf: collectEqualityBindings(from: predicate))
        }
        return bindings
    }

    private func collectEqualityBindings(from predicate: Predicate<T>) -> [KeyFieldBinding] {
        switch predicate {
        case .comparison(let comparison) where comparison.op == .equal:
            let fieldName = T.fieldName(for: comparison.keyPath)
            return [KeyFieldBinding(fieldName: fieldName, value: comparison.value)]
        case .and(let children):
            return children.flatMap { collectEqualityBindings(from: $0) }
        default:
            return []
        }
    }
}

/// Translates a `QueryIR.SelectQuery` targeting a single `.table` source into
/// a `Query<T>` against the concrete Persistable type.
///
/// The planner pushes work into the typed fetch path so that
/// `FDBDataStore.fetchInternalWithTransaction` can engage index selection
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
        partitionValues: [String: String]?,
        options: ReadExecutionOptions
    ) throws -> SelectQueryPushdownPlan<T> {
        var query = Query<T>()

        if let binding = try CanonicalPartitionBinding.makeBinding(
            for: T.self,
            partitionValues: partitionValues
        ) {
            query.partitionBinding = binding
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        query.cachePolicy = execution.cachePolicy

        // Filter: partial AND pushdown.
        var residualFilter: QueryIR.Expression? = nil
        if let filter = selectQuery.filter {
            let split = filter.splitAnd(for: T.self)
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
        let noExternalPagination = options.continuation == nil && options.pageSize == nil
        if noResidualFilter && noOrderBy && noExternalPagination {
            if let limit = selectQuery.limit {
                query.fetchLimit = limit
                limitPushed = true
            }
            if let offset = selectQuery.offset {
                query.fetchOffset = offset
                offsetPushed = true
            }
        }

        // accessPath: honor an explicit index hint when the caller already chose
        // the index. Validation happens here so downstream fetch code can trust
        // the hint's existence. Applicability against the predicate is checked
        // by the fetch path, which has the Sendable IndexableCondition.
        if let accessPath = selectQuery.accessPath {
            try applyAccessPath(accessPath, to: &query, for: T.self)
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
        for type: T.Type
    ) throws {
        switch accessPath {
        case .index(let indexScan):
            guard indexScan.kindIdentifier == "scalar" else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "accessPath with kind '\(indexScan.kindIdentifier)' is not supported for single-table queries"
                )
            }
            guard T.indexDescriptors.contains(where: { $0.name == indexScan.indexName }) else {
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
    private static func combineAnd(_ expressions: [QueryIR.Expression]) -> QueryIR.Expression {
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
            guard T.allFields.contains(column.column) else { return nil }
            let order: SortOrder = sortKey.direction == .ascending ? .ascending : .descending
            descriptors.append(SortDescriptor<T>(fieldName: column.column, order: order))
        }
        return descriptors
    }
}
