// CanonicalReadError.swift
// DatabaseEngine - Unified error type for the canonical QueryIR read path.
//
// Every failure surfaced by the SelectQueryPlanner, executor registry,
// and canonical row dispatcher flows through this single enum so callers
// have one catch arm for canonical-read concerns.

import DatabaseKit

/// Errors raised while translating or executing a canonical `SelectQuery`.
///
/// Design rule: the canonical read path **must not** silently fall back to a
/// full scan or a default value when a piece of the plan cannot be honored.
/// Every such condition is represented here and propagated to the caller.
public enum CanonicalReadError: Error, Sendable {
    // MARK: Source / query structure

    /// The `SelectQuery.source` is not supported by the current executor.
    case unsupportedSource(String)

    /// The shape of the `SelectQuery` is not supported (unknown clause,
    /// unsupported feature, unresolved entity, etc.). Message describes the
    /// specific reason.
    case unsupportedSelectQuery(String)

    /// A `Expression` could not be converted to a canonical operation.
    case unsupportedExpression

    /// A supported scalar expression failed with a typed evaluation error.
    case expressionEvaluation(DatabaseExpressionEvaluationError)

    /// A grouped or aggregate expression failed during canonical reduction.
    case aggregateEvaluation(DatabaseAggregateEvaluationError)

    /// A scalar subquery returned more than one row or more than one column.
    case invalidScalarSubquery(rowCount: Int?, columnCount: Int?)

    /// An IN subquery did not expose exactly one column.
    case invalidMembershipSubquery(columnCount: Int)

    /// A literal value type is incompatible with its target column/parameter.
    case incompatibleLiteralType

    // MARK: Access path / index

    /// `SelectQuery.accessPath` referenced a scheme that cannot be routed
    /// through the current path (e.g., fusion on a single-table query).
    case unsupportedAccessPath(String)

    /// `SelectQuery.accessPath.index` named an index that is not registered
    /// on the target Persistable type.
    case indexHintNotFound(String)

    /// `SelectQuery.accessPath.index` named an existing index, but the pushed
    /// predicate has no indexable condition on the index's leading field.
    case indexHintNotApplicable(String)

    /// `SelectQuery.accessPath.index` named a scalar index whose lifecycle state
    /// does not permit reads.
    case indexHintNotReadable(indexName: String, state: String)

    // MARK: Partition

    /// A typed partition does not exactly match the compiled directory schema.
    case invalidPartition(entity: String, reason: String)

    // MARK: Executor registry

    /// No executor is registered for the requested semantic index type.
    case executorNotRegistered(IndexType)

    // MARK: Pagination

    /// The continuation token provided by the caller is malformed or stale.
    case invalidContinuation

    /// A protocol-sized pagination value cannot be represented by this
    /// runtime's collection index type.
    case paginationValueExceedsRuntimeRange(name: String, value: UInt64)

    // MARK: Storage / encoding

    /// A predicate value could not be encoded into the FDB tuple form used
    /// by the index range scan. Previously this was silently swallowed by
    /// `try?` in the storage layer; it is now a first-class error so the
    /// caller can observe (and the planner can decide) whether to fall back
    /// to a full scan explicitly.
    ///
    /// - Parameters:
    ///   - field: The field whose value failed to encode.
    ///   - valueDescription: A human-readable description of the offending value
    ///     (avoid storing the raw `any Sendable` to keep the error `Sendable`).
    case unencodablePredicateValue(field: String, valueDescription: String)

    // MARK: ResolvedIndex annotations

    /// An index-produced row was missing an annotation the caller expects
    /// (e.g., FullText BM25 `score`, Vector `distance`). Replaces the
    /// previous `?? 0` silent default that produced misleading ranking.
    case missingAnnotation(String)

    // MARK: ResolvedIndex integrity

    /// An index entry's key could not be decoded back into a primary-key
    /// tuple. The entry is physically corrupt; skipping it would silently
    /// shrink query results, so the read fails instead.
    case corruptedIndexEntry(indexName: String, reason: String)

    /// An index entry resolved to a primary key whose canonical row does not
    /// exist in the same transaction snapshot. The index and the row store
    /// disagree; returning the remaining rows would hide the inconsistency.
    case danglingIndexEntry(indexName: String, primaryKey: String)
}
