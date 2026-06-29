// CanonicalReadError.swift
// DatabaseEngine - Unified error type for the canonical QueryIR read path.
//
// Every failure surfaced by the SelectQueryPlanner, executor registry,
// and canonical row dispatcher flows through this single enum so callers
// have one catch arm for canonical-read concerns.

import Foundation

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

    /// A `QueryIR.Expression` could not be converted to a canonical operation.
    case unsupportedExpression

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

    // MARK: Partition

    /// A partition-value key does not correspond to a declared directory field.
    case invalidPartitionField(String)

    // MARK: Executor registry

    /// No executor is registered for the requested kind identifier.
    case executorNotRegistered(String)

    // MARK: Pagination

    /// The continuation token provided by the caller is malformed or stale.
    case invalidContinuation

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

    // MARK: Index annotations

    /// An index-produced row was missing an annotation the caller expects
    /// (e.g., FullText BM25 `score`, Vector `distance`). Replaces the
    /// previous `?? 0` silent default that produced misleading ranking.
    case missingAnnotation(String)
}
