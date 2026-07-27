#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes

/// A row produced by an index-specific read executor.
///
/// Read executors emit rows in a canonical shape: materialized fields plus optional
/// index annotations (e.g. `distance`, `score`, `rank`). The dispatcher in
/// `DatabaseContext+CanonicalRows.swift` applies the common SQL pipeline
/// (`WHERE` / `ORDER BY` / projection / `DISTINCT` / `LIMIT` / `OFFSET`) on top,
/// so executors must not paginate or project themselves.
public struct IndexReadRow: Sendable {
    public let fields: [String: FieldValue]
    public let annotations: [String: FieldValue]
    public let version: PersistableVersionToken?

    public init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.fields = fields
        self.annotations = annotations
        self.version = version
    }

    public static func materializing<T: Persistable>(
        _ item: T,
        annotations: [String: FieldValue] = [:]
    ) throws -> IndexReadRow {
        let row = try QueryRowCodec.encodeAny(item, annotations: annotations)
        return IndexReadRow(fields: row.fields, annotations: row.annotations, version: row.version)
    }

    public static func materializing(
        any item: any Persistable,
        annotations: [String: FieldValue] = [:]
    ) throws -> IndexReadRow {
        let row = try QueryRowCodec.encodeAny(item, annotations: annotations)
        return IndexReadRow(fields: row.fields, annotations: row.annotations, version: row.version)
    }
}

/// The output contract for a read whose ordering is defined by an index.
///
/// `ordering = .orderedByIndex` signals that `rows` is already in the order the
/// underlying index produces (e.g. similarity ascending, rank descending). The
/// dispatcher respects that order only when the outer `SELECT` has no
/// `ORDER BY`; otherwise `ORDER BY` is applied on top.
///
/// `metadata` is passed through to `QueryResponse.metadata` after pagination —
/// useful for facet counters, total counts, and other index-specific summaries.
public struct IndexReadResult: Sendable {
    public enum Ordering: Sendable {
        case orderedByIndex
        case unordered
    }

    public var rows: [IndexReadRow]
    public var ordering: Ordering
    public var metadata: [String: FieldValue]

    public init(
        rows: [IndexReadRow],
        ordering: Ordering = .orderedByIndex,
        metadata: [String: FieldValue] = [:]
    ) {
        self.rows = rows
        self.ordering = ordering
        self.metadata = metadata
    }

    public static let empty: IndexReadResult = IndexReadResult(rows: [])
}
