import Foundation
import Core
import DatabaseClientProtocol

/// A row produced by an index-specific bridge.
///
/// Bridges emit rows in a canonical shape: materialized fields plus optional
/// index annotations (e.g. `distance`, `score`, `rank`). The dispatcher in
/// `FDBContext+CanonicalRows.swift` applies the common SQL pipeline
/// (`WHERE` / `ORDER BY` / projection / `DISTINCT` / `LIMIT` / `OFFSET`) on top,
/// so bridges must not paginate or project themselves.
public struct BridgedRow: Sendable {
    public let fields: [String: FieldValue]
    public let annotations: [String: FieldValue]

    public init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:]
    ) {
        self.fields = fields
        self.annotations = annotations
    }

    public static func encoding<T: Persistable>(
        _ item: T,
        annotations: [String: FieldValue] = [:]
    ) -> BridgedRow {
        let row = QueryRowCodec.encodeAny(item, annotations: annotations)
        return BridgedRow(fields: row.fields, annotations: row.annotations)
    }

    public static func encoding(
        any item: any Persistable,
        annotations: [String: FieldValue] = [:]
    ) -> BridgedRow {
        let row = QueryRowCodec.encodeAny(item, annotations: annotations)
        return BridgedRow(fields: row.fields, annotations: row.annotations)
    }
}

/// The output contract for a bridge's index-native read.
///
/// `ordering = .indexNative` signals that `rows` is already in the order the
/// underlying index produces (e.g. similarity ascending, rank descending). The
/// dispatcher respects that order only when the outer `SELECT` has no
/// `ORDER BY`; otherwise `ORDER BY` is applied on top.
///
/// `metadata` is passed through to `QueryResponse.metadata` after pagination —
/// useful for facet counters, total counts, and other index-specific summaries.
public struct BridgedRowSet: Sendable {
    public enum Ordering: Sendable {
        case indexNative
        case unordered
    }

    public var rows: [BridgedRow]
    public var ordering: Ordering
    public var metadata: [String: FieldValue]

    public init(
        rows: [BridgedRow],
        ordering: Ordering = .indexNative,
        metadata: [String: FieldValue] = [:]
    ) {
        self.rows = rows
        self.ordering = ordering
        self.metadata = metadata
    }

    public static let empty: BridgedRowSet = BridgedRowSet(rows: [])
}
