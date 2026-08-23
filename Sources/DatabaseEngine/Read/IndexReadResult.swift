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
        let row = try QueryRowCodec.encode(item, annotations: annotations)
        return IndexReadRow(fields: row.fields, annotations: row.annotations, version: row.version)
    }

    public static func materializing(
        _ item: PersistedModel,
        annotations: [String: FieldValue] = [:]
    ) throws -> IndexReadRow {
        let row = try QueryRowCodec.encode(item, annotations: annotations)
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

    package let rows: [IndexReadRow]
    public let ordering: Ordering
    private let metadata: [String: FieldValue]

    // Both reservations are reference owners. Copying IndexReadResult shares
    // their exactly-once release state and therefore cannot double-release the
    // request ledger.
    private let rowReservation: DatabaseIntermediateReservation?
    private let metadataReservation: DatabaseIntermediateReservation?
    private let workMeter: DatabaseWorkMeter?

    fileprivate init(
        rows: consuming [IndexReadRow],
        ordering: Ordering,
        metadata: [String: FieldValue],
        rowReservation: DatabaseIntermediateReservation?,
        metadataReservation: DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter?
    ) {
        self.rows = rows
        self.ordering = ordering
        self.metadata = metadata
        self.rowReservation = rowReservation
        self.metadataReservation = metadataReservation
        self.workMeter = workMeter
    }

    /// Builds a result whose owned rows remain charged to the request budget
    /// from the producer append through canonical SQL post-processing.
    ///
    /// Index implementations must use this factory instead of first creating
    /// an unmetered `[IndexReadRow]`. Each append reserves row payload and Array
    /// growth before the row enters retained storage.
    public static func build(
        workMeter: DatabaseWorkMeter,
        ordering: Ordering = .orderedByIndex,
        metadata: [String: FieldValue] = [:],
        expectedCount: Int = 0,
        _ body: (inout IndexReadResultBuilder) throws -> Void
    ) throws -> IndexReadResult {
        var builder = try IndexReadResultBuilder(
            workMeter: workMeter,
            expectedCount: expectedCount
        )
        try body(&builder)
        return try builder.finish(
            ordering: ordering,
            metadata: metadata,
            workMeter: workMeter
        )
    }

    public static let empty = IndexReadResult(
        rows: [],
        ordering: .orderedByIndex,
        metadata: [:],
        rowReservation: nil,
        metadataReservation: nil,
        workMeter: nil
    )

    package func validateWorkMeter(
        _ expected: DatabaseWorkMeter,
        sourceName: String
    ) throws {
        guard let workMeter else {
            guard rows.isEmpty && metadata.isEmpty else {
                throw CanonicalReadError.executorWorkMeterMismatch(
                    sourceName: sourceName
                )
            }
            return
        }
        guard workMeter === expected else {
            throw CanonicalReadError.executorWorkMeterMismatch(
                sourceName: sourceName
            )
        }
    }

    package func retainedMetadata() -> DatabaseRetainedQueryMetadata {
        DatabaseRetainedQueryMetadata(
            values: metadata,
            reservation: metadataReservation,
            workMeter: workMeter
        )
    }

}

/// Scoped producer for a budgeted `IndexReadResult`.
public struct IndexReadResultBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<IndexReadRow>

    fileprivate init(
        workMeter: DatabaseWorkMeter,
        expectedCount: Int
    ) throws {
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: IndexReadRow.self
            ),
            expectedCount: expectedCount
        )
    }

    public mutating func append(
        _ row: IndexReadRow
    ) throws {
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: QueryRow(
                fields: row.fields,
                annotations: row.annotations,
                version: row.version
            ),
            workMeter: storage.workMeter
        )
        try storage.append(
            footprint: footprint,
            at: .indexScan,
            make: { row }
        )
    }

    fileprivate consuming func finish(
        ordering: IndexReadResult.Ordering,
        metadata: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> IndexReadResult {
        let metadataReservation: DatabaseIntermediateReservation?
        if metadata.isEmpty {
            metadataReservation = nil
        } else {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRow(fields: [:], annotations: metadata),
                workMeter: workMeter
            )
            metadataReservation = try workMeter.reserveIntermediate(
                bytes: footprint.bytes,
                at: .indexScan
            )
        }
        let retained = storage.finish().moveRetainingReservation()
        return IndexReadResult(
            rows: retained.elements,
            ordering: ordering,
            metadata: metadata,
            rowReservation: retained.reservation,
            metadataReservation: metadataReservation,
            workMeter: workMeter
        )
    }
}
