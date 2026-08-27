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

    private let rows: DatabaseSharedRetainedArray<IndexReadRow>?
    public let ordering: Ordering
    public let metadata: [String: FieldValue]

    private let metadataReservation: DatabaseIntermediateReservation?

    fileprivate init(
        rows: DatabaseSharedRetainedArray<IndexReadRow>?,
        ordering: Ordering,
        metadata: [String: FieldValue],
        metadataReservation: DatabaseIntermediateReservation?
    ) {
        self.rows = rows
        self.ordering = ordering
        self.metadata = metadata
        self.metadataReservation = metadataReservation
    }

    package var count: Int { rows?.count ?? 0 }
    package var workMeter: DatabaseWorkMeter? {
        rows?.workMeter ?? metadataReservation?.workMeter
    }
    package var retainedMetadataReservation:
        DatabaseIntermediateReservation? {
        metadataReservation
    }

    package func withRow<Failure: Error>(
        at index: Int,
        _ body: (borrowing IndexReadRow) throws(Failure) -> Void
    ) throws(Failure) {
        guard let rows else {
            preconditionFailure("An empty index result has no rows")
        }
        try rows.withElement(at: index, body)
    }

    package func withRow<Failure: Error>(
        at index: Int,
        _ body: (borrowing IndexReadRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        guard let rows else {
            preconditionFailure("An empty index result has no rows")
        }
        try await rows.withElement(at: index, body)
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
            metadata: nil
        )
    }

    /// Builds rows and transfers metadata that was admitted before its values
    /// were materialized. The metadata owner and its claim cannot be split at
    /// this boundary.
    package static func build(
        workMeter: DatabaseWorkMeter,
        ordering: Ordering = .orderedByIndex,
        metadata: consuming DatabaseRetainedIndexMetadata,
        expectedCount: Int = 0,
        _ body: (inout IndexReadResultBuilder) throws -> Void
    ) throws -> IndexReadResult {
        guard metadata.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var builder = try IndexReadResultBuilder(
            workMeter: workMeter,
            expectedCount: expectedCount
        )
        try body(&builder)
        return try builder.finish(
            ordering: ordering,
            metadata: consume metadata
        )
    }

    /// Transfers a retained metadata owner onto an already completed row
    /// result. The transfer is consuming so the row owner and metadata claim
    /// remain coupled to the same request meter without rebuilding the rows.
    package static func attachingMetadata(
        to result: consuming IndexReadResult,
        metadata: consuming DatabaseRetainedIndexMetadata
    ) throws -> IndexReadResult {
        precondition(
            result.metadataReservation == nil,
            "Index read result metadata can only be attached once"
        )
        guard let resultWorkMeter = result.workMeter,
              resultWorkMeter === metadata.workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let retained = metadata.moveToIndexResult()
        return IndexReadResult(
            rows: result.rows,
            ordering: result.ordering,
            metadata: retained.values,
            metadataReservation: retained.reservation
        )
    }

    public static let empty = IndexReadResult(
        rows: nil,
        ordering: .orderedByIndex,
        metadata: [:],
        metadataReservation: nil
    )
}

/// Scoped producer for a budgeted `IndexReadResult`.
public struct IndexReadResultBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<IndexReadRow>

    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    fileprivate init(
        workMeter: DatabaseWorkMeter,
        expectedCount: Int
    ) throws {
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(IndexReadRow.self
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
        try append(
            footprint: footprint,
            make: { row }
        )
    }

    package mutating func append(
        footprint: DatabaseIntermediateFootprint,
        make: () throws -> IndexReadRow
    ) throws {
        try storage.append(
            footprint: footprint,
            at: .indexScan,
            make: make
        )
    }

    fileprivate consuming func finish(
        ordering: IndexReadResult.Ordering,
        metadata: consuming DatabaseRetainedIndexMetadata?
    ) throws -> IndexReadResult {
        let metadataValues: [String: FieldValue]
        let metadataReservation: DatabaseIntermediateReservation?
        if let metadata = consume metadata {
            let retained = metadata.moveToIndexResult()
            metadataValues = retained.values
            metadataReservation = retained.reservation
        } else {
            metadataValues = [:]
            metadataReservation = nil
        }
        let retained = try storage.finish().moveToSharedOwnership(
            at: .indexScan
        )
        return IndexReadResult(
            rows: retained,
            ordering: ordering,
            metadata: metadataValues,
            metadataReservation: metadataReservation
        )
    }
}
