import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

package struct RDFDatasetScanStorageRow: Sendable, Hashable {
    package let quad: RDFQuad
    package let coveringValue: ByteString
    package let includedFieldNames: [String]

    package init(
        quad: RDFQuad,
        coveringValue: ByteString = ByteString(),
        includedFieldNames: [String] = []
    ) {
        self.quad = quad
        self.coveringValue = coveringValue
        self.includedFieldNames = includedFieldNames
    }

    package func decodeProperties() throws -> [String: FieldValue] {
        try CoveringValueBuilder.decode(
            coveringValue,
            includedFieldNames: includedFieldNames
        )
    }

    package func withDecodedProperties<Result, Failure: Error>(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ body: (borrowing [String: FieldValue]) async throws(Failure) -> Result
    ) async throws -> Result {
        let footprint = try CoveringValueBuilder
            .decodedPropertiesWorkspaceFootprint(
                coveringValue,
                includedFieldNames: includedFieldNames
            )
        let reservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
        defer { reservation.release() }
        let properties = try decodeProperties()
        return try await body(properties)
    }
}

/// Linear ownership of one canonical RDF dataset scan.
///
/// Package execution borrows one row while this owner keeps both the backing
/// Array and its request reservation alive. Any retained projection must be
/// admitted to this same request meter before package code copies it out.
public struct RDFDatasetScanResult: ~Copyable, Sendable {
    private let owner: RDFDatasetScanOwner?
    public let physicalScanCount: Int
    package let workMeter: DatabaseWorkMeter

    package init(
        rows: consuming [RDFDatasetScanStorageRow],
        physicalScanCount: Int,
        intermediateReservation: DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter
    ) {
        precondition(
            rows.isEmpty == (intermediateReservation == nil),
            "Retained scan rows and reservation must have the same lifetime"
        )
        if let intermediateReservation {
            precondition(
                intermediateReservation.workMeter === workMeter,
                "Scan storage and reservation must use the same work meter"
            )
        }
        self.owner = rows.isEmpty
            ? nil
            : RDFDatasetScanOwner(
                storage: rows,
                intermediateReservation: intermediateReservation!
            )
        self.physicalScanCount = physicalScanCount
        self.workMeter = workMeter
    }

    /// Constructs admitted scan storage for package tests and injected scanner
    /// implementations. Production physical scans use byte preflight instead.
    package init(
        quads: consuming [RDFQuad],
        physicalScanCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws {
        var reservation: DatabaseIntermediateReservation?
        var rows: [RDFDatasetScanStorageRow] = []
        for quad in quads {
            let metrics = try RDFDatasetScanRetainedMetrics.measure(
                quad,
                mergesNamedGraphs: false
            )
            let candidate = try workMeter.reserveIntermediate(
                rows: metrics.rowCount,
                bytes: metrics.retainedByteCount,
                at: .deduplication
            )
            var transferred = false
            defer {
                if !transferred { candidate.release() }
            }
            if let reservation {
                try reservation.absorbAll(from: candidate)
            } else {
                reservation = candidate
            }
            transferred = true
            rows.append(RDFDatasetScanStorageRow(quad: quad))
        }
        self.init(
            rows: rows,
            physicalScanCount: physicalScanCount,
            intermediateReservation: reservation,
            workMeter: workMeter
        )
    }

    package static func empty(
        physicalScanCount: Int = 0,
        workMeter: DatabaseWorkMeter
    ) -> RDFDatasetScanResult {
        RDFDatasetScanResult(
            rows: [],
            physicalScanCount: physicalScanCount,
            intermediateReservation: nil,
            workMeter: workMeter
        )
    }

    public var count: Int { owner?.storage.count ?? 0 }
    public var isEmpty: Bool { owner == nil }

    /// Package execution must admit any copy before it escapes this borrow.
    package borrowing func withQuad<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFQuad) throws(Failure) -> Void
    ) throws(Failure) {
        guard let owner else { preconditionFailure("Scan index is out of range") }
        precondition(owner.storage.indices.contains(index))
        try body(owner.storage[index].quad)
    }

    /// Keeps the scan owner alive across an asynchronous scoped quad borrow.
    package borrowing func withQuad<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFQuad) async throws(Failure) -> Void
    ) async throws(Failure) {
        guard let owner else { preconditionFailure("Scan index is out of range") }
        precondition(owner.storage.indices.contains(index))
        try await body(owner.storage[index].quad)
    }

    package borrowing func withRow<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFDatasetScanStorageRow) throws(Failure) -> Void
    ) throws(Failure) {
        guard let owner else { preconditionFailure("Scan index is out of range") }
        precondition(owner.storage.indices.contains(index))
        try body(owner.storage[index])
    }

    package borrowing func withRow<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFDatasetScanStorageRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        guard let owner else { preconditionFailure("Scan index is out of range") }
        precondition(owner.storage.indices.contains(index))
        try await body(owner.storage[index])
    }
}

private final class RDFDatasetScanOwner: Sendable {
    // Declaration order keeps storage destruction inside the reservation
    // lifetime, matching the retained-buffer ownership contract.
    let storage: [RDFDatasetScanStorageRow]
    let intermediateReservation: DatabaseIntermediateReservation

    init(
        storage: consuming [RDFDatasetScanStorageRow],
        intermediateReservation: DatabaseIntermediateReservation
    ) {
        self.storage = storage
        self.intermediateReservation = intermediateReservation
    }
}
