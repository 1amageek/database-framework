import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

package struct RDFDatasetScanStorageRow: Sendable, Hashable {
    package let quad: RDFQuad
    package let coveringValue: Bytes
    package let storedFieldNames: [String]

    package init(
        quad: RDFQuad,
        coveringValue: Bytes = Bytes(),
        storedFieldNames: [String] = []
    ) {
        self.quad = quad
        self.coveringValue = coveringValue
        self.storedFieldNames = storedFieldNames
    }

    package func decodeProperties() throws -> [String: FieldValue] {
        try CoveringValueBuilder.decode(
            coveringValue,
            storedFieldNames: storedFieldNames
        )
    }
}

/// Canonical quads returned by one logical dataset scan.
///
/// Each element is an owner-linked row. Copying a row out of the collection
/// therefore retains the request-scoped reservation for as long as any data
/// from that row can be used through the public scan API.
public struct RDFDatasetScanResult: Sendable, RandomAccessCollection {
    public typealias Element = RDFDatasetScanRow
    public typealias Index = Int

    private static let emptyOwner = RDFDatasetScanOwner(
        storage: [],
        intermediateReservation: nil
    )

    private let owner: RDFDatasetScanOwner
    public let physicalScanCount: Int

    package init(
        quads: consuming [RDFQuad],
        physicalScanCount: Int
    ) {
        let rows = quads.map { RDFDatasetScanStorageRow(quad: $0) }
        self.owner = rows.isEmpty
            ? Self.emptyOwner
            : RDFDatasetScanOwner(
                storage: rows,
                intermediateReservation: nil
            )
        self.physicalScanCount = physicalScanCount
    }

    /// Creates an empty scan result without a request-scoped heap owner.
    public static func empty(
        physicalScanCount: Int = 0
    ) -> RDFDatasetScanResult {
        RDFDatasetScanResult(
            quads: [],
            physicalScanCount: physicalScanCount
        )
    }

    /// Takes ownership of admitted result storage and its request reservation.
    ///
    /// The reservation must cover every retained owner represented by `quads`.
    public init(
        quads: consuming [RDFQuad],
        physicalScanCount: Int,
        intermediateReservation: DatabaseIntermediateReservation
    ) {
        let rows = quads.map { RDFDatasetScanStorageRow(quad: $0) }
        self.owner = RDFDatasetScanOwner(
            storage: rows,
            intermediateReservation: intermediateReservation
        )
        self.physicalScanCount = physicalScanCount
    }

    init(
        quads: consuming [RDFQuad],
        physicalScanCount: Int,
        intermediateReservation: DatabaseIntermediateReservation?
    ) {
        let rows = quads.map { RDFDatasetScanStorageRow(quad: $0) }
        if rows.isEmpty, intermediateReservation == nil {
            self.owner = Self.emptyOwner
        } else {
            self.owner = RDFDatasetScanOwner(
                storage: rows,
                intermediateReservation: intermediateReservation
            )
        }
        self.physicalScanCount = physicalScanCount
    }

    package init(
        rows: consuming [RDFDatasetScanStorageRow],
        physicalScanCount: Int,
        intermediateReservation: DatabaseIntermediateReservation?
    ) {
        if rows.isEmpty, intermediateReservation == nil {
            self.owner = Self.emptyOwner
        } else {
            self.owner = RDFDatasetScanOwner(
                storage: rows,
                intermediateReservation: intermediateReservation
            )
        }
        self.physicalScanCount = physicalScanCount
    }

    public var startIndex: Int { owner.storage.startIndex }
    public var endIndex: Int { owner.storage.endIndex }

    public subscript(position: Int) -> RDFDatasetScanRow {
        precondition(owner.storage.indices.contains(position))
        return RDFDatasetScanRow(owner: owner, position: position)
    }

    public func index(after index: Int) -> Int {
        owner.storage.index(after: index)
    }

    public func index(before index: Int) -> Int {
        owner.storage.index(before: index)
    }

    public func index(_ index: Int, offsetBy distance: Int) -> Int {
        owner.storage.index(index, offsetBy: distance)
    }

    public func distance(from start: Int, to end: Int) -> Int {
        owner.storage.distance(from: start, to: end)
    }

}

/// One quad whose lifetime is tied to the scan owner's memory reservation.
///
/// Raw RDF values are package-scoped because they are Copyable and could
/// otherwise outlive the reservation. Framework execution paths must admit
/// any retained projection before releasing this row.
public struct RDFDatasetScanRow: Sendable {
    private let owner: RDFDatasetScanOwner
    private let position: Int

    fileprivate init(owner: RDFDatasetScanOwner, position: Int) {
        self.owner = owner
        self.position = position
    }

    package var quad: RDFQuad { owner.storage[position].quad }
    package var subject: RDFTerm {
        owner.storage[position].quad.subject.term
    }
    package var predicate: RDFTerm {
        owner.storage[position].quad.predicate.term
    }
    package var object: RDFTerm { owner.storage[position].quad.object }
    package var graph: RDFTerm? {
        owner.storage[position].quad.graph?.term
    }
    package func decodeProperties() throws -> [String: FieldValue] {
        try owner.storage[position].decodeProperties()
    }
}

private final class RDFDatasetScanOwner: Sendable {
    // Declaration order is intentional: storage is destroyed before the
    // reservation releases its request ledger claim.
    let storage: [RDFDatasetScanStorageRow]
    let intermediateReservation: DatabaseIntermediateReservation?

    init(
        storage: consuming [RDFDatasetScanStorageRow],
        intermediateReservation: DatabaseIntermediateReservation?
    ) {
        self.storage = storage
        self.intermediateReservation = intermediateReservation
    }
}
