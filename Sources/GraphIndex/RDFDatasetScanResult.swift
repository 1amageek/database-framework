import DatabaseEngine
import DatabaseValue
import Graph

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
        self.owner = quads.isEmpty
            ? Self.emptyOwner
            : RDFDatasetScanOwner(
                storage: quads,
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
        self.owner = RDFDatasetScanOwner(
            storage: quads,
            intermediateReservation: intermediateReservation
        )
        self.physicalScanCount = physicalScanCount
    }

    init(
        quads: consuming [RDFQuad],
        physicalScanCount: Int,
        intermediateReservation: DatabaseIntermediateReservation?
    ) {
        if quads.isEmpty, intermediateReservation == nil {
            self.owner = Self.emptyOwner
        } else {
            self.owner = RDFDatasetScanOwner(
                storage: quads,
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

    package var quad: RDFQuad { owner.storage[position] }
    package var subject: DatabaseRDFTerm { owner.storage[position].subject }
    package var predicate: DatabaseRDFTerm { owner.storage[position].predicate }
    package var object: DatabaseRDFTerm { owner.storage[position].object }
    package var graph: DatabaseRDFTerm? { owner.storage[position].graph }
}

private final class RDFDatasetScanOwner: Sendable {
    // Declaration order is intentional: storage is destroyed before the
    // reservation releases its request ledger claim.
    let storage: [RDFQuad]
    let intermediateReservation: DatabaseIntermediateReservation?

    init(
        storage: consuming [RDFQuad],
        intermediateReservation: DatabaseIntermediateReservation?
    ) {
        self.storage = storage
        self.intermediateReservation = intermediateReservation
    }
}
