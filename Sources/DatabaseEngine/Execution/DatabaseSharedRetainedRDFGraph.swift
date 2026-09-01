import DatabaseKit

/// Shared ownership for a request-accounted RDF graph result.
///
/// A result reaches this type only by consuming its linear
/// ``DatabaseRetainedRDFGraph`` owner. Copies share the same retained storage
/// and the same request reservation, and the reservation is released when the
/// last shared owner is released. Canonical fingerprinting and multi-page
/// emission may therefore continue after the storage snapshot that produced
/// the result has closed.
///
/// The exposed surface is limited to the element count, a scoped element
/// borrow that returns no value, and page materialization. The shared array
/// backing the graph stays module-internal, so no caller takes ownership of the
/// retained storage or holds a view into it after the call that produced it. A
/// materialized page is an independent copy of its range, so the range the
/// caller asks for, not this type, decides how much of the result reaches an
/// ordinary Array.
@_spi(DatabaseExecution)
public struct DatabaseSharedRetainedRDFGraph: Sendable {
    private let storage: DatabaseSharedRetainedArray<RDFQuad>

    package init(storage: consuming DatabaseSharedRetainedArray<RDFQuad>) {
        self.storage = storage
    }

    package var workMeter: DatabaseWorkMeter {
        storage.workMeter
    }

    /// The number of quads the shared result retains.
    public var count: Int {
        storage.count
    }

    /// Borrows one retained quad for the duration of `body`.
    ///
    /// The borrow produces no value, so no element view outlives the call.
    public func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFQuad) throws(Failure) -> Void
    ) throws(Failure) {
        try storage.withElement(at: index, body)
    }

    /// Copies the quads in `range` into an output page.
    ///
    /// Only the requested range is copied and the retained storage is never
    /// promoted, so repeated pages can be emitted from one shared owner. A
    /// range covering every element is a valid single page; the copy it returns
    /// does not release the retained storage or its request reservation.
    public func materializePage(_ range: Range<Int>) -> [RDFQuad] {
        precondition(
            range.lowerBound >= 0 && range.upperBound <= storage.count,
            "Page range is outside the shared retained graph"
        )
        var output: [RDFQuad] = []
        output.reserveCapacity(range.count)
        for index in range {
            storage.withElement(at: index) { quad in
                output.append(copy quad)
            }
        }
        return output
    }
}
