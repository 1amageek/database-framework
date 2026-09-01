import DatabaseKit

/// Shared ownership for a request-accounted canonical row result.
///
/// Rows reach this type only by consuming their linear
/// ``DatabaseRetainedQueryRows`` owner. Copies share the same retained storage
/// and the same request reservation, and the reservation is released when the
/// last shared owner is released. A durable query snapshot may therefore read
/// the complete result count and emit successive bounded pages after the read
/// snapshot that produced the rows has closed.
///
/// The exposed surface is limited to the element count and page
/// materialization. No caller borrows an individual staged row, so this type
/// does not offer a scoped element borrow. The shared array backing the rows
/// stays module-internal, so no caller takes ownership of the retained storage
/// or holds a view into it after the call that produced it. A materialized page
/// is an independent copy of its range, so the range the caller asks for, not
/// this type, decides how much of the result reaches an ordinary Array.
@_spi(DatabaseExecution)
public struct DatabaseSharedRetainedQueryRows: Sendable {
    private let storage: DatabaseSharedRetainedArray<QueryRow>

    package init(storage: consuming DatabaseSharedRetainedArray<QueryRow>) {
        self.storage = storage
    }

    /// Moves a complete linear row result into shared ownership without
    /// releasing its originating request reservation.
    package init(
        rows: consuming DatabaseRetainedQueryRows,
        at stage: DatabaseWorkStage
    ) throws {
        self.storage = try rows.moveToSharedOwnership(at: stage)
    }

    package var workMeter: DatabaseWorkMeter {
        storage.workMeter
    }

    /// The number of rows the shared result retains.
    public var count: Int {
        storage.count
    }

    /// Copies the rows in `range` into an output page.
    ///
    /// Only the requested range is copied and the retained storage is never
    /// promoted, so repeated pages can be emitted from one shared owner. A
    /// range covering every element is a valid single page; the copy it returns
    /// does not release the retained storage or its request reservation.
    public func materializePage(_ range: Range<Int>) -> [QueryRow] {
        precondition(
            range.lowerBound >= 0 && range.upperBound <= storage.count,
            "Page range is outside the shared retained rows"
        )
        var output: [QueryRow] = []
        output.reserveCapacity(range.count)
        for index in range {
            storage.withElement(at: index) { row in
                output.append(copy row)
            }
        }
        return output
    }
}
