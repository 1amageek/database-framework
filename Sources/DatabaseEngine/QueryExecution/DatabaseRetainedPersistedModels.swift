import DatabaseKit

/// Immutable persisted-model batch whose decoded payload and Array storage
/// remain charged to the originating request until every consumer releases it.
package final class DatabaseRetainedPersistedModels:
    RandomAccessCollection,
    Sendable {
    struct Entry: Sendable {
        let model: PersistedModel
        let retainedModelFootprint: DatabaseIntermediateFootprint
        let queryRowFootprint: DatabaseIntermediateFootprint
        let reservation: DatabaseIntermediateReservation
    }

    private let entries: [Entry?]
    private let arrayReservation: DatabaseIntermediateReservation

    init(
        entries: consuming [Entry?],
        arrayReservation: DatabaseIntermediateReservation
    ) {
        self.entries = entries
        self.arrayReservation = arrayReservation
    }

    package var startIndex: Int { entries.startIndex }
    package var endIndex: Int { entries.endIndex }
    package var count: Int { entries.count }

    package func index(after index: Int) -> Int { index + 1 }
    package func index(before index: Int) -> Int { index - 1 }

    package subscript(position: Int) -> PersistedModel? {
        entries[position]?.model
    }

    func withEntry<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing Entry?) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try body(entries[index])
    }
}
