import DatabaseKit

/// Immutable persisted-model batch whose decoded payload and Array storage
/// remain charged to the originating request until every consumer releases it.
package final class DatabaseRetainedPersistedModels:
    RandomAccessCollection,
    Sendable {
    package struct Entry: Sendable {
        private let model: PersistedModel
        package let retainedModelFootprint: DatabaseIntermediateFootprint
        package let queryRowFootprint: DatabaseIntermediateFootprint
        private let reservation: DatabaseIntermediateReservation

        package init(
            model: consuming PersistedModel,
            retainedModelFootprint: DatabaseIntermediateFootprint,
            queryRowFootprint: DatabaseIntermediateFootprint,
            reservation: DatabaseIntermediateReservation
        ) {
            self.model = model
            self.retainedModelFootprint = retainedModelFootprint
            self.queryRowFootprint = queryRowFootprint
            self.reservation = reservation
        }

        package var workMeter: DatabaseWorkMeter {
            reservation.workMeter
        }

        package func withModel<Failure: Error>(
            _ body: (borrowing PersistedModel) throws(Failure) -> Void
        ) throws(Failure) {
            try body(model)
            withExtendedLifetime(reservation) {}
        }

        package func withModel<Failure: Error>(
            _ body: (borrowing PersistedModel) async throws(Failure) -> Void
        ) async throws(Failure) {
            try await body(model)
            withExtendedLifetime(reservation) {}
        }
    }

    private let entries: [Entry?]
    private let arrayReservation: DatabaseIntermediateReservation

    init(
        entries: consuming [Entry?],
        arrayReservation: DatabaseIntermediateReservation
    ) throws {
        guard entries.allSatisfy({ entry in
            guard let entry else { return true }
            return entry.workMeter === arrayReservation.workMeter
        }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        self.entries = entries
        self.arrayReservation = arrayReservation
    }

    package convenience init(
        buffer: consuming DatabaseRetainedBuffer<Entry?>
    ) throws {
        let retained = buffer.moveRetainingReservation()
        try self.init(
            entries: retained.elements,
            arrayReservation: retained.reservation
        )
    }

    package var workMeter: DatabaseWorkMeter {
        arrayReservation.workMeter
    }
    package var startIndex: Int { entries.startIndex }
    package var endIndex: Int { entries.endIndex }
    package var count: Int { entries.count }

    package func index(after index: Int) -> Int { index + 1 }
    package func index(before index: Int) -> Int { index - 1 }

    package subscript(position: Int) -> Entry? {
        entries[position]
    }

    func withEntry<Failure: Error>(
        at index: Int,
        _ body: (borrowing Entry?) throws(Failure) -> Void
    ) throws(Failure) {
        try body(entries[index])
        withExtendedLifetime(arrayReservation) {}
    }

    func withEntry<Failure: Error>(
        at index: Int,
        _ body: (borrowing Entry?) async throws(Failure) -> Void
    ) async throws(Failure) {
        try await body(entries[index])
        withExtendedLifetime(arrayReservation) {}
    }
}
