import StorageKit

/// Request-owned primary keys exposed only through zero-based scoped borrows.
///
/// The protocol deliberately does not refine `Collection` and does not return
/// `Tuple`. A consumer can inspect one key only while the concrete owner and
/// its originating work-meter claim remain alive.
package protocol DatabaseRetainedPrimaryKeyCollection: Sendable {
    var count: Int { get }
    var workMeter: DatabaseWorkMeter { get }

    func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure)
}

extension DatabaseSharedRetainedArray:
    DatabaseRetainedPrimaryKeyCollection where Element == Tuple {
    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        try withElement(at: position, body)
    }
}

/// One decoded primary key coupled to the exact allocation claim produced by
/// tuple decoding. The tuple is never returned independently from that claim.
package struct DatabaseRetainedPrimaryKey: Sendable {
    private let value: Tuple
    private let reservation: DatabaseIntermediateReservation

    package init(
        value: consuming Tuple,
        reservation: DatabaseIntermediateReservation
    ) {
        self.value = value
        self.reservation = reservation
    }

    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    package func withValue<Failure: Error>(
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        try body(value)
        withExtendedLifetime(reservation) {}
    }
}

/// Immutable retained primary-key batch used by canonical physical reads.
package final class DatabaseRetainedPrimaryKeys:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable {
    private let values: DatabaseSharedRetainedArray<DatabaseRetainedPrimaryKey>

    package init(
        buffer: consuming DatabaseRetainedBuffer<DatabaseRetainedPrimaryKey>
    ) throws {
        self.values = try buffer.moveToSharedOwnership(at: .indexScan)
    }

    package var count: Int { values.count }
    package var workMeter: DatabaseWorkMeter { values.workMeter }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        func apply(
            _ retained: borrowing DatabaseRetainedPrimaryKey
        ) throws(Failure) {
            try retained.withValue(body)
        }
        try values.withElement(at: position, apply)
    }
}
