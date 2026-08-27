/// Shared physical owner produced by a linear query-row admission token.
///
/// Every internal alias retains this object, so the exact request claim cannot
/// release before the last retained row alias is destroyed or promoted at a
/// public output boundary.
package final class DatabaseQueryScopedQueryRowOwner: Sendable {
    private let row: QueryRow
    package let footprint: DatabaseIntermediateFootprint
    private let reservation: DatabaseIntermediateReservation

    init(
        row: QueryRow,
        footprint: DatabaseIntermediateFootprint,
        reservation: DatabaseIntermediateReservation
    ) {
        self.row = row
        self.footprint = footprint
        self.reservation = reservation
    }

    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    package func withRow<Result, Failure: Error>(
        _ body: (borrowing QueryRow) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer { withExtendedLifetime(reservation) {} }
        return try body(row)
    }
}
