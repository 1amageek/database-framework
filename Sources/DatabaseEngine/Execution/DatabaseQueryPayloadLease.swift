/// Immutable retention of producer-owned query payload claims.
/// Consumers can retain this lease but cannot release or resize its claims.
package struct DatabaseQueryPayloadLease: Sendable {
    private let reservations: [DatabaseIntermediateReservation]

    package init(retaining reservation: DatabaseIntermediateReservation) {
        self.reservations = [reservation]
    }

    package init(
        retaining reservation: DatabaseIntermediateReservation,
        and additionalReservation: DatabaseIntermediateReservation
    ) {
        precondition(reservation.workMeter === additionalReservation.workMeter,
                     "Query payload claims belong to different work meters")
        self.reservations = [reservation, additionalReservation]
    }

    package var workMeter: DatabaseWorkMeter { reservations[0].workMeter }
}
