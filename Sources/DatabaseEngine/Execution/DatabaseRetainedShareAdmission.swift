/// Linear proof that shared-owner storage was admitted before allocation.
///
/// Abandoning this value rolls its byte claim back. Committing it requires the
/// exact reservation belonging to the unique buffer that requested admission.
package struct DatabaseRetainedShareAdmission<Element: Sendable>: ~Copyable, Sendable {
    private let sourceReservationIdentifier: ObjectIdentifier
    private let ownerReservation: DatabaseIntermediateReservation
    private var isCommitted: Bool

    init(
        sourceReservation: DatabaseIntermediateReservation,
        ownerReservation: DatabaseIntermediateReservation
    ) {
        self.sourceReservationIdentifier = ObjectIdentifier(
            sourceReservation
        )
        self.ownerReservation = ownerReservation
        self.isCommitted = false
    }

    consuming func commit(
        to expectedReservation: DatabaseIntermediateReservation
    ) -> DatabaseIntermediateReservation {
        precondition(
            sourceReservationIdentifier
                == ObjectIdentifier(expectedReservation),
            "Shared-owner admission belongs to a different retained buffer"
        )
        isCommitted = true
        return ownerReservation
    }

    deinit {
        guard !isCommitted else { return }
        ownerReservation.release()
    }
}
