/// Linear proof that an element payload and its append operation were
/// admitted before the element was created.
///
/// Abandonment releases the payload and admission-owner claims. Commit moves
/// the payload claim into the retained Array owner without changing the
/// request-wide total; the transient admission-owner claim is then released.
package struct DatabaseRetainedArrayAppendAdmission<Element: Sendable>: ~Copyable, Sendable {
    private let sourceReservationIdentifier: ObjectIdentifier
    private let claimReservation: DatabaseIntermediateReservation
    private let payloadFootprint: DatabaseIntermediateFootprint
    private let expectedElementCount: Int

    init(
        sourceReservation: DatabaseIntermediateReservation,
        claimReservation: DatabaseIntermediateReservation,
        payloadFootprint: DatabaseIntermediateFootprint,
        expectedElementCount: Int
    ) {
        self.sourceReservationIdentifier = ObjectIdentifier(
            sourceReservation
        )
        self.claimReservation = claimReservation
        self.payloadFootprint = payloadFootprint
        self.expectedElementCount = expectedElementCount
    }

    consuming func commit(
        to expectedReservation: DatabaseIntermediateReservation,
        at currentElementCount: Int
    ) {
        precondition(
            sourceReservationIdentifier
                == ObjectIdentifier(expectedReservation),
            "Append admission belongs to a different retained buffer"
        )
        precondition(
            expectedElementCount == currentElementCount,
            "Retained buffer changed after append admission"
        )
        expectedReservation.absorbGuaranteedPartial(
            from: claimReservation,
            rows: payloadFootprint.rows,
            bytes: payloadFootprint.bytes
        )
    }
}
