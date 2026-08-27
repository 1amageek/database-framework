import DatabaseKit

/// One produced RDF quad whose payload remains request-accounted until its
/// destination has admitted the same retained footprint.
package struct DatabaseQueryScopedRDFQuad: ~Copyable {
    private let quad: RDFQuad
    package let footprint: DatabaseIntermediateFootprint
    private let reservation: DatabaseIntermediateReservation

    private init(
        quad: consuming RDFQuad,
        footprint: DatabaseIntermediateFootprint,
        reservation: DatabaseIntermediateReservation
    ) {
        self.quad = consume quad
        self.footprint = footprint
        self.reservation = reservation
    }

    package static func producingOptional(
        maximumFootprint: DatabaseIntermediateFootprint,
        footprintMeter: DatabaseRDFQuadFootprintMeter,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ makeQuad: () throws -> RDFQuad?
    ) throws -> DatabaseQueryScopedRDFQuad? {
        guard footprintMeter.requestWorkMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let reservation = try workMeter.reserveIntermediate(
            rows: maximumFootprint.rows,
            bytes: maximumFootprint.bytes,
            at: stage
        )
        do {
            guard let quad = try makeQuad() else {
                reservation.release()
                return nil
            }
            let actualFootprint = try footprintMeter.footprint(of: quad)
            guard actualFootprint.rows <= maximumFootprint.rows,
                  actualFootprint.bytes <= maximumFootprint.bytes else {
                throw DatabaseQueryScopedRDFQuadError
                    .payloadFootprintExceeded(
                        maximumRows: maximumFootprint.rows,
                        maximumBytes: maximumFootprint.bytes,
                        actualRows: actualFootprint.rows,
                        actualBytes: actualFootprint.bytes
                    )
            }
            try reservation.releasePartial(
                rows: maximumFootprint.rows - actualFootprint.rows,
                bytes: maximumFootprint.bytes - actualFootprint.bytes
            )
            return DatabaseQueryScopedRDFQuad(
                quad: quad,
                footprint: actualFootprint,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package var workMeter: DatabaseWorkMeter {
        reservation.workMeter
    }

    package borrowing func withQuad<Result, Failure: Error>(
        _ body: (borrowing RDFQuad) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer { withExtendedLifetime(reservation) {} }
        return try body(quad)
    }
}
