import DatabaseTypes

/// Query metadata coupled to the request claim admitted before its values are
/// materialized. The owner can move only as one unit into an index result.
package struct DatabaseRetainedIndexMetadata: ~Copyable, Sendable {
    private let values: [String: FieldValue]
    private let reservation: DatabaseIntermediateReservation

    package static func build(
        workMeter: DatabaseWorkMeter,
        footprint: DatabaseIntermediateFootprint,
        at stage: DatabaseWorkStage = .indexScan,
        _ make: () throws -> [String: FieldValue]
    ) throws -> DatabaseRetainedIndexMetadata {
        let reservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
        let values = try make()
        return DatabaseRetainedIndexMetadata(
            values: values,
            reservation: reservation
        )
    }

    private init(
        values: consuming [String: FieldValue],
        reservation: DatabaseIntermediateReservation
    ) {
        self.values = values
        self.reservation = reservation
    }

    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    consuming func moveToIndexResult() -> (
        values: [String: FieldValue],
        reservation: DatabaseIntermediateReservation
    ) {
        (values, reservation)
    }
}
