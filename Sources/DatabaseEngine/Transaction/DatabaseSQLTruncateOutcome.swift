/// The effective table closure and deterministic physical row count of TRUNCATE.
package struct DatabaseSQLTruncateOutcome: Sendable {
    package let affectedRows: UInt64
    package let relationIDs: [String]
    private let reservation: DatabaseIntermediateReservation

    init(affectedRows: UInt64, relationIDs: [String], reservation: DatabaseIntermediateReservation) {
        self.affectedRows = affectedRows
        self.relationIDs = relationIDs
        self.reservation = reservation
    }
}
