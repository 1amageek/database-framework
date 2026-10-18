import DatabaseKit

/// The exact authorization query shared by its recursive statement binding frames.
final class SQLStatementReadOrigin: Sendable {
    let query: SelectQuery
    private let reservation: DatabaseIntermediateReservation

    init(query: SelectQuery, workMeter: DatabaseWorkMeter) throws {
        reservation = try workMeter.reserveIntermediate(bytes:
            UInt64(MemoryLayout<(SelectQuery, DatabaseIntermediateReservation)>.stride) + 64,
            at: .bindingCandidate)
        self.query = query
    }
}
