import DatabaseKit

/// Immutable invocation metadata shared by recursive lexical binding frames.
final class CanonicalQueryBindingResources: Sendable {
    let parameters: CanonicalParameterBindingState
    let sqlResolution: SQLReadResolution?
    let statementReadOrigin: SQLStatementReadOrigin?
    private let reservation: DatabaseIntermediateReservation

    private init(parameters: CanonicalParameterBindingState, sqlResolution: SQLReadResolution?,
        statementReadOrigin: SQLStatementReadOrigin?,
        reservation: DatabaseIntermediateReservation) {
        self.parameters = parameters
        self.sqlResolution = sqlResolution
        self.statementReadOrigin = statementReadOrigin
        self.reservation = reservation
    }

    static func create(parameters: CanonicalParameterBindingState, sqlResolution: SQLReadResolution?,
        workMeter: DatabaseWorkMeter, statementReadOrigin: SQLStatementReadOrigin? = nil) throws -> CanonicalQueryBindingResources {
        let bytes = UInt64(MemoryLayout<CanonicalParameterBindingState>.stride
            + MemoryLayout<SQLReadResolution?>.stride
            + MemoryLayout<SQLStatementReadOrigin?>.stride
            + MemoryLayout<DatabaseIntermediateReservation>.stride) + 64
        let claim = try workMeter.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        return CanonicalQueryBindingResources(parameters: parameters, sqlResolution: sqlResolution,
            statementReadOrigin: statementReadOrigin, reservation: claim)
    }

    func withStatementReadOrigin(_ query: borrowing SelectQuery,
        workMeter: DatabaseWorkMeter) throws -> CanonicalQueryBindingResources {
        let origin = try SQLStatementReadOrigin(query: copy query, workMeter: workMeter)
        return try Self.create(parameters: parameters, sqlResolution: sqlResolution,
            workMeter: workMeter, statementReadOrigin: origin)
    }
}
