import DatabaseKit

/// Immutable invocation metadata shared by recursive lexical binding frames.
final class CanonicalQueryBindingResources: Sendable {
    let parameters: CanonicalParameterBindingState
    let sqlResolution: SQLReadResolution?
    private let reservation: DatabaseIntermediateReservation

    private init(parameters: CanonicalParameterBindingState, sqlResolution: SQLReadResolution?,
        reservation: DatabaseIntermediateReservation) {
        self.parameters = parameters
        self.sqlResolution = sqlResolution
        self.reservation = reservation
    }

    static func create(parameters: CanonicalParameterBindingState, sqlResolution: SQLReadResolution?,
        workMeter: DatabaseWorkMeter) throws -> CanonicalQueryBindingResources {
        let bytes = UInt64(MemoryLayout<CanonicalParameterBindingState>.stride
            + MemoryLayout<SQLReadResolution?>.stride
            + MemoryLayout<DatabaseIntermediateReservation>.stride) + 64
        let claim = try workMeter.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        return CanonicalQueryBindingResources(parameters: parameters, sqlResolution: sqlResolution, reservation: claim)
    }
}
