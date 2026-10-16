import Synchronization

/// One selected binding's descriptor admission; it retains no query tree or values.
final class CanonicalBindingMetadata: Sendable {
    private let state = Mutex<DatabaseIntermediateReservation?>(nil)

    var payloadLease: DatabaseQueryPayloadLease? {
        state.withLock { $0.map { DatabaseQueryPayloadLease(retaining: $0) } }
    }

    func retain(_ reservation: DatabaseIntermediateReservation?) throws {
        guard let reservation else { return }
        try state.withLock { retained in
            if let retained { try retained.absorbAll(from: reservation) }
            else { retained = reservation }
        }
    }
}
