import Synchronization

/// Linear ownership of one pending bounded point-read admission.
///
/// The allowance is created only after `DatabaseWorkMeter` has reserved its
/// issued maximum in the request-wide pending byte total. It must be either
/// completed with the returned value byte count or released; its mutex makes
/// those transitions exactly-once across success, failure, cancellation, and
/// deinitialization.
final class DatabasePointReadAllowance: Sendable {
    private enum Phase: Sendable, Equatable {
        case pending
        case completed
        case released
    }

    private struct State: Sendable {
        var phase: Phase
    }

    let issuedByteCount: Int
    /// The request-wide retained plus pending byte count captured before this
    /// allowance was issued. Error mapping uses this immutable provenance
    /// instead of rereading mutable meter state after an async operation.
    let consumedByteCount: UInt64

    private let workMeter: DatabaseWorkMeter
    private let state: Mutex<State>

    init(
        workMeter: DatabaseWorkMeter,
        issuedByteCount: Int,
        consumedByteCount: UInt64
    ) {
        precondition(
            issuedByteCount >= 0,
            "Point-read allowance must have a non-negative maximum"
        )
        self.workMeter = workMeter
        self.issuedByteCount = issuedByteCount
        self.consumedByteCount = consumedByteCount
        self.state = Mutex(State(phase: .pending))
    }

    /// Transfers the pending allowance into exact retained ownership.
    ///
    /// The meter performs the pending-to-retained transition under its own
    /// mutex. If the backend reports more bytes than were issued, the
    /// allowance is released before the typed contract failure escapes.
    func complete(
        returnedByteCount: Int
    ) throws -> DatabaseIntermediateReservation {
        try state.withLock { state in
            guard state.phase == .pending else {
                preconditionFailure(
                    "A point-read allowance can be completed only once"
                )
            }
            do {
                let reservation = try workMeter.completePointRead(
                    issuedByteCount: issuedByteCount,
                    returnedByteCount: returnedByteCount
                )
                state.phase = .completed
                return reservation
            } catch {
                workMeter.releasePointRead(
                    issuedByteCount: issuedByteCount
                )
                state.phase = .released
                throw error
            }
        }
    }

    /// Releases the pending admission without retaining a returned value.
    /// Repeated release attempts are harmless.
    func release() {
        state.withLock { state in
            guard state.phase == .pending else { return }
            workMeter.releasePointRead(
                issuedByteCount: issuedByteCount
            )
            state.phase = .released
        }
    }

    deinit {
        release()
    }
}
