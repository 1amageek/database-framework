import Synchronization

/// Ownership token for request-scoped intermediate rows and bytes.
public final class DatabaseIntermediateReservation: Sendable {
    private struct State: Sendable {
        var rows: UInt64
        var bytes: UInt64
        var isReleased: Bool
    }

    package let workMeter: DatabaseWorkMeter
    private let state: Mutex<State>

    init(
        workMeter: DatabaseWorkMeter,
        rows: UInt64,
        bytes: UInt64
    ) {
        self.workMeter = workMeter
        self.state = Mutex(
            State(rows: rows, bytes: bytes, isReleased: false)
        )
    }

    /// Atomically grows this ownership token before the owner retains more
    /// intermediate rows or bytes.
    public func reserveAdditional(
        rows: UInt64 = 0,
        bytes: UInt64 = 0,
        at stage: DatabaseWorkStage
    ) throws {
        try state.withLock { state in
            guard !state.isReleased else {
                throw DatabaseIntermediateReservationError.alreadyReleased
            }
            try workMeter.claimIntermediate(
                rows: rows,
                bytes: bytes,
                at: stage
            )
            state.rows += rows
            state.bytes += bytes
        }
    }

    /// Creates an independent reservation on the same request meter.
    ///
    /// Child ownership is intentionally independent from this token. It is
    /// used when a new owner must be admitted before an existing unique owner
    /// is consumed, while still allowing either lifetime to end first.
    package func reserveChild(
        rows: UInt64 = 0,
        bytes: UInt64 = 0,
        at stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateReservation {
        try workMeter.reserveIntermediate(
            rows: rows,
            bytes: bytes,
            at: stage
        )
    }

    /// Transfers an admitted portion from a linear child claim into this
    /// retained owner without changing the request-wide total.
    ///
    /// Both reservations use the same object-identity lock order as whole
    /// transfers, preventing lock inversion across the two transfer APIs.
    package func absorbGuaranteedPartial(
        from child: DatabaseIntermediateReservation,
        rows: UInt64 = 0,
        bytes: UInt64 = 0
    ) {
        precondition(self !== child, "A reservation cannot absorb itself")
        precondition(
            workMeter === child.workMeter,
            "Reservations belong to different request meters"
        )
        withStatesLocked(with: child) { destination, source in
            precondition(
                !destination.isReleased,
                "Cannot absorb into an already released reservation"
            )
            precondition(
                !source.isReleased,
                "Cannot absorb an already released reservation"
            )
            precondition(
                rows <= source.rows && bytes <= source.bytes,
                "Absorption exceeds the child reservation"
            )
            let (newRows, rowOverflow) = destination.rows
                .addingReportingOverflow(rows)
            let (newBytes, byteOverflow) = destination.bytes
                .addingReportingOverflow(bytes)
            precondition(
                !rowOverflow && !byteOverflow,
                "Absorbed reservation exceeds UInt64"
            )
            source.rows -= rows
            source.bytes -= bytes
            destination.rows = newRows
            destination.bytes = newBytes
        }
    }

    /// Transfers one child's complete live claim into this owner.
    ///
    /// Validation precedes either mutation. Reservation locks are acquired in
    /// object-identity order so two concurrent transfers cannot invert their
    /// lock order. The request-wide meter is not changed because ownership,
    /// rather than the amount retained, is the only state transition.
    package func absorbAll(
        from child: DatabaseIntermediateReservation
    ) throws {
        guard self !== child else {
            throw DatabaseIntermediateReservationError.transferToSelf
        }
        guard workMeter === child.workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }

        try withStatesLocked(with: child) { destination, source in
            try Self.transferAll(
                from: &source,
                to: &destination
            )
        }
    }

    private func withStatesLocked<Result, Failure: Error>(
        with other: DatabaseIntermediateReservation,
        _ body: (inout State, inout State) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        if ObjectIdentifier(self) < ObjectIdentifier(other) {
            return try state.withLock {
                (ownState: inout State) throws(Failure) -> Result in
                try other.state.withLock {
                    (otherState: inout State) throws(Failure) -> Result in
                    try body(&ownState, &otherState)
                }
            }
        }
        return try other.state.withLock {
            (otherState: inout State) throws(Failure) -> Result in
            try state.withLock {
                (ownState: inout State) throws(Failure) -> Result in
                try body(&ownState, &otherState)
            }
        }
    }

    private static func transferAll(
        from source: inout State,
        to destination: inout State
    ) throws {
        guard !source.isReleased, !destination.isReleased else {
            throw DatabaseIntermediateReservationError.alreadyReleased
        }
        let (rows, rowOverflow) = destination.rows.addingReportingOverflow(
            source.rows
        )
        let (bytes, byteOverflow) = destination.bytes.addingReportingOverflow(
            source.bytes
        )
        precondition(
            !rowOverflow && !byteOverflow,
            "Reservation transfer exceeds UInt64"
        )
        destination.rows = rows
        destination.bytes = bytes
        source.rows = 0
        source.bytes = 0
        source.isReleased = true
    }

    /// Releases a successfully claimed portion when the corresponding owner
    /// could not be created or retained.
    public func releasePartial(
        rows: UInt64 = 0,
        bytes: UInt64 = 0
    ) throws {
        try state.withLock { state in
            guard !state.isReleased else {
                throw DatabaseIntermediateReservationError.alreadyReleased
            }
            guard rows <= state.rows, bytes <= state.bytes else {
                throw DatabaseIntermediateReservationError
                    .releaseExceedsReservation(
                        retainedRows: state.rows,
                        retainedBytes: state.bytes,
                        requestedRows: rows,
                        requestedBytes: bytes
                    )
            }
            workMeter.releaseIntermediate(rows: rows, bytes: bytes)
            state.rows -= rows
            state.bytes -= bytes
        }
    }

    /// Rolls back an internal claim whose existence is guaranteed by linear
    /// ownership. Violations indicate a framework implementation defect.
    package func releaseGuaranteedPartial(
        rows: UInt64 = 0,
        bytes: UInt64 = 0
    ) {
        state.withLock { state in
            precondition(
                !state.isReleased,
                "Cannot roll back an already released reservation"
            )
            precondition(
                rows <= state.rows && bytes <= state.bytes,
                "Guaranteed rollback exceeds the reservation"
            )
            workMeter.releaseIntermediate(rows: rows, bytes: bytes)
            state.rows -= rows
            state.bytes -= bytes
        }
    }

    /// Releases the reservation once. Repeated calls are safe no-ops.
    public func release() {
        state.withLock { state in
            guard !state.isReleased else { return }
            workMeter.releaseIntermediate(
                rows: state.rows,
                bytes: state.bytes
            )
            state.isReleased = true
            state.rows = 0
            state.bytes = 0
        }
    }

    deinit {
        release()
    }
}
