import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import Synchronization

package final class DatabasePreparedSQLSelectStorage: Sendable {
    private struct Lease: Sendable {
        let elements: [Expression]
        let reservation: DatabaseIntermediateReservation
    }

    private struct State: Sendable {
        var leases: [Lease]
        var accountedCapacity: Int
    }

    private let workMeter: DatabaseWorkMeter
    private let layout: DatabaseRetainedArrayLayout
    private let storageReservation: DatabaseIntermediateReservation
    private let state: Mutex<State>

    package init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .expressionEvaluation
    ) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Lease.self)
        let initialFootprint = try DatabaseIntermediateFootprint(
            bytes: layout.sharedOwnerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: layout.containerByteCount
            )
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: initialFootprint.bytes,
            at: stage
        )
        self.workMeter = workMeter
        self.layout = layout
        self.storageReservation = reservation
        self.state = Mutex(
            State(leases: [], accountedCapacity: 0)
        )
    }

    deinit {
        state.withLock { state in
            state.leases.removeAll(keepingCapacity: false)
            state.accountedCapacity = 0
        }
        storageReservation.release()
    }

    package func retain(
        elements: consuming [Expression],
        reservation: DatabaseIntermediateReservation,
        at stage: DatabaseWorkStage = .expressionEvaluation
    ) throws {
        guard reservation.workMeter === workMeter else {
            throw DatabasePreparedSQLSelectError.workMeterMismatch
        }
        let lease = Lease(
            elements: elements,
            reservation: reservation
        )
        try state.withLock { state in
            let (requiredCount, countOverflow) = state.leases.count
                .addingReportingOverflow(1)
            guard !countOverflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: state.accountedCapacity
                )
            }
            let growth = try layout.growth(
                from: state.accountedCapacity,
                toFit: requiredCount
            )
            try storageReservation.reserveAdditional(
                bytes: growth.additionalByteCount,
                at: stage
            )
            if growth.capacity != state.accountedCapacity {
                state.leases.reserveCapacity(growth.capacity)
                state.accountedCapacity = growth.capacity
            }
            state.leases.append(lease)
        }
    }
}
