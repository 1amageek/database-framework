import DatabaseKit
import StorageKit
import Synchronization

public final class DatabaseWorkMeter: Sendable {
    private struct State: Sendable {
        var consumedRows: UInt32 = 0
        var consumedWorkUnits: UInt64 = 0
        var retainedIntermediateRows: UInt64 = 0
        var retainedIntermediateBytes: UInt64 = 0
        var peakIntermediateRows: UInt64 = 0
        var peakIntermediateBytes: UInt64 = 0
    }

    public let budget: ExecutionBudget

    private let monotonicClock: any StorageMonotonicClock
    private let deadline: StorageInstant
    private let state = Mutex(State())

    public init(
        budget: ExecutionBudget,
        monotonicClock: any StorageMonotonicClock
    ) {
        self.budget = budget
        self.monotonicClock = monotonicClock
        self.deadline = monotonicClock.now.advanced(
            by: .milliseconds(Int64(budget.timeoutMilliseconds))
        )
    }

    public func consume(
        _ workUnits: UInt64 = 1,
        at stage: DatabaseWorkStage
    ) throws {
        try claim(workUnits: workUnits, rows: 0, at: stage)
    }

    public func recordOutputRows(
        _ rows: UInt32 = 1,
        at stage: DatabaseWorkStage = .resultMaterialization
    ) throws {
        try claim(workUnits: 0, rows: rows, at: stage)
    }

    public func checkpoint(at stage: DatabaseWorkStage) throws {
        try ensureDatabaseTaskIsActive()
        try checkDeadline(at: stage)
    }

    /// Atomically reserves request-scoped intermediate memory.
    ///
    /// The returned owner releases the reservation explicitly or when its last
    /// reference is destroyed. Callers must retain it for exactly as long as
    /// the corresponding rows and bytes remain retained in memory.
    public func reserveIntermediate(
        rows: UInt64 = 0,
        bytes: UInt64 = 0,
        at stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateReservation {
        try claimIntermediate(rows: rows, bytes: bytes, at: stage)
        return DatabaseIntermediateReservation(
            workMeter: self,
            rows: rows,
            bytes: bytes
        )
    }

    /// Returns a physical storage cursor limit derived only from the remaining
    /// work-unit budget. Retained-row and retained-byte limits are enforced at
    /// the point where a decoded value is admitted into request-owned memory.
    /// Combining those limits here would truncate scans whose physical prefix
    /// contains filtered or duplicate rows before a logical result is found.
    public func storageWorkReadLimitWithSentinel(
        at stage: DatabaseWorkStage = .indexScan
    ) throws -> Int {
        try state.withLock { state in
            try ensureDatabaseTaskIsActive()
            try checkDeadline(at: stage)
            let remaining = budget.maximumWorkUnits - state.consumedWorkUnits
            let withSentinel = remaining == UInt64.max
                ? UInt64.max
                : remaining + 1
            return max(
                1,
                Int(
                    min(
                        withSentinel,
                        UInt64(Int.max)
                    )
                )
            )
        }
    }

    public var consumedRows: UInt32 {
        state.withLock { $0.consumedRows }
    }

    public var consumedWorkUnits: UInt64 {
        state.withLock { $0.consumedWorkUnits }
    }

    public var retainedIntermediateRows: UInt64 {
        state.withLock { $0.retainedIntermediateRows }
    }

    public var retainedIntermediateBytes: UInt64 {
        state.withLock { $0.retainedIntermediateBytes }
    }

    public var peakIntermediateRows: UInt64 {
        state.withLock { $0.peakIntermediateRows }
    }

    public var peakIntermediateBytes: UInt64 {
        state.withLock { $0.peakIntermediateBytes }
    }

    func releaseIntermediate(rows: UInt64, bytes: UInt64) {
        state.withLock { state in
            precondition(
                rows <= state.retainedIntermediateRows
                    && bytes <= state.retainedIntermediateBytes,
                "Intermediate reservation release exceeds retained resources"
            )
            state.retainedIntermediateRows -= rows
            state.retainedIntermediateBytes -= bytes
        }
    }

    func claimIntermediate(
        rows: UInt64,
        bytes: UInt64,
        at stage: DatabaseWorkStage
    ) throws {
        try state.withLock { state in
            try ensureDatabaseTaskIsActive()
            try checkDeadline(at: stage)
            let maximumRows = UInt64(budget.maximumIntermediateRows)
            guard rows <= maximumRows - state.retainedIntermediateRows else {
                throw DatabaseWorkLimitError.maximumIntermediateRows(
                    stage: stage,
                    consumed: state.retainedIntermediateRows,
                    requested: rows,
                    maximum: maximumRows
                )
            }
            let maximumBytes = budget.maximumIntermediateBytes
            guard bytes <= maximumBytes - state.retainedIntermediateBytes else {
                throw DatabaseWorkLimitError.maximumIntermediateBytes(
                    stage: stage,
                    consumed: state.retainedIntermediateBytes,
                    requested: bytes,
                    maximum: maximumBytes
                )
            }
            state.retainedIntermediateRows += rows
            state.retainedIntermediateBytes += bytes
            state.peakIntermediateRows = max(
                state.peakIntermediateRows,
                state.retainedIntermediateRows
            )
            state.peakIntermediateBytes = max(
                state.peakIntermediateBytes,
                state.retainedIntermediateBytes
            )
        }
    }

    private func claim(
        workUnits: UInt64,
        rows: UInt32,
        at stage: DatabaseWorkStage
    ) throws {
        try state.withLock { state in
            try ensureDatabaseTaskIsActive()
            try checkDeadline(at: stage)
            guard rows <= budget.maximumRows - state.consumedRows else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: stage,
                    consumed: state.consumedRows,
                    requested: rows,
                    maximum: budget.maximumRows
                )
            }
            guard workUnits
                    <= budget.maximumWorkUnits - state.consumedWorkUnits else {
                throw DatabaseWorkLimitError.maximumWorkUnits(
                    stage: stage,
                    consumed: state.consumedWorkUnits,
                    requested: workUnits,
                    maximum: budget.maximumWorkUnits
                )
            }
            state.consumedRows += rows
            state.consumedWorkUnits += workUnits
        }
    }

    private func checkDeadline(at stage: DatabaseWorkStage) throws {
        guard monotonicClock.now < deadline else {
            throw DatabaseWorkLimitError.deadline(stage: stage)
        }
    }
}
