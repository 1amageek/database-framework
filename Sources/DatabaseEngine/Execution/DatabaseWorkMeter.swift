import DatabaseKit
import StorageKit
import Synchronization

public final class DatabaseWorkMeter: Sendable {
    private struct State: Sendable {
        var consumedRows: UInt32 = 0
        var consumedWorkUnits: UInt64 = 0
        var retainedIntermediateRows: UInt64 = 0
        var retainedIntermediateBytes: UInt64 = 0
        var pendingPointReadBytes: UInt64 = 0
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

    public func storageReadLimitWithSentinel(
        at stage: DatabaseWorkStage = .indexScan
    ) throws -> Int {
        try state.withLock { state in
            try ensureDatabaseTaskIsActive()
            try checkDeadline(at: stage)
            let remaining = budget.maximumWorkUnits - state.consumedWorkUnits
            let withSentinel = remaining == UInt64.max
                ? UInt64.max
                : remaining + 1
            let intermediateLimit = UInt64(budget.maximumIntermediateRows)
            let boundedIntermediateLimit = intermediateLimit == UInt64.max
                ? UInt64.max
                : intermediateLimit + 1
            return max(
                1,
                Int(
                    min(
                        withSentinel,
                        boundedIntermediateLimit,
                        UInt64(Int.max)
                    )
                )
            )
        }
    }

    /// Atomically admits the maximum bytes that one bounded point read may
    /// return. The pending allowance is part of the same request-wide budget
    /// as retained intermediate bytes, so concurrent reads cannot all issue
    /// the same stale remaining maximum before their backend awaits.
    func admitPointRead(
        at stage: DatabaseWorkStage
    ) throws -> DatabasePointReadAllowance {
        try state.withLock { state in
            try ensureDatabaseTaskIsActive()
            try checkDeadline(at: stage)
            let maximumBytes = budget.maximumIntermediateBytes
            let (retainedAndPending, overflow) = state.retainedIntermediateBytes
                .addingReportingOverflow(state.pendingPointReadBytes)
            precondition(
                !overflow,
                "Point-read intermediate accounting overflowed"
            )
            guard retainedAndPending <= maximumBytes else {
                throw DatabaseWorkLimitError.maximumIntermediateBytes(
                    stage: stage,
                    consumed: retainedAndPending,
                    requested: 0,
                    maximum: maximumBytes
                )
            }
            let issuedByteCount = Int(
                min(
                    maximumBytes - retainedAndPending,
                    UInt64(Int.max)
                )
            )
            state.pendingPointReadBytes += UInt64(issuedByteCount)
            return DatabasePointReadAllowance(
                workMeter: self,
                issuedByteCount: issuedByteCount,
                consumedByteCount: retainedAndPending
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

    var pendingPointReadBytes: UInt64 {
        state.withLock { $0.pendingPointReadBytes }
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
            let (retainedAndPending, overflow) = state.retainedIntermediateBytes
                .addingReportingOverflow(state.pendingPointReadBytes)
            precondition(
                !overflow,
                "Intermediate byte accounting overflowed"
            )
            guard retainedAndPending <= maximumBytes else {
                throw DatabaseWorkLimitError.maximumIntermediateBytes(
                    stage: stage,
                    consumed: retainedAndPending,
                    requested: bytes,
                    maximum: maximumBytes
                )
            }
            guard bytes <= maximumBytes - retainedAndPending else {
                throw DatabaseWorkLimitError.maximumIntermediateBytes(
                    stage: stage,
                    consumed: retainedAndPending,
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

    /// Completes a pending point-read allowance by transferring exactly the
    /// returned value bytes into retained intermediate ownership. The pending
    /// allowance is removed and the retained count is updated while one mutex
    /// is held, so no concurrent claim can observe a partially completed read.
    func completePointRead(
        issuedByteCount: Int,
        returnedByteCount: Int
    ) throws -> DatabaseIntermediateReservation {
        try state.withLock { state in
            precondition(
                issuedByteCount >= 0 && returnedByteCount >= 0,
                "Point-read byte counts must be non-negative"
            )
            let issued = UInt64(issuedByteCount)
            let returned = UInt64(returnedByteCount)
            precondition(
                issued <= state.pendingPointReadBytes,
                "Point-read completion exceeds pending admission"
            )
            guard returned <= issued else {
                throw StorageError(
                    code: .backendContractViolation,
                    operation: .read,
                    backend: .unknown,
                    message: "Bounded point read returned more bytes than admitted",
                    underlyingDescription: "observedByteCount=\(returned) maximumByteCount=\(issued)",
                    byteLimitViolation: StorageByteLimitViolation(
                        resource: .value,
                        observedByteCount: returned,
                        maximumByteCount: issued,
                        measurement: .exact
                    )
                )
            }
            let (newRetainedBytes, overflow) = state.retainedIntermediateBytes
                .addingReportingOverflow(returned)
            precondition(
                !overflow && newRetainedBytes <= budget.maximumIntermediateBytes,
                "Point-read completion exceeds intermediate byte budget"
            )
            state.pendingPointReadBytes -= issued
            state.retainedIntermediateBytes = newRetainedBytes
            state.peakIntermediateBytes = max(
                state.peakIntermediateBytes,
                newRetainedBytes
            )
            return DatabaseIntermediateReservation(
                workMeter: self,
                rows: 0,
                bytes: returned
            )
        }
    }

    /// Releases a pending point-read allowance exactly once. The allowance
    /// object serializes calls so this method only receives valid linear
    /// releases from its owner.
    func releasePointRead(issuedByteCount: Int) {
        precondition(
            issuedByteCount >= 0,
            "Point-read byte counts must be non-negative"
        )
        state.withLock { state in
            let issued = UInt64(issuedByteCount)
            precondition(
                issued <= state.pendingPointReadBytes,
                "Point-read release exceeds pending admission"
            )
            state.pendingPointReadBytes -= issued
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
