// ScenarioAdmissionLedger.swift
// Counts and gates the storage work one serialized scenario admitted.

import StorageKit
import Synchronization

/// What one scenario engine observed between sealing and base shutdown.
public struct ScenarioAdmissionReport: Sendable, Equatable {
    /// Operations refused because the scenario had already sealed.
    ///
    /// This is a diagnostic rather than a complete count: a refusal raised
    /// after the report is taken belongs to no scenario the coordinator can
    /// still identify.
    public let rejectedOperationCount: Int

    /// How many refusals each operation kind accounts for.
    public let rejectedOperations: [StorageOperation: Int]

    /// Whether the scenario reached its boundary with nothing refused.
    ///
    /// Outstanding work is not a component of this answer. The report is taken
    /// only after the ledger has become quiescent, so an engine that still had
    /// backend work running never produces a report at all.
    public var isTerminal: Bool {
        rejectedOperationCount == 0
    }

    /// Names the operation kinds behind the count, so one integration run
    /// identifies what it caught without being repeated.
    public var diagnosticDescription: String {
        "rejected [\(Self.describe(rejectedOperations))]"
    }

    private static func describe(_ operations: [StorageOperation: Int]) -> String {
        operations
            .sorted { $0.key.rawValue < $1.key.rawValue }
            .map { "\($0.key.rawValue)=\($0.value)" }
            .joined(separator: ", ")
    }
}

/// Admits, counts, and finally refuses the storage operations of one engine.
///
/// Every admitted operation is counted in a balanced pair, so an operation is
/// outstanding exactly while it runs. No count depends on when a reference is
/// released, and a leaked owner cannot make the ledger read zero by staying
/// alive.
///
/// The ledger releases its waiters on exactly one condition:
///
///     quiescent iff admission is closed and no admitted operation is running
///
/// Nothing else ends the wait. A surviving idle transaction or cursor does not
/// hold the gate, because it is admitted nothing further; a running backend
/// operation does hold it, whatever owns the object that issued it.
public final class ScenarioAdmissionLedger: Sendable {
    private struct State: Sendable {
        var isOpen = true
        var inFlightCount = 0
        var rejectedOperationCount = 0
        var inFlightOperations: [StorageOperation: Int] = [:]
        var rejectedOperations: [StorageOperation: Int] = [:]
        var quiescenceWaiters: [CheckedContinuation<Void, Never>] = []

        var isQuiescent: Bool {
            !isOpen && inFlightCount == 0
        }
    }

    private let state = Mutex(State())
    private let backend: StorageBackend

    public init(backend: StorageBackend) {
        self.backend = backend
    }

    public var inFlightCount: Int {
        state.withLock { $0.inFlightCount }
    }

    /// Refuses the operation once the scenario has sealed, without counting it
    /// as outstanding. Nonthrowing factories use this to decide whether they
    /// may build a backend resource at all.
    public func requireOpen(_ operation: StorageOperation) throws {
        try state.withLock { state in
            guard state.isOpen else {
                state.rejectedOperationCount += 1
                state.rejectedOperations[operation, default: 0] += 1
                throw rejection(operation)
            }
        }
    }

    public func begin(_ operation: StorageOperation) throws {
        try state.withLock { state in
            guard state.isOpen else {
                state.rejectedOperationCount += 1
                state.rejectedOperations[operation, default: 0] += 1
                throw rejection(operation)
            }
            state.inFlightCount += 1
            state.inFlightOperations[operation, default: 0] += 1
        }
    }

    /// Ends the operation `begin` admitted. The kind is supplied again so a
    /// blocked wait names what is still running rather than only how much.
    public func end(_ operation: StorageOperation) {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.inFlightCount -= 1
            let remaining = (state.inFlightOperations[operation] ?? 0) - 1
            if remaining <= 0 {
                state.inFlightOperations.removeValue(forKey: operation)
            } else {
                state.inFlightOperations[operation] = remaining
            }
            return Self.takeWaiters(&state)
        }
        Self.resume(waiters)
    }

    public func withAdmission<Result>(
        _ operation: StorageOperation,
        _ body: () async throws -> Result
    ) async throws -> Result {
        try begin(operation)
        defer { end(operation) }
        return try await body()
    }

    /// Closes admission. Operations already counted keep running.
    public func close() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.isOpen = false
            return Self.takeWaiters(&state)
        }
        Self.resume(waiters)
    }

    /// Waits until admission is closed and every admitted operation has ended,
    /// then reports what was refused.
    ///
    /// The wait has no bound and no cancellation exit. A bound would release
    /// the gate while backend work was still running, which is the exact state
    /// the gate exists to prevent, and a cancellation exit would do the same
    /// whenever the scenario failed. The external test timeout owns the case
    /// where an admitted operation never ends.
    ///
    /// In practice the wait terminates because every admitted operation is
    /// bounded by the backend itself: the FoundationDB scenario coordinator
    /// configures a transaction timeout, so an operation that cannot finish
    /// fails instead of running forever.
    public func waitUntilQuiescent() async -> ScenarioAdmissionReport {
        await withCheckedContinuation {
            (continuation: CheckedContinuation<Void, Never>) in
            let blockedOn = state.withLock { state -> [StorageOperation: Int]? in
                guard !state.isQuiescent else { return nil }
                state.quiescenceWaiters.append(continuation)
                return state.inFlightOperations
            }
            guard let blockedOn else {
                continuation.resume()
                return
            }
            Self.reportBlockedWait(blockedOn)
        }
        return state.withLock { state in
            ScenarioAdmissionReport(
                rejectedOperationCount: state.rejectedOperationCount,
                rejectedOperations: state.rejectedOperations
            )
        }
    }

    /// Removes the waiters that quiescence has released. The caller resumes
    /// them outside the lock.
    private static func takeWaiters(
        _ state: inout State
    ) -> [CheckedContinuation<Void, Never>] {
        guard state.isQuiescent, !state.quiescenceWaiters.isEmpty else {
            return []
        }
        defer { state.quiescenceWaiters.removeAll() }
        return state.quiescenceWaiters
    }

    private static func resume(_ waiters: [CheckedContinuation<Void, Never>]) {
        for waiter in waiters {
            waiter.resume()
        }
    }

    /// Names what the scenario is waiting on, once, at the moment the wait has
    /// to block. It reports and does not decide: no timer observes it and no
    /// control flow reads it.
    private static func reportBlockedWait(_ operations: [StorageOperation: Int]) {
        guard !operations.isEmpty else {
            print("scenario quiescence: waiting for admission to close")
            return
        }
        let described = operations
            .sorted { $0.key.rawValue < $1.key.rawValue }
            .map { "\($0.key.rawValue)=\($0.value)" }
            .joined(separator: ", ")
        print("scenario quiescence: waiting for [\(described)]")
    }

    private func rejection(_ operation: StorageOperation) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: backend,
            message: "The serialized scenario that owns this storage engine "
                + "has ended"
        )
    }
}

/// Collects the terminal reports of every engine one scenario handed out.
public final class ScenarioAdmissionRecorder: Sendable {
    private let reports = Mutex<[ScenarioAdmissionReport]>([])

    public init() {}

    public func record(_ report: ScenarioAdmissionReport) {
        reports.withLock { $0.append(report) }
    }

    /// The aggregate of every engine report, or `nil` when no engine sealed.
    public var aggregate: ScenarioAdmissionReport? {
        reports.withLock { reports in
            guard !reports.isEmpty else { return nil }
            return ScenarioAdmissionReport(
                rejectedOperationCount: reports.reduce(0) {
                    $0 + $1.rejectedOperationCount
                },
                rejectedOperations: reports.reduce(into: [:]) {
                    $0.merge($1.rejectedOperations, uniquingKeysWith: +)
                }
            )
        }
    }
}
