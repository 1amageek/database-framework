import Synchronization

/// Coordinates the offline boundary of an application-owned migration plan.
/// It rejects new data operations, drains admitted work, and admits only the
/// maintenance task that owns the migration transition.
final class DatabaseMigrationAdmissionGate: Sendable {
    private enum Phase: Sendable, Equatable {
        case open
        case migrationRequired
        case migrating
    }

    private final class DrainIdentity: Sendable {}

    private struct DrainAttempt: Sendable {
        let identity: DrainIdentity
        let previousPhase: Phase
        var continuation: CheckedContinuation<Void, any Error>?
    }

    private struct State: Sendable {
        var phase = Phase.open
        var activeDataOperationCount = 0
        var minimumAdmittedSchemaGeneration: UInt64?
        var drainAttempt: DrainAttempt?
    }

    private enum DrainRegistration {
        case waiting
        case completed
        case cancelled
    }

    private struct BeginPreparation {
        let previousPhase: Phase
        let requiresDrain: Bool
    }

    private let state = Mutex(State())

    func requireMigration() {
        state.withLock { state in
            precondition(
                state.activeDataOperationCount == 0,
                "Migration admission must close before the container escapes"
            )
            precondition(
                state.phase != .migrating,
                "An active migration cannot be replaced"
            )
            state.phase = .migrationRequired
        }
    }

    func enterDataOperation(schemaGeneration: UInt64) throws {
        try state.withLock { state in
            switch state.phase {
            case .open:
                if let minimum = state.minimumAdmittedSchemaGeneration,
                    schemaGeneration < minimum
                {
                    throw
                        DatabaseMigrationAdmissionError
                        .staleSchemaGeneration(
                            required: minimum,
                            actual: schemaGeneration
                        )
                }
                let incremented = state.activeDataOperationCount
                    .addingReportingOverflow(1)
                guard !incremented.overflow else {
                    throw DatabaseMigrationAdmissionError
                        .operationLimitExceeded
                }
                state.activeDataOperationCount = incremented.partialValue
            case .migrationRequired:
                throw DatabaseMigrationAdmissionError.migrationRequired
            case .migrating:
                throw DatabaseMigrationAdmissionError.migrationInProgress
            }
        }
    }

    func leaveDataOperation() {
        let continuation = state.withLock { state in
            precondition(
                state.activeDataOperationCount > 0,
                "An admitted data operation must leave exactly once"
            )
            state.activeDataOperationCount -= 1
            guard state.activeDataOperationCount == 0,
                let attempt = state.drainAttempt,
                let continuation = attempt.continuation
            else {
                return Optional<CheckedContinuation<Void, any Error>>.none
            }
            state.drainAttempt = nil
            return continuation
        }
        continuation?.resume()
    }

    func beginMigration() async throws {
        try ensureDatabaseTaskIsActive()
        let identity = DrainIdentity()
        let identifier = ObjectIdentifier(identity)
        let preparation = try state.withLock { state in
            guard state.phase != .migrating else {
                throw DatabaseMigrationAdmissionError.migrationInProgress
            }
            let previousPhase = state.phase
            state.phase = .migrating
            let requiresDrain = state.activeDataOperationCount > 0
            if requiresDrain {
                state.drainAttempt = DrainAttempt(
                    identity: identity,
                    previousPhase: previousPhase,
                    continuation: nil
                )
            }
            return BeginPreparation(
                previousPhase: previousPhase,
                requiresDrain: requiresDrain
            )
        }
        guard preparation.requiresDrain else {
            do {
                try ensureDatabaseTaskIsActive()
            } catch {
                restoreAdmissionAfterCancelledBegin(
                    previousPhase: preparation.previousPhase
                )
                throw error
            }
            return
        }

        do {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let registration = state.withLock { state in
                        guard var attempt = state.drainAttempt,
                            ObjectIdentifier(attempt.identity) == identifier
                        else {
                            return DrainRegistration.cancelled
                        }
                        if Task.isCancelled {
                            state.phase = attempt.previousPhase
                            state.drainAttempt = nil
                            return DrainRegistration.cancelled
                        }
                        guard state.activeDataOperationCount > 0 else {
                            state.drainAttempt = nil
                            return DrainRegistration.completed
                        }
                        attempt.continuation = continuation
                        state.drainAttempt = attempt
                        return DrainRegistration.waiting
                    }
                    switch registration {
                    case .waiting:
                        break
                    case .completed:
                        continuation.resume()
                    case .cancelled:
                        continuation.resume(throwing: CancellationError())
                    }
                }
            } onCancel: { [self] in
                let continuation = state.withLock { state in
                    guard let attempt = state.drainAttempt,
                        ObjectIdentifier(attempt.identity) == identifier
                    else {
                        return Optional<CheckedContinuation<Void, any Error>>.none
                    }
                    state.phase = attempt.previousPhase
                    state.drainAttempt = nil
                    return attempt.continuation
                }
                continuation?.resume(throwing: CancellationError())
            }
            try ensureDatabaseTaskIsActive()
        } catch {
            restoreAdmissionAfterCancelledBegin(
                previousPhase: preparation.previousPhase
            )
            throw error
        }
    }

    func finishMigration(
        isComplete: Bool,
        publishedSchemaGeneration: UInt64
    ) {
        state.withLock { state in
            precondition(
                state.phase == .migrating,
                "Only an admitted migration can finish"
            )
            precondition(
                state.drainAttempt == nil,
                "Migration work cannot start before data operations drain"
            )
            if isComplete {
                state.minimumAdmittedSchemaGeneration = max(
                    state.minimumAdmittedSchemaGeneration ?? 0,
                    publishedSchemaGeneration
                )
                state.phase = .open
            } else {
                state.phase = .migrationRequired
            }
        }
    }

    func failMigration() {
        state.withLock { state in
            precondition(
                state.phase == .migrating,
                "Only an admitted migration can fail"
            )
            precondition(
                state.drainAttempt == nil,
                "A draining migration must be cancelled through its waiter"
            )
            state.phase = .migrationRequired
        }
    }

    private func restoreAdmissionAfterCancelledBegin(
        previousPhase: Phase
    ) {
        state.withLock { state in
            guard state.phase == .migrating,
                state.drainAttempt == nil
            else {
                return
            }
            state.phase = previousPhase
        }
    }
}
