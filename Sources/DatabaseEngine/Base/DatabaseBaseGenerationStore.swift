#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import Synchronization

/// Publishes immutable Base generations and coordinates lifecycle draining.
package final class DatabaseBaseGenerationStore: Sendable {
    private struct Entry: Sendable {
        var generation: DatabaseBaseGeneration
        var admitsOperations: Bool
        var activeLeaseCount: Int
        var drainWaiters: [CheckedContinuation<Void, Never>]
    }

    private struct State: Sendable {
        var entries: [Base.ID: Entry]
    }

    private let state: Mutex<State>

    package init(generations: [DatabaseBaseGeneration]) {
        var entries: [Base.ID: Entry] = [:]
        entries.reserveCapacity(generations.count)
        for generation in generations {
            entries[generation.record.id] = Entry(
                generation: generation,
                admitsOperations: generation.record.lifecycle == .active,
                activeLeaseCount: 0,
                drainWaiters: []
            )
        }
        self.state = Mutex(State(entries: entries))
    }

    package func acquire(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        let generation = try state.withLock { state in
            guard var entry = state.entries[id] else {
                throw DatabaseBaseExecutionError.baseNotFound(id)
            }
            guard entry.admitsOperations,
                  entry.generation.record.lifecycle == .active else {
                throw DatabaseBaseExecutionError.baseUnavailable(
                    id,
                    lifecycle: entry.generation.record.lifecycle.rawValue
                )
            }
            let (count, overflow) = entry.activeLeaseCount
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseBaseExecutionError.leaseCountOverflow(id)
            }
            entry.activeLeaseCount = count
            state.entries[id] = entry
            return entry.generation
        }
        let token = DatabaseBaseLeaseToken { [self] in
            release(id)
        }
        return DatabaseBaseLease(
            generation: generation,
            token: token,
            permitsDataOperations: true
        )
    }

    package func acquireAdministration(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        let generation = try state.withLock { state in
            guard let entry = state.entries[id] else {
                throw DatabaseBaseExecutionError.baseNotFound(id)
            }
            guard entry.generation.record.lifecycle != .tombstone else {
                throw DatabaseBaseExecutionError.baseUnavailable(
                    id,
                    lifecycle: entry.generation.record.lifecycle.rawValue
                )
            }
            return entry.generation
        }
        return DatabaseBaseLease(
            generation: generation,
            token: DatabaseBaseLeaseToken(finishOperation: {}),
            permitsDataOperations: false
        )
    }

    /// Acquires a counted internal lease for schema work against an active or
    /// retired Base. It never admits an external data request.
    package func acquireSchemaMaintenance(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        let generation = try state.withLock { state in
            guard var entry = state.entries[id] else {
                throw DatabaseBaseExecutionError.baseNotFound(id)
            }
            switch entry.generation.record.lifecycle {
            case .active, .retired:
                break
            case .provisioning, .retiring, .moving, .deleting, .tombstone:
                throw DatabaseBaseExecutionError.baseUnavailable(
                    id,
                    lifecycle: entry.generation.record.lifecycle.rawValue
                )
            }
            let (count, overflow) = entry.activeLeaseCount
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseBaseExecutionError.leaseCountOverflow(id)
            }
            entry.activeLeaseCount = count
            state.entries[id] = entry
            return entry.generation
        }
        return DatabaseBaseLease(
            generation: generation,
            token: DatabaseBaseLeaseToken { [self] in release(id) },
            permitsDataOperations: true,
            permitsInactiveMaintenance: true
        )
    }

    package func publish(_ generation: DatabaseBaseGeneration) {
        let waiters = state.withLock { state in
            let id = generation.record.id
            let previous = state.entries[id]
            let activeLeaseCount = previous?.activeLeaseCount ?? 0
            let drainWaiters = previous?.drainWaiters ?? []
            let admitsOperations = generation.record.lifecycle == .active
            state.entries[id] = Entry(
                generation: generation,
                admitsOperations: admitsOperations,
                activeLeaseCount: activeLeaseCount,
                drainWaiters: activeLeaseCount == 0 ? [] : drainWaiters
            )
            return activeLeaseCount == 0 ? drainWaiters : []
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    package func stopAdmissionAndDrain(_ id: Base.ID) async throws {
        let needsWait = try state.withLock { state in
            guard var entry = state.entries[id] else {
                throw DatabaseBaseExecutionError.baseNotFound(id)
            }
            entry.admitsOperations = false
            state.entries[id] = entry
            return entry.activeLeaseCount > 0
        }
        guard needsWait else { return }
        await withCheckedContinuation { continuation in
            let shouldResume = state.withLock { state in
                guard var entry = state.entries[id] else {
                    preconditionFailure(
                        "A draining Base generation cannot disappear"
                    )
                }
                guard entry.activeLeaseCount > 0 else {
                    return true
                }
                entry.drainWaiters.append(continuation)
                state.entries[id] = entry
                return false
            }
            if shouldResume {
                continuation.resume()
            }
        }
    }

    package func snapshot() -> [DatabaseBaseGeneration] {
        state.withLock { state in
            state.entries.values
                .map { $0.generation }
                .sorted { $0.record.id < $1.record.id }
        }
    }

    private func release(_ id: Base.ID) {
        let waiters = state.withLock { state in
            guard var entry = state.entries[id] else {
                preconditionFailure("A leased Base generation cannot disappear")
            }
            precondition(
                entry.activeLeaseCount > 0,
                "A Base operation lease must finish exactly once"
            )
            entry.activeLeaseCount -= 1
            guard entry.activeLeaseCount == 0 else {
                state.entries[id] = entry
                return [CheckedContinuation<Void, Never>]()
            }
            let waiters = entry.drainWaiters
            entry.drainWaiters.removeAll(keepingCapacity: false)
            state.entries[id] = entry
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }
}

#endif
