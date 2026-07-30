import DatabaseEngine
import DatabaseWire
import Synchronization

struct SPARQLSubqueryCacheKey: Sendable, Hashable {
    let occurrenceIdentifier: UInt64
    let graphScope: RDFGraphScanScope
}

enum SPARQLSubqueryCacheError: Error, Sendable, Equatable {
    case capacityOverflow
    case mutationVersionOverflow
}

final class SPARQLSubqueryResultCache: Sendable {
    private struct Entry: Sendable {
        let bindings: SPARQLSharedBindingSnapshot
        let reservation: DatabaseIntermediateReservation
    }

    private struct State: Sendable {
        var values: [SPARQLSubqueryCacheKey: Entry] = [:]
        var accountedCapacity = 0
        var mutationVersion: UInt64 = 0
    }

    private enum InsertionOutcome: Sendable {
        case inserted
        case existing(SPARQLSharedBindingSnapshot)
        case retry
    }

    private static let containerByteCount: UInt64 = 64
    private static let entryOwnerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 128

    private let workMeter: DatabaseWorkMeter
    private let containerReservation: DatabaseIntermediateReservation
    private let state: Mutex<State>

    private init(
        workMeter: DatabaseWorkMeter,
        containerReservation: DatabaseIntermediateReservation
    ) {
        self.workMeter = workMeter
        self.containerReservation = containerReservation
        self.state = Mutex(State())
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLSubqueryResultCache {
        let reservation = try workMeter.reserveIntermediate(
            bytes: containerByteCount,
            at: .subqueryCache
        )
        return SPARQLSubqueryResultCache(
            workMeter: workMeter,
            containerReservation: reservation
        )
    }

    func value(
        for key: SPARQLSubqueryCacheKey
    ) -> SPARQLRetainedBindings? {
        let snapshot = state.withLock { state in
            state.values[key]?.bindings
        }
        return snapshot?.retainedBindings()
    }

    /// Moves unique result storage into immutable shared ownership and stores
    /// only a copyable owner handle. The current evaluation and the cache keep
    /// the same Array buffer; no binding payload is materialized.
    func store(
        _ bindings: consuming SPARQLRetainedBindings,
        for key: SPARQLSubqueryCacheKey
    ) throws -> SPARQLRetainedBindings {
        if let existing = value(for: key) {
            return existing
        }

        let sharedOwnership = try bindings.sharingForFanOut(
            at: .subqueryCache
        )
        let snapshot = sharedOwnership.snapshot

        while true {
            let observed = state.withLock { state in
                (
                    state.values[key]?.bindings,
                    state.values.count,
                    state.accountedCapacity,
                    state.mutationVersion
                )
            }
            if let existing = observed.0 {
                return existing.retainedBindings()
            }

            let requiredCount = try Self.checkedIncrement(observed.1)
            let nextMutationVersion = try Self.nextMutationVersion(
                after: observed.3
            )
            let targetCapacity = try Self.targetCapacity(
                current: observed.2,
                requiredCount: requiredCount
            )
            let additionalCapacity = targetCapacity - observed.2
            let capacityBytes = try Self.checkedMultiply(
                UInt64(additionalCapacity),
                Self.capacitySlotByteCount
            )
            let reservation = try workMeter.reserveIntermediate(
                bytes: try Self.checkedAdd(
                    Self.entryOwnerByteCount,
                    capacityBytes
                ),
                at: .subqueryCache
            )

            let outcome = state.withLock { state -> InsertionOutcome in
                if let existing = state.values[key]?.bindings {
                    return .existing(existing)
                }
                guard state.values.count == observed.1,
                      state.accountedCapacity == observed.2,
                      state.mutationVersion == observed.3 else {
                    return .retry
                }
                if targetCapacity != state.accountedCapacity {
                    state.values.reserveCapacity(targetCapacity)
                    state.accountedCapacity = targetCapacity
                }
                state.values[key] = Entry(
                    bindings: snapshot,
                    reservation: reservation
                )
                state.mutationVersion = nextMutationVersion
                return .inserted
            }

            switch outcome {
            case .inserted:
                return consume sharedOwnership.retained
            case .existing(let existing):
                reservation.release()
                return existing.retainedBindings()
            case .retry:
                reservation.release()
                continue
            }
        }
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        guard current > 0 else { return 1 }
        let (doubled, overflow) = current.multipliedReportingOverflow(by: 2)
        guard !overflow else {
            throw SPARQLSubqueryCacheError.capacityOverflow
        }
        return max(requiredCount, doubled)
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLSubqueryCacheError.capacityOverflow
        }
        return result
    }

    /// Advances the optimistic insertion epoch without permitting ABA through
    /// integer wraparound.
    static func nextMutationVersion(after value: UInt64) throws -> UInt64 {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLSubqueryCacheError.mutationVersionOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else {
            throw SPARQLSubqueryCacheError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw SPARQLSubqueryCacheError.capacityOverflow
        }
        return result
    }
}
