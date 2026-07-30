import DatabaseEngine
import Synchronization

public final class GraphAlgorithmWorkBudget: Sendable {
    private struct State: Sendable {
        var consumedComputeUnits: UInt64 = 0
        var consumedPhysicalReads: UInt64 = 0
        var reservedPhysicalReads: UInt64 = 0
        var limitReached = false
    }

    public let maximumWorkUnits: UInt64
    private let state = Mutex(State())

    public init(maximumWorkUnits: UInt64) {
        self.maximumWorkUnits = maximumWorkUnits
    }

    public func consume(_ units: UInt64 = 1) throws -> Bool {
        try ensureDatabaseTaskIsActive()
        return state.withLock { state in
            guard !state.limitReached else { return false }
            guard units <= maximumWorkUnits - state.consumedComputeUnits else {
                state.limitReached = true
                return false
            }
            state.consumedComputeUnits += units
            return true
        }
    }

    public var consumedWorkUnits: UInt64 {
        state.withLock {
            max($0.consumedComputeUnits, $0.consumedPhysicalReads)
        }
    }

    public var limitReason: LimitReason? {
        state.withLock { state in
            guard state.limitReached else { return nil }
            return .maxWorkUnitsReached(
                consumed: max(
                    state.consumedComputeUnits,
                    state.consumedPhysicalReads
                ),
                limit: maximumWorkUnits
            )
        }
    }

    package func reservePhysicalReads() throws -> GraphPhysicalReadReservation? {
        try ensureDatabaseTaskIsActive()
        let allocation = state.withLock { state -> UInt64 in
            guard !state.limitReached else { return 0 }
            let committed = state.consumedPhysicalReads + state.reservedPhysicalReads
            guard committed < maximumWorkUnits else {
                state.limitReached = true
                return 0
            }
            let available = maximumWorkUnits - committed
            let allocation = min(available, UInt64(Int.max))
            state.reservedPhysicalReads += allocation
            return allocation
        }
        guard allocation > 0 else { return nil }
        return GraphPhysicalReadReservation(
            budget: self,
            allocation: allocation
        )
    }

    fileprivate func settlePhysicalReads(
        allocation: UInt64,
        charged: UInt64,
        exhaustedReservation: Bool
    ) {
        state.withLock { state in
            guard allocation <= state.reservedPhysicalReads,
                  charged <= allocation else {
                state.limitReached = true
                return
            }
            state.reservedPhysicalReads -= allocation
            state.consumedPhysicalReads += charged
            if exhaustedReservation {
                state.limitReached = true
            }
        }
    }
}

package final class GraphPhysicalReadReservation: Sendable {
    private struct State: Sendable {
        var observedReads: UInt64 = 0
        var settled = false
    }

    package let limit: Int
    private let budget: GraphAlgorithmWorkBudget
    private let allocation: UInt64
    private let state = Mutex(State())

    fileprivate init(
        budget: GraphAlgorithmWorkBudget,
        allocation: UInt64
    ) {
        self.budget = budget
        self.allocation = allocation
        self.limit = Int(allocation)
    }

    package func recordPhysicalRead() throws {
        try ensureDatabaseTaskIsActive()
        try state.withLock { state in
            guard !state.settled,
                  state.observedReads < allocation else {
                throw GraphAlgorithmWorkBudgetError.invalidPhysicalReadReservation
            }
            state.observedReads += 1
        }
    }

    package func finishAtRangeEnd() {
        settle(chargeFullAllocation: false)
    }

    deinit {
        // A cursor abandoned before its natural end may already have prefetched
        // up to its backend limit, so only the full reservation is safe to charge.
        settle(chargeFullAllocation: true)
    }

    private func settle(chargeFullAllocation: Bool) {
        let settlement = state.withLock { state -> (UInt64, Bool)? in
            guard !state.settled else { return nil }
            state.settled = true
            let charged = chargeFullAllocation
                ? allocation
                : state.observedReads
            return (charged, charged == allocation)
        }
        guard let settlement else { return }
        budget.settlePhysicalReads(
            allocation: allocation,
            charged: settlement.0,
            exhaustedReservation: settlement.1
        )
    }
}

public enum GraphAlgorithmWorkBudgetError: Error, Sendable {
    case invalidPhysicalReadReservation
}
