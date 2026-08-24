import DatabaseKit
import DatabaseTypes
import Synchronization

/// Revocable Engine-owned mapping from graph vertices to result candidates.
///
/// The graph feature can submit only semantic vertex identifiers and hop
/// counts. Result rows, primary keys, candidate restriction, Top-K ordering,
/// and retained output remain inaccessible outside DatabaseEngine.
package final class FusionConnectedMatchSink: Sendable {
    private struct State: Sendable {
        var isActive = true
        var topK: TopK?
    }

    private let candidates: FusionCandidateDomain
    private let resultFieldName: String
    private let workMeter: DatabaseWorkMeter
    private let state: Mutex<State>

    init(
        candidates: FusionCandidateDomain,
        resultFieldName: String,
        limit: Int,
        workMeter: DatabaseWorkMeter
    ) throws {
        self.candidates = candidates
        self.resultFieldName = resultFieldName
        self.workMeter = workMeter
        self.state = Mutex(
            State(topK: try TopK(
                limit: limit,
                candidates: candidates,
                workMeter: workMeter
            ))
        )
    }

    /// Considers every result candidate whose declared result field denotes
    /// this vertex. The feature observes only whether the final limit has been
    /// reached, never the matching rows or their count.
    package func submit(
        vertexIdentifier: String,
        hops: UInt64
    ) throws {
        try state.withLock { state in
            guard state.isActive, var topK = state.topK else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            defer { state.topK = topK }
            for candidateIndex in 0..<candidates.count {
                try workMeter.consume(at: .bindingCandidate)
                let matches = try candidates.withEntry(
                    at: candidateIndex
                ) { candidate in
                    guard let value = candidate.row.fields[resultFieldName]
                    else {
                        throw FusionExecutionContractError
                            .missingConnectedResultField(
                                field: resultFieldName
                            )
                    }
                    switch value {
                    case .null:
                        return false
                    case .string(let value):
                        try DatabaseByteProcessingMeter.consume(
                            byteCount: max(
                                value.utf8.count,
                                vertexIdentifier.utf8.count
                            ),
                            workMeter: workMeter,
                            stage: .bindingCandidate
                        )
                        return value == vertexIdentifier
                    default:
                        throw FusionExecutionContractError
                            .invalidConnectedResultValue(
                                field: resultFieldName
                            )
                    }
                }
                if matches {
                    try topK.consider(
                        candidateIndex: candidateIndex,
                        hops: hops
                    )
                }
            }
        }
    }

    /// True only after the bounded result heap contains the requested count.
    /// A breadth-first executor may stop before a deeper level, but never in
    /// the middle of the current level where equal-hop ties remain possible.
    package var hasReachedLimit: Bool {
        state.withLock { state in
            guard state.isActive, let topK = state.topK else { return false }
            return topK.isFull
        }
    }

    func freeze(into output: FusionMatchSink) throws -> Int {
        var topK: TopK = try state.withLock { state in
            guard state.isActive, let topK = state.topK else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            state.isActive = false
            state.topK = nil
            return topK
        }
        return try topK.emit(into: output)
    }

    func invalidate() {
        state.withLock { state in
            state.isActive = false
            state.topK = nil
        }
    }
}

private extension FusionConnectedMatchSink {
    struct TopK: Sendable {
        struct Match: Sendable {
            let candidateIndex: Int
            let hops: UInt64
        }

        private var matches: [Match] = []
        private let limit: Int
        private let candidates: FusionCandidateDomain
        private let workMeter: DatabaseWorkMeter
        private let layout: DatabaseRetainedArrayLayout
        private let reservation: DatabaseIntermediateReservation
        private var accountedCapacity = 0

        init(
            limit: Int,
            candidates: FusionCandidateDomain,
            workMeter: DatabaseWorkMeter
        ) throws {
            precondition(limit >= 0)
            self.limit = limit
            self.candidates = candidates
            self.workMeter = workMeter
            self.layout = try DatabaseRetainedArrayLayout.forElement(
                Match.self
            )
            self.reservation = try workMeter.reserveIntermediate(
                bytes: layout.containerByteCount,
                at: .indexScan
            )
        }

        var isFull: Bool {
            matches.count == limit
        }

        mutating func consider(
            candidateIndex: Int,
            hops: UInt64
        ) throws {
            guard limit > 0 else { return }
            for existing in matches {
                try workMeter.consume(at: .bindingCandidate)
                if existing.candidateIndex == candidateIndex {
                    guard existing.hops == hops else {
                        throw FusionExecutionContractError
                            .executionContractViolation
                    }
                    return
                }
            }
            let proposed = Match(
                candidateIndex: candidateIndex,
                hops: hops
            )
            if matches.count < limit {
                let requiredCount = matches.count + 1
                let growth = try layout.growth(
                    from: accountedCapacity,
                    toFit: requiredCount
                )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: growth.additionalByteCount,
                    at: .indexScan
                )
                if growth.capacity != accountedCapacity {
                    matches.reserveCapacity(growth.capacity)
                    accountedCapacity = growth.capacity
                }
                matches.append(proposed)
                try siftUp(from: matches.count - 1)
                return
            }
            try workMeter.consume(at: .sortComparison)
            guard try isBetter(proposed, than: matches[0]) else { return }
            matches[0] = proposed
            try siftDown(from: 0, through: matches.count - 1)
        }

        mutating func emit(into output: FusionMatchSink) throws -> Int {
            if matches.count > 1 {
                var end = matches.count - 1
                while end > 0 {
                    matches.swapAt(0, end)
                    end -= 1
                    try siftDown(from: 0, through: end)
                }
            }
            for match in matches {
                guard let signal = Double(exactly: match.hops) else {
                    throw FusionExecutionContractError.invalidScoreSignal
                }
                try output.submit(
                    primaryKey: candidates.primaryKey(
                        at: match.candidateIndex
                    ),
                    numericSignal: signal
                )
            }
            return matches.count
        }

        private mutating func siftUp(from start: Int) throws {
            var child = start
            while child > 0 {
                let parent = (child - 1) / 2
                try workMeter.consume(at: .sortComparison)
                guard try isBetter(matches[parent], than: matches[child])
                else { return }
                matches.swapAt(parent, child)
                child = parent
            }
        }

        private mutating func siftDown(
            from start: Int,
            through end: Int
        ) throws {
            var parent = start
            while true {
                let left = parent * 2 + 1
                guard left <= end else { return }
                let right = left + 1
                var worseChild = left
                if right <= end {
                    try workMeter.consume(at: .sortComparison)
                    if try isBetter(matches[left], than: matches[right]) {
                        worseChild = right
                    }
                }
                try workMeter.consume(at: .sortComparison)
                guard try isBetter(
                    matches[parent],
                    than: matches[worseChild]
                ) else { return }
                matches.swapAt(parent, worseChild)
                parent = worseChild
            }
        }

        private func isBetter(
            _ lhs: borrowing Match,
            than rhs: borrowing Match
        ) throws -> Bool {
            if lhs.hops != rhs.hops {
                return lhs.hops < rhs.hops
            }
            return try FusionCandidateDomain.primaryKeyOrder(
                candidates.primaryKey(at: lhs.candidateIndex),
                candidates.primaryKey(at: rhs.candidateIndex),
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
    }
}
