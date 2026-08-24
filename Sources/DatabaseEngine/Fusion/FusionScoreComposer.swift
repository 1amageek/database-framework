import DatabaseKit
import DatabaseTypes

/// Combines index-native observations after the final candidate domain is known.
enum FusionScoreComposer {
    private struct ScoredEntry: Sendable {
        let candidate: FusionCandidateDomain.Entry
        let score: Double
    }

    static func compose(
        candidates: FusionCandidateDomain,
        scoredInputs: DatabaseSharedRetainedArray<FusionScoredInput>,
        strategy: FusionStrategy,
        maximumResultCount: Int?,
        workMeter: DatabaseWorkMeter
    ) throws -> IndexReadResult {
        if maximumResultCount == 0 {
            return try IndexReadResult.build(
                workMeter: workMeter,
                ordering: .orderedByIndex,
                expectedCount: 0
            ) { _ in }
        }
        let scoreLayout = try DatabaseRetainedArrayLayout.forElement(
            Double?.self
        )
        let scoreGrowth = try scoreLayout.growth(
            from: 0,
            toFit: candidates.count
        )
        let scoreStorage = try DatabaseIntermediateFootprint(
            bytes: scoreLayout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: scoreGrowth.additionalByteCount
            )
        )
        let scoreReservation = try workMeter.reserveIntermediate(
            rows: UInt64(candidates.count),
            bytes: scoreStorage.bytes,
            at: .projection
        )
        defer { scoreReservation.release() }
        var combinedScores = [Double?](
            repeating: nil,
            count: candidates.count
        )
        try accumulateScores(
            into: &combinedScores,
            candidates: candidates,
            scoredInputs: scoredInputs,
            strategy: strategy,
            workMeter: workMeter
        )

        if let maximumResultCount {
            return try composeTopK(
                candidates: candidates,
                combinedScores: combinedScores,
                limit: maximumResultCount,
                workMeter: workMeter
            )
        }

        var builder = try DatabaseRetainedArrayBuilder<ScoredEntry>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                ScoredEntry.self
            ),
            expectedCount: candidates.count
        )
        var candidateIndex = 0
        try candidates.forEachEntry { candidate in
            guard let score = combinedScores[candidateIndex] else {
                throw FusionExecutionContractError.missingFusedScore(
                    candidate.packedPrimaryKey
                )
            }
            candidateIndex += 1
            guard score.isFinite else {
                throw FusionExecutionContractError.scoreOverflow(
                    candidate.packedPrimaryKey
                )
            }
            try builder.append(
                footprint: try candidate.rowFootprint.adding(
                    DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(candidate.packedPrimaryKey.count) + 16
                    )
                )
            ) {
                ScoredEntry(candidate: candidate, score: score)
            }
        }
        let sorted = try builder.finish().sortingElements { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return try isBetter(lhs, than: rhs, workMeter: workMeter)
        }
        return try sorted.withSpan { entries in
            try IndexReadResult.build(
                workMeter: workMeter,
                ordering: .orderedByIndex,
                expectedCount: entries.count
            ) { rows in
                for index in 0..<entries.count {
                    let entry = entries[index]
                    let scoreValue = FieldValue.float64(entry.score)
                    let footprint = try CanonicalRelationalFootprintMeter
                        .footprint(
                            entry.candidate.rowFootprint,
                            appendingAnnotationNamed:
                                FusionExecutor.scoreAnnotation,
                            value: scoreValue,
                            workMeter: workMeter
                        )
                    try rows.append(footprint: footprint) {
                        var annotations = entry.candidate.row.annotations
                        annotations[FusionExecutor.scoreAnnotation] = scoreValue
                        return IndexReadRow(
                            fields: entry.candidate.row.fields,
                            annotations: annotations,
                            version: entry.candidate.row.version
                        )
                    }
                }
            }
        }
    }

    private static func composeTopK(
        candidates: FusionCandidateDomain,
        combinedScores: [Double?],
        limit: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> IndexReadResult {
        var topK = try TopK(limit: limit, workMeter: workMeter)
        var candidateIndex = 0
        try candidates.forEachEntry { candidate in
            guard let score = combinedScores[candidateIndex] else {
                throw FusionExecutionContractError.missingFusedScore(
                    candidate.packedPrimaryKey
                )
            }
            candidateIndex += 1
            guard score.isFinite else {
                throw FusionExecutionContractError.scoreOverflow(
                    candidate.packedPrimaryKey
                )
            }
            try topK.consider(
                ScoredEntry(candidate: candidate, score: score)
            )
        }
        return try topK.finish(workMeter: workMeter)
    }

    private static func accumulateScores(
        into combinedScores: inout [Double?],
        candidates: FusionCandidateDomain,
        scoredInputs: DatabaseSharedRetainedArray<FusionScoredInput>,
        strategy: FusionStrategy,
        workMeter: DatabaseWorkMeter
    ) throws {
        for (sourceIndex, input) in scoredInputs.enumerated() {
            let normalization = try normalization(
                for: input,
                workMeter: workMeter
            )
            for rank in input.result.matches.indices {
                try workMeter.consume(at: .projection)
                let match = input.result.matches[rank]
                guard let candidateIndex = try candidates.index(
                    forPrimaryKey: match.primaryKey,
                    workMeter: workMeter
                ) else {
                    continue
                }
                let contribution = try contribution(
                    from: input,
                    rank: rank,
                    normalization: normalization,
                    strategy: strategy
                )
                let weighted: Double
                if case .weighted(let weights) = strategy {
                    weighted = contribution * weights[sourceIndex]
                } else {
                    weighted = contribution
                }
                guard weighted.isFinite else {
                    throw FusionExecutionContractError.scoreOverflow(match.primaryKey)
                }
                if case .maximum = strategy {
                    combinedScores[candidateIndex] = max(
                        combinedScores[candidateIndex] ?? -Double.infinity,
                        weighted
                    )
                } else {
                    let combined = (combinedScores[candidateIndex] ?? 0)
                        + weighted
                    guard combined.isFinite else {
                        throw FusionExecutionContractError.scoreOverflow(
                            match.primaryKey
                        )
                    }
                    combinedScores[candidateIndex] = combined
                }
            }
        }
    }

    private static func isBetter(
        _ lhs: ScoredEntry,
        than rhs: ScoredEntry,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        if lhs.score == rhs.score {
            return try FusionCandidateDomain.primaryKeyOrder(
                lhs.candidate.packedPrimaryKey,
                rhs.candidate.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        return lhs.score > rhs.score
    }

    private struct TopK {
        private var entries: [ScoredEntry] = []
        private let limit: Int
        private let reservation: DatabaseIntermediateReservation
        private let layout: DatabaseRetainedArrayLayout
        private let workMeter: DatabaseWorkMeter
        private var accountedCapacity = 0

        init(limit: Int, workMeter: DatabaseWorkMeter) throws {
            precondition(limit > 0)
            let layout = try DatabaseRetainedArrayLayout.forElement(
                ScoredEntry.self
            )
            self.limit = limit
            self.workMeter = workMeter
            self.layout = layout
            self.reservation = try workMeter.reserveIntermediate(
                bytes: layout.containerByteCount,
                at: .projection
            )
        }

        mutating func consider(_ entry: ScoredEntry) throws {
            try workMeter.consume(at: .projection)
            if entries.count < limit {
                let requiredCount = entries.count + 1
                let growth = try layout.growth(
                    from: accountedCapacity,
                    toFit: requiredCount
                )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: try DatabaseIntermediateFootprint(
                        bytes: UInt64(entry.candidate.packedPrimaryKey.count)
                    ).adding(
                        DatabaseIntermediateFootprint(
                            bytes: growth.additionalByteCount
                        )
                    ).bytes,
                    at: .projection
                )
                if growth.capacity != accountedCapacity {
                    entries.reserveCapacity(growth.capacity)
                    accountedCapacity = growth.capacity
                }
                entries.append(entry)
                try siftUp(from: entries.count - 1)
                return
            }
            try workMeter.consume(at: .sortComparison)
            guard try FusionScoreComposer.isBetter(
                entry,
                than: entries[0],
                workMeter: workMeter
            ) else {
                return
            }
            let oldKeyBytes = UInt64(
                entries[0].candidate.packedPrimaryKey.count
            )
            let newKeyBytes = UInt64(entry.candidate.packedPrimaryKey.count)
            if newKeyBytes > oldKeyBytes {
                try reservation.reserveAdditional(
                    bytes: newKeyBytes - oldKeyBytes,
                    at: .projection
                )
            }
            entries[0] = entry
            if newKeyBytes < oldKeyBytes {
                try reservation.releasePartial(
                    bytes: oldKeyBytes - newKeyBytes
                )
            }
            try siftDown(from: 0, through: entries.count - 1)
        }

        mutating func finish(
            workMeter: DatabaseWorkMeter
        ) throws -> IndexReadResult {
            if entries.count > 1 {
                var end = entries.count - 1
                while end > 0 {
                    entries.swapAt(0, end)
                    end -= 1
                    try Self.siftDown(
                        entries: &entries,
                        from: 0,
                        through: end,
                        workMeter: workMeter
                    )
                }
            }
            return try IndexReadResult.build(
                workMeter: workMeter,
                ordering: .orderedByIndex,
                expectedCount: entries.count
            ) { rows in
                for entry in entries {
                    let scoreValue = FieldValue.float64(entry.score)
                    let footprint = try CanonicalRelationalFootprintMeter
                        .footprint(
                            entry.candidate.rowFootprint,
                            appendingAnnotationNamed:
                                FusionExecutor.scoreAnnotation,
                            value: scoreValue,
                            workMeter: workMeter
                        )
                    try rows.append(footprint: footprint) {
                        var annotations = entry.candidate.row.annotations
                        annotations[FusionExecutor.scoreAnnotation] = scoreValue
                        return IndexReadRow(
                            fields: entry.candidate.row.fields,
                            annotations: annotations,
                            version: entry.candidate.row.version
                        )
                    }
                }
            }
        }

        private mutating func siftUp(from start: Int) throws {
            var child = start
            while child > 0 {
                let parent = (child - 1) / 2
                try workMeter.consume(at: .sortComparison)
                guard try FusionScoreComposer.isBetter(
                    entries[parent],
                    than: entries[child],
                    workMeter: workMeter
                ) else {
                    return
                }
                entries.swapAt(parent, child)
                child = parent
            }
        }

        private mutating func siftDown(
            from start: Int,
            through end: Int
        ) throws {
            try Self.siftDown(
                entries: &entries,
                from: start,
                through: end,
                workMeter: workMeter
            )
        }

        private static func siftDown(
            entries: inout [ScoredEntry],
            from start: Int,
            through end: Int,
            workMeter: DatabaseWorkMeter
        ) throws {
            var parent = start
            while true {
                let left = parent * 2 + 1
                guard left <= end else { return }
                let right = left + 1
                var worseChild = left
                if right <= end {
                    try workMeter.consume(at: .sortComparison)
                    if try FusionScoreComposer.isBetter(
                        entries[left],
                        than: entries[right],
                        workMeter: workMeter
                    ) {
                        worseChild = right
                    }
                }
                try workMeter.consume(at: .sortComparison)
                guard try FusionScoreComposer.isBetter(
                    entries[parent],
                    than: entries[worseChild],
                    workMeter: workMeter
                ) else {
                    return
                }
                entries.swapAt(parent, worseChild)
                parent = worseChild
            }
        }
    }

    private struct Normalization: Sendable {
        let minimum: Double
        let maximum: Double
    }

    private static func normalization(
        for input: FusionScoredInput,
        workMeter: DatabaseWorkMeter
    ) throws -> Normalization? {
        guard case .annotation = input.scoring else { return nil }
        var minimum = Double.infinity
        var maximum = -Double.infinity
        for match in input.result.matches {
            try workMeter.consume(at: .projection)
            guard let value = match.numericSignal, value.isFinite else {
                throw FusionExecutionContractError.invalidScoreSignal
            }
            minimum = min(minimum, value)
            maximum = max(maximum, value)
        }
        return Normalization(minimum: minimum, maximum: maximum)
    }

    private static func contribution(
        from input: FusionScoredInput,
        rank: Int,
        normalization: Normalization?,
        strategy: FusionStrategy
    ) throws -> Double {
        if case .reciprocalRank(let rankConstant) = strategy {
            return 1.0 / (Double(rankConstant) + Double(rank) + 1.0)
        }
        switch input.scoring {
        case .position:
            return 1.0 / (Double(rank) + 1.0)
        case .annotation(_, let order):
            guard let signal = input.result.matches[rank].numericSignal else {
                throw FusionExecutionContractError.invalidScoreSignal
            }
            guard let normalization else {
                throw FusionExecutionContractError.invalidScoreSignal
            }
            let minimum = normalization.minimum
            let maximum = normalization.maximum
            guard minimum != maximum else { return 1 }
            switch order {
            case .higherIsBetter:
                return (signal - minimum) / (maximum - minimum)
            case .lowerIsBetter:
                return (maximum - signal) / (maximum - minimum)
            }
        }
    }
}
