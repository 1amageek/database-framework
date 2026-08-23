import DatabaseKit

package enum CanonicalFusionSignal: Sendable, Equatable {
    case higherIsBetter(Double)
    case lowerIsBetter(Double)
    case position
}

package enum CanonicalFusionAlgebraError<Identity>: Error, Sendable
where Identity: Hashable & Sendable {
    case weightCountMismatch(expected: Int, actual: Int)
    case nonFiniteWeight(index: Int)
    case negativeWeight(index: Int)
    case duplicateIdentity(sourceIndex: Int, identity: Identity)
    case inconsistentPayload(identity: Identity)
    case inconsistentSignal(sourceIndex: Int)
    case nonFiniteSignal(sourceIndex: Int)
    case unorderedRankSource(sourceIndex: Int)
    case scoreOverflow(identity: Identity)
    case inputCountOverflow
}

package struct CanonicalFusionAlgebraResult<Identity, Payload>: Sendable
where Identity: Hashable & Comparable & Sendable, Payload: Sendable {
    package let identity: Identity
    package let payload: Payload
    package let score: Double
}

/// One implementation of fusion validation, normalization, accumulation, and
/// deterministic ordering shared by typed builders and canonical QueryIR.
package enum CanonicalFusionAlgebra {
    private enum SignalKind: Equatable {
        case higher
        case lower
        case position
    }

    private struct Accumulated<Identity, Payload>: Sendable
    where Identity: Hashable & Comparable & Sendable, Payload: Sendable {
        let identity: Identity
        let payload: Payload
        var score: Double?
    }

    package static func fuse<Sources, Source, OrderedSources, Identity, Payload>(
        sources: Sources,
        orderedSources: OrderedSources,
        strategy: FusionStrategy,
        isEligible: (borrowing Payload) -> Bool,
        workMeter: DatabaseWorkMeter,
        identity: (borrowing Payload) throws -> Identity,
        signal: (borrowing Payload) throws -> CanonicalFusionSignal,
        payloadsAreEquivalent: (
            borrowing Payload,
            borrowing Payload
        ) throws -> Bool
    ) throws -> DatabaseSharedRetainedArray<
        CanonicalFusionAlgebraResult<Identity, Payload>
    > where Sources: Collection,
            Sources.Element == Source,
            Source: Collection,
            Source.Element == Payload,
            OrderedSources: Collection,
            OrderedSources.Element == Bool,
            Identity: Hashable & Comparable & Sendable,
            Payload: Sendable {
        try validate(
            strategy: strategy,
            sourceCount: sources.count,
            identity: Identity.self
        )
        guard orderedSources.count == sources.count else {
            throw CanonicalFusionAlgebraError<Identity>.inputCountOverflow
        }

        let totalCount = try sources.reduce(into: 0) { count, source in
            let (next, overflow) = count.addingReportingOverflow(source.count)
            guard !overflow else {
                throw CanonicalFusionAlgebraError<Identity>.inputCountOverflow
            }
            count = next
        }
        let accumulatedReservation = try workMeter.reserveIntermediate(
            rows: UInt64(totalCount),
            bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(1, MemoryLayout<Identity>.stride)
                        + max(
                            1,
                            MemoryLayout<
                                Accumulated<Identity, Payload>
                            >.stride
                        )
                        + 64
                )
            ).multiplied(by: UInt64(totalCount)).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(
                        MemoryLayout<[
                            Identity: Accumulated<Identity, Payload>
                        ]>.stride
                    )
                )
            ).bytes,
            at: .deduplication
        )
        defer { accumulatedReservation.release() }
        var accumulated: [Identity: Accumulated<Identity, Payload>] = [:]
        accumulated.reserveCapacity(totalCount)

        for (sourceIndex, source) in sources.enumerated() {
            guard !source.isEmpty else { continue }
            try workMeter.consume(UInt64(source.count), at: .deduplication)
            let sourceIdentityReservation = try workMeter.reserveIntermediate(
                rows: UInt64(source.count),
                bytes: try DatabaseIntermediateFootprint(
                    bytes: UInt64(max(1, MemoryLayout<Identity>.stride)) + 48
                ).multiplied(by: UInt64(source.count)).adding(
                    DatabaseIntermediateFootprint(
                        bytes: UInt64(MemoryLayout<Set<Identity>>.stride)
                    )
                ).bytes,
                at: .deduplication
            )
            defer { sourceIdentityReservation.release() }
            var seen = Set<Identity>()
            seen.reserveCapacity(source.count)

            var sourceKind: SignalKind?
            var minimum = Double.infinity
            var maximum = -Double.infinity
            var eligibleCount = 0
            for payload in source {
                let itemIdentity = try identity(payload)
                guard seen.insert(itemIdentity).inserted else {
                    throw CanonicalFusionAlgebraError<Identity>
                        .duplicateIdentity(
                            sourceIndex: sourceIndex,
                            identity: itemIdentity
                        )
                }
                if let existing = accumulated[itemIdentity] {
                    let equivalent = try payloadsAreEquivalent(
                        existing.payload,
                        payload
                    )
                    guard equivalent else {
                        throw CanonicalFusionAlgebraError<Identity>
                            .inconsistentPayload(identity: itemIdentity)
                    }
                } else {
                    accumulated[itemIdentity] = Accumulated(
                        identity: itemIdentity,
                        payload: payload,
                        score: nil
                    )
                }

                let itemSignal = try signal(payload)
                let kind = signalKind(itemSignal)
                if let sourceKind, sourceKind != kind {
                    throw CanonicalFusionAlgebraError<Identity>
                        .inconsistentSignal(sourceIndex: sourceIndex)
                }
                sourceKind = kind
                let eligible = isEligible(payload)
                if eligible { eligibleCount += 1 }
                if case .position = itemSignal {
                    continue
                }
                let value = signalValue(itemSignal)
                guard value.isFinite else {
                    throw CanonicalFusionAlgebraError<Identity>
                        .nonFiniteSignal(sourceIndex: sourceIndex)
                }
                guard eligible else { continue }
                minimum = Swift.min(minimum, value)
                maximum = Swift.max(maximum, value)
            }

            let kind = sourceKind ?? .position
            if kind == .position {
                minimum = 0
                maximum = Double(Swift.max(0, eligibleCount - 1))
            }
            let orderIndex = orderedSources.index(
                orderedSources.startIndex,
                offsetBy: sourceIndex
            )
            if !orderedSources[orderIndex],
               kind == .position || isReciprocalRank(strategy) {
                throw CanonicalFusionAlgebraError<Identity>
                    .unorderedRankSource(sourceIndex: sourceIndex)
            }

            var eligibleRank = 0
            for payload in source {
                let itemIdentity = try identity(payload)
                guard isEligible(payload) else {
                    continue
                }
                let contribution = try contribution(
                    signal: try signal(payload),
                    rank: eligibleRank,
                    minimum: minimum,
                    maximum: maximum,
                    strategy: strategy,
                    sourceIndex: sourceIndex,
                    identity: itemIdentity
                )
                eligibleRank += 1
                guard var existing = accumulated[itemIdentity] else {
                    preconditionFailure(
                        "Fusion validation must retain every source identity"
                    )
                }
                if let currentScore = existing.score {
                    switch strategy {
                    case .maximum:
                        existing.score = Swift.max(
                            currentScore,
                            contribution
                        )
                    case .reciprocalRank, .sum, .weighted:
                        existing.score = currentScore + contribution
                    }
                    guard existing.score?.isFinite == true else {
                        throw CanonicalFusionAlgebraError<Identity>
                            .scoreOverflow(identity: itemIdentity)
                    }
                } else {
                    existing.score = contribution
                }
                accumulated[itemIdentity] = existing
            }
        }

        var builder = try DatabaseRetainedArrayBuilder<
            CanonicalFusionAlgebraResult<Identity, Payload>
        >(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: CanonicalFusionAlgebraResult<Identity, Payload>.self
            ),
            expectedCount: accumulated.count
        )
        for value in accumulated.values {
            guard let score = value.score else { continue }
            try builder.append(
                footprint: DatabaseIntermediateFootprint(bytes: 64),
                at: .sortInput
            ) {
                CanonicalFusionAlgebraResult(
                    identity: value.identity,
                    payload: value.payload,
                    score: score
                )
            }
        }
        try workMeter.consume(UInt64(accumulated.count), at: .sortInput)
        let sorted = try builder.finish().sortingElements { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            if lhs.score == rhs.score {
                return lhs.identity < rhs.identity
            }
            return lhs.score > rhs.score
        }
        return try sorted.moveToSharedOwnership(at: .sortInput)
    }

    private static func validate<Identity>(
        strategy: FusionStrategy,
        sourceCount: Int,
        identity _: Identity.Type
    ) throws where Identity: Hashable & Sendable {
        guard case .weighted(let weights) = strategy else { return }
        guard weights.count == sourceCount else {
            throw CanonicalFusionAlgebraError<Identity>.weightCountMismatch(
                expected: sourceCount,
                actual: weights.count
            )
        }
        for (index, weight) in weights.enumerated() where !weight.isFinite {
            throw CanonicalFusionAlgebraError<Identity>
                .nonFiniteWeight(index: index)
        }
        for (index, weight) in weights.enumerated() where weight < 0 {
            throw CanonicalFusionAlgebraError<Identity>
                .negativeWeight(index: index)
        }
    }

    private static func contribution<Identity>(
        signal: CanonicalFusionSignal,
        rank: Int,
        minimum: Double,
        maximum: Double,
        strategy: FusionStrategy,
        sourceIndex: Int,
        identity: Identity
    ) throws -> Double where Identity: Hashable & Sendable {
        if case .reciprocalRank(let rankConstant) = strategy {
            return 1.0 / (Double(rankConstant) + Double(rank) + 1.0)
        }
        let normalized: Double
        switch signal {
        case .position:
            normalized = maximum == minimum
                ? 1
                : (maximum - Double(rank)) / (maximum - minimum)
        case .higherIsBetter(let value):
            normalized = maximum == minimum
                ? 1
                : (value - minimum) / (maximum - minimum)
        case .lowerIsBetter(let value):
            normalized = maximum == minimum
                ? 1
                : (maximum - value) / (maximum - minimum)
        }
        guard normalized.isFinite else {
            throw CanonicalFusionAlgebraError<Identity>
                .scoreOverflow(identity: identity)
        }
        if case .weighted(let weights) = strategy {
            let weighted = normalized * weights[sourceIndex]
            guard weighted.isFinite else {
                throw CanonicalFusionAlgebraError<Identity>
                    .scoreOverflow(identity: identity)
            }
            return weighted
        }
        return normalized
    }

    private static func signalKind(
        _ signal: CanonicalFusionSignal
    ) -> SignalKind {
        switch signal {
        case .higherIsBetter: .higher
        case .lowerIsBetter: .lower
        case .position: .position
        }
    }

    private static func signalValue(
        _ signal: CanonicalFusionSignal
    ) -> Double {
        switch signal {
        case .higherIsBetter(let value), .lowerIsBetter(let value): value
        case .position: 0
        }
    }

    private static func isReciprocalRank(
        _ strategy: FusionStrategy
    ) -> Bool {
        if case .reciprocalRank = strategy { return true }
        return false
    }
}
