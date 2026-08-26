import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

/// Engine-owned, revocable output boundary for one physical Fusion input.
///
/// It rejects identities outside the incoming domain, duplicates, invalid
/// scores, and output beyond the admitted input limit before retaining data.
package final class FusionMatchSink: Sendable {
    private struct State: Sendable {
        var isActive = true
        var candidates: FusionCandidateDomain?
        var matches: [FusionIndexMatch]
        var primaryKeys: Set<ByteString>
        var accountedMatchCapacity: Int
        var accountedHashCapacity: Int
        var reservation: DatabaseIntermediateReservation?
    }

    private struct PrimaryKeySlot: Sendable {
        let primaryKey: ByteString
    }

    private let scoring: FusionScoring?
    private let limit: Int
    private let workMeter: DatabaseWorkMeter
    private let layout: DatabaseRetainedArrayLayout
    private let hashLayout: DatabaseRetainedHashTableLayout
    private let state: Mutex<State>

    init(
        candidates: FusionCandidateDomain?,
        scoring: FusionScoring?,
        limit: Int,
        workMeter: DatabaseWorkMeter
    ) throws {
        precondition(limit >= 0)
        let layout = try DatabaseRetainedArrayLayout.forElement(FusionIndexMatch.self)
        let hashLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(MemoryLayout<Set<ByteString>>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<PrimaryKeySlot>.stride)
            )
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: layout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: hashLayout.containerByteCount
                )
            ).bytes,
            at: .indexScan
        )
        self.scoring = scoring
        self.limit = limit
        self.workMeter = workMeter
        self.layout = layout
        self.hashLayout = hashLayout
        self.state = Mutex(
            State(
                candidates: candidates,
                matches: [],
                primaryKeys: [],
                accountedMatchCapacity: 0,
                accountedHashCapacity: 0,
                reservation: reservation
            )
        )
    }

    /// Submits one index-native match. Its exact destination claim precedes
    /// detachment into a framework-owned key; lifecycle and semantic
    /// validation are repeated before that key is committed.
    package func submit(
        primaryKey: ByteString,
        numericSignal: Double?
    ) throws {
        try submit(
            primaryKeyByteCount: primaryKey.count,
            numericSignal: numericSignal
        ) { reservation in
            try DatabaseRetainedByteString.copying(
                primaryKey,
                reservation: reservation,
                at: .indexScan
            )
        }
    }

    /// Submits one framework-sealed tuple key. Its exact byte claim is
    /// established before packing, and the tuple's self-contained owner moves
    /// into the sink without another payload copy.
    package func submit(
        primaryKeyTuple: Tuple,
        numericSignal: Double?
    ) throws {
        let primaryKeyByteCount = primaryKeyTuple.packedByteCount
        try submit(
            primaryKeyByteCount: primaryKeyByteCount,
            numericSignal: numericSignal
        ) { reservation in
            let primaryKey = primaryKeyTuple.pack()
            guard primaryKey.count == primaryKeyByteCount else {
                throw FusionExecutionContractError.inconsistentPayload(
                    primaryKey
                )
            }
            return try DatabaseRetainedByteString.make(
                primaryKey,
                reservation: reservation,
                at: .indexScan
            )
        }
    }

    private func submit(
        primaryKeyByteCount: Int,
        numericSignal: Double?,
        retainPrimaryKey: (
            DatabaseIntermediateReservation
        ) throws -> ByteString
    ) throws {
        guard let keyBytes = UInt64(exactly: primaryKeyByteCount) else {
            throw FusionExecutionContractError.executionContractViolation
        }
        let preparation = try state.withLock {
            state -> (
                destination: DatabaseIntermediateReservation,
                key: DatabaseIntermediateReservation
            ) in
            guard state.isActive else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            guard let reservation = state.reservation else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            guard state.matches.count < limit else {
                throw FusionExecutionContractError.matchLimitExceeded(maximum: limit)
            }
            try workMeter.consume(at: .indexScan)
            switch scoring {
            case nil, .position:
                guard numericSignal == nil else {
                    throw FusionExecutionContractError.invalidScoreSignal
                }
            case .annotation:
                guard let numericSignal, numericSignal.isFinite else {
                    throw FusionExecutionContractError.invalidScoreSignal
                }
            }
            return (
                reservation,
                try reservation.reserveChild(
                    bytes: keyBytes,
                    at: .indexScan
                )
            )
        }

        // Caller code and allocation must remain outside the lifecycle lock.
        // The independent child claim survives a concurrent invalidation, and
        // is released if the producer or the second lifecycle check fails.
        let retainedPrimaryKey: ByteString
        do {
            retainedPrimaryKey = try retainPrimaryKey(preparation.key)
            guard retainedPrimaryKey.count == primaryKeyByteCount else {
                throw FusionExecutionContractError.inconsistentPayload(
                    retainedPrimaryKey
                )
            }
        } catch {
            preparation.key.release()
            throw error
        }

        do {
            try state.withLock { state in
                guard state.isActive,
                      let reservation = state.reservation,
                      reservation === preparation.destination else {
                    throw FusionExecutionContractError.matchSinkInvalidated
                }
                guard state.matches.count < limit else {
                    throw FusionExecutionContractError
                        .matchLimitExceeded(maximum: limit)
                }
                try validate(
                    primaryKey: retainedPrimaryKey,
                    state: state
                )
                let (requiredCount, countOverflow) = state.matches.count
                    .addingReportingOverflow(1)
                guard !countOverflow else {
                    throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                        currentCapacity: state.accountedMatchCapacity
                    )
                }
                let growth = try layout.growth(
                    from: state.accountedMatchCapacity,
                    toFit: requiredCount
                )
                let hashGrowth = try hashLayout.growth(
                    from: state.accountedHashCapacity,
                    toFit: requiredCount
                )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: try DatabaseIntermediateFootprint(bytes: 64).adding(
                        DatabaseIntermediateFootprint(
                            bytes: growth.additionalByteCount
                        )
                    ).adding(
                        DatabaseIntermediateFootprint(
                            bytes: hashGrowth.additionalByteCount
                        )
                    ).bytes,
                    at: .indexScan
                )
                if growth.capacity != state.accountedMatchCapacity {
                    state.matches.reserveCapacity(growth.capacity)
                    state.accountedMatchCapacity = growth.capacity
                }
                if hashGrowth.capacity != state.accountedHashCapacity {
                    state.primaryKeys.reserveCapacity(hashGrowth.capacity)
                    state.accountedHashCapacity = hashGrowth.capacity
                }
                reservation.absorbGuaranteedPartial(
                    from: preparation.key,
                    bytes: keyBytes
                )
                state.primaryKeys.insert(retainedPrimaryKey)
                state.matches.append(
                    FusionIndexMatch(
                        primaryKey: retainedPrimaryKey,
                        numericSignal: numericSignal
                    )
                )
            }
        } catch {
            preparation.key.release()
            throw error
        }
    }

    private func validate(
        primaryKey: ByteString,
        state: borrowing State
    ) throws {
        try validateCanonicalPrimaryKey(primaryKey)
        if let candidates = state.candidates,
           try !candidates.contains(
               primaryKey: primaryKey,
               workMeter: workMeter
           ) {
            throw FusionExecutionContractError.candidateDomainViolation(primaryKey)
        }
        guard !state.primaryKeys.contains(primaryKey) else {
            throw FusionExecutionContractError.duplicateMatch(primaryKey)
        }
    }

    /// Atomically revokes feature write authority and transfers the retained
    /// result out of the sink. An escaped sink retains neither candidates,
    /// matches, nor the request-memory reservation after this call returns.
    func freeze(
        coverage: FusionInputCoverage
    ) throws -> FusionIndexReadResult {
        try state.withLock { state in
            guard state.isActive else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            guard let reservation = state.reservation else {
                throw FusionExecutionContractError.matchSinkInvalidated
            }
            let releasedHashBytes = try DatabaseIntermediateFootprint(
                bytes: hashLayout.elementCapacitySlotByteCount
            ).multiplied(
                by: UInt64(state.accountedHashCapacity)
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: hashLayout.containerByteCount
                )
            ).bytes
            state.isActive = false
            state.candidates = nil
            state.reservation = nil
            let matches = state.matches
            reservation.releaseGuaranteedPartial(
                bytes: releasedHashBytes
            )
            state.matches = []
            state.primaryKeys = []
            state.accountedMatchCapacity = 0
            state.accountedHashCapacity = 0
            return FusionIndexReadResult(
                matches: matches,
                coverage: coverage,
                reservation: reservation
            )
        }
    }

    func invalidate() {
        let reservation = state.withLock { state in
            state.isActive = false
            state.candidates = nil
            state.matches = []
            state.primaryKeys = []
            state.accountedMatchCapacity = 0
            state.accountedHashCapacity = 0
            defer { state.reservation = nil }
            return state.reservation
        }
        reservation?.release()
    }

    private func validateCanonicalPrimaryKey(
        _ primaryKey: ByteString
    ) throws {
        let byteCount = UInt64(primaryKey.count)
        let temporaryBytes = try DatabaseIntermediateFootprint(
            bytes: 128
        ).adding(
            try DatabaseIntermediateFootprint(bytes: byteCount)
                .multiplied(by: 96)
        )
        let temporary = try workMeter.reserveIntermediate(
            bytes: temporaryBytes.bytes,
            at: .indexScan
        )
        defer { temporary.release() }
        try DatabaseByteProcessingMeter.consume(
            byteCount: primaryKey.count,
            passes: 2,
            workMeter: workMeter,
            stage: .indexScan
        )
        do {
            guard try Tuple(packed: primaryKey).pack() == primaryKey else {
                throw FusionExecutionContractError.inconsistentPayload(
                    primaryKey
                )
            }
        } catch is FusionExecutionContractError {
            throw FusionExecutionContractError.inconsistentPayload(primaryKey)
        } catch {
            throw FusionExecutionContractError.inconsistentPayload(primaryKey)
        }
    }
}
