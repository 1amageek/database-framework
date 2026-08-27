// RankScanner.swift
// RankIndex - Bounded ordered scanner for the rank scores subspace
//
// Replaces the ascending-scan-then-sort pattern (with 100k silent truncation)
// with bounded ordered range scans through StorageKit. The scores subspace is
// `[scoresSubspace][score][primaryKey]`; canonical tuple encoding preserves
// numeric score ordering, so
// `reverse: true, limit: k` yields the top-k highest scores in O(k).

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

enum RankScannerError: Error, Sendable, Equatable {
    case invalidRange(from: Int, to: Int)
    case negativeIndex(Int)
    case inconsistentCount(totalCount: Int, returnedCount: Int)
    case keyOutsideScoresSubspace
    case malformedEntry(elementCount: Int)
    case missingElement(index: Int)
    case rankOverflow
}

/// Scanner for the rank scores subspace.
///
/// **Key structure**: `[scoresSubspace][score][primaryKey...]`. StorageKit
/// orders keys lexicographically, while canonical tuple encoding preserves
/// numeric ordering for encoded scores.
///
/// **Ordering contract**:
/// - `top(k)` returns descending-score order (highest first, rank 0 = index 0)
/// - `bottom(k)` returns ascending-score order (lowest first)
/// - `rangeDescending(from:to:)` returns [from, to) from the top (rank 0 = highest)
struct RankScanner {
    let scoresSubspace: Subspace
    let transaction: any TransactionReadAccess
    let workMeter: DatabaseWorkMeter

    init(
        scoresSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) {
        self.scoresSubspace = scoresSubspace
        self.transaction = transaction
        self.workMeter = workMeter
    }

    /// Top-K: highest-score entries in descending order. O(K).
    ///
    /// - `k < 0` throws `RankScannerError.negativeIndex` — never silently swallowed.
    /// - `k == 0` returns an empty retained result (caller asked for zero results).
    func top(k: Int) async throws -> RankScanResult {
        guard k >= 0 else { throw RankScannerError.negativeIndex(k) }
        guard k > 0 else { return try emptyResult() }
        return try await collectEntries(
            limit: k,
            reverse: true,
            firstRank: 0,
            rankStep: 1
        )
    }

    /// Bottom-K: lowest-score entries in ascending order. O(K).
    ///
    /// - `k < 0` throws `RankScannerError.negativeIndex` — never silently swallowed.
    /// - `k == 0` returns an empty retained result (caller asked for zero results).
    func bottom(
        k: Int,
        startRank: Int
    ) async throws -> RankScanResult {
        guard k >= 0 else { throw RankScannerError.negativeIndex(k) }
        guard k > 0 else { return try emptyResult() }
        guard startRank >= 0 else {
            throw RankScannerError.negativeIndex(startRank)
        }
        return try await collectEntries(
            limit: k,
            reverse: false,
            firstRank: startRank,
            rankStep: -1
        )
    }

    /// Rank range [from, to) in descending order. O(to).
    /// Reads `to` highest storage entries and decodes only the requested page.
    func rangeDescending(
        from: Int,
        to: Int
    ) async throws -> RankScanResult {
        guard from >= 0, to > from else {
            throw RankScannerError.invalidRange(from: from, to: to)
        }
        return try await collectEntries(
            limit: to,
            reverse: true,
            skipping: from,
            firstRank: from,
            rankStep: 1
        )
    }

    /// Read the Nth-highest entry (0-based, 0 = highest). O(N+1).
    /// Used by percentile when total count is known in O(1).
    func nthFromTop(_ n: Int) async throws -> RankScanResult {
        guard n >= 0 else {
            throw RankScannerError.negativeIndex(n)
        }
        guard n < Int.max else {
            throw RankScannerError.rankOverflow
        }
        return try await collectEntries(
            limit: n + 1,
            reverse: true,
            skipping: n,
            firstRank: n,
            rankStep: 1
        )
    }

    /// Returns the descending leaderboard position of the first entry returned
    /// by `bottom(k)`. The bottom scan itself is ascending by score, so later
    /// entries decrement this position.
    static func bottomStartPosition(
        totalCount: Int,
        returnedCount: Int
    ) throws -> Int {
        guard totalCount >= 0,
              returnedCount >= 0,
              totalCount >= returnedCount else {
            throw RankScannerError.inconsistentCount(
                totalCount: totalCount,
                returnedCount: returnedCount
            )
        }
        return totalCount - 1
    }

    // MARK: - Parsing

    private func collectEntries(
        limit: Int,
        reverse: Bool,
        skipping skippedCount: Int = 0,
        firstRank: Int,
        rankStep: Int
    ) async throws -> RankScanResult {
        let range = scoresSubspace.range()
        var entries = try DatabaseRetainedArrayBuilder<RankScanEntry>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                RankScanEntry.self
            ),
            expectedCount: max(0, limit - skippedCount)
        )
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: limit,
            reverse: reverse,
            snapshot: true,
            streamingMode: .iterator
        )
        var encounteredCount = 0
        var retainedCount = 0
        do {
            while let row = try await cursor.next() {
                try workMeter.consume(at: .indexScan)
                defer { encounteredCount += 1 }
                guard encounteredCount >= skippedCount else { continue }
                let rank = try Self.rank(
                    first: firstRank,
                    offset: retainedCount,
                    step: rankStep
                )
                let valueBytes = UInt64(row.1.count)
                let payload = try DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: valueBytes
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: UInt64(MemoryLayout<RankScanEntry>.stride)
                    )
                )
                let admission = try entries.prepareAppend(
                    footprint: payload,
                    at: .indexScan
                )
                let entry = try Self.decodeEntry(
                    key: row.0,
                    scoresSubspace: scoresSubspace,
                    workMeter: workMeter,
                    rank: rank
                )
                entries.append(entry, using: admission)
                retainedCount += 1
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        return try RankScanResult(buffer: entries.finish())
    }

    private func emptyResult() throws -> RankScanResult {
        let entries = try DatabaseRetainedArrayBuilder<RankScanEntry>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                RankScanEntry.self
            )
        )
        return try RankScanResult(buffer: entries.finish())
    }

    private static func rank(
        first: Int,
        offset: Int,
        step: Int
    ) throws -> Int {
        let (scaled, scaledOverflow) = offset.multipliedReportingOverflow(
            by: step
        )
        guard !scaledOverflow else { throw RankScannerError.rankOverflow }
        let (rank, rankOverflow) = first.addingReportingOverflow(scaled)
        guard !rankOverflow else { throw RankScannerError.rankOverflow }
        return rank
    }

    static func decodeEntry(
        key: ByteString,
        scoresSubspace: Subspace,
        workMeter: DatabaseWorkMeter,
        rank: Int = 0
    ) throws -> RankScanEntry {
        guard scoresSubspace.contains(key) else {
            throw RankScannerError.keyOutsideScoresSubspace
        }
        // Range cursors may return keys backed by a transaction-owned result
        // buffer. Retain a self-contained key before any tuple view is kept;
        // DatabaseRetainedByteString makes one exact copy only for enclosing
        // or otherwise unmeasurable backend owners.
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(key.count),
            at: .indexScan
        )
        do {
            let retainedSourceKey = try DatabaseRetainedByteString.make(
                key,
                reservation: reservation,
                at: .indexScan
            )
            var cursor = try scoresSubspace.tupleCursor(
                for: retainedSourceKey
            )
            let scoreElement: any TupleElement
            guard let score = try cursor.next(admitting: { additionalBytes in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalBytes),
                    at: .indexScan
                )
            }) else {
                throw RankScannerError.malformedEntry(elementCount: 0)
            }
            scoreElement = score

            let primaryKeyTuple = try cursor.remainingTuple(
                admitting: { additionalBytes in
                    try reservation.reserveAdditional(
                        bytes: UInt64(additionalBytes),
                        at: .indexScan
                    )
                }
            )
            guard !primaryKeyTuple.isEmpty else {
                throw RankScannerError.malformedEntry(elementCount: 1)
            }
            let primaryKey = DatabaseRetainedPrimaryKey(
                value: primaryKeyTuple,
                reservation: reservation
            )
            return RankScanEntry(
                scoreElement: scoreElement,
                primaryKey: primaryKey,
                rank: rank,
                retainedSourceKey: retainedSourceKey
            )
        } catch {
            reservation.release()
            throw error
        }
    }
}
