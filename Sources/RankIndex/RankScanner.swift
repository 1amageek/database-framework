// RankScanner.swift
// RankIndex - Bounded ordered scanner for the rank scores subspace
//
// Replaces the ascending-scan-then-sort pattern (with 100k silent truncation)
// with bounded ordered range scans through StorageKit. The scores subspace is
// `[scoresSubspace][score][primaryKey]`; canonical tuple encoding preserves
// numeric score ordering, so
// `reverse: true, limit: k` yields the top-k highest scores in O(k).

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Raw entry read from the rank scores subspace.
///
/// Score is kept as `any TupleElement` so callers can decode into their own
/// score type (Double for query APIs, Score generic for maintainer APIs).
struct RankScanEntry: Sendable {
    let scoreElement: any TupleElement
    let primaryKey: Tuple
}

enum RankScannerError: Error, Sendable, Equatable {
    case invalidRange(from: Int, to: Int)
    case negativeIndex(Int)
    case inconsistentCount(totalCount: Int, returnedCount: Int)
    case keyOutsideScoresSubspace
    case malformedEntry(elementCount: Int)
    case missingElement(index: Int)
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
    let transaction: any TransactionAccess

    init(scoresSubspace: Subspace, transaction: any TransactionAccess) {
        self.scoresSubspace = scoresSubspace
        self.transaction = transaction
    }

    /// Top-K: highest-score entries in descending order. O(K).
    ///
    /// - `k < 0` throws `RankScannerError.negativeIndex` — never silently swallowed.
    /// - `k == 0` returns `[]` by contract (caller asked for zero results).
    func top(k: Int) async throws -> [RankScanEntry] {
        guard k >= 0 else { throw RankScannerError.negativeIndex(k) }
        guard k > 0 else { return [] }
        let sequence = try await collect(limit: k, reverse: true)
        return try parse(sequence)
    }

    /// Bottom-K: lowest-score entries in ascending order. O(K).
    ///
    /// - `k < 0` throws `RankScannerError.negativeIndex` — never silently swallowed.
    /// - `k == 0` returns `[]` by contract (caller asked for zero results).
    func bottom(k: Int) async throws -> [RankScanEntry] {
        guard k >= 0 else { throw RankScannerError.negativeIndex(k) }
        guard k > 0 else { return [] }
        let sequence = try await collect(limit: k, reverse: false)
        return try parse(sequence)
    }

    /// Rank range [from, to) in descending order. O(to).
    /// Reads `to` highest storage entries and decodes only the requested page.
    func rangeDescending(from: Int, to: Int) async throws -> [RankScanEntry] {
        guard from >= 0, to > from else {
            throw RankScannerError.invalidRange(from: from, to: to)
        }
        let sequence = try await collect(limit: to, reverse: true)
        guard sequence.count > from else { return [] }
        return try parse(sequence.dropFirst(from))
    }

    /// Read the Nth-highest entry (0-based, 0 = highest). O(N+1).
    /// Used by percentile when total count is known in O(1).
    func nthFromTop(_ n: Int) async throws -> RankScanEntry? {
        guard n >= 0 else {
            throw RankScannerError.negativeIndex(n)
        }
        let sequence = try await collect(limit: n + 1, reverse: true)
        guard sequence.count == n + 1, let key = sequence.last?.0 else {
            return nil
        }
        return try Self.decodeEntry(key: key, scoresSubspace: scoresSubspace)
    }

    /// Returns the descending leaderboard position of the first entry returned
    /// by `bottom(k)`. The bottom scan itself is ascending by score, so later
    /// entries decrement this position.
    static func bottomStartPosition(
        totalCount: Int,
        returnedCount: Int
    ) throws -> Int {
        guard totalCount >= returnedCount else {
            throw RankScannerError.inconsistentCount(
                totalCount: totalCount,
                returnedCount: returnedCount
            )
        }
        return totalCount - 1
    }

    // MARK: - Parsing

    private func collect(
        limit: Int,
        reverse: Bool
    ) async throws -> [(Bytes, Bytes)] {
        let range = scoresSubspace.range()
        return try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: limit,
            reverse: reverse,
            snapshot: true
        )
    }

    private func parse<Entries: Collection>(
        _ sequence: Entries
    ) throws -> [RankScanEntry] where Entries.Element == (Bytes, Bytes) {
        var entries: [RankScanEntry] = []
        entries.reserveCapacity(sequence.count)
        for (key, _) in sequence {
            entries.append(
                try Self.decodeEntry(key: key, scoresSubspace: scoresSubspace)
            )
        }
        return entries
    }

    static func decodeEntry(
        key: Bytes,
        scoresSubspace: Subspace
    ) throws -> RankScanEntry {
        guard scoresSubspace.contains(key) else {
            throw RankScannerError.keyOutsideScoresSubspace
        }
        let tuple = try scoresSubspace.unpack(key)
        guard tuple.count >= 2 else {
            throw RankScannerError.malformedEntry(elementCount: tuple.count)
        }

        let scoreElement: any TupleElement
        do {
            scoreElement = try tuple.element(at: 0)
        } catch {
            throw RankScannerError.missingElement(index: 0)
        }

        var primaryKeyElements: [any TupleElement] = []
        primaryKeyElements.reserveCapacity(tuple.count - 1)
        for index in 1..<tuple.count {
            do {
                primaryKeyElements.append(try tuple.element(at: index))
            } catch {
                throw RankScannerError.missingElement(index: index)
            }
        }
        return RankScanEntry(
            scoreElement: scoreElement,
            primaryKey: Tuple(primaryKeyElements)
        )
    }
}
