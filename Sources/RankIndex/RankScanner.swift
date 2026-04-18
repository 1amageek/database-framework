// RankScanner.swift
// RankIndex - Native descending scanner for rank scores subspace
//
// Replaces the ascending-scan-then-sort pattern (with 100k silent truncation)
// with direct reverse range scans on FoundationDB. The scores subspace is
// `[scoresSubspace][score][primaryKey]` — FDB preserves tuple ordering, so
// `reverse: true, limit: k` yields the top-k highest scores in O(k).

import Foundation
import Core
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
}

/// Scanner for the rank scores subspace.
///
/// **Key structure**: `[scoresSubspace][score][primaryKey...]`. FDB stores
/// tuples in lexicographic byte order, which preserves numeric ordering for
/// Tuple-encoded scores.
///
/// **Ordering contract**:
/// - `top(k)` returns descending-score order (highest first, rank 0 = index 0)
/// - `bottom(k)` returns ascending-score order (lowest first)
/// - `rangeDescending(from:to:)` returns [from, to) from the top (rank 0 = highest)
struct RankScanner {
    let scoresSubspace: Subspace
    let transaction: any Transaction

    init(scoresSubspace: Subspace, transaction: any Transaction) {
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
        let range = scoresSubspace.range()
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: k,
            reverse: true,
            snapshot: true
        )
        return try parse(sequence)
    }

    /// Bottom-K: lowest-score entries in ascending order. O(K).
    ///
    /// - `k < 0` throws `RankScannerError.negativeIndex` — never silently swallowed.
    /// - `k == 0` returns `[]` by contract (caller asked for zero results).
    func bottom(k: Int) async throws -> [RankScanEntry] {
        guard k >= 0 else { throw RankScannerError.negativeIndex(k) }
        guard k > 0 else { return [] }
        let range = scoresSubspace.range()
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: k,
            reverse: false,
            snapshot: true
        )
        return try parse(sequence)
    }

    /// Rank range [from, to) in descending order. O(to).
    /// Reads `to` highest entries and drops the first `from`.
    func rangeDescending(from: Int, to: Int) async throws -> [RankScanEntry] {
        guard from >= 0, to > from else {
            throw RankScannerError.invalidRange(from: from, to: to)
        }
        let all = try await top(k: to)
        guard all.count > from else { return [] }
        return Array(all[from..<min(to, all.count)])
    }

    /// Read the Nth-highest entry (0-based, 0 = highest). O(N+1).
    /// Used by percentile when total count is known in O(1).
    func nthFromTop(_ n: Int) async throws -> RankScanEntry? {
        guard n >= 0 else {
            throw RankScannerError.negativeIndex(n)
        }
        let entries = try await top(k: n + 1)
        guard entries.count == n + 1 else { return nil }
        return entries[n]
    }

    // MARK: - Parsing

    private func parse(_ sequence: [(Bytes, Bytes)]) throws -> [RankScanEntry] {
        var entries: [RankScanEntry] = []
        entries.reserveCapacity(sequence.count)
        for (key, _) in sequence {
            guard scoresSubspace.contains(key) else { continue }
            let tuple = try scoresSubspace.unpack(key)
            guard tuple.count >= 2, let scoreElement = tuple[0] else { continue }
            var primaryKeyElements: [any TupleElement] = []
            primaryKeyElements.reserveCapacity(tuple.count - 1)
            for i in 1..<tuple.count {
                if let element = tuple[i] {
                    primaryKeyElements.append(element)
                }
            }
            entries.append(RankScanEntry(
                scoreElement: scoreElement,
                primaryKey: Tuple(primaryKeyElements)
            ))
        }
        return entries
    }
}
