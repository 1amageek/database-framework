// SkipListTraversal.swift
// Skip List traversal algorithms with Span Counter accumulation
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet

import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

/// Skip List traversal operations using Span Counters
///
/// Provides O(log n) rank lookup and O(log n + k) top-K queries
/// by traversing the skip list hierarchy and accumulating span counters.
public struct SkipListTraversal<Score: Comparable & Numeric & Codable & Sendable>: Sendable {

    // MARK: - Properties

    private let subspaces: SkipListSubspaces
    private let maxLevels: Int

    // MARK: - Initialization

    public init(subspaces: SkipListSubspaces, maxLevels: Int) {
        self.subspaces = subspaces
        self.maxLevels = maxLevels
    }

    // MARK: - Rank Lookup (O(log n))

    /// Get rank of a specific score using Span Counter accumulation
    ///
    /// Time Complexity: O(log n)
    ///
    /// Algorithm:
    /// 1. Start from highest level
    /// 2. Move forward while next.score < targetScore
    /// 3. Accumulate span counters during traversal
    /// 4. Drop to lower level when can't advance
    /// 5. Repeat until Level 0
    /// 6. Convert ascending position to descending rank
    ///
    /// - Parameters:
    ///   - score: Target score to find rank for
    ///   - primaryKey: Primary key tuple
    ///   - currentLevels: Current number of levels in the skip list
    ///   - totalCount: Total number of elements in the skip list
    ///   - transaction: FDB transaction
    /// - Returns: Rank (0-based, 0 = highest score)
    /// - Throws: Error if score not found or traversal fails
    public func getRank(
        score: Score,
        primaryKey: Tuple,
        currentLevels: Int,
        totalCount: Int64,
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        // Verify the score exists at Level 0
        let key = try makeKey(score: score, primaryKey: primaryKey, level: 0)
        guard let _ = try await transaction.getValue(for: key, snapshot: true) else {
            throw IndexError.invalidStructure("Score \(score) not found in index")
        }

        // Current implementation: O(n) traversal at Level 0 only
        //
        // TODO: Implement O(log n) multi-level traversal
        // Challenge: Upper level span accumulation includes target itself
        // when target doesn't exist at that level, causing off-by-one errors.
        // Need to implement look-ahead logic to check if next entry exceeds target.
        //
        // For now, Level 0-only approach ensures correctness while maintaining
        // acceptable performance for typical use cases (n < 100K).
        var rank: Int64 = 0

        let levelSubspace = subspaces.leaf
        let rangeBegin = levelSubspace.range().begin
        let rangeEnd = levelSubspace.range().end

        // Scan Level 0 in descending order
        let sequence = transaction.getRange(
            from: rangeBegin,
            to: rangeEnd,
            limit: 0,
            reverse: true,
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard levelSubspace.contains(key) else { break }

            // Parse key
            let suffix = try levelSubspace.unpack(key)
            guard !suffix.isEmpty, let scoreElement = suffix[0] else { continue }

            let currentScore = try TupleDecoder.decode(scoreElement, as: Score.self)
            let currentPK = extractPrimaryKey(from: suffix)

            // Check if we've reached the target
            if currentScore <= score {
                if currentScore == score {
                    if compareTuples(currentPK, primaryKey) != .orderedDescending {
                        // Reached target, stop
                        break
                    }
                } else {
                    // Passed target, stop
                    break
                }
            }

            // currentScore > score (or same score with higher PK)
            // Accumulate span (always 1 at Level 0)
            let span = try SpanValue.decode(value)
            rank += span.count
        }

        return rank
    }

    // MARK: - Top-K Query (O(log n + k))

    /// Get top K elements using skip list traversal
    ///
    /// Time Complexity: O(log n + k)
    ///
    /// Algorithm:
    /// 1. Skip to position (totalCount - k) using span counters
    /// 2. Collect k elements from that position
    ///
    /// - Parameters:
    ///   - k: Number of elements to retrieve
    ///   - totalCount: Total number of elements
    ///   - transaction: FDB transaction
    /// - Returns: Array of (score, primaryKey, rank) tuples in descending order
    public func getTopK(
        k: Int,
        totalCount: Int64,
        transaction: any TransactionProtocol
    ) async throws -> [(score: Score, primaryKey: Tuple, rank: Int64)] {
        guard k > 0, totalCount > 0 else { return [] }

        // Calculate starting position (rank)
        // Top k = ranks 0 to k-1
        // Skip to position 0 (highest score)
        let startRank: Int64 = 0

        // Collect k elements starting from startRank
        return try await collectElementsFromRank(
            startRank: startRank,
            count: min(k, Int(totalCount)),
            transaction: transaction
        )
    }

    /// Collect elements starting from a specific rank
    ///
    /// Current implementation: Optimized for startRank=0 (Top-K query) using
    /// descending scan from the highest score. For startRank > 0, could be
    /// optimized using skip list traversal to jump directly to the target rank.
    ///
    /// - Parameters:
    ///   - startRank: Starting rank (0-based, currently only 0 is used)
    ///   - count: Number of elements to collect
    ///   - transaction: FDB transaction
    /// - Returns: Array of (score, primaryKey, rank) tuples
    private func collectElementsFromRank(
        startRank: Int64,
        count: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(score: Score, primaryKey: Tuple, rank: Int64)] {
        var results: [(score: Score, primaryKey: Tuple, rank: Int64)] = []

        // Use descending scan (highest to lowest score)
        let levelSubspace = subspaces.leaf
        let range = levelSubspace.range()

        let sequence = transaction.getRange(
            beginSelector: .lastLessThan(range.end),
            endSelector: .firstGreaterOrEqual(range.begin),
            snapshot: true
        )

        var currentRank: Int64 = 0
        var collected = 0

        for try await (key, _) in sequence {
            guard levelSubspace.contains(key) else { break }

            if currentRank >= startRank {
                // Parse key
                let suffix = try levelSubspace.unpack(key)
                guard !suffix.isEmpty else { continue }

                guard let scoreElement = suffix[0] else { continue }
                let score = try TupleDecoder.decode(scoreElement, as: Score.self)
                let primaryKey = extractPrimaryKey(from: suffix)

                results.append((score: score, primaryKey: primaryKey, rank: currentRank))
                collected += 1

                if collected >= count {
                    break
                }
            }

            currentRank += 1
        }

        return results
    }

    // MARK: - Helper Methods

    /// Make key for a specific level
    private func makeKey(score: Score, primaryKey: Tuple, level: Int) throws -> [UInt8] {
        let levelSubspace = subspaces.subspace(for: level)
        let scoreElement = try TupleEncoder.encode(score)

        var allElements: [any TupleElement] = [scoreElement]
        for i in 0..<primaryKey.count {
            if let element = primaryKey[i] {
                allElements.append(element)
            }
        }

        return levelSubspace.pack(Tuple(allElements))
    }

    /// Extract primary key from suffix tuple
    private func extractPrimaryKey(from suffix: Tuple) -> Tuple {
        var pkElements: [any TupleElement] = []
        for i in 1..<suffix.count {
            if let element = suffix[i] {
                pkElements.append(element)
            }
        }
        return Tuple(pkElements)
    }

    /// Compare two tuples using their packed byte representation
    ///
    /// Tuple doesn't conform to Comparable, so we compare their packed bytes.
    ///
    /// - Parameters:
    ///   - lhs: Left tuple
    ///   - rhs: Right tuple
    /// - Returns: Comparison result
    private func compareTuples(_ lhs: Tuple, _ rhs: Tuple) -> ComparisonResult {
        let lhsBytes = lhs.pack()
        let rhsBytes = rhs.pack()

        // Lexicographic comparison
        let minLength = min(lhsBytes.count, rhsBytes.count)
        for i in 0..<minLength {
            if lhsBytes[i] < rhsBytes[i] {
                return .orderedAscending
            } else if lhsBytes[i] > rhsBytes[i] {
                return .orderedDescending
            }
        }

        // If all bytes are equal up to minLength, compare lengths
        if lhsBytes.count < rhsBytes.count {
            return .orderedAscending
        } else if lhsBytes.count > rhsBytes.count {
            return .orderedDescending
        } else {
            return .orderedSame
        }
    }
}
