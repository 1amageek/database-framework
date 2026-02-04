// SkipListInsertion.swift
// Skip List insertion with accurate Span Counter maintenance
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet
//
// Algorithm (from plan Section 1.5):
// 1. Find insertion position (top to bottom)
// 2. Record rank at each level
// 3. Determine new node's level
// 4. Insert at each level and update Span Counters
// 5. Increment span at higher levels

import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

/// Skip List insertion with Span Counter maintenance
///
/// Implements the insertion algorithm with accurate Span Counter updates
/// to enable O(log n) rank lookup.
///
/// **Key Design**: Uses ascending order (inverted scores) for standard Skip List algorithm.
/// High scores are stored as negative values to maintain rank 0 = highest score.
public struct SkipListInsertion<Score: Comparable & Numeric & Codable & Sendable>: Sendable {

    // MARK: - Properties

    private let subspaces: SkipListSubspaces
    private let levelAssignment: LevelAssignment

    // MARK: - Initialization

    public init(subspaces: SkipListSubspaces, levelAssignment: LevelAssignment) {
        self.subspaces = subspaces
        self.levelAssignment = levelAssignment
    }

    // MARK: - Score Inversion

    // MARK: - Score Ordering

    // NOTE: Using DESCENDING order (high score = rank 0)
    // Scores are stored as-is (NOT inverted)
    // This provides intuitive key layout and natural API semantics

    // MARK: - Insertion Algorithm

    /// Insert entry with accurate Span Counter updates
    ///
    /// Algorithm:
    /// ```
    /// Phase 1: Find insertion position at each level (top → bottom)
    ///   - Track rank[level] = cumulative position at each level
    ///   - Track update[level] = node before insertion point at each level
    ///
    /// Phase 2: Assign level to new node (probabilistic)
    ///
    /// Phase 3: Insert at each level and update Span Counters
    ///   - newNode.span[level] = update[level].span[level] - (rank[0] - rank[level])
    ///   - update[level].span[level] = (rank[0] - rank[level]) + 1
    ///
    /// Phase 4: Increment span at higher levels
    ///   - update[level].span[level] += 1
    /// ```
    ///
    /// - Parameters:
    ///   - score: Score value
    ///   - primaryKey: Primary key tuple
    ///   - currentLevels: Current number of levels
    ///   - transaction: FDB transaction
    /// - Returns: Rank of inserted entry (0-based)
    public func insert(
        score: Score,
        primaryKey: Tuple,
        currentLevels: Int,
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        // Get total count for span calculation when level is empty
        let totalCountBefore = try await getCurrentCount(transaction: transaction)

        // Phase 0: Pre-read first entry at each level (for updateKey == nil case)
        var firstKeys: [[UInt8]?] = []
        var firstSpans: [Int64?] = []
        var firstScores: [Score?] = []
        var firstPKs: [Tuple?] = []

        for level in 0..<currentLevels {
            let levelSubspace = subspaces.subspace(for: level)
            let range = levelSubspace.range()
            let firstSeq = transaction.getRange(
                from: range.begin,
                to: range.end,
                limit: 1,
                reverse: true,  // Descending: get highest score (first entry)
                snapshot: true
            )
            var firstKey: [UInt8]? = nil
            var firstSpan: Int64? = nil
            var firstScore: Score? = nil
            var firstPK: Tuple? = nil

            for try await (key, value) in firstSeq {
                guard levelSubspace.contains(key) else { break }
                firstKey = key
                firstSpan = try SpanValue.decode(value).count

                // Parse score and PK
                let suffix = try levelSubspace.unpack(key)
                if !suffix.isEmpty, let scoreElement = suffix[0] {
                    firstScore = try TupleDecoder.decode(scoreElement, as: Score.self)
                    firstPK = extractPrimaryKey(from: suffix)
                }
                break
            }
            firstKeys.append(firstKey)
            firstSpans.append(firstSpan)
            firstScores.append(firstScore)
            firstPKs.append(firstPK)
        }

        // Calculate Level 0 rank for each first entry (needed for span calculation)
        var firstRanks: [Int64?] = []
        for level in 0..<currentLevels {
            if let firstScore = firstScores[level], let firstPK = firstPKs[level] {
                // Calculate this entry's Level 0 rank
                let result = try await findInsertionPoint(
                    level: 0,
                    targetScore: firstScore,
                    targetPrimaryKey: firstPK,
                    transaction: transaction
                )
                firstRanks.append(result.accumulatedRank)
            } else {
                firstRanks.append(nil)
            }
        }

        // Phase 1: Find insertion position (standard Skip List algorithm)
        // Traverse from top to bottom, accumulating rank at each level
        var rank: [Int64] = Array(repeating: 0, count: currentLevels)
        var updateKeys: [[UInt8]?] = Array(repeating: nil, count: currentLevels)
        var updateSpans: [Int64?] = Array(repeating: nil, count: currentLevels)

        for level in stride(from: currentLevels - 1, through: 0, by: -1) {
            // Inherit rank from level above
            rank[level] = (level == currentLevels - 1) ? 0 : rank[level + 1]

            // Scan this level from the beginning (standard algorithm)
            let result = try await findInsertionPoint(
                level: level,
                targetScore: score,
                targetPrimaryKey: primaryKey,
                transaction: transaction
            )

            rank[level] += result.accumulatedRank
            updateKeys[level] = result.lastKeyBeforeInsert
            if let updateKey = result.lastKeyBeforeInsert {
                // Read span of updateKey
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: true) {
                    updateSpans[level] = try SpanValue.decode(spanBytes).count
                }
            }
        }

        // Phase 2: Assign level to new node
        let newLevel = min(levelAssignment.randomLevel(), currentLevels)

        // Phase 3: Insert at each level with standard Span Counter updates
        for level in 0..<newLevel {
            let key = try makeKey(score: score, primaryKey: primaryKey, level: level)

            let newSpan: Int64

            if level == 0 {
                // Level 0: all entries have span = 1 (always)
                newSpan = 1
            } else if let updateKey = updateKeys[level], let oldSpan = updateSpans[level] {
                // Standard formula: works for all cases (middle, end)
                newSpan = oldSpan - (rank[0] - rank[level])

                // Update span of the node before insertion point
                let newUpdateSpan = (rank[0] - rank[level]) + 1
                transaction.setValue(
                    SpanValue(count: newUpdateSpan).encoded(),
                    for: updateKey
                )
            } else {
                // updateKey == nil: inserting at beginning of level (highest score)
                if let firstRank = firstRanks[level] {
                    // Level has entries, new entry becomes first (descending order)
                    //
                    // Descending order (high score = rank 0):
                    // - rank[0] = new entry's Level 0 rank (before insertion)
                    // - firstRank = old first entry's Level 0 rank (before insertion)
                    //
                    // After insertion:
                    // - new entry: rank = rank[0]
                    // - old first entry: rank = firstRank + 1 (pushed down by 1)
                    //
                    // newSpan = (old first entry's new rank) - (new entry's rank)
                    //         = (firstRank + 1) - rank[0]
                    //
                    // Example 1: new entry at top
                    // - rank[0] = 0, firstRank = 0 (old first was at rank 0)
                    // - newSpan = (0 + 1) - 0 = 1
                    //
                    // Example 2: new entry not at top (middle insertion)
                    // - rank[0] = 5, firstRank = 3
                    // - newSpan = (3 + 1) - 5 = -1 (ERROR: this shouldn't happen)
                    //   (because updateKey would not be nil if rank[0] > firstRank)

                    let calculatedSpan = (firstRank + 1) - rank[0]

                    // Validation: span must be positive
                    guard calculatedSpan > 0 else {
                        throw IndexError.invalidStructure(
                            "Invalid span calculation at level \(level): firstRank=\(firstRank), rank[0]=\(rank[0]), calculatedSpan=\(calculatedSpan), score=\(score)"
                        )
                    }

                    newSpan = calculatedSpan

                    // IMPORTANT: In descending order, old first entry's span does NOT change
                    // because it still points to the same Level 0 elements below it
                    // NO UPDATE to firstKey
                } else {
                    // Level is truly empty
                    newSpan = totalCountBefore - rank[0] + 1
                }
            }

            // Insert new entry with calculated span
            transaction.setValue(SpanValue(count: newSpan).encoded(), for: key)
        }

        // Phase 4: Increment span at higher levels (levels >= newLevel)
        for level in newLevel..<currentLevels {
            if let updateKey = updateKeys[level], let currentSpan = updateSpans[level] {
                // Standard case: increment span by 1 (one new Level 0 entry added)
                let newSpan = currentSpan + 1
                transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
            } else if let firstKey = firstKeys[level], let firstSpan = firstSpans[level] {
                // updateKey == nil: new entry inserted at beginning of level
                // Increment old first entry's span (it now skips one more Level 0 entry)
                let newSpan = firstSpan + 1
                transaction.setValue(SpanValue(count: newSpan).encoded(), for: firstKey)
            }
        }

        return rank[0]
    }

    // MARK: - Helper Methods

    /// Get current count before insertion
    private func getCurrentCount(transaction: any TransactionProtocol) async throws -> Int64 {
        guard let value = try await transaction.getValue(for: subspaces.countKey, snapshot: false) else {
            return 0
        }
        return ByteConversion.bytesToInt64(value)
    }

    /// Find insertion point at a specific level (descending order)
    ///
    /// Scans in descending order (high to low scores).
    /// Accumulates span counters until reaching the insertion point.
    ///
    /// Returns:
    /// - accumulatedRank: Total span traversed at this level
    /// - lastKeyBeforeInsert: Key of the last entry before insertion point (nil if inserting at beginning)
    private func findInsertionPoint(
        level: Int,
        targetScore: Score,
        targetPrimaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> (accumulatedRank: Int64, lastKeyBeforeInsert: [UInt8]?) {
        var accumulatedRank: Int64 = 0
        var lastKey: [UInt8]? = nil

        let levelSubspace = subspaces.subspace(for: level)
        let range = levelSubspace.range()

        // Scan in descending order (high to low scores)
        let sequence = transaction.getRange(
            beginSelector: .lastLessThan(range.end),
            endSelector: .firstGreaterOrEqual(range.begin),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard levelSubspace.contains(key) else { break }

            // Parse key
            let suffix = try levelSubspace.unpack(key)
            guard !suffix.isEmpty else { continue }

            guard let scoreElement = suffix[0] else { continue }
            let currentScore = try TupleDecoder.decode(scoreElement, as: Score.self)

            // In descending order: stop when we reach or pass the target
            if currentScore <= targetScore {
                // Check if exact match or below target
                if currentScore == targetScore {
                    let currentPK = extractPrimaryKey(from: suffix)
                    if compareTuples(currentPK, targetPrimaryKey) != .orderedDescending {
                        // Current key <= target, stop here
                        break
                    }
                } else {
                    // Current score is lower than target, stop here
                    break
                }
            }

            // currentScore > targetScore (or same score with higher PK), accumulate span
            let span = try SpanValue.decode(value)
            accumulatedRank += span.count
            lastKey = key
        }

        return (accumulatedRank, lastKey)
    }

    /// Make key for a specific level
    ///
    /// Stores score as-is (descending order: high score = rank 0)
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

    /// Compare two tuples using packed byte representation
    private func compareTuples(_ lhs: Tuple, _ rhs: Tuple) -> ComparisonResult {
        let lhsBytes = lhs.pack()
        let rhsBytes = rhs.pack()

        let minLength = min(lhsBytes.count, rhsBytes.count)
        for i in 0..<minLength {
            if lhsBytes[i] < rhsBytes[i] {
                return .orderedAscending
            } else if lhsBytes[i] > rhsBytes[i] {
                return .orderedDescending
            }
        }

        if lhsBytes.count < rhsBytes.count {
            return .orderedAscending
        } else if lhsBytes.count > rhsBytes.count {
            return .orderedDescending
        } else {
            return .orderedSame
        }
    }
}
