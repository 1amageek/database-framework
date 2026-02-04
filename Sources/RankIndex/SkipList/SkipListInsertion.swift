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
                // Standard algorithm: find next entry and calculate span
                if level == 0 {
                    // Level 0: all entries have span = 1 (always)
                    newSpan = 1
                } else {
                    // Level > 0: find next entry (current first entry) and calculate span
                    let nextEntry = try await findFirstEntryAtLevel(level, transaction)
                    if let (nextScore, nextPK) = nextEntry {
                        // Calculate next entry's Level 0 rank
                        let nextResult = try await findInsertionPoint(
                            level: 0,
                            targetScore: nextScore,
                            targetPrimaryKey: nextPK,
                            transaction: transaction
                        )
                        let nextLevel0Rank = nextResult.accumulatedRank
                        // newSpan = distance from new entry to next entry at Level 0
                        // After insertion, new entry will be at rank[0], next entry at nextLevel0Rank + 1
                        newSpan = (nextLevel0Rank + 1) - rank[0]
                    } else {
                        // Level is empty, span = distance to end
                        newSpan = totalCountBefore - rank[0] + 1
                    }
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
            } else {
                // updateKey == nil: new entry inserted at beginning of level
                // Increment old first entry's span (it now skips one more Level 0 entry)
                let firstEntry = try await findFirstEntryAtLevel(level, transaction)
                if let (firstScore, firstPK) = firstEntry {
                    let firstKey = try makeKey(score: firstScore, primaryKey: firstPK, level: level)
                    if let spanBytes = try await transaction.getValue(for: firstKey, snapshot: false) {
                        let firstSpan = try SpanValue.decode(spanBytes).count
                        let newSpan = firstSpan + 1
                        transaction.setValue(SpanValue(count: newSpan).encoded(), for: firstKey)
                    }
                }
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

    /// Find first entry at a specific level (highest score)
    ///
    /// Returns:
    /// - (score, primaryKey) of the first entry, or nil if level is empty
    private func findFirstEntryAtLevel(
        _ level: Int,
        _ transaction: any TransactionProtocol
    ) async throws -> (score: Score, primaryKey: Tuple)? {
        let levelSubspace = subspaces.subspace(for: level)
        let range = levelSubspace.range()

        let sequence = transaction.getRange(
            from: range.begin,
            to: range.end,
            limit: 1,
            reverse: true,  // Descending: get highest score (first entry)
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard levelSubspace.contains(key) else { break }

            let suffix = try levelSubspace.unpack(key)
            guard !suffix.isEmpty, let scoreElement = suffix[0] else { continue }

            let score = try TupleDecoder.decode(scoreElement, as: Score.self)
            let primaryKey = extractPrimaryKey(from: suffix)

            return (score, primaryKey)
        }

        return nil
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
