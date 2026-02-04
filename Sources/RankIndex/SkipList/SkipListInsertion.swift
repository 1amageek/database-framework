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
public struct SkipListInsertion<Score: Comparable & Numeric & Codable & Sendable>: Sendable {

    // MARK: - Properties

    private let subspaces: SkipListSubspaces
    private let levelAssignment: LevelAssignment

    // MARK: - Initialization

    public init(subspaces: SkipListSubspaces, levelAssignment: LevelAssignment) {
        self.subspaces = subspaces
        self.levelAssignment = levelAssignment
    }

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
        // Phase 1: Find insertion position and track ranks
        var rank: [Int64] = Array(repeating: 0, count: currentLevels)
        var updateKeys: [[UInt8]?] = Array(repeating: nil, count: currentLevels)

        for level in stride(from: currentLevels - 1, through: 0, by: -1) {
            // Inherit rank from level above
            rank[level] = (level == currentLevels - 1) ? 0 : rank[level + 1]

            // Scan at this level to find insertion point
            let (accumulatedRank, lastKeyBeforeInsert) = try await findInsertionPoint(
                level: level,
                targetScore: score,
                targetPrimaryKey: primaryKey,
                transaction: transaction
            )

            rank[level] += accumulatedRank
            updateKeys[level] = lastKeyBeforeInsert
        }

        // Phase 2: Assign level to new node
        let newLevel = min(levelAssignment.randomLevel(), currentLevels)

        // Phase 3: Insert at each level with Span Counter updates
        for level in 0..<newLevel {
            let key = try makeKey(score: score, primaryKey: primaryKey, level: level)

            // Calculate new span for this entry
            // newSpan = oldSpan - (rank[0] - rank[level])
            var newSpan: Int64 = 1

            if let updateKey = updateKeys[level] {
                // Read old span from the node before insertion point
                if let oldSpanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let oldSpan = try SpanValue.decode(oldSpanBytes)
                    newSpan = oldSpan.count - (rank[0] - rank[level])

                    // Update span of the node before insertion point
                    // newUpdateSpan = (rank[0] - rank[level]) + 1
                    let newUpdateSpan = (rank[0] - rank[level]) + 1
                    transaction.setValue(
                        SpanValue(count: newUpdateSpan).encoded(),
                        for: updateKey
                    )
                }
            }

            // Insert new entry with calculated span
            transaction.setValue(SpanValue(count: newSpan).encoded(), for: key)
        }

        // Phase 4: Increment span at higher levels (levels >= newLevel)
        for level in newLevel..<currentLevels {
            if let updateKey = updateKeys[level] {
                // Read current span
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let currentSpan = try SpanValue.decode(spanBytes)
                    // Increment by 1
                    let newSpan = currentSpan.count + 1
                    transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
                }
            }
        }

        return rank[0]
    }

    // MARK: - Helper Methods

    /// Find insertion point at a specific level
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

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard levelSubspace.contains(key) else { break }

            // Parse key
            let suffix = try levelSubspace.unpack(key)
            guard !suffix.isEmpty else { continue }

            guard let scoreElement = suffix[0] else { continue }
            let currentScore = try TupleDecoder.decode(scoreElement, as: Score.self)

            // Compare scores
            if currentScore >= targetScore {
                // Check if exact match or after target
                if currentScore == targetScore {
                    let currentPK = extractPrimaryKey(from: suffix)
                    if compareTuples(currentPK, targetPrimaryKey) != .orderedAscending {
                        // Current key >= target, stop here
                        break
                    }
                } else {
                    // Current score > target, stop here
                    break
                }
            }

            // currentScore < targetScore (or same score with lower PK), accumulate span
            let span = try SpanValue.decode(value)
            accumulatedRank += span.count
            lastKey = key
        }

        return (accumulatedRank, lastKey)
    }

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
