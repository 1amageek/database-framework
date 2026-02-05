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

        // Phase 1: Find insertion position (FoundationDB Record Layer approach)
        // Each level independently finds insertion position
        // No position carried forward - ensures correctness
        var rank: [Int64] = Array(repeating: 0, count: currentLevels)
        var updateKeys: [[UInt8]?] = Array(repeating: nil, count: currentLevels)
        var updateSpans: [Int64?] = Array(repeating: nil, count: currentLevels)

        // For updateKey == nil levels, record the first entry
        var firstEntriesAtInsertionTime: [(score: Score, primaryKey: Tuple)?] = Array(repeating: nil, count: currentLevels)

        for level in 0..<currentLevels {
            // Each level independently scans from the beginning
            let result = try await findInsertionPoint(
                level: level,
                targetScore: score,
                targetPrimaryKey: primaryKey,
                transaction: transaction
            )

            rank[level] = result.accumulatedRank
            updateKeys[level] = result.lastKeyBeforeInsert
            if let updateKey = result.lastKeyBeforeInsert {
                // Read span of updateKey
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: true) {
                    updateSpans[level] = try SpanValue.decode(spanBytes).count
                }
            } else {
                // updateKey == nil: will insert at the beginning of this level
                firstEntriesAtInsertionTime[level] = try await findFirstEntryAtLevel(level, transaction)
            }
        }

        // Phase 2: Assign level to new node
        let newLevel = min(levelAssignment.randomLevel(), currentLevels)

        // Phase 2.5: For updateKey == nil levels < newLevel, calculate first entry info
        // This is done after newLevel is known to minimize unnecessary reads
        var firstEntryRanks: [Int64?] = Array(repeating: nil, count: currentLevels)
        var firstEntrySpans: [Int64?] = Array(repeating: nil, count: currentLevels)

        for level in 0..<newLevel {
            if updateKeys[level] == nil, let firstEntry = firstEntriesAtInsertionTime[level] {
                // Calculate first entry's Level 0 rank
                let firstRank = try await findInsertionPoint(
                    level: 0,
                    targetScore: firstEntry.score,
                    targetPrimaryKey: firstEntry.primaryKey,
                    transaction: transaction
                ).accumulatedRank
                firstEntryRanks[level] = firstRank

                // Read first entry's current span
                let firstKey = try makeKey(score: firstEntry.score, primaryKey: firstEntry.primaryKey, level: level)
                guard let spanBytes = try await transaction.getValue(for: firstKey, snapshot: true) else {
                    throw IndexError.invalidStructure(
                        "Expected span value for first entry at level \(level) (score: \(firstEntry.score)), but key not found"
                    )
                }
                let span = try SpanValue.decode(spanBytes).count
                firstEntrySpans[level] = span
            }
        }

        // Phase 3: Insert at each level with Pugh 1990 Span Counter formula
        for level in 0..<newLevel {
            let key = try makeKey(score: score, primaryKey: primaryKey, level: level)

            let newSpan: Int64

            if level == 0 {
                // Level 0: all entries have span = 1 (always)
                newSpan = 1
            } else if let updateKey = updateKeys[level], let oldSpan = updateSpans[level] {
                // Standard Pugh 1990 formula
                // update[level].span = (rank[0] - rank[level]) + 1
                // newNode.span = oldSpan - (rank[0] - rank[level])
                let updateSpan = (rank[0] - rank[level]) + 1
                newSpan = oldSpan - (rank[0] - rank[level])

                // Update span of the node before insertion point
                transaction.setValue(
                    SpanValue(count: updateSpan).encoded(),
                    for: updateKey
                )
            } else {
                // updateKey == nil: inserting at beginning of level (highest score)
                // This new entry becomes the first entry at this level
                //
                // Key insight: We use headSpan[level] to track "Level 0 entries before first entry".
                // When a new entry becomes the first, we need to:
                // 1. Include the old headSpan in the new entry's span calculation
                // 2. Reset headSpan to 0 (no entries before the new first entry)
                if let firstRank = firstEntryRanks[level] {
                    // First entry exists at this level
                    // newSpan = (old headSpan + distance from new entry to old first entry)
                    // Since rank[0] is the new entry's rank (always 0 for highest score insertions),
                    // and firstRank is the old first entry's rank (before insertion):
                    // newSpan = (firstRank + 1) - rank[0]
                    // This naturally includes the old headSpan if firstRank was calculated correctly
                    newSpan = (firstRank + 1) - rank[0]

                    // NOTE: Existing first entry's span does NOT change
                    // Reason: Distance from old first entry to its next entry remains the same
                } else {
                    // Level is empty (no entries at this level)
                    // newSpan = distance from new node to end
                    newSpan = (totalCountBefore + 1) - rank[0]
                }

                // Reset headSpan for this level (new entry is now the first)
                if level > 0 {
                    let headSpanKey = subspaces.headSpanKey(for: level)
                    transaction.setValue(ByteConversion.int64ToBytes(0), for: headSpanKey)
                }
            }

            // Insert new entry with calculated span
            transaction.setValue(SpanValue(count: newSpan).encoded(), for: key)
        }

        // Phase 4: Increment span at higher levels (levels >= newLevel)
        // One new Level 0 entry was added, so span at higher levels must be incremented
        //
        // Key insight: Pugh 1990 uses virtual HEAD at each level. FDB doesn't have HEAD.
        // We use headSpan[level] to track "number of Level 0 entries before first entry".
        //
        // When updateKey != nil: new entry falls within updateKey's span range
        //                        → updateKey.span += 1
        // When updateKey == nil: new entry is inserted before first entry
        //                        → headSpan[level] += 1 (equivalent to HEAD.span += 1)
        for level in newLevel..<currentLevels {
            if let updateKey = updateKeys[level], let currentSpan = updateSpans[level] {
                // updateKey != nil: new entry is inserted AFTER updateKey
                // This means the new entry falls within updateKey's span range
                // Therefore, updateKey.span must be incremented by 1
                let newSpan = currentSpan + 1
                transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
            } else if updateKeys[level] == nil && level > 0 {
                // updateKey == nil: new entry is inserted BEFORE the first entry
                // Increment headSpan[level] (equivalent to Pugh 1990's HEAD.span += 1)
                let headSpanKey = subspaces.headSpanKey(for: level)
                var headSpan: Int64 = 0
                if let headSpanBytes = try await transaction.getValue(for: headSpanKey, snapshot: false) {
                    headSpan = ByteConversion.bytesToInt64(headSpanBytes)
                }
                transaction.setValue(ByteConversion.int64ToBytes(headSpan + 1), for: headSpanKey)
            }
            // Level 0 has no headSpan (all entries are at Level 0)
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
            snapshot: false  // Must see current transaction's changes
        )

        for try await (key, _) in sequence {
            guard levelSubspace.contains(key) else { break }

            let suffix = try levelSubspace.unpack(key)
            guard !suffix.isEmpty, let scoreElement = suffix[0] else { continue }

            let score = try TupleDecoder.decode(scoreElement, as: Score.self)
            let primaryKey = SkipListSubspaces.extractPrimaryKey(from: suffix)

            return (score, primaryKey)
        }

        return nil
    }

    /// Find insertion point at a specific level (descending order) - Zero-Copy Implementation
    ///
    /// Scans in descending order (high to low scores).
    /// Accumulates span counters until reaching the insertion point.
    ///
    /// **Zero-Copy Design**: Uses direct byte comparison of packed FDB keys.
    /// FDB Tuple Layer guarantees lexicographic byte order, so packed keys
    /// `[score][primaryKey]` can be compared without unpacking.
    ///
    /// Based on FoundationDB Record Layer approach: each level independently
    /// scans from the beginning to find the insertion point.
    ///
    /// Parameters:
    /// - level: Level to scan
    /// - targetScore: Target score
    /// - targetPrimaryKey: Target primary key
    /// - transaction: FDB transaction
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

        // Zero-copy: Pre-compute target key once (includes levelSubspace prefix)
        let targetKey = try makeKey(score: targetScore, primaryKey: targetPrimaryKey, level: level)

        let levelSubspace = subspaces.subspace(for: level)
        let range = levelSubspace.range()

        // Scan in descending order (high to low scores)
        let sequence = transaction.getRange(
            from: range.begin,
            to: range.end,
            limit: 0,
            reverse: true,
            snapshot: true  // Use snapshot: true for optimistic locking (Skip List is probabilistic)
        )

        for try await (key, value) in sequence {
            guard levelSubspace.contains(key) else { break }

            // Zero-copy: Direct byte comparison without unpack/pack cycle
            // FDB Tuple Layer guarantees: packed([score][pk]) maintains lexicographic order
            //
            // Descending order stop condition: key <= targetKey
            // Using lexicographicallyPrecedes: key < targetKey || key == targetKey
            if !targetKey.lexicographicallyPrecedes(key) {
                // key <= targetKey: stop here, do NOT accumulate this entry's span
                break
            }

            // key > targetKey: accumulate span and continue
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

        var allElements: [any TupleElement] = []
        allElements.reserveCapacity(1 + primaryKey.count)
        allElements.append(scoreElement)
        for i in 0..<primaryKey.count {
            if let element = primaryKey[i] {
                allElements.append(element)
            }
        }

        return levelSubspace.pack(Tuple(allElements))
    }

}
