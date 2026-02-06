// SkipListDeletion.swift
// Skip List deletion with accurate Span Counter maintenance
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet

import Foundation
import FoundationDB
import Core
import DatabaseEngine

/// Skip List deletion with Span Counter maintenance
///
/// Implements the deletion algorithm with accurate Span Counter updates
/// to maintain O(log n) rank lookup performance.
public struct SkipListDeletion<Score: Comparable & Numeric & Codable & Sendable>: Sendable {

    // MARK: - Properties

    private let subspaces: SkipListSubspaces

    // MARK: - Initialization

    public init(subspaces: SkipListSubspaces) {
        self.subspaces = subspaces
    }

    // MARK: - Deletion Algorithm

    /// Delete entry with accurate Span Counter updates
    ///
    /// Algorithm:
    /// ```
    /// Phase 1: Find deletion position at each level (top → bottom)
    ///   - Track update[level] = node before deletion point at each level
    ///   - Track deletedSpan[level] = span of deleted node at each level
    ///
    /// Phase 2: Delete at each level and update Span Counters
    ///   - update[level].span += deletedSpan[level] - 1
    ///
    /// Phase 3: Decrement span at higher levels (where node doesn't exist)
    ///   - update[level].span -= 1
    /// ```
    ///
    /// - Parameters:
    ///   - score: Score value
    ///   - primaryKey: Primary key tuple
    ///   - currentLevels: Current number of levels
    ///   - transaction: FDB transaction
    public func delete(
        score: Score,
        primaryKey: Tuple,
        currentLevels: Int,
        transaction: any TransactionProtocol
    ) async throws {
        // Phase 1: Find deletion position and track update nodes
        var updateKeys: [[UInt8]?] = Array(repeating: nil, count: currentLevels)
        var deletedSpans: [Int64?] = Array(repeating: nil, count: currentLevels)
        var maxDeletedLevel = -1

        for level in stride(from: currentLevels - 1, through: 0, by: -1) {
            // Find the node before deletion point
            let (lastKeyBefore, deletedSpan) = try await findDeletionPoint(
                level: level,
                targetScore: score,
                targetPrimaryKey: primaryKey,
                transaction: transaction
            )

            updateKeys[level] = lastKeyBefore
            deletedSpans[level] = deletedSpan

            if deletedSpan != nil {
                maxDeletedLevel = max(maxDeletedLevel, level)
            }
        }

        guard maxDeletedLevel >= 0 else {
            // Entry not found
            throw IndexError.invalidStructure("Entry not found for deletion")
        }

        // Phase 2: Delete at each level and update Span Counters
        for level in 0...maxDeletedLevel {
            let deleteKey = try makeKey(score: score, primaryKey: primaryKey, level: level)

            // Delete the entry
            transaction.clear(key: deleteKey)

            // Update span of the node before deletion point
            if let updateKey = updateKeys[level],
               let deletedSpan = deletedSpans[level] {
                // Standard case: update node before deletion point
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let currentSpan = try SpanValue.decode(spanBytes)
                    // newSpan = currentSpan + deletedSpan - 1
                    let newSpan = currentSpan.count + deletedSpan - 1
                    transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
                }
            } else if let deletedSpan = deletedSpans[level] {
                // updateKey == nil: deleting first entry at this level
                // Next entry (new first) needs to absorb the deleted span
                let nextEntry = try await findFirstEntryAtLevel(level, transaction)
                if let (nextScore, nextPK) = nextEntry {
                    let nextKey = try makeKey(score: nextScore, primaryKey: nextPK, level: level)
                    if let spanBytes = try await transaction.getValue(for: nextKey, snapshot: false) {
                        let nextSpan = try SpanValue.decode(spanBytes).count
                        // New first entry's span = its current span + deleted span - 1
                        let newSpan = nextSpan + deletedSpan - 1
                        transaction.setValue(SpanValue(count: newSpan).encoded(), for: nextKey)
                    }
                }
            }
        }

        // Phase 3: Decrement span at higher levels (where node doesn't exist)
        for level in (maxDeletedLevel + 1)..<currentLevels {
            if let updateKey = updateKeys[level] {
                // Standard case: decrement span of update node
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let currentSpan = try SpanValue.decode(spanBytes)
                    // Decrement by 1 (one Level 0 entry removed)
                    let newSpan = currentSpan.count - 1
                    transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
                }
            } else {
                // updateKey == nil: deleted entry was before first entry at this level
                // Decrement first entry's span
                let firstEntry = try await findFirstEntryAtLevel(level, transaction)
                if let (firstScore, firstPK) = firstEntry {
                    let firstKey = try makeKey(score: firstScore, primaryKey: firstPK, level: level)
                    if let spanBytes = try await transaction.getValue(for: firstKey, snapshot: false) {
                        let firstSpan = try SpanValue.decode(spanBytes).count
                        // Decrement by 1 (one Level 0 entry removed)
                        let newSpan = firstSpan - 1
                        transaction.setValue(SpanValue(count: newSpan).encoded(), for: firstKey)
                    }
                }
            }
        }
    }

    // MARK: - Helper Methods

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
            let primaryKey = SkipListSubspaces.extractPrimaryKey(from: suffix)

            return (score, primaryKey)
        }

        return nil
    }

    /// Find deletion point at a specific level - Zero-Copy Implementation
    ///
    /// **Zero-Copy Design**: Uses direct byte comparison of packed FDB keys.
    /// FDB Tuple Layer guarantees lexicographic byte order, so packed keys
    /// `[score][primaryKey]` can be compared without unpacking.
    ///
    /// Returns:
    /// - lastKeyBeforeDelete: Key of the last entry before deletion point
    /// - deletedSpan: Span of the entry to be deleted (nil if not found at this level)
    private func findDeletionPoint(
        level: Int,
        targetScore: Score,
        targetPrimaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> (lastKeyBeforeDelete: [UInt8]?, deletedSpan: Int64?) {
        var lastKey: [UInt8]? = nil
        var deletedSpan: Int64? = nil

        // Zero-copy: Pre-compute target key once (includes levelSubspace prefix)
        let targetKey = try makeKey(score: targetScore, primaryKey: targetPrimaryKey, level: level)

        let levelSubspace = subspaces.subspace(for: level)
        let range = levelSubspace.range()

        // Scan in descending order (highest to lowest score)
        let sequence = transaction.getRange(
            from: range.begin,
            to: range.end,
            limit: 0,
            reverse: true,
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard levelSubspace.contains(key) else { break }

            // Zero-copy: Direct byte comparison without unpack/pack cycle
            if key == targetKey {
                // Found the entry to delete at this level
                let span = try SpanValue.decode(value)
                deletedSpan = span.count
                break
            } else if key.lexicographicallyPrecedes(targetKey) {
                // key < targetKey: passed the target in descending order
                break
            }

            // key > targetKey: update lastKey and continue
            lastKey = key
        }

        return (lastKey, deletedSpan)
    }

    /// Make key for a specific level
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
