// SkipListDeletion.swift
// Skip List deletion with accurate Span Counter maintenance
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet

import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

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
                // Read current span of update node
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let currentSpan = try SpanValue.decode(spanBytes)
                    // newSpan = currentSpan + deletedSpan - 1
                    let newSpan = currentSpan.count + deletedSpan - 1
                    transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
                }
            }
        }

        // Phase 3: Decrement span at higher levels (where node doesn't exist)
        for level in (maxDeletedLevel + 1)..<currentLevels {
            if let updateKey = updateKeys[level] {
                // Read current span
                if let spanBytes = try await transaction.getValue(for: updateKey, snapshot: false) {
                    let currentSpan = try SpanValue.decode(spanBytes)
                    // Decrement by 1
                    let newSpan = currentSpan.count - 1
                    transaction.setValue(SpanValue(count: newSpan).encoded(), for: updateKey)
                }
            }
        }
    }

    // MARK: - Helper Methods

    /// Find deletion point at a specific level
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
            if currentScore == targetScore {
                let currentPK = extractPrimaryKey(from: suffix)
                if compareTuples(currentPK, targetPrimaryKey) == .orderedSame {
                    // Found the entry to delete at this level
                    let span = try SpanValue.decode(value)
                    deletedSpan = span.count
                    break
                } else if compareTuples(currentPK, targetPrimaryKey) == .orderedDescending {
                    // Passed the target
                    break
                }
            } else if currentScore > targetScore {
                // Passed the target
                break
            }

            // currentScore < targetScore (or same score with lower PK)
            lastKey = key
        }

        return (lastKey, deletedSpan)
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
