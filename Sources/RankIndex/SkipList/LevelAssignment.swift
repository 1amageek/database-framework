// LevelAssignment.swift
// Probabilistic level assignment for Skip List
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet (hash-based assignment)

import Foundation

/// Level assignment strategy for skip list nodes
public enum LevelAssignmentStrategy: Sendable {
    /// Probabilistic assignment: 1/2 chance to promote to next level
    case probabilistic

    /// Hash-based assignment: deterministic based on key hash
    case hashBased
}

/// Level assignment for skip list nodes
///
/// Determines which levels a node should appear in.
///
/// Distribution (probabilistic, p=0.5):
/// - Level 0: 100% (all elements)
/// - Level 1: 50%
/// - Level 2: 25%
/// - Level 3: 12.5%
/// - Level 4: 6.25%
public struct LevelAssignment: Sendable {

    // MARK: - Properties

    /// Maximum number of levels (default: 16)
    public let maxLevel: Int

    /// Assignment strategy
    public let strategy: LevelAssignmentStrategy

    /// Promotion probability (for probabilistic strategy)
    public let promotionProbability: Double

    // MARK: - Initialization

    /// Initialize level assignment
    ///
    /// - Parameters:
    ///   - maxLevel: Maximum number of levels (default: 16)
    ///   - strategy: Assignment strategy (default: .probabilistic)
    ///   - promotionProbability: Probability to promote to next level (default: 0.5)
    public init(
        maxLevel: Int = 16,
        strategy: LevelAssignmentStrategy = .probabilistic,
        promotionProbability: Double = 0.5
    ) {
        self.maxLevel = maxLevel
        self.strategy = strategy
        self.promotionProbability = promotionProbability
    }

    // MARK: - Level Assignment

    /// Assign a random level to a new node
    ///
    /// - Returns: Level number (1 to maxLevel)
    public func randomLevel() -> Int {
        switch strategy {
        case .probabilistic:
            return randomLevelProbabilistic()
        case .hashBased:
            // For hash-based, we need the key hash
            // This is called with random seed for now
            let hash = Int.random(in: 0..<Int.max)
            return levelForHash(hash)
        }
    }

    /// Assign level based on key hash (deterministic)
    ///
    /// - Parameter keyHash: Hash of the key
    /// - Returns: Level number (1 to maxLevel)
    public func levelForKey(hash keyHash: Int) -> Int {
        levelForHash(keyHash)
    }

    // MARK: - Private Implementation

    /// Probabilistic level assignment
    ///
    /// Each level has `promotionProbability` chance to promote to next level.
    private func randomLevelProbabilistic() -> Int {
        var level = 1

        while level < maxLevel && Double.random(in: 0..<1) < promotionProbability {
            level += 1
        }

        return level
    }

    /// Hash-based level assignment (FoundationDB Record Layer approach)
    ///
    /// Uses bit pattern of hash to determine level.
    /// If lowest k bits are all 0, node appears in level k.
    private func levelForHash(_ hash: Int) -> Int {
        var level = 1
        var mask = 1

        while level < maxLevel && (hash & mask) == 0 {
            level += 1
            mask <<= 1
        }

        return level
    }

    // MARK: - Dynamic Level Recommendation

    /// Recommend number of levels based on element count
    ///
    /// Uses log₂(count) + 2 as base, capped at maxLevel.
    ///
    /// Examples:
    /// - 1,000 entries → 12 levels
    /// - 10,000 entries → 15 levels
    /// - 100,000 entries → 16 levels (capped)
    ///
    /// - Parameter count: Number of elements in the skip list
    /// - Returns: Recommended number of levels
    public func recommendedLevels(for count: Int) -> Int {
        guard count > 0 else { return 4 }

        let base = Int(log2(Double(count))) + 2
        let recommended = max(4, base)
        return min(recommended, maxLevel)
    }
}
