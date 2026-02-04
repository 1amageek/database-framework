// SkipListIndexMaintainer.swift
// Skip List with Span Counters implementation for RankIndex
//
// References:
// - Skip Lists: A Probabilistic Alternative to Balanced Trees (Pugh 1990)
// - FoundationDB Record Layer RankedSet
// - Redis Skip List implementation
//
// Performance:
// - Top-K: O(log n + k) vs O(n log k) with TopKHeap
// - getRank: O(log n) vs O(n - rank) with partial scan
// - getCount: O(1) (unchanged)

import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

/// Skip List index maintainer with Span Counters
///
/// This implementation provides O(log n) rank lookup and O(log n + k) top-K queries
/// using a probabilistic hierarchical data structure.
///
/// **Algorithm**: Skip List with Span Counters
/// - Multi-level skip list where each node has forward pointers at multiple levels
/// - Each link stores a "span counter" - the number of elements it skips
/// - Rank is computed by accumulating spans during traversal (no full scan needed)
///
/// **Key Layout**:
/// ```
/// [leaf]/[score][primaryKey] = SpanValue(count=1)              # Level 0
/// [levels]/[level]/[score][primaryKey] = SpanValue(count=n)    # Level 1-N
/// [metadata]/_numLevels = Int64
/// [metadata]/_count = Int64 (atomic)
/// ```
///
/// **Current Status**: Phase 1, Week 1 - Basic structure with O(k) fallback
/// TODO: Implement full O(log n) span-based rank lookup and top-K traversal
public struct SkipListIndexMaintainer<Item: Persistable, Score: Comparable & Numeric & Codable & Sendable>: SubspaceIndexMaintainer {

    // MARK: - Properties

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    /// Subspace layout
    private let subspaces: SkipListSubspaces

    /// Level assignment strategy
    private let levelAssignment: LevelAssignment

    /// Default number of levels for new skip lists
    private let defaultLevels: Int

    // MARK: - Initialization

    /// Initialize skip list index maintainer
    ///
    /// - Parameters:
    ///   - index: Index descriptor
    ///   - subspace: Base subspace for the index
    ///   - idExpression: Key expression for primary key
    ///   - defaultLevels: Default number of levels (default: 4)
    ///   - maxLevels: Maximum number of levels (default: 16)
    ///   - strategy: Level assignment strategy (default: .probabilistic)
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        defaultLevels: Int = 4,
        maxLevels: Int = 16,
        strategy: LevelAssignmentStrategy = .probabilistic
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.subspaces = SkipListSubspaces(base: subspace)
        self.defaultLevels = defaultLevels
        self.levelAssignment = LevelAssignment(
            maxLevel: maxLevels,
            strategy: strategy
        )
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        switch (oldItem, newItem) {
        case (nil, let new?):
            // Insert
            try await insertEntry(item: new, id: nil, transaction: transaction)

        case (let old?, nil):
            // Delete
            try await deleteEntry(item: old, id: nil, transaction: transaction)

        case (let old?, let new?):
            // Update
            try await deleteEntry(item: old, id: nil, transaction: transaction)
            try await insertEntry(item: new, id: nil, transaction: transaction)

        case (nil, nil):
            return
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        try await insertEntry(item: item, id: id, transaction: transaction)
    }

    public func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes] {
        // Get score from item
        guard let (score, primaryKey) = try await extractScore(from: item, id: id) else {
            return []
        }

        // For skip list, we compute the leaf level key
        // Higher level keys depend on random/hash assignment
        let leafKey = try makeKey(score: score, primaryKey: primaryKey, level: 0)
        return [leafKey]
    }

    // MARK: - Query Operations

    /// Get top K elements (highest scores)
    ///
    /// Time Complexity: O(k) scan (temporary fallback)
    /// TODO: Implement O(log n + k) using skip list traversal
    ///
    /// - Parameters:
    ///   - k: Number of elements to retrieve
    ///   - transaction: FDB transaction
    /// - Returns: Array of (score, primaryKey) tuples in descending order
    public func getTopK(
        k: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(score: Score, primaryKey: [any TupleElement])] {
        guard k > 0 else { return [] }

        // Temporary: O(k) scan from leaf level
        return try await scanTopKFromLeaf(k: k, transaction: transaction)
    }

    /// Get rank of a specific score
    ///
    /// Time Complexity: O(n - rank) scan (temporary fallback)
    /// TODO: Implement O(log n) using span accumulation
    ///
    /// - Parameters:
    ///   - score: Score to find rank for
    ///   - transaction: FDB transaction
    /// - Returns: Rank (0-based, 0 = highest score)
    /// - Throws: `IndexError.invalidStructure` if score doesn't exist
    public func getRank(
        score: Score,
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        // Temporary: O(n) scan from leaf level
        return try await scanRankFromLeaf(score: score, transaction: transaction)
    }

    /// Get total element count
    ///
    /// Time Complexity: O(1)
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: Total number of elements
    public func getCount(transaction: any TransactionProtocol) async throws -> Int64 {
        guard let value = try await transaction.getValue(for: subspaces.countKey, snapshot: true) else {
            return 0
        }
        return ByteConversion.bytesToInt64(value)
    }

    // MARK: - Private Implementation

    /// Extract score and primary key from item
    private func extractScore(from item: Item, id: Tuple?) async throws -> (score: Score, primaryKey: Tuple)? {
        // Evaluate index expression
        let scoreValues: [any TupleElement]
        do {
            scoreValues = try DataAccess.evaluateIndexFields(
                from: item,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            return nil
        }

        guard !scoreValues.isEmpty else {
            return nil
        }

        // Extract score
        let score = try TupleDecoder.decode(scoreValues[0], as: Score.self)

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        return (score: score, primaryKey: primaryKeyTuple)
    }

    /// Insert entry into skip list
    private func insertEntry(
        item: Item,
        id: Tuple?,
        transaction: any TransactionProtocol
    ) async throws {
        guard let (score, primaryKey) = try await extractScore(from: item, id: id) else {
            return
        }

        // Phase 1: Get current number of levels
        let currentLevels = try await getCurrentLevels(transaction: transaction)

        // Phase 2: Assign level to new node
        let newLevel = levelAssignment.randomLevel()

        // Phase 3: Insert at each level
        for level in 0..<min(newLevel, currentLevels) {
            let key = try makeKey(score: score, primaryKey: primaryKey, level: level)
            let span = SpanValue(count: 1)
            transaction.setValue(span.encoded(), for: key)
        }

        // Phase 4: Update count (atomic)
        let incrementBytes = ByteConversion.int64ToBytes(1)
        transaction.atomicOp(key: subspaces.countKey, param: incrementBytes, mutationType: .add)

        // TODO: Implement proper span counter updates
        // This is a simplified version - full implementation requires:
        // 1. Find insertion position at each level
        // 2. Update span counters for all affected links
        // 3. Handle level expansion if needed
    }

    /// Delete entry from skip list
    private func deleteEntry(
        item: Item,
        id: Tuple?,
        transaction: any TransactionProtocol
    ) async throws {
        guard let (score, primaryKey) = try await extractScore(from: item, id: id) else {
            return
        }

        let currentLevels = try await getCurrentLevels(transaction: transaction)

        // Delete from all levels where it exists
        for level in 0..<currentLevels {
            let key = try makeKey(score: score, primaryKey: primaryKey, level: level)
            transaction.clear(key: key)
        }

        // Update count (atomic)
        let decrementBytes = ByteConversion.int64ToBytes(-1)
        transaction.atomicOp(key: subspaces.countKey, param: decrementBytes, mutationType: .add)

        // TODO: Implement proper span counter updates on deletion
    }

    /// Get current number of levels
    private func getCurrentLevels(transaction: any TransactionProtocol) async throws -> Int {
        let value = try? await transaction.getValue(for: subspaces.numLevelsKey, snapshot: true)

        if let value = value, !value.isEmpty {
            return Int(ByteConversion.bytesToInt64(value))
        } else {
            // Initialize with default levels
            transaction.setValue(ByteConversion.int64ToBytes(Int64(defaultLevels)), for: subspaces.numLevelsKey)
            return defaultLevels
        }
    }

    /// Make key for a specific level
    private func makeKey(score: Score, primaryKey: Tuple, level: Int) throws -> [UInt8] {
        let levelSubspace = subspaces.subspace(for: level)
        let scoreElement = try TupleEncoder.encode(score)

        // Build tuple: [scoreElement] + primaryKey elements
        var allElements: [any TupleElement] = [scoreElement]
        for i in 0..<primaryKey.count {
            if let element = primaryKey[i] {
                allElements.append(element)
            }
        }

        return levelSubspace.pack(Tuple(allElements))
    }

    // MARK: - Fallback Implementations (Temporary)

    /// Scan top K from leaf level (O(k) scan, temporary)
    private func scanTopKFromLeaf(
        k: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(score: Score, primaryKey: [any TupleElement])] {
        let range = subspaces.leaf.range()
        var results: [(score: Score, primaryKey: [any TupleElement])] = []

        // Scan in reverse order (highest scores first)
        let sequence = transaction.getRange(
            beginSelector: .lastLessThan(range.end),
            endSelector: .firstGreaterOrEqual(range.begin),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard subspaces.leaf.contains(key) else { break }

            let suffix = try subspaces.leaf.unpack(key)

            // Extract score and primary key
            var elements: [any TupleElement] = []
            for i in 0..<suffix.count {
                if let element = suffix[i] {
                    elements.append(element)
                }
            }

            guard elements.count >= 2 else { continue }

            let score = try TupleDecoder.decode(elements[0], as: Score.self)
            let primaryKey = Array(elements.dropFirst())

            results.append((score: score, primaryKey: primaryKey))

            if results.count >= k {
                break
            }
        }

        return results
    }

    /// Scan rank from leaf level (O(n) scan, temporary)
    private func scanRankFromLeaf(
        score: Score,
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        let range = subspaces.leaf.range()
        var rank: Int64 = 0
        var found = false

        // Scan in reverse order (highest scores first)
        let sequence = transaction.getRange(
            beginSelector: .lastLessThan(range.end),
            endSelector: .firstGreaterOrEqual(range.begin),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard subspaces.leaf.contains(key) else { break }

            let suffix = try subspaces.leaf.unpack(key)

            // Extract score
            var elements: [any TupleElement] = []
            for i in 0..<suffix.count {
                if let element = suffix[i] {
                    elements.append(element)
                }
            }

            guard !elements.isEmpty else { continue }

            let currentScore = try TupleDecoder.decode(elements[0], as: Score.self)

            if currentScore == score {
                found = true
                break
            }

            rank += 1
        }

        guard found else {
            throw IndexError.invalidStructure("Score \(score) not found in index")
        }

        return rank
    }
}
