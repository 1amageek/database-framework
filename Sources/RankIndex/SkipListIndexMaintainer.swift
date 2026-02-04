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

    /// Skip list traversal for O(log n) operations
    private let traversal: SkipListTraversal<Score>

    /// Skip list insertion with Span Counter maintenance
    private let insertion: SkipListInsertion<Score>

    /// Skip list deletion with Span Counter maintenance
    private let deletion: SkipListDeletion<Score>

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
        self.traversal = SkipListTraversal<Score>(
            subspaces: subspaces,
            maxLevels: maxLevels
        )
        self.insertion = SkipListInsertion<Score>(
            subspaces: subspaces,
            levelAssignment: levelAssignment
        )
        self.deletion = SkipListDeletion<Score>(subspaces: subspaces)
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
    /// Time Complexity: O(log n + k) using skip list traversal
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

        // Get total count
        let totalCount = try await getCount(transaction: transaction)
        guard totalCount > 0 else { return [] }

        // Use O(log n + k) traversal
        let results = try await traversal.getTopK(
            k: k,
            totalCount: totalCount,
            transaction: transaction
        )

        // Convert to expected format
        return results.map { (score, primaryKey, _) in
            var elements: [any TupleElement] = []
            for i in 0..<primaryKey.count {
                if let element = primaryKey[i] {
                    elements.append(element)
                }
            }
            return (score: score, primaryKey: elements)
        }
    }

    /// Get rank of a specific score
    ///
    /// Time Complexity: O(log n) using span accumulation
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
        // We need a primary key to identify the exact entry
        // For now, use the first entry with this score
        // TODO: Accept primaryKey parameter for exact lookup

        // Find any entry with this score at Level 0
        let levelSubspace = subspaces.leaf
        let scoreElement = try TupleEncoder.encode(score)

        // Build prefix for keys with this score
        // Keys are: levelSubspace.pack(Tuple([score, pk...]))
        // So prefix is: levelSubspace.pack(Tuple([score]))
        let scorePrefixBytes = levelSubspace.pack(Tuple([scoreElement]))
        let rangeEnd = levelSubspace.range().end

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(scorePrefixBytes),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var foundPrimaryKey: Tuple?
        for try await (key, _) in sequence {
            // Check if key starts with our score prefix
            guard key.count > scorePrefixBytes.count,
                  key.prefix(scorePrefixBytes.count).elementsEqual(scorePrefixBytes) else {
                break
            }

            // Extract full tuple and get primary key part
            let fullTuple = try levelSubspace.unpack(key)
            guard fullTuple.count > 1 else { continue }

            // Primary key is everything after the score
            var pkElements: [any TupleElement] = []
            for i in 1..<fullTuple.count {
                if let element = fullTuple[i] {
                    pkElements.append(element)
                }
            }
            foundPrimaryKey = Tuple(pkElements)
            break
        }

        guard let primaryKey = foundPrimaryKey else {
            throw IndexError.invalidStructure("Score \(score) not found in index")
        }

        // Get current levels and total count
        let currentLevels = try await getCurrentLevels(transaction: transaction)
        let totalCount = try await getCount(transaction: transaction)

        // Use O(log n) traversal
        return try await traversal.getRank(
            score: score,
            primaryKey: primaryKey,
            currentLevels: currentLevels,
            totalCount: totalCount,
            transaction: transaction
        )
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

    /// Insert entry into skip list with accurate Span Counter updates
    private func insertEntry(
        item: Item,
        id: Tuple?,
        transaction: any TransactionProtocol
    ) async throws {
        guard let (score, primaryKey) = try await extractScore(from: item, id: id) else {
            return
        }

        let currentLevels = try await getCurrentLevels(transaction: transaction)

        // Use SkipListInsertion for accurate Span Counter maintenance
        _ = try await insertion.insert(
            score: score,
            primaryKey: primaryKey,
            currentLevels: currentLevels,
            transaction: transaction
        )

        // Update count (atomic)
        let incrementBytes = ByteConversion.int64ToBytes(1)
        transaction.atomicOp(key: subspaces.countKey, param: incrementBytes, mutationType: .add)
    }

    /// Delete entry from skip list with accurate Span Counter updates
    private func deleteEntry(
        item: Item,
        id: Tuple?,
        transaction: any TransactionProtocol
    ) async throws {
        guard let (score, primaryKey) = try await extractScore(from: item, id: id) else {
            return
        }

        let currentLevels = try await getCurrentLevels(transaction: transaction)

        // Use SkipListDeletion for accurate Span Counter maintenance
        try await deletion.delete(
            score: score,
            primaryKey: primaryKey,
            currentLevels: currentLevels,
            transaction: transaction
        )

        // Update count (atomic)
        let decrementBytes = ByteConversion.int64ToBytes(-1)
        transaction.atomicOp(key: subspaces.countKey, param: decrementBytes, mutationType: .add)
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

}
