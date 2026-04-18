// RankIndexMaintainer.swift
// RankIndexLayer - Index maintainer for RANK indexes
//
// Maintains rank indexes using Range Tree algorithm for efficient ranking queries.

import Foundation
import Core
import DatabaseEngine
import StorageKit

/// Maintainer for RANK indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Score` type parameter preserves the score type at compile time
/// - Query results preserve the original Score type
///
/// **Functionality**:
/// - Leaderboard queries (top-K)
/// - Rank lookup (what's my rank?)
/// - Percentile queries (95th percentile)
/// - Count queries (how many above/below score?)
///
/// **Algorithm**: Range Tree (simplified version)
/// - Leaf level: Individual score entries
/// - Count nodes: Hierarchical bucket counts (future optimization)
///
/// **Index Structure** (simplified):
/// ```
/// // Score entries
/// Key: [indexSubspace]["scores"][score][primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Note**: This is a simplified implementation for basic ranking.
/// For production use with large datasets (>100K items), consider implementing
/// the full Range Tree algorithm from fdb-record-layer with hierarchical buckets.
///
/// **Usage**:
/// ```swift
/// let maintainer = RankIndexMaintainer<Player, Int64>(
///     index: rankIndex,
///     bucketSize: 100,
///     subspace: rankSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct RankIndexMaintainer<Item: Persistable, Score: Comparable & Numeric & Codable & Sendable>: SubspaceIndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let bucketSize: Int

    // Subspace for score entries
    private let scoresSubspace: Subspace

    // Key for atomic entry count (O(1) count queries)
    private let countKey: [UInt8]

    public init(
        index: Index,
        bucketSize: Int,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.bucketSize = bucketSize
        self.scoresSubspace = subspace.subspace("scores")
        self.countKey = subspace.pack(Tuple("_count"))
    }

    /// Update index when item changes
    ///
    /// **Process**:
    /// 1. Remove old score entry (if exists) and decrement count
    /// 2. Add new score entry and increment count
    ///
    /// **Note**: Full implementation would also update count nodes in Range Tree
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any Transaction
    ) async throws {
        if let oldItem = oldItem {
            if let oldKey = try buildScoreKey(for: oldItem) {
                transaction.clear(key: oldKey)
                let decrementBytes = ByteConversion.int64ToBytes(-1)
                transaction.atomicOp(key: countKey, param: decrementBytes, mutationType: .add)
            }
        }

        if let newItem = newItem {
            if let newKey = try buildScoreKey(for: newItem) {
                let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
                transaction.setValue(value, for: newKey)
                let incrementBytes = ByteConversion.int64ToBytes(1)
                transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
            }
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any Transaction
    ) async throws {
        if let scoreKey = try buildScoreKey(for: item, id: id) {
            let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
            transaction.setValue(value, for: scoreKey)
            // Increment count atomically
            let incrementBytes = ByteConversion.int64ToBytes(1)
            transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        if let key = try buildScoreKey(for: item, id: id) {
            return [key]
        }
        return []
    }

    /// Get top K items (highest scores).
    ///
    /// **Algorithm**: Native FDB reverse range scan with `limit: k`. O(K).
    /// The scores subspace is `[subspace]["scores"][score][primaryKey]`, and FDB
    /// preserves tuple ordering, so `reverse: true` yields highest scores first
    /// without reading more than K entries.
    ///
    /// - Parameters:
    ///   - k: Number of items to return
    ///   - transaction: FDB transaction
    /// - Returns: Array of (score, primaryKey) tuples, sorted by score descending
    public func getTopK(
        k: Int,
        transaction: any Transaction
    ) async throws -> [(score: Score, primaryKey: [any TupleElement])] {
        guard k > 0 else { return [] }

        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)
        let entries = try await scanner.top(k: k)

        var results: [(score: Score, primaryKey: [any TupleElement])] = []
        results.reserveCapacity(entries.count)
        for entry in entries {
            let score = try TupleDecoder.decode(entry.scoreElement, as: Score.self)
            let primaryKey: [any TupleElement] = (0..<entry.primaryKey.count).compactMap { entry.primaryKey[$0] }
            results.append((score: score, primaryKey: primaryKey))
        }
        return results
    }

    /// Get rank for a specific score
    ///
    /// **Type-Safe**: Accepts Score type instead of forcing Int64.
    ///
    /// Returns the number of items with score greater than the given score.
    /// Rank 0 = highest score.
    ///
    /// **Optimization**: Only scans entries with score > target.
    /// Previous implementation: O(n) full scan.
    /// Current implementation: O(n - rank) where rank is the result.
    /// (High scores have low rank, so fewer entries to scan.)
    ///
    /// **Note**: Full Range Tree implementation would be O(log n).
    ///
    /// - Parameters:
    ///   - score: Score to find rank for
    ///   - transaction: FDB transaction
    /// - Returns: Rank (0-based, 0 = highest)
    public func getRank(
        score: Score,
        transaction: any Transaction
    ) async throws -> Int64 {
        // Count entries with score strictly greater than target.
        // Key structure: [scoresSubspace][score][pk]
        // Tuple encoding preserves numeric ordering.
        //
        // Use score prefix + 0xFF as boundary to skip all entries with
        // the same score (regardless of primary key suffix).
        // This works correctly for both Int64 and Double score types.

        let rangeEnd = scoresSubspace.range().end

        let scoreElement = try TupleEncoder.encode(score)

        // Build prefix for this score, then append 0xFF to get past all entries with this score
        let scorePrefixEnd = scoresSubspace.pack(Tuple(scoreElement)) + [0xFF]

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(scorePrefixEnd),
            to: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var count: Int64 = 0
        for (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }
            count += 1
        }

        return count
    }

    /// Get total count of entries in the index
    ///
    /// **Optimization**: Uses atomic counter for O(1) lookup.
    /// Previous implementation: O(n) full scan.
    /// Current implementation: O(1) single key read.
    ///
    /// - Parameter transaction: FDB transaction
    /// - Returns: Total number of entries
    public func getCount(
        transaction: any Transaction
    ) async throws -> Int64 {
        guard let bytes = try await transaction.getValue(for: countKey, snapshot: true) else {
            return 0
        }
        return ByteConversion.bytesToInt64(bytes)
    }

    /// Get score at a given percentile
    ///
    /// **Type-Safe**: Returns Score type instead of forcing Int64.
    ///
    /// **Optimization**: Uses getTopK with bounded heap instead of full sort.
    /// Previous implementation: O(n log n) full scan and sort.
    /// Current implementation: O(n log k) where k = targetRank + 1.
    /// For high percentiles (95th), k is small (5% of n), making this much faster.
    ///
    /// - Parameters:
    ///   - percentile: Percentile value (0.0 to 1.0, e.g., 0.95 for 95th percentile)
    ///   - transaction: FDB transaction
    /// - Returns: Score at the given percentile, or nil if empty
    public func getPercentile(
        _ percentile: Double,
        transaction: any Transaction
    ) async throws -> Score? {
        guard percentile >= 0.0 && percentile <= 1.0 else {
            throw RankIndexError.invalidScore("Percentile must be between 0.0 and 1.0")
        }

        let totalCount = try await getCount(transaction: transaction)
        guard totalCount > 0 else { return nil }

        // Calculate how many top entries we need
        // 95th percentile = score at rank 5% = need top (5% + 1) entries
        let targetRank = Int(Double(totalCount) * (1.0 - percentile))
        let k = min(targetRank + 1, Int(totalCount))

        if k <= 0 {
            // 100th percentile - return highest score
            let topOne = try await getTopK(k: 1, transaction: transaction)
            return topOne.first?.score
        }

        // Get top k entries using optimized heap-based method
        let topK = try await getTopK(k: k, transaction: transaction)

        // Return the score at targetRank position (last in the top-k list)
        return topK.last?.score
    }

    // MARK: - Private Methods

    /// Build score key for an item
    ///
    /// Key structure: [scoresSubspace][score][primaryKey]
    ///
    /// **Sparse index behavior**:
    /// If the score field is nil, returns nil (no index entry).
    ///
    /// **Type-Safe**: Uses Score type parameter for type-safe extraction.
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildScoreKey(for item: Item, id: Tuple? = nil) throws -> [UInt8]? {
        // Evaluate index expression using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        // Sparse index: if score field is nil, return nil (no index entry)
        let scoreValues: [any TupleElement]
        do {
            scoreValues = try DataAccess.evaluateIndexFields(
                from: item,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil score is not indexed
            return nil
        }

        guard !scoreValues.isEmpty else {
            return nil
        }

        // Extract score as Score type (type-safe)
        let score = try TupleDecoder.decode(scoreValues[0], as: Score.self)

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Build key: [score][primaryKey...]
        // Score conforms to Numeric, which includes TupleElement-compatible types
        let scoreElement = try TupleEncoder.encode(score)
        var allElements: [any TupleElement] = [scoreElement]
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allElements.append(element)
            }
        }

        return try packAndValidate(Tuple(allElements), in: scoresSubspace)
    }
}

