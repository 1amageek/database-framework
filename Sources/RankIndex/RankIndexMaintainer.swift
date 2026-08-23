// RankIndexMaintainer.swift
// RankIndexLayer - Index maintainer for RANK indexes
//
// Maintains score-ordered entries and an atomic entry count for rank queries.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
/// **Algorithm**: ordered score keys
/// - Score entries are ordered by the storage engine's tuple encoding
/// - A single atomic counter stores the total number of indexed entries
///
/// **Index Structure**:
/// ```
/// // Score entries
/// Key: [indexSubspace]["scores"][score][primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = RankIndexMaintainer<Player, Int64>(
///     index: rankIndex,
///     subspace: rankSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct RankIndexMaintainer<
    Item: PersistedEntityValue,
    Score: IndexNumericValue & TupleDecodable
>: SubspaceIndexMaintainer {
    public let index: ResolvedIndex
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // Subspace for score entries
    private let scoresSubspace: Subspace

    // Key for atomic entry count (O(1) count queries)
    private let countKey: ByteString

    public init(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.scoresSubspace = subspace.subspace("scores")
        self.countKey = subspace.pack(Tuple("_count"))
    }

    /// Update index when item changes
    ///
    /// **Process**:
    /// 1. Remove old score entry (if exists) and decrement count
    /// 2. Add new score entry and increment count
    ///
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem = oldItem {
            if let oldKey = try buildScoreKey(for: oldItem) {
                try transaction.clear(key: oldKey)
                let decrementBytes = ByteConversion.int64ToBytes(-1)
                try transaction.atomicOp(key: countKey, param: decrementBytes, mutationType: .add)
            }
        }

        if let newItem = newItem {
            if let newKey = try buildScoreKey(for: newItem) {
                let value = try CoveringValueBuilder.build(for: newItem, index: index)
                try transaction.setValue(value, for: newKey)
                let incrementBytes = ByteConversion.int64ToBytes(1)
                try transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
            }
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        if let scoreKey = try buildScoreKey(for: item, id: id) {
            let value = try CoveringValueBuilder.build(for: item, index: index)
            try transaction.setValue(value, for: scoreKey)
            // Increment count atomically
            let incrementBytes = ByteConversion.int64ToBytes(1)
            try transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        if let key = try buildScoreKey(for: item, id: id) {
            return [key]
        }
        return []
    }

    /// Get top K items (highest scores).
    ///
    /// **Algorithm**: bounded reverse storage range scan with `limit: k`. O(K).
    /// The scores subspace is `[subspace]["scores"][score][primaryKey]`, and
    /// canonical tuple ordering yields highest scores first when reversed,
    /// without reading more than K entries.
    ///
    /// - Parameters:
    ///   - k: Number of items to return
    ///   - transaction: Storage transaction
    /// - Returns: Array of (score, primaryKey) tuples, sorted by score descending
    public func getTopK(
        k: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [(score: Score, primaryKey: [any TupleElement])] {
        let scanner = RankScanner(scoresSubspace: scoresSubspace, transaction: transaction)
        let entries = try await scanner.top(k: k)

        var results: [(score: Score, primaryKey: [any TupleElement])] = []
        results.reserveCapacity(entries.count)
        for entry in entries {
            let score = try TupleDecoder.decode(entry.scoreElement, as: Score.self)
            let primaryKey = try entry.primaryKey.elements()
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
    /// Current implementation: O(rank), where rank is the number of entries
    /// whose score is strictly greater than the requested score.
    ///
    /// - Parameters:
    ///   - score: Score to find rank for
    ///   - transaction: Storage transaction
    /// - Returns: Rank (0-based, 0 = highest)
    public func getRank(
        score: Score,
        transaction: any TransactionReadAccess
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

        // Build prefix for this score, then append 0xFF to get past all entries with this score.
        let scorePrefixEnd = try strinc(
            scoresSubspace.pack(Tuple(scoreElement))
        )

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(scorePrefixEnd),
            to: .firstGreaterOrEqual(rangeEnd),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
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
    /// - Parameter transaction: Storage transaction
    /// - Returns: Total number of entries
    public func getCount(
        transaction: any TransactionReadAccess
    ) async throws -> Int64 {
        guard let bytes = try await transaction.getValue(for: countKey, snapshot: true) else {
            return 0
        }
        return try RankCounterCodec.decode(bytes)
    }

    /// Get score at a given percentile
    ///
    /// **Type-Safe**: Returns Score type instead of forcing Int64.
    ///
    /// Uses the O(1) atomic count and an ordered reverse range read limited to
    /// `targetRank + 1` entries. The read cost is O(targetRank + 1).
    ///
    /// - Parameters:
    ///   - percentile: Percentile value (0.0 to 1.0, e.g., 0.95 for 95th percentile)
    ///   - transaction: Storage transaction
    /// - Returns: Score at the given percentile, or nil if empty
    public func getPercentile(
        _ percentile: Double,
        transaction: any TransactionReadAccess
    ) async throws -> Score? {
        guard percentile >= 0.0 && percentile <= 1.0 else {
            throw RankIndexMaintenanceError.invalidPercentile(percentile)
        }

        let totalCount = try await getCount(transaction: transaction)
        guard totalCount > 0 else { return nil }
        guard let totalCountInt = Int(exactly: totalCount) else {
            throw RankCounterError.exceedsPlatformInt(totalCount)
        }

        // Calculate how many top entries we need
        // 95th percentile = score at rank 5% = need top (5% + 1) entries
        let targetRank = Int(Double(totalCount) * (1.0 - percentile))
        let k = min(targetRank + 1, totalCountInt)

        if k <= 0 {
            // 100th percentile - return highest score
            let topOne = try await getTopK(k: 1, transaction: transaction)
            return topOne.first?.score
        }

        // Read only the prefix ending at the target rank.
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
    private func buildScoreKey(for item: Item, id: Tuple? = nil) throws -> ByteString? {
        // Evaluate index expression using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        // Sparse index: if score field is nil, return nil (no index entry)
        let scoreValues: [any TupleElement]
        do {
            scoreValues = try DataAccess.evaluate(
                item: item,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil score is not indexed
            return nil
        }

        guard scoreValues.count == 1 else {
            throw RankIndexMaintenanceError.invalidScoreFieldCount(
                actual: scoreValues.count
            )
        }

        // Extract score as Score type (type-safe)
        let score = try TupleDecoder.decode(scoreValues[0], as: Score.self)
        switch score.fieldValue {
        case .float32(let value) where !value.isFinite:
            throw RankIndexMaintenanceError.unorderedFloatingPoint(
                indexName: index.name
            )
        case .float64(let value) where !value.isFinite:
            throw RankIndexMaintenanceError.unorderedFloatingPoint(
                indexName: index.name
            )
        default:
            break
        }

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
        allElements.append(contentsOf: try primaryKeyTuple.elements())

        return try packAndValidate(Tuple(allElements), in: scoresSubspace)
    }
}
