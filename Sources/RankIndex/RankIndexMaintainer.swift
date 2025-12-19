// RankIndexMaintainer.swift
// RankIndexLayer - Index maintainer for RANK indexes
//
// Maintains rank indexes using Range Tree algorithm for efficient ranking queries.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old score entry
        if let oldItem = oldItem {
            if let oldKey = try buildScoreKey(for: oldItem) {
                transaction.clear(key: oldKey)
                // Decrement count atomically
                let decrementBytes = ByteConversion.int64ToBytes(-1)
                transaction.atomicOp(key: countKey, param: decrementBytes, mutationType: .add)
            }
        }

        // Add new score entry
        if let newItem = newItem {
            if let newKey = try buildScoreKey(for: newItem) {
                transaction.setValue([], for: newKey)
                // Increment count atomically
                let incrementBytes = ByteConversion.int64ToBytes(1)
                transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
            }
        }
    }

    /// Scan item during batch indexing
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        if let scoreKey = try buildScoreKey(for: item, id: id) {
            transaction.setValue([], for: scoreKey)
            // Increment count atomically
            let incrementBytes = ByteConversion.int64ToBytes(1)
            transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        if let key = try buildScoreKey(for: item, id: id) {
            return [key]
        }
        return []
    }

    /// Get top K items (highest scores)
    ///
    /// **Type-Safe**: Returns Score type instead of forcing Int64.
    ///
    /// **Algorithm**: Uses max-heap to maintain top-k during O(n) scan.
    /// Previous implementation: O(n log n) full sort.
    /// Current implementation: O(n log k) using bounded heap.
    ///
    /// **Note**: Could be further optimized to O(k) with reverse range scan,
    /// but fdb-swift-bindings currently doesn't support reverse iteration.
    ///
    /// - Parameters:
    ///   - k: Number of items to return
    ///   - transaction: FDB transaction
    /// - Returns: Array of (score, primaryKey) tuples, sorted by score descending
    public func getTopK(
        k: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(score: Score, primaryKey: [any TupleElement])] {
        guard k > 0 else { return [] }

        let range = scoresSubspace.range()

        // Use min-heap of size k to track top-k highest scores
        // Min-heap because we want to evict the smallest of our "top" candidates
        var topKHeap = TopKHeap<(score: Score, primaryKey: [any TupleElement])>(
            k: k,
            comparator: { $0.score < $1.score }  // Min-heap: smallest score at top
        )

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }

            // Unpack key: [score][primaryKey] - skip corrupt entries
            guard let keyTuple = try? scoresSubspace.unpack(key),
                  let elements = try? Tuple.unpack(from: keyTuple.pack()),
                  elements.count >= 2 else {
                continue
            }

            // First element is score - extract as Score type
            guard let score = try? extractScore(from: elements[0]) else {
                continue
            }

            // Remaining elements are primary key
            let primaryKey = Array(elements.dropFirst())

            // Insert into bounded heap - O(log k) per insertion
            topKHeap.insert((score: score, primaryKey: primaryKey))
        }

        // Extract sorted results (highest scores first)
        return topKHeap.toSortedArrayDescending()
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
        transaction: any TransactionProtocol
    ) async throws -> Int64 {
        // Optimization: Start scan from score + 1 (only count entries with higher scores)
        // Key structure: [scoresSubspace][score][pk]
        // Tuple encoding preserves numeric ordering

        let rangeEnd = scoresSubspace.range().end

        // Build key for score + 1 (first key with score > target)
        // Convert Score to TupleElement for key building
        let nextScore = score + 1 as! any TupleElement
        let boundaryKey = scoresSubspace.pack(Tuple(nextScore))

        // Scan only entries with score > target
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(boundaryKey),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var count: Int64 = 0
        for try await (key, _) in sequence {
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
        transaction: any TransactionProtocol
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
        transaction: any TransactionProtocol
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
        let score = try extractScore(from: scoreValues[0])

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        // Build key: [score][primaryKey...]
        // Score conforms to Numeric, which includes TupleElement-compatible types
        var allElements: [any TupleElement] = [score as! any TupleElement]
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allElements.append(element)
            }
        }

        return try packAndValidate(Tuple(allElements), in: scoresSubspace)
    }

    /// Extract score from tuple element (type-safe)
    private func extractScore(from element: any TupleElement) throws -> Score {
        switch Score.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw RankIndexError.invalidScore("Expected Int64, got \(type(of: element))")
            }
            return value as! Score

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw RankIndexError.invalidScore("Expected Int (as Int64), got \(type(of: element))")
            }
            return Int(value) as! Score

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw RankIndexError.invalidScore("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return Int32(value) as! Score

        case is Double.Type:
            guard let value = element as? Double else {
                throw RankIndexError.invalidScore("Expected Double, got \(type(of: element))")
            }
            return value as! Score

        case is Float.Type:
            guard let value = element as? Double else {
                throw RankIndexError.invalidScore("Expected Float (as Double), got \(type(of: element))")
            }
            return Float(value) as! Score

        default:
            // Fallback: try direct cast
            guard let value = element as? Score else {
                throw RankIndexError.invalidScore(
                    "Cannot convert \(type(of: element)) to \(Score.self)"
                )
            }
            return value
        }
    }
}

// MARK: - TopKHeap

/// A bounded min-heap for maintaining top-k elements efficiently.
///
/// **Algorithm**:
/// - Maintains a min-heap of at most k elements
/// - New elements are inserted if heap is not full
/// - If full, new element replaces root only if it's "better" (larger for top-k highest)
/// - Complexity: O(log k) per insertion
///
/// **Usage for Top-K Highest Scores**:
/// ```swift
/// var heap = TopKHeap<(score: Int64, pk: String)>(k: 10) { $0.score < $1.score }
/// for item in items { heap.insert(item) }
/// let topK = heap.toSortedArrayDescending()
/// ```
internal struct TopKHeap<Element> {
    private var elements: [Element] = []
    private let k: Int
    private let comparator: (Element, Element) -> Bool  // Returns true if $0 should be closer to root

    init(k: Int, comparator: @escaping (Element, Element) -> Bool) {
        self.k = k
        self.comparator = comparator
        elements.reserveCapacity(k)
    }

    var count: Int { elements.count }
    var isEmpty: Bool { elements.isEmpty }
    var top: Element? { elements.first }

    /// Insert element, maintaining at most k elements.
    /// If heap is full and new element is "worse" than root, it's discarded.
    mutating func insert(_ element: Element) {
        if elements.count < k {
            // Heap not full: just insert
            elements.append(element)
            siftUp(elements.count - 1)
        } else if let root = elements.first, comparator(root, element) {
            // Heap full and new element is "better" than root: replace root
            // For top-k highest with min-heap: root has smallest score,
            // so we replace if new element has higher score
            elements[0] = element
            siftDown(0)
        }
        // Otherwise: element is worse than all top-k, discard
    }

    /// Extract elements sorted in descending order (highest first for top-k).
    func toSortedArrayDescending() -> [Element] {
        // Sort by reverse comparator to get descending order
        return elements.sorted { comparator($1, $0) }
    }

    private mutating func siftUp(_ index: Int) {
        var i = index
        while i > 0 {
            let parent = (i - 1) / 2
            if comparator(elements[i], elements[parent]) {
                elements.swapAt(i, parent)
                i = parent
            } else {
                break
            }
        }
    }

    private mutating func siftDown(_ index: Int) {
        var i = index
        while true {
            let left = 2 * i + 1
            let right = 2 * i + 2
            var smallest = i

            if left < elements.count && comparator(elements[left], elements[smallest]) {
                smallest = left
            }
            if right < elements.count && comparator(elements[right], elements[smallest]) {
                smallest = right
            }

            if smallest == i { break }
            elements.swapAt(i, smallest)
            i = smallest
        }
    }
}

