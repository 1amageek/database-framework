// TimeWindowLeaderboardIndexMaintainer.swift
// LeaderboardIndexLayer - Index maintainer for TIME_WINDOW_LEADERBOARD indexes
//
// Time-windowed ranking with automatic window rotation.
// Reference: FDB Record Layer TIME_WINDOW_LEADERBOARD index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for TIME_WINDOW_LEADERBOARD indexes with compile-time type safety
///
/// **Type-Safe Design**:
/// - `Score` type parameter preserves the score type at compile time
/// - Query results preserve the original Score type
///
/// **Functionality**:
/// - Time-windowed rankings (hourly, daily, weekly, etc.)
/// - Automatic window rotation
/// - Historical window queries
/// - Efficient top-K queries within windows
///
/// **Index Structure**:
/// ```
/// // Current window entries (sorted by score descending)
/// Key: [indexSubspace]["window"][windowId][groupKey...][invertedScore][primaryKey]
/// Value: '' (empty)
///
/// // Window metadata
/// Key: [indexSubspace]["meta"]["current"]
/// Value: Int64 (current windowId)
///
/// // Window start times
/// Key: [indexSubspace]["meta"]["start"][windowId]
/// Value: Int64 (Unix timestamp)
///
/// // Record's current window position (for updates)
/// Key: [indexSubspace]["pos"][primaryKey]
/// Value: Tuple(windowId, score)
/// ```
///
/// **Score Inversion**:
/// To enable descending order scans (top scores first), we store inverted scores:
/// `invertedScore = Int64.max - score`
///
/// **Window IDs**:
/// Windows are identified by `floor(timestamp / windowDuration)`.
/// For daily windows, this gives sequential day numbers since epoch.
public struct TimeWindowLeaderboardIndexMaintainer<Item: Persistable, Score: Comparable & Numeric & Codable & Sendable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Window type
    public let window: LeaderboardWindowType

    /// Number of windows to keep
    public let windowCount: Int

    // Subspaces
    private var windowSubspace: Subspace { subspace.subspace("window") }
    private var metaSubspace: Subspace { subspace.subspace("meta") }
    private var posSubspace: Subspace { subspace.subspace("pos") }

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        window: LeaderboardWindowType,
        windowCount: Int
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.window = window
        self.windowCount = windowCount
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
    ///
    /// **Sparse index behavior**:
    /// If the score field is nil, the item is not indexed.
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let now = Date()
        let currentWindowId = windowId(for: now)

        // Get primary keys
        let oldPK: Tuple? = try oldItem.map { try DataAccess.extractId(from: $0, using: idExpression) }
        let newPK: Tuple? = try newItem.map { try DataAccess.extractId(from: $0, using: idExpression) }

        // Get new item values (old values not needed - we delete by PK and reinsert)
        // Sparse index: if score field is nil, treat as no score
        let newScore: Int64?
        if let item = newItem {
            do {
                newScore = try extractScore(from: item)
            } catch DataAccessError.nilValueCannotBeIndexed {
                newScore = nil
            }
        } else {
            newScore = nil
        }

        let newGroup: [any TupleElement]?
        if let item = newItem {
            do {
                newGroup = try extractGrouping(from: item)
            } catch DataAccessError.nilValueCannotBeIndexed {
                newGroup = nil
            }
        } else {
            newGroup = nil
        }

        switch (oldPK, newPK) {
        case (nil, let pk?):
            // Insert
            if let score = newScore {
                try await insertEntry(
                    pk: pk,
                    score: score,
                    grouping: newGroup ?? [],
                    windowId: currentWindowId,
                    transaction: transaction
                )
            }

        case (let pk?, nil):
            // Delete
            try await deleteEntry(pk: pk, transaction: transaction)

        case (let oldPK?, let newPK?):
            // Update
            let oldPKBytes = oldPK.pack()
            let newPKBytes = newPK.pack()

            if oldPKBytes == newPKBytes {
                // Same record
                if let newScore = newScore {
                    try await updateEntry(
                        pk: oldPK,
                        newScore: newScore,
                        newGrouping: newGroup ?? [],
                        currentWindowId: currentWindowId,
                        transaction: transaction
                    )
                }
            } else {
                // Primary key changed (unusual)
                try await deleteEntry(pk: oldPK, transaction: transaction)
                if let score = newScore {
                    try await insertEntry(
                        pk: newPK,
                        score: score,
                        grouping: newGroup ?? [],
                        windowId: currentWindowId,
                        transaction: transaction
                    )
                }
            }

        case (nil, nil):
            break
        }

        // Cleanup old windows
        try await cleanupOldWindows(currentWindowId: currentWindowId, transaction: transaction)
    }

    /// Build index entries for an item during batch indexing
    ///
    /// **Sparse index behavior**:
    /// If the score field is nil, the item is not indexed.
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Sparse index: if score field is nil, skip indexing
        let score: Int64
        let grouping: [any TupleElement]
        do {
            score = try extractScore(from: item)
            grouping = try extractGrouping(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return
        }

        let now = Date()
        let currentWindowId = windowId(for: now)

        try await insertEntry(
            pk: id,
            score: score,
            grouping: grouping,
            windowId: currentWindowId,
            transaction: transaction
        )
    }

    /// Compute expected index keys for an item
    ///
    /// **Sparse index behavior**:
    /// If the score field is nil, returns an empty array.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Sparse index: if score field is nil, no index entry
        let score: Int64
        let grouping: [any TupleElement]
        do {
            score = try extractScore(from: item)
            grouping = try extractGrouping(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }

        let now = Date()
        let currentWindowId = windowId(for: now)

        return [try makeWindowEntryKey(
            windowId: currentWindowId,
            grouping: grouping,
            score: score,
            pk: id
        )]
    }

    // MARK: - Private Helpers

    /// Calculate window ID from a date
    private func windowId(for date: Date) -> Int64 {
        let timestamp = Int64(date.timeIntervalSince1970)
        return timestamp / Int64(window.durationSeconds)
    }

    /// Invert score for descending order storage
    ///
    /// Uses UInt64 conversion to handle the full Int64 range without overflow:
    /// 1. Convert score to UInt64 (preserving bit pattern)
    /// 2. Subtract from UInt64.max (higher scores → smaller values)
    /// 3. Convert back to Int64 (preserving bit pattern)
    ///
    /// FDB tuple encoding preserves signed Int64 order, so this ensures:
    /// - score 100  → -101 (smallest in signed order)
    /// - score -50  → 49
    /// - score -100 → 99 (largest in signed order)
    /// This gives correct descending order: 100, -50, -100
    private func invertScore(_ score: Int64) -> Int64 {
        let unsigned = UInt64(bitPattern: score)
        let inverted = UInt64.max - unsigned
        return Int64(bitPattern: inverted)
    }

    /// Extract Int64 from a TupleElement that may be decoded as Int or Int64
    ///
    /// FoundationDB's Tuple layer may decode small integers (including 0) as Int
    /// instead of Int64. This helper handles both cases.
    private func extractInt64(from element: (any TupleElement)?) -> Int64? {
        guard let element = element else { return nil }
        if let value = element as? Int64 {
            return value
        }
        if let value = element as? Int {
            return Int64(value)
        }
        return nil
    }

    /// Extract score from item (type-safe)
    private func extractScore(from item: Item) throws -> Int64 {
        // The last field in keyPaths is the score field
        guard let keyPaths = index.keyPaths, let scoreKeyPath = keyPaths.last else {
            throw IndexError.invalidConfiguration("Leaderboard index requires a score field")
        }

        let values = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: [scoreKeyPath],
            expression: index.rootExpression
        )

        guard let first = values.first else {
            throw IndexError.invalidConfiguration("Score field value is nil")
        }

        // Type-safe extraction based on Score type
        return try extractScoreValue(from: first)
    }

    /// Extract score value from tuple element (type-safe)
    private func extractScoreValue(from element: any TupleElement) throws -> Int64 {
        switch Score.self {
        case is Int64.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int64, got \(type(of: element))")
            }
            return value

        case is Int.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int (as Int64), got \(type(of: element))")
            }
            return value

        case is Int32.Type:
            guard let value = element as? Int64 else {
                throw IndexError.invalidConfiguration("Expected Int32 (as Int64), got \(type(of: element))")
            }
            return value

        case is Double.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Double, got \(type(of: element))")
            }
            return Int64(value)

        case is Float.Type:
            guard let value = element as? Double else {
                throw IndexError.invalidConfiguration("Expected Float (as Double), got \(type(of: element))")
            }
            return Int64(value)

        default:
            // Fallback: try converting from known numeric types
            if let value = element as? Int64 { return value }
            if let value = element as? Int { return Int64(value) }
            if let value = element as? Double { return Int64(value) }
            throw IndexError.invalidConfiguration(
                "Cannot convert \(type(of: element)) to Int64 for leaderboard score"
            )
        }
    }

    /// Extract grouping fields from item (all fields except the last which is score)
    private func extractGrouping(from item: Item) throws -> [any TupleElement] {
        guard let keyPaths = index.keyPaths else {
            return []
        }
        let groupingKeyPaths = keyPaths.dropLast()
        if groupingKeyPaths.isEmpty {
            return []
        }

        return try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: Array(groupingKeyPaths),
            expression: index.rootExpression
        )
    }

    /// Make window entry key
    private func makeWindowEntryKey(
        windowId: Int64,
        grouping: [any TupleElement],
        score: Int64,
        pk: Tuple
    ) throws -> FDB.Bytes {
        var elements: [any TupleElement] = [windowId]
        elements.append(contentsOf: grouping)
        elements.append(invertScore(score))
        for i in 0..<pk.count {
            if let elem = pk[i] {
                elements.append(elem)
            }
        }
        return windowSubspace.pack(Tuple(elements))
    }

    /// Insert a new entry
    private func insertEntry(
        pk: Tuple,
        score: Int64,
        grouping: [any TupleElement],
        windowId: Int64,
        transaction: any TransactionProtocol
    ) async throws {
        // Create window entry
        let entryKey = try makeWindowEntryKey(
            windowId: windowId,
            grouping: grouping,
            score: score,
            pk: pk
        )
        transaction.setValue([], for: entryKey)

        // Save position for updates
        let posKey = posSubspace.pack(pk)
        var posElements: [any TupleElement] = [windowId, score]
        posElements.append(contentsOf: grouping)
        transaction.setValue(Tuple(posElements).pack(), for: posKey)

        // Update window metadata
        try await ensureWindowMetadata(windowId: windowId, transaction: transaction)
    }

    /// Delete an entry
    private func deleteEntry(
        pk: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Get current position
        let posKey = posSubspace.pack(pk)
        guard let posBytes = try await transaction.getValue(for: posKey) else {
            return
        }

        let posTuple = try Tuple.unpack(from: posBytes)
        guard let windowId = extractInt64(from: posTuple[0]),
              let score = extractInt64(from: posTuple[1]) else {
            return
        }

        // Extract grouping from position
        var grouping: [any TupleElement] = []
        for i in 2..<posTuple.count {
            grouping.append(posTuple[i])
        }

        // Delete window entry
        let entryKey = try makeWindowEntryKey(
            windowId: windowId,
            grouping: grouping,
            score: score,
            pk: pk
        )
        transaction.clear(key: entryKey)

        // Delete position
        transaction.clear(key: posKey)
    }

    /// Update an entry
    private func updateEntry(
        pk: Tuple,
        newScore: Int64,
        newGrouping: [any TupleElement],
        currentWindowId: Int64,
        transaction: any TransactionProtocol
    ) async throws {
        // Get current position
        let posKey = posSubspace.pack(pk)
        if let posBytes = try await transaction.getValue(for: posKey) {
            let posTuple = try Tuple.unpack(from: posBytes)
            if let oldWindowId = extractInt64(from: posTuple[0]),
               let oldScore = extractInt64(from: posTuple[1]) {
                // Extract old grouping
                var oldGrouping: [any TupleElement] = []
                for i in 2..<posTuple.count {
                    oldGrouping.append(posTuple[i])
                }

                // Delete old entry
                let oldKey = try makeWindowEntryKey(
                    windowId: oldWindowId,
                    grouping: oldGrouping,
                    score: oldScore,
                    pk: pk
                )
                transaction.clear(key: oldKey)
            }
        }

        // Insert new entry in current window
        try await insertEntry(
            pk: pk,
            score: newScore,
            grouping: newGrouping,
            windowId: currentWindowId,
            transaction: transaction
        )
    }

    /// Ensure window metadata exists
    private func ensureWindowMetadata(
        windowId: Int64,
        transaction: any TransactionProtocol
    ) async throws {
        let startKey = metaSubspace.subspace("start").pack(Tuple(windowId))
        if try await transaction.getValue(for: startKey) == nil {
            let startTime = windowId * Int64(window.durationSeconds)
            transaction.setValue(ByteConversion.int64ToBytes(startTime), for: startKey)
        }
    }

    /// Cleanup old windows beyond windowCount
    ///
    /// Uses `clearRange` for efficient bulk deletion without needing to scan keys.
    /// This avoids issues with early `break` from async sequences conflicting with commit.
    private func cleanupOldWindows(
        currentWindowId: Int64,
        transaction: any TransactionProtocol
    ) async throws {
        let oldestAllowedWindow = currentWindowId - Int64(windowCount)

        // Guard: nothing to clean up if oldestAllowedWindow <= 0
        guard oldestAllowedWindow > 0 else { return }

        // Use clearRange for efficient bulk deletion
        // Delete all window entries with windowId < oldestAllowedWindow
        let startKey = windowSubspace.pack(Tuple([Int64(0)]))
        let endKey = windowSubspace.pack(Tuple([oldestAllowedWindow]))
        transaction.clearRange(beginKey: startKey, endKey: endKey)
    }

    // MARK: - Query Methods

    /// Get top K entries in current window
    ///
    /// - Parameters:
    ///   - k: Number of entries to return
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Array of (primaryKey, score) tuples
    public func getTopK(
        k: Int,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, score: Int64)] {
        let now = Date()
        let currentWindowId = windowId(for: now)

        return try await getTopK(
            k: k,
            windowId: currentWindowId,
            grouping: grouping,
            transaction: transaction
        )
    }

    /// Get top K entries in a specific window
    ///
    /// - Parameters:
    ///   - k: Number of entries to return
    ///   - windowId: Window ID to query
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Array of (primaryKey, score) tuples
    public func getTopK(
        k: Int,
        windowId: Int64,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, score: Int64)] {
        // Build range for this window (optionally with grouping)
        var prefixElements: [any TupleElement] = [windowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        // Range: All keys with prefix pack([windowId, grouping...])
        // Use FDB.strinc on prefix bytes to get exclusive upper bound
        // This includes ALL scores (including 0, which has invertedScore=Int64.max)
        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let rangeEnd: FDB.Bytes
        do {
            rangeEnd = try FDB.strinc(rangeStart)
        } catch {
            // Fallback: append 0xFF (should never happen in practice)
            rangeEnd = rangeStart + [0xFF]
        }

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var results: [(pk: Tuple, score: Int64)] = []
        var count = 0

        for try await (key, _) in sequence {
            guard windowSubspace.contains(key), count < k else { break }

            let keyTuple = try windowSubspace.unpack(key)

            // Extract inverted score and primary key
            // Key structure: windowId, [grouping...], invertedScore, [pk...]
            let groupingCount = grouping?.count ?? 0
            let invertedScoreIndex = 1 + groupingCount

            guard let invertedScore = keyTuple[invertedScoreIndex] as? Int64 else {
                continue
            }

            // Reverse the inversion (same formula is self-inverse)
            let unsigned = UInt64(bitPattern: invertedScore)
            let score = Int64(bitPattern: UInt64.max - unsigned)

            // Extract primary key (remaining elements)
            var pkElements: [any TupleElement] = []
            for i in (invertedScoreIndex + 1)..<keyTuple.count {
                if let elem = keyTuple[i] {
                    pkElements.append(elem)
                }
            }

            results.append((pk: Tuple(pkElements), score: score))
            count += 1
        }

        return results
    }

    /// Get rank of a specific record in current window
    ///
    /// - Parameters:
    ///   - pk: Primary key of the record
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Rank (1-based) or nil if not found
    public func getRank(
        pk: Tuple,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> Int? {
        let now = Date()
        let currentWindowId = windowId(for: now)

        // Get current position
        let posKey = posSubspace.pack(pk)
        guard let posBytes = try await transaction.getValue(for: posKey) else {
            return nil
        }

        let posTuple = try Tuple.unpack(from: posBytes)
        guard let recordWindowId = extractInt64(from: posTuple[0]),
              let score = extractInt64(from: posTuple[1]),
              recordWindowId == currentWindowId else {
            return nil
        }

        // Count entries with higher score
        var prefixElements: [any TupleElement] = [currentWindowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let targetKey = try makeWindowEntryKey(
            windowId: currentWindowId,
            grouping: grouping ?? [],
            score: score,
            pk: pk
        )

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(targetKey),
            snapshot: true
        )

        var rank = 1
        for try await _ in sequence {
            rank += 1
        }

        return rank
    }

    /// Get all available window IDs
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of window IDs (newest first)
    public func getAvailableWindows(
        transaction: any TransactionProtocol
    ) async throws -> [Int64] {
        let range = metaSubspace.subspace("start").range()
        var windowIds: [Int64] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            let keyTuple = try metaSubspace.subspace("start").unpack(key)
            if let wid = extractInt64(from: keyTuple[0]) {
                windowIds.append(wid)
            }
        }

        return windowIds.sorted(by: >)  // Newest first
    }

    // MARK: - Bottom-K Queries

    /// Get bottom K entries (lowest scores) in current window
    ///
    /// **Performance**: O(total entries) due to reverse scan requirement.
    /// For large datasets, consider using a separate index sorted by ascending score.
    ///
    /// - Parameters:
    ///   - k: Number of entries to return
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Array of (primaryKey, score) tuples ordered by ascending score
    public func getBottomK(
        k: Int,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, score: Int64)] {
        let now = Date()
        let currentWindowId = windowId(for: now)

        return try await getBottomK(
            k: k,
            windowId: currentWindowId,
            grouping: grouping,
            transaction: transaction
        )
    }

    /// Get bottom K entries (lowest scores) in a specific window
    ///
    /// **Performance**: O(n) where n is total entries in the window.
    /// The index is optimized for top-K queries (descending score order).
    /// For bottom-K, we scan and keep a sliding window of the K lowest scores.
    ///
    /// - Parameters:
    ///   - k: Number of entries to return
    ///   - windowId: Window ID to query
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Array of (primaryKey, score) tuples ordered by ascending score
    public func getBottomK(
        k: Int,
        windowId: Int64,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> [(pk: Tuple, score: Int64)] {
        // Build range for this window (optionally with grouping)
        var prefixElements: [any TupleElement] = [windowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let rangeEnd: FDB.Bytes
        do {
            rangeEnd = try FDB.strinc(rangeStart)
        } catch {
            rangeEnd = rangeStart + [0xFF]
        }

        // Forward iteration - collect all entries, then return last K
        // Since scores are stored inverted, forward scan gives highest scores first
        // So we collect all and take the tail (lowest scores)
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        // Use a sliding window to keep only the last K entries (lowest scores)
        var allEntries: [(pk: Tuple, score: Int64)] = []

        for try await (key, _) in sequence {
            guard windowSubspace.contains(key) else { break }

            let keyTuple = try windowSubspace.unpack(key)

            let groupingCount = grouping?.count ?? 0
            let invertedScoreIndex = 1 + groupingCount

            guard let invertedScore = keyTuple[invertedScoreIndex] as? Int64 else {
                continue
            }

            // Reverse the inversion
            let unsigned = UInt64(bitPattern: invertedScore)
            let score = Int64(bitPattern: UInt64.max - unsigned)

            // Extract primary key
            var pkElements: [any TupleElement] = []
            for i in (invertedScoreIndex + 1)..<keyTuple.count {
                if let elem = keyTuple[i] {
                    pkElements.append(elem)
                }
            }

            allEntries.append((pk: Tuple(pkElements), score: score))
        }

        // Return last K entries (lowest scores), reversed to ascending order
        let bottomK = Array(allEntries.suffix(k))
        return bottomK.reversed()
    }

    // MARK: - Percentile Queries

    /// Get score at a given percentile in current window
    ///
    /// **Time Complexity**: O(n) where n is the total number of entries
    ///
    /// **Percentile Calculation**: Uses the "exclusive" method where
    /// percentile p returns the score where approximately p% of scores are below it.
    ///
    /// - Parameters:
    ///   - percentile: Percentile value between 0.0 and 1.0 (e.g., 0.5 for median)
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Score at the given percentile, or nil if no entries
    public func getPercentile(
        _ percentile: Double,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> Int64? {
        let now = Date()
        let currentWindowId = windowId(for: now)

        return try await getPercentile(
            percentile,
            windowId: currentWindowId,
            grouping: grouping,
            transaction: transaction
        )
    }

    /// Get score at a given percentile in a specific window
    ///
    /// - Parameters:
    ///   - percentile: Percentile value between 0.0 and 1.0
    ///   - windowId: Window ID to query
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Score at the given percentile, or nil if no entries
    public func getPercentile(
        _ percentile: Double,
        windowId: Int64,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> Int64? {
        guard percentile >= 0 && percentile <= 1 else {
            return nil
        }

        // First, count total entries
        let totalCount = try await getTotalCount(
            windowId: windowId,
            grouping: grouping,
            transaction: transaction
        )

        guard totalCount > 0 else {
            return nil
        }

        // Calculate target rank (1-based from highest score)
        // For percentile p, we want the score at rank ceil((1-p) * count)
        // e.g., 90th percentile means top 10%, so rank = ceil(0.1 * count)
        let targetRank = max(1, Int(ceil((1.0 - percentile) * Double(totalCount))))

        // Get the entry at target rank
        let entries = try await getTopK(
            k: targetRank,
            windowId: windowId,
            grouping: grouping,
            transaction: transaction
        )

        return entries.last?.score
    }

    /// Get total count of entries in a window
    private func getTotalCount(
        windowId: Int64,
        grouping: [any TupleElement]?,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        var prefixElements: [any TupleElement] = [windowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let rangeEnd: FDB.Bytes
        do {
            rangeEnd = try FDB.strinc(rangeStart)
        } catch {
            rangeEnd = rangeStart + [0xFF]
        }

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var count = 0
        for try await _ in sequence {
            count += 1
        }

        return count
    }

    // MARK: - Dense Ranking

    /// Ranking strategy for getRank operations
    public enum RankingStrategy: Sendable {
        /// Competition ranking: ties get same rank, next rank is skipped
        /// e.g., scores [100, 90, 90, 80] → ranks [1, 2, 2, 4]
        case competition

        /// Dense ranking: ties get same rank, next rank is NOT skipped
        /// e.g., scores [100, 90, 90, 80] → ranks [1, 2, 2, 3]
        case dense
    }

    /// Get rank of a specific record using dense ranking
    ///
    /// Dense ranking counts unique scores higher than the target.
    /// Ties receive the same rank, but the next rank is incremented by 1.
    ///
    /// **Example**:
    /// Scores: [100, 90, 90, 80, 70]
    /// - Score 100: Dense rank = 1
    /// - Score 90: Dense rank = 2 (both players with 90 share this rank)
    /// - Score 80: Dense rank = 3 (not 4)
    /// - Score 70: Dense rank = 4
    ///
    /// - Parameters:
    ///   - pk: Primary key of the record
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Dense rank (1-based) or nil if not found
    public func getRankDense(
        pk: Tuple,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> Int? {
        let now = Date()
        let currentWindowId = windowId(for: now)

        // Get current position
        let posKey = posSubspace.pack(pk)
        guard let posBytes = try await transaction.getValue(for: posKey) else {
            return nil
        }

        let posTuple = try Tuple.unpack(from: posBytes)
        guard let recordWindowId = extractInt64(from: posTuple[0]),
              let targetScore = extractInt64(from: posTuple[1]),
              recordWindowId == currentWindowId else {
            return nil
        }

        // Count distinct scores higher than target
        return try await countDistinctScoresAbove(
            score: targetScore,
            windowId: currentWindowId,
            grouping: grouping,
            transaction: transaction
        ) + 1
    }

    /// Get rank using specified ranking strategy
    ///
    /// - Parameters:
    ///   - pk: Primary key of the record
    ///   - strategy: Ranking strategy (competition or dense)
    ///   - grouping: Optional grouping filter
    ///   - transaction: The transaction to use
    /// - Returns: Rank (1-based) or nil if not found
    public func getRank(
        pk: Tuple,
        strategy: RankingStrategy,
        grouping: [any TupleElement]? = nil,
        transaction: any TransactionProtocol
    ) async throws -> Int? {
        switch strategy {
        case .competition:
            return try await getRank(pk: pk, grouping: grouping, transaction: transaction)
        case .dense:
            return try await getRankDense(pk: pk, grouping: grouping, transaction: transaction)
        }
    }

    /// Count distinct scores above a target score
    private func countDistinctScoresAbove(
        score: Int64,
        windowId: Int64,
        grouping: [any TupleElement]?,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        var prefixElements: [any TupleElement] = [windowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))

        // Build end key: prefix + inverted target score
        // We want entries with inverted score < inverted target score
        // (i.e., actual score > target score)
        var endElements = prefixElements
        endElements.append(invertScore(score))
        let rangeEnd = windowSubspace.pack(Tuple(endElements))

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(rangeStart),
            endSelector: .firstGreaterOrEqual(rangeEnd),
            snapshot: true
        )

        var distinctScores = Set<Int64>()
        let groupingCount = grouping?.count ?? 0
        let invertedScoreIndex = 1 + groupingCount

        for try await (key, _) in sequence {
            guard windowSubspace.contains(key) else { break }

            let keyTuple = try windowSubspace.unpack(key)
            guard let invertedScore = keyTuple[invertedScoreIndex] as? Int64 else {
                continue
            }

            // Reverse the inversion
            let unsigned = UInt64(bitPattern: invertedScore)
            let actualScore = Int64(bitPattern: UInt64.max - unsigned)
            distinctScores.insert(actualScore)
        }

        return distinctScores.count
    }
}
