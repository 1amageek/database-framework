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
        let newScore = try newItem.map { try extractScore(from: $0) }
        let newGroup = try newItem.map { try extractGrouping(from: $0) }

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
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let score = try extractScore(from: item)
        let grouping = try extractGrouping(from: item)
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
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let score = try extractScore(from: item)
        let grouping = try extractGrouping(from: item)
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
        guard let windowId = posTuple[0] as? Int64,
              let score = posTuple[1] as? Int64 else {
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
            if let oldWindowId = posTuple[0] as? Int64,
               let oldScore = posTuple[1] as? Int64 {
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
    private func cleanupOldWindows(
        currentWindowId: Int64,
        transaction: any TransactionProtocol
    ) async throws {
        let oldestAllowedWindow = currentWindowId - Int64(windowCount)

        // Scan for old windows and delete
        let range = windowSubspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: false
        )

        var keysToDelete: [FDB.Bytes] = []
        for try await (key, _) in sequence {
            guard windowSubspace.contains(key) else { break }

            let keyTuple = try windowSubspace.unpack(key)
            if let keyWindowId = keyTuple[0] as? Int64, keyWindowId < oldestAllowedWindow {
                keysToDelete.append(key)
            } else {
                // Windows are ordered, so we can stop early
                break
            }
        }

        for key in keysToDelete {
            transaction.clear(key: key)
        }
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
        guard let recordWindowId = posTuple[0] as? Int64,
              let score = posTuple[1] as? Int64,
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
            if let wid = keyTuple[0] as? Int64 {
                windowIds.append(wid)
            }
        }

        return windowIds.sorted(by: >)  // Newest first
    }
}
