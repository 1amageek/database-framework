// Leaderboard.swift
// LeaderboardIndex - Leaderboard ranking query for Fusion
//
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

/// Leaderboard ranking query for Fusion
///
/// Returns top K items from the leaderboard, scored by their ranking position.
/// Higher ranked items (better scores) get higher fusion scores.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(GameScore.self) {
///     // Get top 100 from leaderboard
///     Leaderboard(#field(\GameScore.score)).top(100)
///
///     // Combine with user preferences
///     Similar(\.playerProfile, dimensions: 128).nearest(to: userVector, k: 50)
/// }
/// .algorithm(.rrf())
/// .execute()
///
/// // With grouping (e.g., by region)
/// let results = try await context.fuse(GameScore.self) {
///     Leaderboard(
///         #field(\GameScore.score),
///         groupBy: #field(\GameScore.region)
///     ).top(50).group("asia")
/// }
/// .execute()
/// ```
public struct Leaderboard<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let scoreFieldName: String
    private let groupByFieldName: String?
    private var k: Int = 100
    private var groupValue: FieldValue?
    private var windowId: Int64?

    // MARK: - Initialization (FusionContext)

    /// Create a Leaderboard query for a score field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(GameScore.self) {
    ///     Leaderboard(#field(\GameScore.score)).top(100)
    /// }
    /// ```
    /// Create a Leaderboard query for an Int64 score field
    public init(_ scoreField: Field<T, Int64>) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = scoreField.name
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query with grouping
    ///
    /// **Usage**:
    /// ```swift
    /// Leaderboard(
    ///     #field(\GameScore.score),
    ///     groupBy: #field(\GameScore.region)
    /// ).top(50).group("asia")
    /// ```
    public init<G>(
        _ scoreField: Field<T, Int64>,
        groupBy groupField: Field<T, G>
    ) {
        guard let context = FusionContext.current else {
            fatalError("Leaderboard must be used within context.fuse { } block")
        }
        self.scoreFieldName = scoreField.name
        self.groupByFieldName = groupField.name
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Leaderboard query with explicit context
    /// Create a Leaderboard query for Int64 with explicit context
    public init(_ scoreField: Field<T, Int64>, context: IndexQueryContext) {
        self.scoreFieldName = scoreField.name
        self.groupByFieldName = nil
        self.queryContext = context
    }

    /// Create a Leaderboard query with grouping and explicit context
    public init<G>(
        _ scoreField: Field<T, Int64>,
        groupBy groupField: Field<T, G>,
        context: IndexQueryContext
    ) {
        self.scoreFieldName = scoreField.name
        self.groupByFieldName = groupField.name
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set the number of top entries to retrieve
    ///
    /// - Parameter count: Number of top entries (default: 100)
    /// - Returns: Updated query
    public func top(_ count: Int) -> Self {
        var copy = self
        copy.k = count
        return copy
    }

    /// Filter by group value
    ///
    /// - Parameter value: Group value to filter by
    /// - Returns: Updated query
    public func group<V: FieldValueRepresentable>(_ value: V) -> Self {
        var copy = self
        copy.groupValue = value.fieldValue
        return copy
    }

    /// Query a specific historical window
    ///
    /// - Parameter windowId: Window ID to query
    /// - Returns: Updated query
    public func window(_ windowId: Int64) -> Self {
        var copy = self
        copy.windowId = windowId
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor and kind for leaderboard
    private func findIndexDescriptorAndKind() throws -> (
        descriptor: IndexDescriptor,
        configuration: TimeWindowLeaderboardConfiguration
    )? {
        for descriptor in queryContext.indexDescriptors(for: T.self) {
            guard descriptor.kind.identifier == "time_window_leaderboard" else {
                continue
            }
            let configuration = try TimeWindowLeaderboardConfiguration(
                metadata: descriptor.kind
            )
            if configuration.scoreFieldName == scoreFieldName {
                return (descriptor, configuration)
            }
        }
        return nil
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard let (descriptor, indexKind) = try findIndexDescriptorAndKind() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: scoreFieldName,
                kind: "leaderboard"
            )
        }

        let indexName = descriptor.name
        let windowDurationSeconds = Int64(indexKind.window.durationSeconds)

        // Execute leaderboard query within transaction
        let grouping = try groupValue.map {
            [try $0.toTupleElement() as any TupleElement]
        }
        let topKResults: [(pk: Tuple, score: Int64)] = try await queryContext
            .withReadableIndex(
                named: indexName,
                kindIdentifier: descriptor.kindIdentifier,
                for: T.self
            ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            return try await self.readTopK(
                indexSubspace: readableIndex.subspace,
                k: self.k,
                grouping: grouping,
                windowId: self.windowId,
                windowDurationSeconds: windowDurationSeconds,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(
            ids: topKResults.map { $0.pk },
            type: T.self
        )

        // Filter to candidates if provided
        if let candidateIDs = candidates {
            items = items.filter { candidateIDs.contains($0.id) }
        }

        // Match fetched items to the canonical packed identifiers returned by
        // the index. This supports every Persistable identifier shape without
        // runtime type casts or string conversion.
        var scoresByIdentifier: [ByteString: Int64] = [:]
        scoresByIdentifier.reserveCapacity(topKResults.count)
        for result in topKResults {
            scoresByIdentifier[result.pk.pack()] = result.score
        }
        var leaderboardResults: [(item: T, score: Int64)] = []
        leaderboardResults.reserveCapacity(items.count)
        for item in items {
            let identifier = try DataAccess.extractId(
                from: item,
                using: FieldKeyExpression(fieldName: "id")
            ).pack()
            if let score = scoresByIdentifier[identifier] {
                leaderboardResults.append((item: item, score: score))
            }
        }

        // Sort by leaderboard score descending
        leaderboardResults.sort { $0.score > $1.score }

        // Convert leaderboard rank to fusion score
        // Rank 1 (top) gets highest score, lower ranks get lower scores
        let count = Double(leaderboardResults.count)
        return leaderboardResults.enumerated().map { index, result in
            // Higher rank (lower index) = higher score
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: result.item, score: score)
        }
    }

    // MARK: - Leaderboard Index Reading

    /// Read top K entries from leaderboard index
    ///
    /// Index structure:
    /// - `[indexSubspace]["window"][windowId][groupKey...][invertedScore][primaryKey]` -> empty
    ///
    /// Score inversion: `invertedScore = Int64.max - score` for descending order
    private func readTopK(
        indexSubspace: Subspace,
        k: Int,
        grouping: [any TupleElement]?,
        windowId: Int64?,
        windowDurationSeconds: Int64,
        transaction: any TransactionAccess
    ) async throws -> [(pk: Tuple, score: Int64)] {
        // Calculate current window ID if not specified
        let effectiveWindowId = windowId ?? {
            queryContext.context.container.wallClock.now.secondsSinceUnixEpoch
                / windowDurationSeconds
        }()

        let windowSubspace = indexSubspace.subspace("window")

        // Build range prefix
        var prefixElements: [any TupleElement] = [effectiveWindowId]
        if let g = grouping {
            prefixElements.append(contentsOf: g)
        }

        let rangeStart = windowSubspace.pack(Tuple(prefixElements))
        let rangeEnd = windowSubspace.pack(Tuple(prefixElements + [Int64.max]))

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(rangeStart),
            to: .firstGreaterOrEqual(rangeEnd),
            limit: k,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [(pk: Tuple, score: Int64)] = []
        var count = 0
        let groupingCount = grouping?.count ?? 0

        for (key, _) in sequence {
            guard windowSubspace.contains(key), count < k else { break }

            let keyTuple = try windowSubspace.unpack(key)

            // Extract inverted score and primary key
            // Key structure: windowId, [grouping...], invertedScore, [pk...]
            let invertedScoreIndex = 1 + groupingCount

            guard let invertedScoreElement = keyTuple[invertedScoreIndex] else {
                throw FusionQueryError.invalidConfiguration(
                    "Leaderboard index '\(indexSubspace)' contains a malformed score entry"
                )
            }
            let invertedScore = try TupleDecoder.decodeInt64(
                invertedScoreElement
            )

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
}
