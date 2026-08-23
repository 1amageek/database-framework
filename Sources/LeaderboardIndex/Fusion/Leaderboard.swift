import DatabaseEngine
// Leaderboard.swift
// LeaderboardIndex - Leaderboard ranking query for Fusion
//
import DatabaseKit
import DatabaseTypes
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
/// .strategy(.reciprocalRank())
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

    private let queryContext: IndexQueryContext!
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
        let context = FusionContext.current
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
        let context = FusionContext.current
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

    /// Find the leaderboard descriptor and its typed configuration.
    private func findIndexDescriptorAndConfiguration() throws -> (
        descriptor: IndexDescriptor,
        configuration: TimeWindowLeaderboardConfiguration
    )? {
        let requestedGroupingFields = groupByFieldName.map { [$0] } ?? []
        var scoreFieldCandidate: TimeWindowLeaderboardConfiguration?
        for descriptor in queryContext.indexDescriptors(for: T.self) {
            guard descriptor.type == .leaderboard else {
                continue
            }
            let configuration = try TimeWindowLeaderboardConfiguration(
                definition: descriptor.declaration.definition
            )
            guard configuration.scoreFieldName == scoreFieldName else {
                continue
            }
            scoreFieldCandidate = configuration
            if configuration.groupingFieldNames == requestedGroupingFields {
                return (descriptor, configuration)
            }
        }
        if let scoreFieldCandidate {
            throw FusionQueryError.invalidConfiguration(
                "Leaderboard grouping fields \(requestedGroupingFields) do not match declared fields \(scoreFieldCandidate.groupingFieldNames)"
            )
        }
        return nil
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Leaderboard requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: k,
                offset: nil,
                orderBy: [scoreFieldName]
            ),
            indexDescriptor: {
                guard let (descriptor, _) =
                        try self.findIndexDescriptorAndConfiguration() else {
                    throw FusionQueryError.indexNotFound(
                        entity: T.persistableType,
                        field: self.scoreFieldName,
                        indexType: .leaderboard
                    )
                }
                return descriptor
            },
            operation: { [self] candidates, execution in
                try await executeBound(
                    candidates: candidates,
                    execution: execution
                )
            }
        )
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        guard k > 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Leaderboard result count must be positive"
            )
        }
        guard let (descriptor, indexConfiguration) =
                try findIndexDescriptorAndConfiguration() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: scoreFieldName,
                indexType: .leaderboard
            )
        }

        let indexName = descriptor.name
        guard indexConfiguration.groupingFieldNames.isEmpty
                == (groupValue == nil) else {
            throw FusionQueryError.invalidConfiguration(
                indexConfiguration.groupingFieldNames.isEmpty
                    ? "An ungrouped leaderboard does not accept a group value"
                    : "A grouped leaderboard requires a value for every grouping field"
            )
        }
        guard let windowDurationSeconds = Int64(
            exactly: indexConfiguration.window.durationSeconds
        ), windowDurationSeconds > 0 else {
            throw LeaderboardQueryError.invalidConfiguration(
                "Leaderboard window duration must be a positive whole-second Int64 value"
            )
        }

        // Execute leaderboard query within transaction
        let grouping = try groupValue.map {
            [try $0.toTupleElement() as any TupleElement]
        }
        let intermediateReservation = try execution.workMeter
            .reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: min(k, Int(exactly: execution.workMeter.budget.maximumIntermediateRows) ?? k),
                    element: (pk: Tuple, score: Int64).self
                ).bytes,
                at: .indexScan
            )
        defer { intermediateReservation.release() }
        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Entity '\(T.persistableType)' is not present in the active schema"
            )
        }
        return try await queryContext.withReadableIndex(
                named: indexName,
                indexType: descriptor.type,
                for: T.self,
                authorization: IndexReadAuthorization(
                    limit: k,
                    offset: nil,
                    orderBy: [scoreFieldName]
                )
            ) { readableIndex, transaction -> FusionQueryResult<T> in
            guard let readableIndex else {
                return try FusionQueryResultBuilder<T>(
                    execution: execution
                ).finish()
            }
            let candidateReservation: DatabaseIntermediateReservation?
            let candidateKeys: Set<ByteString>?
            if let candidates {
                let reservation = try execution.workMeter
                    .reserveIntermediate(
                        bytes: UInt64(MemoryLayout<Set<ByteString>>.stride),
                        at: .indexScan
                    )
                var packedCandidates: Set<ByteString> = []
                packedCandidates.reserveCapacity(candidates.count)
                do {
                    for candidate in candidates {
                        let packed = try PersistableIdentifierKeyCodec
                            .tuple(for: candidate).pack()
                        guard !packedCandidates.contains(packed) else {
                            continue
                        }
                        try reservation.reserveAdditional(
                            rows: 1,
                            bytes: UInt64(packed.count) + 64,
                            at: .indexScan
                        )
                        packedCandidates.insert(packed)
                    }
                } catch {
                    reservation.release()
                    throw error
                }
                candidateReservation = reservation
                candidateKeys = packedCandidates
            } else {
                candidateReservation = nil
                candidateKeys = nil
            }
            defer { candidateReservation?.release() }

            let topKResults = try await self.readTopK(
                indexSubspace: readableIndex.subspace,
                k: self.k,
                grouping: grouping,
                windowId: self.windowId,
                windowDurationSeconds: windowDurationSeconds,
                candidateKeys: candidateKeys,
                transaction: transaction,
                workMeter: execution.workMeter,
                reservation: intermediateReservation
            )

            let primaryKeyReservation = try execution.workMeter
                .reserveIntermediate(
                    bytes: try DatabaseIntermediateCollectionMeter
                        .arrayFootprint(
                            count: topKResults.count,
                            element: Tuple.self
                        ).bytes,
                    at: .storageRow
                )
            defer { primaryKeyReservation.release() }
            var primaryKeys: [Tuple] = []
            primaryKeys.reserveCapacity(topKResults.count)
            for result in topKResults {
                try primaryKeyReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(result.pk.pack().count) + 32,
                    at: .storageRow
                )
                primaryKeys.append(result.pk)
            }

            let models = try await transaction
                .fetchPersistedModelsPreservingOrder(
                    entity: entity,
                    primaryKeys: primaryKeys,
                    partitions: queryContext.partitionValues,
                    workMeter: execution.workMeter
                )
            let resultReservation = try execution.workMeter.reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: models.count,
                    element: (item: T, score: Int64).self
                ).bytes,
                at: .projection
            )
            defer { resultReservation.release() }
            var leaderboardResults: [(item: T, score: Int64)] = []
            leaderboardResults.reserveCapacity(models.count)
            for index in models.indices {
                guard let model = models[index] else {
                    throw LeaderboardQueryError.indexedItemMissing(
                        index: indexName,
                        primaryKey: primaryKeys[index].pack()
                    )
                }
                try execution.workMeter.consume(at: .projection)
                let modelFootprint = try CanonicalRelationalFootprintMeter
                    .footprint(
                        of: model,
                        workMeter: execution.workMeter
                    )
                let retainedFootprint = try modelFootprint.adding(
                    DatabaseIntermediateFootprint(bytes: 32)
                )
                try resultReservation.reserveAdditional(
                    rows: retainedFootprint.rows,
                    bytes: retainedFootprint.bytes,
                    at: .projection
                )
                let item = try model.decode(as: T.self)
                leaderboardResults.append(
                    (item: item, score: topKResults[index].score)
                )
            }

            try execution.workMeter.consume(
                UInt64(leaderboardResults.count),
                at: .sortInput
            )
            leaderboardResults.sort {
                if $0.score == $1.score {
                    return $0.item.id.persistableIdentifierValue
                        < $1.item.id.persistableIdentifierValue
                }
                return $0.score > $1.score
            }

            let count = Double(leaderboardResults.count)
            var output = try FusionQueryResultBuilder<T>(
                execution: execution,
                expectedCount: leaderboardResults.count
            )
            for (index, result) in leaderboardResults.enumerated() {
                let score = count > 1
                    ? 1.0 - Double(index) / (count - 1)
                    : 1.0
                try output.append(
                    ScoredResult(item: result.item, score: score)
                )
            }
            return try output.finish()
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
        candidateKeys: Set<ByteString>?,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        reservation: DatabaseIntermediateReservation
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
        let rangeEnd = try strinc(rangeStart)

        let storageLimit = try workMeter.storageWorkReadLimitWithSentinel()
        guard candidateKeys?.isEmpty != true else {
            return []
        }
        let cursorLimit = candidateKeys == nil ? min(k, storageLimit) : storageLimit
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(rangeStart),
            to: .firstGreaterOrEqual(rangeEnd),
            limit: cursorLimit,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [(pk: Tuple, score: Int64)] = []
        let groupingCount = grouping?.count ?? 0

        do {
            while results.count < k, let (key, _) = try await cursor.next() {
                guard windowSubspace.contains(key) else { break }
                try workMeter.consume(at: .indexScan)
                let keyTuple = try windowSubspace.unpack(key)

            // Extract inverted score and primary key
            // Key structure: windowId, [grouping...], invertedScore, [pk...]
                let invertedScoreIndex = 1 + groupingCount

                guard let invertedScoreElement = keyTuple[
                    invertedScoreIndex
                ] else {
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
                for index in (invertedScoreIndex + 1)..<keyTuple.count {
                    if let element = keyTuple[index] {
                        pkElements.append(element)
                    }
                }

                let primaryKey = Tuple(pkElements)
                if let candidateKeys,
                   !candidateKeys.contains(primaryKey.pack()) {
                    continue
                }
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(key.count) + 48,
                    at: .indexScan
                )
                results.append((pk: primaryKey, score: score))
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()

        return results
    }
}
