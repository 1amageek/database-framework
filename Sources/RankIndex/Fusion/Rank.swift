// Rank.swift
// RankIndex - Rank-based scoring query for Fusion
//
// This file is part of RankIndex module, not DatabaseEngine.
// Rank is a reranking operation that scores items based on a numeric field.

import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Rank-based scoring query for Fusion
///
/// Scores items based on a numeric field value.
/// Used for reranking results by popularity, rating, price, etc.
///
/// **Note**: Rank requires candidates from previous stages.
/// It should not be used as the first stage in a fusion pipeline.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.description, context: context.indexQueryContext).terms(["coffee"])
///     Rank(\.rating, context: context.indexQueryContext).order(.descending)
/// }
/// .execute()
///
/// // Or for sales rank (lower = better)
/// Rank(\.salesRank, context: context.indexQueryContext).order(.ascending)
/// ```
public struct Rank<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    /// Sort order for ranking
    public enum Order: Sendable {
        /// Lower value = higher score (e.g., sales rank, price)
        case ascending
        /// Higher value = higher score (e.g., rating, popularity)
        case descending
    }

    private let queryContext: IndexQueryContext!
    private let field: FieldIdentity
    private var order: Order = .descending

    // MARK: - Initialization (FusionContext)

    /// Create a Rank query for an exact numeric field.
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Search(\.description).terms(["coffee"])
    ///     Rank(\.rating).order(.descending)
    /// }
    /// ```
    public init<Value: RankNumericValue>(_ field: Field<T, Value>) {
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Rank query for an optional exact numeric field.
    public init<Value: RankNumericValue>(_ field: Field<T, Value?>) {
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Rank query for an exact numeric field with explicit context.
    public init<Value: RankNumericValue>(
        _ field: Field<T, Value>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Rank query for an optional exact numeric field with explicit context.
    public init<Value: RankNumericValue>(
        _ field: Field<T, Value?>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set the sort order
    ///
    /// - Parameter order: Sort order (.ascending or .descending)
    /// - Returns: Updated query
    public func order(_ order: Order) -> Self {
        var copy = self
        copy.order = order
        return copy
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Rank requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["rank"]
            ),
            fieldNames: [field.name],
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
        // Rank query requires candidates from previous stages
        // It should not be used as the first stage
        guard let candidateIDs = candidates else {
            throw FusionQueryError.missingCandidates(stage: "Rank")
        }
        guard !candidateIDs.isEmpty else {
            return try FusionQueryResultBuilder<T>(
                execution: execution
            ).finish()
        }

        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Entity '\(T.persistableType)' is not present in the active schema"
            )
        }

        return try await queryContext.withPersistenceRead { transaction in
            let identifierReservation = try execution.workMeter
                .reserveIntermediate(
                    bytes: try DatabaseIntermediateCollectionMeter
                        .arrayFootprint(
                            count: candidateIDs.count,
                            element: T.ID.self
                        ).bytes,
                    at: .indexScan
                )
            defer { identifierReservation.release() }
            var orderedIdentifiers: [T.ID] = []
            orderedIdentifiers.reserveCapacity(candidateIDs.count)
            orderedIdentifiers.append(contentsOf: candidateIDs)
            orderedIdentifiers.sort {
                $0.persistableIdentifierValue < $1.persistableIdentifierValue
            }

            let primaryKeyReservation = try execution.workMeter
                .reserveIntermediate(
                    bytes: try DatabaseIntermediateCollectionMeter
                        .arrayFootprint(
                            count: orderedIdentifiers.count,
                            element: Tuple.self
                        ).bytes,
                    at: .indexScan
                )
            defer { primaryKeyReservation.release() }
            var primaryKeys: [Tuple] = []
            primaryKeys.reserveCapacity(orderedIdentifiers.count)
            for identifier in orderedIdentifiers {
                let primaryKey = try PersistableIdentifierKeyCodec.tuple(
                    for: identifier
                )
                try primaryKeyReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(primaryKey.pack().count) + 32,
                    at: .indexScan
                )
                primaryKeys.append(primaryKey)
            }

            let models = try await transaction
                .fetchPersistedModelsPreservingOrder(
                    entity: entity,
                    primaryKeys: primaryKeys,
                    partitions: queryContext.partitionValues,
                    workMeter: execution.workMeter
                )
            let entryReservation = try execution.workMeter.reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: models.count,
                    element: RankValueEntry<T>.self
                ).bytes,
                at: .indexScan
            )
            defer { entryReservation.release() }
            var entries: [RankValueEntry<T>] = []
            entries.reserveCapacity(models.count)
            for index in models.indices {
                guard let model = models[index] else {
                    throw FusionQueryError.danglingCandidate(
                        entity: T.persistableType,
                        primaryKey: primaryKeys[index].pack()
                    )
                }
                try execution.workMeter.consume(at: .storageRow)
                let modelFootprint = try CanonicalRelationalFootprintMeter
                    .footprint(
                        of: model,
                        workMeter: execution.workMeter
                    )
                try entryReservation.reserveAdditional(
                    rows: modelFootprint.rows,
                    bytes: modelFootprint.bytes,
                    at: .indexScan
                )
                let item = try model.decode(as: T.self)
                let value = try RankValueOrdering.numericValue(
                    from: try item.persistedFieldValue(for: field),
                    fieldName: field.name
                )
                let identifierKey = try RankValueOrdering.identifierKey(
                    for: item.id
                )
                try entryReservation.reserveAdditional(
                    bytes: UInt64(identifierKey.count) + 64,
                    at: .indexScan
                )
                entries.append(
                    RankValueEntry(
                        item: item,
                        value: value,
                        identifierKey: identifierKey
                    )
                )
            }

            let direction: RankValueDirection
            switch order {
            case .ascending:
                direction = .ascending
            case .descending:
                direction = .descending
            }
            let sorted = try RankValueOrdering.sorted(
                consume entries,
                direction: direction,
                workMeter: execution.workMeter
            )

            let count = Double(sorted.count)
            var output = try FusionQueryResultBuilder<T>(
                execution: execution,
                expectedCount: sorted.count
            )
            for (index, tuple) in sorted.enumerated() {
                let score = count > 1
                    ? 1.0 - Double(index) / (count - 1)
                    : 1.0
                try output.append(
                    ScoredResult(item: tuple.item, score: score)
                )
            }
            return try output.finish()
        }
    }
}
