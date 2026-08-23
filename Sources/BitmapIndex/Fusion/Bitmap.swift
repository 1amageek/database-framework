// Bitmap.swift
// BitmapIndex - Bitmap filter query for Fusion
//
// This file is part of BitmapIndex module, not DatabaseEngine.
// DatabaseEngine remains independent of bitmap execution behavior.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Bitmap filter query for Fusion
///
/// Filters items using bitmap index for efficient set operations.
/// All matching items receive a score of 1.0 (pass/fail filter).
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(User.self) {
///     Bitmap(User.fields.status, equals: "active")
///     Search(\.bio).terms(["developer"])
/// }
/// .execute()
///
/// // OR query
/// let results = try await context.fuse(User.self) {
///     Bitmap(User.fields.status, in: ["active", "pending"])
///     Similar(\.embedding, dimensions: 384).nearest(to: vector, k: 100)
/// }
/// .execute()
/// ```
public struct Bitmap<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext!
    private let fieldName: String
    private var predicate: BitmapPredicate

    private enum BitmapPredicate: Sendable {
        case equals(FieldValue)
        case `in`([FieldValue])
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Bitmap query for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(User.self) {
    ///     Bitmap(User.fields.status, equals: "active")
    /// }
    /// ```
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        equals value: V
    ) {
        let context = FusionContext.current
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V?>,
        equals value: V
    ) {
        let context = FusionContext.current
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Bitmap query for set membership (OR)
    ///
    /// Returns items matching ANY of the provided values.
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        in values: [V]
    ) {
        let context = FusionContext.current
        self.fieldName = field.name
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Bitmap query for equality comparison with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Bitmap query for set membership with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Finds the bitmap index descriptor for the requested field.
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        guard let descriptor = queryContext.indexDescriptors(
            for: T.self
        ).first(where: {
                $0.type == .bitmap
                    && $0.fieldNames.contains(fieldName)
        }) else {
            return nil
        }
        return descriptor
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Bitmap requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            ),
            indexDescriptor: {
                guard let descriptor = try self.findIndexDescriptor() else {
                    throw FusionQueryError.indexNotFound(
                        entity: T.persistableType,
                        field: self.fieldName,
                        indexType: .bitmap
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
        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: fieldName,
                indexType: .bitmap
            )
        }

        let indexName = descriptor.name

        // Resolve bitmap identifiers and models on one transaction snapshot.
        let retained = try await queryContext.withReadableIndex(
            named: indexName,
            indexType: .bitmap,
            for: T.self,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            )
        ) { readableIndex, transaction -> (
            primaryKeys: DatabaseSharedRetainedArray<Tuple>,
            models: DatabaseSharedRetainedArray<PersistedModel?>
        )? in
            guard let readableIndex else {
                return nil
            }
            let reader = BitmapIndexReader(subspace: readableIndex.subspace)
            let bitmap: BitmapIndexRetainedBitmap
            switch self.predicate {
            case .equals(let value):
                bitmap = try await reader.retainedBitmap(
                    for: [try TupleEncoder.encode(value)],
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
            case .in(let values):
                bitmap = try await reader.retainedUnion(
                    of: try values.map {
                        [try TupleEncoder.encode($0)]
                    },
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
            }
            let primaryKeys = try await reader.retainedPrimaryKeys(
                for: bitmap,
                transaction: transaction,
                workMeter: execution.workMeter
            )
            let models = try await transaction
                .fetchPersistedModelsPreservingOrder(
                    entity: try T.schemaEntity,
                    primaryKeys: primaryKeys,
                    partitions: queryContext.partitionValues,
                    workMeter: execution.workMeter
                )
            return (primaryKeys: primaryKeys, models: models)
        }

        // All matching items get score 1.0 (pass/fail filter).
        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: retained?.models.count ?? 0
        )
        guard let retained else { return try output.finish() }
        for (primaryKey, model) in zip(
            retained.primaryKeys,
            retained.models
        ) {
            guard let model else {
                throw BitmapQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: primaryKey.pack()
                )
            }
            let item = try model.decode(as: T.self)
            if let candidates, !candidates.contains(item.id) { continue }
            try output.append(ScoredResult(item: item, score: 1.0))
        }
        return try output.finish()
    }

}
