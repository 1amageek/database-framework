// Similar.swift
// VectorIndex - Vector similarity search query for Fusion
//
// This file is part of VectorIndex module, not DatabaseEngine.
// DatabaseEngine does not own vector index execution.

@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

private struct RetainedVectorMatch<Item: Persistable>: Sendable {
    let item: Item
    let distance: Double
    let reservation: DatabaseIntermediateReservation
}

private struct RetainedVectorMatchBatch<Item: Persistable>: Sendable {
    let elements: [RetainedVectorMatch<Item>]
    let arrayReservation: DatabaseIntermediateReservation
}

/// Vector similarity search query for Fusion
///
/// Searches vector embeddings using distance metrics.
/// Supports candidate-restricted vector filtering.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Similar(Product.fields.embedding, dimensions: 384)
///         .nearest(to: queryVector, k: 100)
///         .metric(.cosine)
/// }
/// .execute()
/// ```
public struct Similar<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext!
    private let fieldIdentity: FieldIdentity
    private let fieldName: String
    private let dimensions: Int
    private var queryElements: [Float]?
    private var retainedQueryVector: Vector?
    private var k: Int = 10
    private var metric: VectorDistanceMetric = .cosine

    // MARK: - Initialization

    /// Create a Similar query for a vector field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameters:
    ///   - field: Compiled vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Similar(Product.fields.embedding, dimensions: 384)
    ///         .nearest(to: vector, k: 100)
    /// }
    /// ```
    public init(_ field: Field<T, Vector>, dimensions: Int) {
        let context = FusionContext.current
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field
    ///
    /// - Parameters:
    ///   - field: Compiled optional vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    public init(_ field: Field<T, Vector?>, dimensions: Int) {
        let context = FusionContext.current
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query with explicit context
    ///
    /// - Parameters:
    ///   - field: Compiled vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, Vector>,
        dimensions: Int,
        context: IndexQueryContext
    ) {
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    /// Create a Similar query for an optional vector field with explicit context
    ///
    /// - Parameters:
    ///   - field: Compiled optional vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, Vector?>,
        dimensions: Int,
        context: IndexQueryContext
    ) {
        self.fieldIdentity = field.identity
        self.fieldName = field.name
        self.dimensions = dimensions
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Find nearest neighbors to a query vector
    ///
    /// - Parameters:
    ///   - vector: The query vector to find neighbors for
    ///   - k: Number of nearest neighbors to return
    /// - Returns: Updated query
    public func nearest(to vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryElements = vector
        copy.retainedQueryVector = nil
        copy.k = k
        return copy
    }

    /// Finds nearest neighbors while retaining the canonical vector owner.
    /// No query element buffer is copied at this API boundary.
    public func nearest(to vector: Vector, k: Int) -> Self {
        var copy = self
        copy.queryElements = nil
        copy.retainedQueryVector = vector
        copy.k = k
        return copy
    }

    /// Set the distance metric
    ///
    /// - Parameter metric: Distance metric (.cosine, .euclidean, .dotProduct)
    /// - Returns: Updated query
    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.metric = metric
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using indexType and fieldName
    ///
    /// This approach:
    /// 1. Filters by indexType ("vector") for efficiency
    /// 2. Matches by fieldName within the kind
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        try queryContext.indexDescriptors(for: T.self).first { descriptor in
            guard
                descriptor.type
                    == .vector,
                descriptor.fieldNames == [fieldName] else {
                return false
            }
            let specification = try VectorIndexSpecification(
                descriptor.declaration.definition
            )
            return specification.dimensions == dimensions
                && specification.metric.rawValue == metric.rawValue
        }
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Similar requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: k,
                offset: nil,
                orderBy: ["distance"]
            ),
            indexDescriptor: {
                guard let descriptor = try self.findIndexDescriptor() else {
                    throw FusionQueryError.indexNotFound(
                        entity: T.persistableType,
                        field: self.fieldName,
                        indexType: .vector
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
        guard dimensions > 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Vector dimensions must be positive"
            )
        }
        guard k > 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Vector result count must be positive"
            )
        }
        guard queryElements != nil || retainedQueryVector != nil else {
            throw FusionQueryError.invalidConfiguration(
                "A nearest-neighbor query vector is required"
            )
        }
        let queryCount = retainedQueryVector?.count ?? queryElements?.count ?? 0
        guard queryCount == dimensions else {
            throw FusionQueryError.invalidConfiguration(
                "Vector query dimension mismatch: expected \(dimensions), got \(queryCount)"
            )
        }
        let vector: Vector
        if let retainedQueryVector {
            guard retainedQueryVector.elementType == .float32 else {
                throw FusionQueryError.invalidConfiguration(
                    "Vector queries require Float32 elements"
                )
            }
            vector = retainedQueryVector
        } else {
            do {
                vector = try Vector(float32: queryElements ?? [])
            } catch {
                throw FusionQueryError.invalidConfiguration(
                    "Vector query contains a non-finite Float32 element: \(error)"
                )
            }
        }

        // Find index descriptor
        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: fieldName,
                indexType: .vector
            )
        }

        let indexName = descriptor.name

        if let candidateIDs = candidates {
            guard !candidateIDs.isEmpty else {
                return try FusionQueryResultBuilder<T>(
                    execution: execution
                ).finish()
            }
            return try await executeWithCandidates(
                queryVector: vector,
                candidateIDs: candidateIDs,
                execution: execution
            )
        }
        return try await executeVectorSearch(
            indexName: indexName,
            queryVector: vector,
            k: k,
            execution: execution
        )
    }

    // MARK: - Vector Index Reading

    /// Executes the canonical vector query path so Fusion observes the same
    /// runtime policy, persisted layout, metric, and primary-key contract as
    /// ordinary vector queries.
    private func executeVectorSearch(
        indexName: String,
        queryVector: Vector,
        k: Int,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        let builder = VectorQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName,
            dimensions: dimensions,
            selectedIndexName: indexName
        ).metric(metric)
        let query = try builder
            .query(queryVector, k: k)
            .toSelectQuery()
        let rows = try await queryContext.context
            .executeRetainedCanonicalQueryRows(
            query,
            execution: execution,
            graphPartitions: queryContext.partitionValues
        )
        guard !rows.isEmpty else {
            return try FusionQueryResultBuilder<T>(
                execution: execution
            ).finish()
        }
        var minimumDistance = Double.infinity
        var maximumDistance = -Double.infinity
        for index in 0..<rows.count {
            try execution.workMeter.consume(at: .projection)
            let retainedRow = rows.row(at: index)
            guard let distance = retainedRow.float64Annotation(
                named: "distance"
            ) else {
                throw CanonicalReadError.missingAnnotation("distance")
            }
            guard distance.isFinite else {
                throw FusionQueryError.invalidConfiguration(
                    "Vector search produced a non-finite distance"
                )
            }
            minimumDistance = min(minimumDistance, distance)
            maximumDistance = max(maximumDistance, distance)
        }
        let distanceRange = maximumDistance - minimumDistance
        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: rows.count
        )
        for index in 0..<rows.count {
            let retainedRow = rows.row(at: index)
            guard let distance = retainedRow.float64Annotation(
                named: "distance"
            ) else {
                throw CanonicalReadError.missingAnnotation("distance")
            }
            try output.appendDecodedRow(
                retainedRow,
                score: distanceRange == 0
                    ? 1.0
                    : (maximumDistance - distance) / distanceRange
            )
        }
        return try output.finish()
    }

    // MARK: - Candidate-Aware Search

    /// Execute vector search with candidate awareness
    ///
    /// Candidate membership is a semantic restriction, so execution computes
    /// exact distances over that set instead of applying an unrestricted ANN
    /// search followed by a lossy post-filter. Fetches are bounded so candidate
    /// models and vector owners are released between batches.
    private func executeWithCandidates(
        queryVector: Vector,
        candidateIDs: Set<T.ID>,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        let fetchBatchSize = 128
        try queryContext.authorizeListAccess(
            entityName: T.persistableType,
            authorization: IndexReadAuthorization(
                limit: candidateIDs.count,
                offset: nil,
                orderBy: ["distance"]
            )
        )
        let heapArrayReservation = try execution.workMeter
            .reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: min(k, candidateIDs.count),
                    element: RetainedVectorMatch<T>.self
                ).bytes,
                at: .indexScan
            )
        defer { heapArrayReservation.release() }
        var nearest = MinHeap<RetainedVectorMatch<T>>(
            maxSize: k,
            heapType: .max,
            comparator: {
                if $0.distance == $1.distance {
                    return $0.item.id.persistableIdentifierValue
                        > $1.item.id.persistableIdentifierValue
                }
                return $0.distance > $1.distance
            }
        )
        try execution.workMeter.consume(
            UInt64(candidateIDs.count),
            at: .sortInput
        )
        let orderedIdentifierReservation = try execution.workMeter
            .reserveIntermediate(
                rows: UInt64(candidateIDs.count),
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: candidateIDs.count,
                    element: T.ID.self
                ).bytes,
                at: .sortInput
            )
        defer { orderedIdentifierReservation.release() }
        let orderedIdentifiers = candidateIDs.sorted(by: {
            $0.persistableIdentifierValue < $1.persistableIdentifierValue
        })
        var batchStart = orderedIdentifiers.startIndex
        while batchStart < orderedIdentifiers.endIndex {
            let batchEnd = min(
                batchStart + fetchBatchSize,
                orderedIdentifiers.endIndex
            )
            let results = try await computeDistancesForCandidateBatch(
                    queryVector: queryVector,
                    identifiers: orderedIdentifiers[batchStart..<batchEnd],
                    execution: execution
                )
            for result in results.elements {
                nearest.insert(result)
            }
            batchStart = batchEnd
        }
        guard !nearest.isEmpty else {
            return try FusionQueryResultBuilder<T>(
                execution: execution
            ).finish()
        }
        let sortedArrayReservation = try execution.workMeter
            .reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: nearest.count,
                    element: RetainedVectorMatch<T>.self
                ).bytes,
                at: .sortInput
            )
        defer { sortedArrayReservation.release() }
        try execution.workMeter.consume(
            UInt64(nearest.count),
            at: .sortInput
        )
        let matches = nearest.sorted()
        var minimumDistance = Double.infinity
        var maximumDistance = -Double.infinity
        for match in matches {
            guard match.distance.isFinite else {
                throw FusionQueryError.invalidConfiguration(
                    "Vector search produced a non-finite distance"
                )
            }
            minimumDistance = min(minimumDistance, match.distance)
            maximumDistance = max(maximumDistance, match.distance)
        }
        let distanceRange = maximumDistance - minimumDistance
        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: matches.count
        )
        for match in matches {
            try output.append(
                ScoredResult(
                    item: match.item,
                    score: distanceRange == 0
                        ? 1.0
                        : (maximumDistance - match.distance) / distanceRange
                )
            )
        }
        return try output.finish()
    }

    /// Computes one bounded candidate batch. The returned array cannot exceed
    /// the fixed fetch batch size and is drained into the result heap before the
    /// next batch is loaded.
    private func computeDistancesForCandidateBatch(
        queryVector: Vector,
        identifiers: ArraySlice<T.ID>,
        execution: ReadExecutionContext
    ) async throws -> RetainedVectorMatchBatch<T> {
        let tupleArrayReservation = try execution.workMeter
            .reserveIntermediate(
                rows: UInt64(identifiers.count),
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: identifiers.count,
                    element: Tuple.self
                ).bytes,
                at: .storageRow
            )
        defer { tupleArrayReservation.release() }
        var primaryKeys: [Tuple] = []
        primaryKeys.reserveCapacity(identifiers.count)
        for identifier in identifiers {
            let primaryKey = try PersistableIdentifierKeyCodec.tuple(
                for: identifier
            )
            try tupleArrayReservation.reserveAdditional(
                bytes: UInt64(primaryKey.pack().count),
                at: .storageRow
            )
            primaryKeys.append(primaryKey)
        }
        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Entity '\(T.persistableType)' is not present in the active schema"
            )
        }
        let models = try await queryContext.context
            .fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: queryContext.partitionValues,
                workMeter: execution.workMeter
            )
        let resultArrayReservation = try execution.workMeter
            .reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: models.count,
                    element: RetainedVectorMatch<T>.self
                ).bytes,
                at: .indexScan
            )
        var results: [RetainedVectorMatch<T>] = []
        results.reserveCapacity(models.count)
        for model in models {
            guard let model else { continue }
            let item = try model.decode(as: T.self)
            try execution.workMeter.consume(at: .filterEvaluation)
            guard let vector = try float32Vector(from: item) else {
                continue
            }

            guard vector.count == dimensions else {
                throw FusionQueryError.invalidConfiguration(
                    "Persisted vector field '\(fieldName)' has dimension \(vector.count); expected \(dimensions)"
                )
            }

            let distance = try VectorConversion.distance(
                metric: indexMetric,
                from: queryVector,
                to: vector
            )
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: item,
                annotations: ["distance": .float64(distance)],
                workMeter: execution.workMeter
            )
            let itemReservation = try execution.workMeter.reserveIntermediate(
                rows: footprint.rows,
                bytes: footprint.bytes,
                at: .indexScan
            )
            results.append(
                RetainedVectorMatch(
                    item: item,
                    distance: distance,
                    reservation: itemReservation
                )
            )
        }
        return RetainedVectorMatchBatch(
            elements: results,
            arrayReservation: resultArrayReservation
        )
    }

    private func float32Vector(from item: borrowing T) throws -> Vector? {
        guard let value = try PersistableFieldEncoder.value(
            for: fieldIdentity,
            in: item
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Persisted vector field '\(fieldName)' was not emitted"
            )
        }
        if case .null = value {
            return nil
        }
        guard case .vector(let vector) = value else {
            throw FusionQueryError.invalidConfiguration(
                "Persisted field '\(fieldName)' is not a vector"
            )
        }
        do {
            return try VectorConversion.float32Vector(from: vector)
        } catch {
            throw FusionQueryError.invalidConfiguration(
                "Persisted vector field '\(fieldName)' cannot be converted to Float32"
            )
        }
    }

    private var indexMetric: VectorMetric {
        switch metric {
        case .euclidean:
            return .euclidean
        case .cosine:
            return .cosine
        case .dotProduct:
            return .dotProduct
        }
    }

}
