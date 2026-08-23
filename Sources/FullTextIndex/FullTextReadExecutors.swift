@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum FullTextReadParameter {
    static let fieldName = "fieldName"
    static let terms = "terms"
    static let matchMode = "matchMode"
    static let limit = "limit"
    static let returnScores = "returnScores"
    static let includeFacets = "includeFacets"
    static let bm25K1 = "bm25.k1"
    static let bm25B = "bm25.b"
    static let facetFields = "facetFields"
    static let facetLimit = "facetLimit"
    static let totalCount = "fulltext.totalCount"
    static let facetMetadataPrefix = "fulltext.facets."
}

public enum FullTextReadExecutors {
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicFullTextReadExecutor()
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(FullTextReadExecutor())
    }
}

private enum FullTextReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidResultCount(field: String, count: Int64)
    case invalidExecutionPath(String)
    case fetchedItemCountMismatch(expected: Int, actual: Int)
    case missingFetchedEntity(ByteString)
}

fileprivate typealias FullTextRetainedArray<Element: Sendable> =
    DatabaseSharedRetainedArray<Element>

private final class FullTextRetainedFacetedResult<Item: Sendable>: Sendable {
    let items: FullTextRetainedArray<Item>
    let facets: [String: [(value: String, count: Int64)]]
    let totalCount: Int
    private let facetReservation: DatabaseIntermediateReservation

    init(
        items: FullTextRetainedArray<Item>,
        facets: [String: [(value: String, count: Int64)]],
        totalCount: Int,
        facetReservation: DatabaseIntermediateReservation
    ) {
        self.items = items
        self.facets = facets
        self.totalCount = totalCount
        self.facetReservation = facetReservation
    }
}

private func retainFullTextTuples<Elements: Sequence>(
    _ tuples: Elements,
    expectedCount: Int,
    workMeter: DatabaseWorkMeter
) throws -> FullTextRetainedArray<Tuple>
where Elements.Element == Tuple {
    var builder = try DatabaseRetainedArrayBuilder<Tuple>(
        workMeter: workMeter,
        stage: .indexScan,
        layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
            for: Tuple.self
        ),
        expectedCount: expectedCount
    )
    for tuple in tuples {
        try builder.append(
            footprint: DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(tuple.packedByteCount) + 32
            ),
            at: .indexScan,
            make: { tuple }
        )
    }
    return try builder.finish().moveToSharedOwnership(
        at: .indexScan
    )
}

private func retainFullTextCandidateTuples<Candidates: Collection>(
    _ candidates: Candidates,
    workMeter: DatabaseWorkMeter
) throws -> FullTextRetainedArray<Tuple>
where Candidates.Element == [any TupleElement] {
    try retainFullTextTuples(
        candidates.lazy.map(Tuple.init),
        expectedCount: candidates.count,
        workMeter: workMeter
    )
}

private func limitFullTextTuples(
    _ values: FullTextRetainedArray<Tuple>,
    limit: Int?,
    workMeter: DatabaseWorkMeter
) throws -> FullTextRetainedArray<Tuple> {
    guard let limit else { return values }
    guard limit >= 0 else {
        throw FullTextReadError.invalidParameter(
            FullTextReadParameter.limit
        )
    }
    guard values.count > limit else { return values }
    return try retainFullTextTuples(
        values.prefix(limit),
        expectedCount: limit,
        workMeter: workMeter
    )
}

private func retainFullTextScoredTuples<Elements: Sequence>(
    _ tuples: Elements,
    expectedCount: Int,
    workMeter: DatabaseWorkMeter
) throws -> FullTextRetainedArray<(id: Tuple, score: Double)>
where Elements.Element == (id: Tuple, score: Double) {
    var builder = try DatabaseRetainedArrayBuilder<(
        id: Tuple,
        score: Double
    )>(
        workMeter: workMeter,
        stage: .indexScan,
        layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
            for: (id: Tuple, score: Double).self
        ),
        expectedCount: expectedCount
    )
    for tuple in tuples {
        try builder.append(
            footprint: DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(tuple.id.packedByteCount) + 32
            ),
            at: .indexScan,
            make: { tuple }
        )
    }
    return try builder.finish().moveToSharedOwnership(
        at: .indexScan
    )
}

private func reserveFullTextFacetMetadataAdmission(
    _ facets: [String: [(value: String, count: Int64)]],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[String: FieldValue]>.stride) + 64
    )
    for (field, buckets) in facets {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(
                    FullTextReadParameter.facetMetadataPrefix.utf8.count
                        + field.utf8.count
                ) + 160
            )
        )
        for bucket in buckets {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(bucket.value.utf8.count) + 192
                )
            )
        }
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextMapEntry(
    key: ByteString,
    elements: [any TupleElement],
    in reservation: DatabaseIntermediateReservation
) throws {
    let elementBytes = try DatabaseIntermediateFootprint(
        bytes: UInt64(max(1, MemoryLayout<any TupleElement>.stride + 16))
    ).multiplied(by: UInt64(elements.count)).bytes
    try reservation.reserveAdditional(
        rows: 1,
        bytes: try DatabaseIntermediateFootprint(
            bytes: UInt64(key.count) + 96
        ).adding(
            DatabaseIntermediateFootprint(bytes: elementBytes)
        ).bytes,
        at: .indexScan
    )
}

private func reserveFullTextFacetEntry(
    _ value: String,
    in reservation: DatabaseIntermediateReservation
) throws {
    try reservation.reserveAdditional(
        rows: 1,
        bytes: UInt64(value.utf8.count) + 64,
        at: .indexScan
    )
}

struct FullTextReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .text(.fullText)

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let requestedLimit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        let budgetLimit = try options.workMeter
            .storageWorkReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        guard index.type == indexType,
            index.fieldNames.contains(fieldName),
              entity.fieldMapByName[fieldName] != nil else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.fieldName
            )
        }
        try context.authorizeCanonicalListAccess(
            entity: entity,
            selectQuery: selectQuery
        )
        let authorization = try IndexReadAuthorization(
            selectQuery: selectQuery
        )
        let configuration = try FullTextIndexConfiguration(definition: index.declaration.definition)
        let bm25Parameters: BM25Parameters?
        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            bm25Parameters = try BM25Parameters.validated(k1: k1, b: b)
        } else {
            bm25Parameters = nil
        }

        if limit == 0, !includeFacets {
            return .empty
        }

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = try optionalInteger(
                FullTextReadParameter.facetLimit,
                from: indexScan.parameters
            ) ?? 10
            let result = try await executeFacetedSearch(
                context: context,
                entity: entity,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                facetFields: facetFields,
                facetLimit: facetLimit,
                index: index,
                partitions: partitions,
                authorization: authorization,
                execution: execution,
                workMeter: options.workMeter
            )
            let metadataAdmission = try reserveFullTextFacetMetadataAdmission(
                result.facets,
                workMeter: options.workMeter
            )
            defer { metadataAdmission.release() }
            let metadata = try facetMetadata(
                totalCount: result.totalCount,
                facets: result.facets
            )
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                metadata: metadata,
                expectedCount: result.items.count
            ) { rows in
                for item in result.items {
                    try rows.append(try IndexReadRow.materializing(item))
                }
            }
        }

        if returnScores {
            guard let bm25Parameters else {
                throw FullTextReadError.invalidParameter(
                    FullTextReadParameter.returnScores
                )
            }
            let results = try await executeScoredSearch(
                context: context,
                entity: entity,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Parameters: bm25Parameters,
                index: index,
                partitions: partitions,
                authorization: authorization,
                execution: execution,
                workMeter: options.workMeter
            )
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                expectedCount: results.count
            ) { rows in
                for result in results {
                    try rows.append(
                        try IndexReadRow.materializing(
                            result.item,
                            annotations: [
                                "score": .float64(result.score)
                            ]
                        )
                    )
                }
            }
        }

        let results = try await executePlainSearch(
            context: context,
            entity: entity,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            index: index,
            partitions: partitions,
            authorization: authorization,
            execution: execution,
            workMeter: options.workMeter
        )
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: results.count
        ) { rows in
            for item in results {
                try rows.append(try IndexReadRow.materializing(item))
            }
        }
    }

    private func executePlainSearch(
        context: DatabaseContext,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        index: IndexDescriptor,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<PersistedModel> {
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions,
            authorization: authorization,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return try FullTextRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
            let identifiers: FullTextRetainedArray<Tuple>
            if matchMode == .phrase {
                identifiers = try await search.searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            } else {
                identifiers = try await search.searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            let limited = try limitFullTextTuples(
                identifiers,
                limit: limit,
                workMeter: workMeter
            )
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: limited,
                partitions: partitions,
                workMeter: workMeter
            )
            return try requireModels(
                identifiers: limited,
                fetched: fetched,
                workMeter: workMeter
            )
        }
    }

    private func executeScoredSearch(
        context: DatabaseContext,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        bm25Parameters: BM25Parameters,
        index: IndexDescriptor,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<(
        item: PersistedModel,
        score: Double
    )> {
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions,
            authorization: authorization,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return try FullTextRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
            let scored = try await search.searchWithScores(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                bm25Params: bm25Parameters,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                limit: limit,
                workMeter: workMeter
            )
            let identifiers = try retainFullTextTuples(
                scored.lazy.map(\.id),
                expectedCount: scored.count,
                workMeter: workMeter
            )
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                workMeter: workMeter
            )
            let models = try requireModels(
                identifiers: identifiers,
                fetched: fetched,
                workMeter: workMeter
            )
            return try retainScoredModels(
                models: models,
                scores: scored,
                workMeter: workMeter
            )
        }
    }

    /// Builds a Fusion source directly from the request-accounted scored-model
    /// path. This avoids promoting canonical QueryRows to an unmetered public
    /// response and then retaining a second decoded result collection.
    func executeScoredFusion<T: Persistable>(
        queryContext: IndexQueryContext,
        descriptor: IndexDescriptor,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        parameters: BM25Parameters,
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        guard let entity = queryContext.schema.entity(named: T.persistableType)
        else {
            throw IndexQueryContextError.entityNotFound(T.persistableType)
        }
        try queryContext.authorizeListAccess(
            entityName: entity.name,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["score"]
            )
        )
        guard !terms.isEmpty else {
            return try FusionQueryResultBuilder<T>(
                execution: execution
            ).finish()
        }
        let canonicalRead = CanonicalReadExecution.resolve(
            requested: execution.options.consistency,
            default: .snapshot
        )
        let retained = try await executeScoredSearch(
            context: queryContext.context,
            entity: entity,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: nil,
            bm25Parameters: parameters,
            index: descriptor,
            partitions: queryContext.partitionValues,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["score"]
            ),
            execution: canonicalRead,
            workMeter: execution.workMeter
        )
        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: retained.count
        )
        for scored in retained {
            try output.appendDecodedModel(
                scored.item,
                score: scored.score,
                where: { candidates?.contains($0.id) ?? true }
            )
        }
        return try output.finish()
    }

    private func executeFacetedSearch(
        context: DatabaseContext,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        facetFields: [String],
        facetLimit: Int,
        index: IndexDescriptor,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedFacetedResult<PersistedModel> {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions,
            authorization: authorization,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                let items = try FullTextRetainedArray<PersistedModel>.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
                let reservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[
                            String: [(value: String, count: Int64)]
                        ]>.stride
                    ) + 64,
                    at: .indexScan
                )
                return FullTextRetainedFacetedResult(
                    items: items,
                    facets: [:],
                    totalCount: 0,
                    facetReservation: reservation
                )
            }
            let identifiers: FullTextRetainedArray<Tuple>
            if matchMode == .phrase {
                identifiers = try await search.searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            } else {
                identifiers = try await search.searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                workMeter: workMeter
            )
            let allModels = try requireModels(
                identifiers: identifiers,
                fetched: fetched,
                workMeter: workMeter
            )
            var facets: [String: [(value: String, count: Int64)]] = [:]
            let facetOutputReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[
                        String: [(value: String, count: Int64)]
                    ]>.stride
                ) + 64,
                at: .indexScan
            )
            for fieldName in facetFields {
                guard let field = entity.fieldMapByName[fieldName] else {
                    throw FullTextReadError.invalidParameter(fieldName)
                }
                var counts: [String: Int64] = [:]
                let countsReservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
                    at: .indexScan
                )
                defer { countsReservation.release() }
                for model in allModels {
                    try workMeter.consume(at: .indexScan)
                    for value in try FullTextFieldValueExtractor.strings(
                        from: model,
                        entity: entity.name,
                        field: FieldIdentity(
                            name: field.name,
                            number: field.fieldNumber
                        )
                    ) where !value.isEmpty {
                        if counts[value] == nil {
                            try reserveFullTextFacetEntry(
                                value,
                                in: countsReservation
                            )
                        }
                        counts[value, default: 0] += 1
                    }
                }
                let bucketScratch = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[(value: String, count: Int64)]>.stride
                    ),
                    at: .indexScan
                )
                defer { bucketScratch.release() }
                for value in counts.keys {
                    try reserveFullTextFacetEntry(
                        value,
                        in: bucketScratch
                    )
                }
                var buckets: [(value: String, count: Int64)] = counts.map {
                    (value: $0.key, count: $0.value)
                }
                buckets.sort {
                    $0.count == $1.count
                        ? $0.value < $1.value
                        : $0.count > $1.count
                }
                try facetOutputReservation.reserveAdditional(
                    bytes: UInt64(fieldName.utf8.count) + 48,
                    at: .indexScan
                )
                for bucket in buckets.prefix(facetLimit) {
                    try reserveFullTextFacetEntry(
                        bucket.value,
                        in: facetOutputReservation
                    )
                }
                let visibleBuckets = Array(buckets.prefix(facetLimit))
                facets[fieldName] = visibleBuckets
            }
            let items = try limitModels(
                allModels,
                limit: limit,
                workMeter: workMeter
            )
            return FullTextRetainedFacetedResult(
                items: items,
                facets: facets,
                totalCount: allModels.count,
                facetReservation: facetOutputReservation
            )
        }
    }

    private func requireModels<Identifiers, Fetched>(
        identifiers: Identifiers,
        fetched: Fetched,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<PersistedModel>
    where Identifiers: RandomAccessCollection,
          Identifiers.Element == Tuple,
          Fetched: RandomAccessCollection,
          Fetched.Element == PersistedModel? {
        guard identifiers.count == fetched.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: fetched.count
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<PersistedModel>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: PersistedModel.self
            ),
            expectedCount: fetched.count
        )
        for (identifier, model) in zip(identifiers, fetched) {
            guard let model else {
                throw FullTextReadError.missingFetchedEntity(
                    identifier.pack()
                )
            }
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(model),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { model }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func retainScoredModels<Models, Scores>(
        models: Models,
        scores: Scores,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<(
        item: PersistedModel,
        score: Double
    )>
    where Models: RandomAccessCollection,
          Models.Element == PersistedModel,
          Scores: RandomAccessCollection,
          Scores.Element == (id: Tuple, score: Double) {
        guard models.count == scores.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: scores.count,
                actual: models.count
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<(
            item: PersistedModel,
            score: Double
        )>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: (item: PersistedModel, score: Double).self
            ),
            expectedCount: models.count
        )
        for (model, scored) in zip(models, scores) {
            let score = scored.score
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(
                    model,
                    annotations: ["score": .float64(score)]
                ),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { (item: model, score: score) }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func limitModels(
        _ models: FullTextRetainedArray<PersistedModel>,
        limit: Int?,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<PersistedModel> {
        guard let limit else { return models }
        guard limit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        guard models.count > limit else { return models }
        var builder = try DatabaseRetainedArrayBuilder<PersistedModel>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: PersistedModel.self
            ),
            expectedCount: limit
        )
        for model in models.prefix(limit) {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(model),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { model }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func facetMetadata(
        totalCount: Int,
        facets: [String: [(value: String, count: Int64)]]
    ) throws -> [String: FieldValue] {
        guard let totalCount = UInt64(exactly: totalCount) else {
            throw FullTextReadError.invalidResultCount(
                field: FullTextReadParameter.totalCount,
                count: Int64(totalCount)
            )
        }
        var metadata: [String: FieldValue] = [
            FullTextReadParameter.totalCount: .uint64(totalCount)
        ]
        for (field, buckets) in facets {
            metadata[FullTextReadParameter.facetMetadataPrefix + field] = .array(
                try buckets.map { bucket in
                    guard let count = UInt64(exactly: bucket.count) else {
                        throw FullTextReadError.invalidResultCount(
                            field: field,
                            count: bucket.count
                        )
                    }
                    return .array([.string(bucket.value), .uint64(count)])
                }
            )
        }
        return metadata
    }

    private func decodeMatchMode(
        from parameters: [String: FieldValue]
    ) throws -> TextMatchMode {
        let rawValue = try requireString(FullTextReadParameter.matchMode, from: parameters)
        switch rawValue {
        case "all":
            return .all
        case "any":
            return .any
        case "phrase":
            return .phrase
        default:
            throw FullTextReadError.invalidParameter(FullTextReadParameter.matchMode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func optionalInteger(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> Int? {
        guard let value = parameters[name] else {
            return nil
        }
        guard let integer = value.int64Value,
              let result = Int(exactly: integer) else {
            throw FullTextReadError.invalidParameter(name)
        }
        return result
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [String] {
        guard let values = parameters[key]?.arrayValue else {
            throw FullTextReadError.missingParameter(key)
        }

        var strings: [String] = []
        strings.reserveCapacity(values.count)
        for value in values {
            guard let string = value.stringValue else {
                throw FullTextReadError.invalidParameter(key)
            }
            strings.append(string)
        }
        return strings
    }
}

private struct PolymorphicFullTextReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .text(.fullText)

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let requestedLimit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        let budgetLimit = try options.workMeter
            .storageWorkReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: try runtimeInteger(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try runtimeInteger(
                selectQuery.offset,
                parameter: "offset"
            ),
            orderBy: orderByFields
        )

        guard index.type == indexType,
            index.fieldNames.contains(fieldName) else {
            throw FullTextReadError.invalidParameter(index.name)
        }
        let configuration = try FullTextIndexConfiguration(
            definition: index.definition
        )
        let bm25Parameters: BM25Parameters?
        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            bm25Parameters = try BM25Parameters.validated(k1: k1, b: b)
        } else {
            bm25Parameters = nil
        }

        if limit == 0, !includeFacets {
            return .empty
        }

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = try optionalInteger(
                FullTextReadParameter.facetLimit,
                from: indexScan.parameters
            ) ?? 10
            let result = try await executeFacetedSearch(
                context: context,
                group: group,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                facetFields: facetFields,
                facetLimit: facetLimit,
                index: index,
                execution: execution,
                workMeter: options.workMeter
            )
            let metadataAdmission = try reserveFullTextFacetMetadataAdmission(
                result.facets,
                workMeter: options.workMeter
            )
            defer { metadataAdmission.release() }
            let metadata = try facetMetadata(
                totalCount: result.totalCount,
                facets: result.facets
            )
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                metadata: metadata,
                expectedCount: result.items.count
            ) { rows in
                for entity in result.items {
                    try rows.append(
                        try IndexReadRow.materializing(
                            entity.item,
                            annotations: [
                                PolymorphicRowAnnotation.typeName:
                                    .string(entity.typeName),
                                PolymorphicRowAnnotation.typeCode:
                                    .int64(entity.typeCode),
                            ]
                        )
                    )
                }
            }
        }

        if returnScores {
            guard let bm25Parameters else {
                throw FullTextReadError.invalidParameter(
                    FullTextReadParameter.returnScores
                )
            }
            let results = try await executeScoredSearch(
                context: context,
                group: group,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Params: bm25Parameters,
                index: index,
                execution: execution,
                workMeter: options.workMeter
            )
            return try IndexReadResult.build(
                workMeter: options.workMeter,
                expectedCount: results.count
            ) { rows in
                for result in results {
                    try rows.append(
                        try IndexReadRow.materializing(
                            result.entity.item,
                            annotations: [
                                PolymorphicRowAnnotation.typeName:
                                    .string(result.entity.typeName),
                                PolymorphicRowAnnotation.typeCode:
                                    .int64(result.entity.typeCode),
                                "score": .float64(result.score),
                            ]
                        )
                    )
                }
            }
        }

        let results = try await executePlainSearch(
            context: context,
            group: group,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            index: index,
            execution: execution,
            workMeter: options.workMeter
        )
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: results.count
        ) { rows in
            for entity in results {
                try rows.append(
                    try IndexReadRow.materializing(
                        entity.item,
                        annotations: [
                            PolymorphicRowAnnotation.typeName:
                                .string(entity.typeName),
                            PolymorphicRowAnnotation.typeCode:
                                .int64(entity.typeCode),
                        ]
                    )
                )
            }
        }
    }

    private func facetMetadata(
        totalCount: Int,
        facets: [String: [(value: String, count: Int64)]]
    ) throws -> [String: FieldValue] {
        guard let totalCount = UInt64(exactly: totalCount) else {
            throw FullTextReadError.invalidResultCount(
                field: FullTextReadParameter.totalCount,
                count: Int64(totalCount)
            )
        }
        var metadata: [String: FieldValue] = [
            FullTextReadParameter.totalCount: .uint64(totalCount)
        ]
        for (field, buckets) in facets {
            metadata[FullTextReadParameter.facetMetadataPrefix + field] = .array(
                try buckets.map { bucket in
                    guard let count = UInt64(exactly: bucket.count) else {
                        throw FullTextReadError.invalidResultCount(
                            field: field,
                            count: bucket.count
                        )
                    }
                    return .array([.string(bucket.value), .uint64(count)])
                }
            )
        }
        return metadata
    }

    private func executePlainSearch(
        context: DatabaseContext,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<PolymorphicEntity> {
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return try FullTextRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
            let matchingIDs: FullTextRetainedArray<Tuple>
            if matchMode == .phrase {
                matchingIDs = try await searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            } else {
                matchingIDs = try await searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            let limited = try limitFullTextTuples(
                matchingIDs,
                limit: limit,
                workMeter: workMeter
            )
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: limited,
                workMeter: workMeter
            )
            return try requireEntities(
                identifiers: limited,
                fetched: fetched,
                workMeter: workMeter
            )
        }
    }

    private func executeScoredSearch(
        context: DatabaseContext,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        bm25Params: BM25Parameters,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<(
        entity: PolymorphicEntity,
        score: Double
    )> {
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return try FullTextRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
            let scoredResults = try await searchWithScores(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                bm25Params: bm25Params,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                limit: limit,
                workMeter: workMeter
            )
            let identifiers = try retainFullTextTuples(
                scoredResults.lazy.map(\.id),
                expectedCount: scoredResults.count,
                workMeter: workMeter
            )
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: identifiers,
                workMeter: workMeter
            )
            let entities = try requireEntities(
                identifiers: identifiers,
                fetched: fetched,
                workMeter: workMeter
            )
            return try retainScoredEntities(
                entities: entities,
                scores: scoredResults,
                workMeter: workMeter
            )
        }
    }

    private func executeFacetedSearch(
        context: DatabaseContext,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        facetFields: [String],
        facetLimit: Int,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedFacetedResult<PolymorphicEntity> {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                let items = try FullTextRetainedArray<PolymorphicEntity>.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
                let reservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[
                            String: [(value: String, count: Int64)]
                        ]>.stride
                    ) + 64,
                    at: .indexScan
                )
                return FullTextRetainedFacetedResult(
                    items: items,
                    facets: [:],
                    totalCount: 0,
                    facetReservation: reservation
                )
            }
            let matchingIDs: FullTextRetainedArray<Tuple>
            if matchMode == .phrase {
                matchingIDs = try await searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            } else {
                matchingIDs = try await searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: matchingIDs,
                workMeter: workMeter
            )
            let allEntities = try requireEntities(
                identifiers: matchingIDs,
                fetched: fetched,
                workMeter: workMeter
            )
            let totalCount = allEntities.count

            var facets: [String: [(value: String, count: Int64)]] = [:]
            let facetOutputReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[
                        String: [(value: String, count: Int64)]
                    ]>.stride
                ) + 64,
                at: .indexScan
            )
            for field in facetFields {
                var counts: [String: Int64] = [:]
                let countsReservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
                    at: .indexScan
                )
                defer { countsReservation.release() }
                for entity in allEntities {
                    try workMeter.consume(at: .indexScan)
                    let values = try facetValues(
                        fieldName: field,
                        from: entity,
                        context: context
                    )
                    for value in values where !value.isEmpty {
                        if counts[value] == nil {
                            try reserveFullTextFacetEntry(
                                value,
                                in: countsReservation
                            )
                        }
                        counts[value, default: 0] += 1
                    }
                }
                let bucketScratch = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[(value: String, count: Int64)]>.stride
                    ),
                    at: .indexScan
                )
                defer { bucketScratch.release() }
                for value in counts.keys {
                    try reserveFullTextFacetEntry(
                        value,
                        in: bucketScratch
                    )
                }
                var buckets = counts.map {
                    (value: $0.key, count: $0.value)
                }
                buckets.sort {
                    if $0.count == $1.count {
                        return $0.value < $1.value
                    }
                    return $0.count > $1.count
                }
                try facetOutputReservation.reserveAdditional(
                    bytes: UInt64(field.utf8.count) + 48,
                    at: .indexScan
                )
                for bucket in buckets.prefix(facetLimit) {
                    try reserveFullTextFacetEntry(
                        bucket.value,
                        in: facetOutputReservation
                    )
                }
                let visibleBuckets = Array(buckets.prefix(facetLimit))
                facets[field] = visibleBuckets
            }

            let items = try limitEntities(
                allEntities,
                limit: limit,
                workMeter: workMeter
            )
            return FullTextRetainedFacetedResult(
                items: items,
                facets: facets,
                totalCount: totalCount,
                facetReservation: facetOutputReservation
            )
        }
    }

    private func requireEntities<Identifiers, Fetched>(
        identifiers: Identifiers,
        fetched: Fetched,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<PolymorphicEntity>
    where Identifiers: RandomAccessCollection,
          Identifiers.Element == Tuple,
          Fetched: RandomAccessCollection,
          Fetched.Element == PolymorphicEntity? {
        guard identifiers.count == fetched.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: fetched.count
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<PolymorphicEntity>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: PolymorphicEntity.self
            ),
            expectedCount: fetched.count
        )
        for (identifier, entity) in zip(identifiers, fetched) {
            guard let entity else {
                throw FullTextReadError.missingFetchedEntity(
                    identifier.pack()
                )
            }
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName:
                            .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode:
                            .int64(entity.typeCode),
                    ]
                ),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { entity }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func retainScoredEntities<Entities, Scores>(
        entities: Entities,
        scores: Scores,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<(
        entity: PolymorphicEntity,
        score: Double
    )>
    where Entities: RandomAccessCollection,
          Entities.Element == PolymorphicEntity,
          Scores: RandomAccessCollection,
          Scores.Element == (id: Tuple, score: Double) {
        guard entities.count == scores.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: scores.count,
                actual: entities.count
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<(
            entity: PolymorphicEntity,
            score: Double
        )>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: (entity: PolymorphicEntity, score: Double).self
            ),
            expectedCount: entities.count
        )
        for (entity, scored) in zip(entities, scores) {
            let score = scored.score
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName:
                            .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode:
                            .int64(entity.typeCode),
                        "score": .float64(score),
                    ]
                ),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { (entity: entity, score: score) }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func limitEntities(
        _ entities: FullTextRetainedArray<PolymorphicEntity>,
        limit: Int?,
        workMeter: DatabaseWorkMeter
    ) throws -> FullTextRetainedArray<PolymorphicEntity> {
        guard let limit else { return entities }
        guard limit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        guard entities.count > limit else { return entities }
        var builder = try DatabaseRetainedArrayBuilder<PolymorphicEntity>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: PolymorphicEntity.self
            ),
            expectedCount: limit
        )
        for entity in entities.prefix(limit) {
            let footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName:
                            .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode:
                            .int64(entity.typeCode),
                    ]
                ),
                workMeter: workMeter
            )
            try builder.append(
                footprint: footprint,
                at: .indexScan,
                make: { entity }
            )
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    fileprivate func searchPhrase(
        configuration: FullTextIndexConfiguration,
        terms: [String],
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<Tuple> {
        guard configuration.storePositions else {
            throw FullTextIndexError.invalidQuery(
                "Phrase search requires storePositions=true"
            )
        }
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        let normalizedTerms = normalizer
            .normalizedTerms(from: terms.joined(separator: " "))
        guard !normalizedTerms.isEmpty else {
            return try FullTextRetainedArray.empty(
                workMeter: workMeter,
                stage: .indexScan
            )
        }

        let termsSubspace = indexSubspace.subspace("terms")
        let candidates = try await searchTermsAND(
            normalizedTerms,
            termsSubspace: termsSubspace,
            transaction: transaction,
            workMeter: workMeter
        )
        let matchReservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: candidates.count,
                element: Tuple.self
            ).bytes,
            at: .indexScan
        )
        var transferredMatchReservation = false
        defer {
            if !transferredMatchReservation {
                matchReservation.release()
            }
        }
        var matches: [Tuple] = []
        matches.reserveCapacity(candidates.count)
        for elements in candidates {
            try workMeter.consume(at: .indexScan)
            let identifier = Tuple(elements)
            let positionCollectionFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: normalizedTerms.count,
                    element: [Int].self
                )
            var positionsByTerm: [[Int]] = []
            let positionReservation = try workMeter.reserveIntermediate(
                bytes: positionCollectionFootprint.bytes,
                at: .indexScan
            )
            defer { positionReservation.release() }
            positionsByTerm.reserveCapacity(normalizedTerms.count)
            for term in normalizedTerms {
                try workMeter.consume(at: .indexScan)
                let key = termsSubspace.subspace(term).pack(identifier)
                guard let value = try await transaction.getValue(
                    for: key,
                    snapshot: true
                ) else {
                    try positionReservation.reserveAdditional(
                        rows: 1,
                        at: .indexScan
                    )
                    positionsByTerm.append([])
                    continue
                }
                let positions = try FullTextStorageDecoder.posting(
                    from: value,
                    positionsStored: true,
                    term: term,
                    reservingPositionsWith: { count in
                        try positionReservation.reserveAdditional(
                            rows: 1,
                            bytes: try DatabaseIntermediateCollectionMeter
                                .arrayFootprint(
                                    count: count,
                                    element: Int.self
                                ).bytes,
                            at: .indexScan
                        )
                    }
                ).positions
                positionsByTerm.append(positions)
            }
            if try containsConsecutivePositions(
                positionsByTerm,
                workMeter: workMeter
            ) {
                try matchReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(identifier.packedByteCount) + 32,
                    at: .indexScan
                )
                matches.append(identifier)
            }
        }
        let retainedMatches = try FullTextRetainedArray.adopting(
            matches,
            reservation: matchReservation,
            workMeter: workMeter,
            stage: .indexScan
        )
        transferredMatchReservation = true
        return retainedMatches
    }

    fileprivate func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        bm25Params: BM25Parameters,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        limit: Int?,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<(
        id: Tuple,
        score: Double
    )> {
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })
        let matchingIdentifiers: FullTextRetainedArray<Tuple>
        switch matchMode {
        case .all:
            matchingIdentifiers = try await searchFullText(
                terms: terms,
                matchMode: .all,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
        case .any:
            matchingIdentifiers = try await searchFullText(
                terms: terms,
                matchMode: .any,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
        case .phrase:
            matchingIdentifiers = try await searchPhrase(
                configuration: configuration,
                terms: terms,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        guard !matchingIdentifiers.isEmpty else {
            return try FullTextRetainedArray.empty(
                workMeter: workMeter,
                stage: .indexScan
            )
        }

        let statsSubspace = indexSubspace.subspace("stats")
        let totalDocuments = try await statistic(
            key: statsSubspace.pack(Tuple("N")),
            transaction: transaction
        )
        let totalLength = try await statistic(
            key: statsSubspace.pack(Tuple("totalLength")),
            transaction: transaction
        )
        guard totalDocuments > 0, totalLength > 0 else {
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        let scorer = BM25Scorer(
            params: bm25Params,
            statistics: BM25Statistics(
                totalDocuments: totalDocuments,
                totalLength: totalLength
            )
        )

        let documentFrequencySubspace = indexSubspace.subspace("df")
        let frequencyCapacityFootprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<[String: Int64]>.stride)
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(max(1, MemoryLayout<(String, Int64)>.stride))
            ).multiplied(by: UInt64(normalizedTerms.count))
        )
        var frequencies: [String: Int64] = [:]
        let frequencyReservation = try workMeter.reserveIntermediate(
            bytes: frequencyCapacityFootprint.bytes,
            at: .indexScan
        )
        defer { frequencyReservation.release() }
        frequencies.reserveCapacity(normalizedTerms.count)
        for term in normalizedTerms {
            try reserveFullTextFacetEntry(
                term,
                in: frequencyReservation
            )
            frequencies[term] = try await statistic(
                key: documentFrequencySubspace.pack(Tuple(term)),
                transaction: transaction
            )
        }

        let termsSubspace = indexSubspace.subspace("terms")
        let documentsSubspace = indexSubspace.subspace("docs")
        let scoredBuildReservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: matchingIdentifiers.count,
                element: (id: Tuple, score: Double).self
            ).bytes,
            at: .indexScan
        )
        var transferredScoredReservation = false
        defer {
            if !transferredScoredReservation {
                scoredBuildReservation.release()
            }
        }
        var scored: [(id: Tuple, score: Double)] = []
        scored.reserveCapacity(matchingIdentifiers.count)
        for identifier in matchingIdentifiers {
            try workMeter.consume(at: .indexScan)
            let metadataKey = documentsSubspace.pack(identifier)
            guard let metadataValue = try await transaction.getValue(
                for: metadataKey,
                snapshot: true
            ) else {
                throw FullTextStorageError.missingDocumentMetadata
            }
            let metadata = try FullTextStorageDecoder.documentMetadata(
                from: metadataValue
            )
            var termFrequencies: [String: Int] = [:]
            let termFrequencyReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[String: Int]>.stride),
                at: .indexScan
            )
            defer { termFrequencyReservation.release() }
            for term in normalizedTerms {
                let postingKey = termsSubspace.subspace(term).pack(identifier)
                guard let postingValue = try await transaction.getValue(
                    for: postingKey,
                    snapshot: true
                ) else {
                    continue
                }
                try reserveFullTextFacetEntry(
                    term,
                    in: termFrequencyReservation
                )
                termFrequencies[term] = try FullTextStorageDecoder.termFrequency(
                    from: postingValue,
                    positionsStored: configuration.storePositions,
                    term: term
                )
            }
            try scoredBuildReservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(identifier.packedByteCount) + 32,
                at: .indexScan
            )
            scored.append(
                (
                    id: identifier,
                    score: try scorer.score(
                        termFrequencies: termFrequencies,
                        documentFrequencies: frequencies,
                        docLength: try documentLength(metadata.docLength),
                        orderedTerms: normalizedTerms
                    )
                )
            )
        }
        let sortScratch = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(
                        1,
                        MemoryLayout<(id: Tuple, score: Double)>.stride
                    )
                )
            ).multiplied(by: UInt64(scored.count)).bytes,
            at: .indexScan
        )
        defer { sortScratch.release() }
        scored.sort {
            if $0.score == $1.score {
                return stableKey($0.id).lexicographicallyPrecedes(
                    stableKey($1.id)
                )
            }
            return $0.score > $1.score
        }
        if let limit {
            guard limit >= 0 else {
                throw FullTextReadError.invalidParameter(
                    FullTextReadParameter.limit
                )
            }
            let limited = try retainFullTextScoredTuples(
                scored.prefix(limit),
                expectedCount: min(limit, scored.count),
                workMeter: workMeter
            )
            return limited
        }
        let retainedScores = try FullTextRetainedArray.adopting(
            scored,
            reservation: scoredBuildReservation,
            workMeter: workMeter,
            stage: .indexScan
        )
        transferredScoredReservation = true
        return retainedScores
    }

    private func statistic(
        key: ByteString,
        transaction: any TransactionReadAccess
    ) async throws -> Int64 {
        guard let value = try await transaction.getValue(
            for: key,
            snapshot: true
        ) else {
            return 0
        }
        return try ByteConversion.bytesToInt64(value)
    }

    private func containsConsecutivePositions(
        _ positionsByTerm: [[Int]],
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        guard let first = positionsByTerm.first, !first.isEmpty else {
            return false
        }
        for start in first {
            try workMeter.consume(at: .indexScan)
            var matches = true
            for (offset, positions) in positionsByTerm.enumerated() {
                try workMeter.consume(at: .indexScan)
                let (expectedPosition, overflow) = start
                    .addingReportingOverflow(offset)
                guard !overflow else {
                    throw FullTextStorageError.corruptedPosting(
                        term: "phrase"
                    )
                }
                if !positions.contains(expectedPosition) {
                    matches = false
                    break
                }
            }
            if matches {
                return true
            }
        }
        return false
    }

    private func documentLength(_ value: Int64) throws -> Int {
        guard let length = Int(exactly: value) else {
            throw FullTextStorageError.corruptedDocumentMetadata
        }
        return length
    }

    fileprivate func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<Tuple> {
        let termsSubspace = indexSubspace.subspace("terms")
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

        let matchingIDs: FullTextRetainedArray<[any TupleElement]>
        switch matchMode {
        case .all:
            matchingIDs = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
        case .any:
            do {
                var union: FullTextRetainedArray<[any TupleElement]>?

                for group in termGroups {
                    let matches = try await searchTermsAND(
                        group,
                        termsSubspace: termsSubspace,
                        transaction: transaction,
                        workMeter: workMeter
                    )
                    guard !matches.isEmpty else { continue }
                    guard let existingUnion = union else {
                        union = matches
                        continue
                    }

                    let (mergedCapacity, overflow) = existingUnion.count
                        .addingReportingOverflow(matches.count)
                    guard !overflow else {
                        throw FullTextReadError.invalidExecutionPath(
                            "Full-text posting union exceeds the runtime range"
                        )
                    }
                    let mergedReservation = try workMeter.reserveIntermediate(
                        bytes: try DatabaseIntermediateCollectionMeter
                            .arrayFootprint(
                                count: mergedCapacity,
                                element: [any TupleElement].self
                            ).bytes,
                        at: .indexScan
                    )
                    var transferredMergedReservation = false
                    defer {
                        if !transferredMergedReservation {
                            mergedReservation.release()
                        }
                    }
                    let merged = try FullTextPostingListAlgebra.union(
                        existingUnion,
                        matches,
                        reservingCapacity: false
                    ) { elements, key in
                        try reserveFullTextMapEntry(
                            key: key,
                            elements: elements,
                            in: mergedReservation
                        )
                    }
                    let retainedMerged = try FullTextRetainedArray.adopting(
                        merged,
                        reservation: mergedReservation,
                        workMeter: workMeter,
                        stage: .indexScan
                    )
                    transferredMergedReservation = true
                    union = retainedMerged
                }
                matchingIDs = try union ?? FullTextRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
        case .phrase:
            throw FullTextReadError.invalidExecutionPath(
                "Phrase matching must use the position-aware search path"
            )
        }

        return try retainFullTextCandidateTuples(
            matchingIDs,
            workMeter: workMeter
        )
    }

    private func normalizeQueryTermGroups(
        _ terms: [String],
        configuration: FullTextIndexConfiguration
    ) -> [[String]] {
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        return terms.map { term in
            uniqueTerms(normalizer.normalizedTerms(from: term))
        }
        .filter { !$0.isEmpty }
    }

    private func uniqueTerms(_ terms: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []
        result.reserveCapacity(terms.count)
        for term in terms where !seen.contains(term) {
            seen.insert(term)
            result.append(term)
        }
        return result
    }

    private func searchTermsAND(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<[any TupleElement]> {
        guard !terms.isEmpty else {
            return try FullTextRetainedArray.empty(
                workMeter: workMeter,
                stage: .indexScan
            )
        }

        var intersection: FullTextRetainedArray<[any TupleElement]>?

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
            if let existing = intersection {
                let reducedReservation = try workMeter.reserveIntermediate(
                    bytes: try DatabaseIntermediateCollectionMeter
                        .arrayFootprint(
                            count: min(existing.count, results.count),
                            element: [any TupleElement].self
                        ).bytes,
                    at: .indexScan
                )
                var transferredReducedReservation = false
                defer {
                    if !transferredReducedReservation {
                        reducedReservation.release()
                    }
                }
                let reduced = try FullTextPostingListAlgebra.intersection(
                    existing,
                    results,
                    reservingCapacity: false
                ) { elements, key in
                    try reserveFullTextMapEntry(
                        key: key,
                        elements: elements,
                            in: reducedReservation
                        )
                    }
                let retainedReduced = try FullTextRetainedArray.adopting(
                    reduced,
                    reservation: reducedReservation,
                    workMeter: workMeter,
                    stage: .indexScan
                )
                transferredReducedReservation = true
                if reduced.isEmpty {
                    return retainedReduced
                }
                intersection = retainedReduced
            } else {
                intersection = results
            }
        }

        return try intersection ?? FullTextRetainedArray.empty(
            workMeter: workMeter,
            stage: .indexScan
        )
    }

    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> FullTextRetainedArray<[any TupleElement]> {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()
        let scanLimit = try workMeter.storageWorkReadLimitWithSentinel()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: scanLimit,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results = try DatabaseRetainedArrayBuilder<[any TupleElement]>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: [any TupleElement].self
            ),
            expectedCount: min(scanLimit, 256)
        )
        do {
            while let (key, _) = try await cursor.next() {
                guard termSubspace.contains(key) else { break }
                try workMeter.consume(at: .indexScan)
                let keyTuple = try termSubspace.unpack(key)
                let elements = try keyTuple.elements()
                let packedByteCount = keyTuple.packedByteCount
                try results.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(packedByteCount) + 48
                    ),
                    at: .indexScan,
                    make: { elements }
                )
            }
        } catch let cleanupError as StorageRangeCleanupError {
            throw cleanupError
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
        return try results.finish().moveToSharedOwnership(
            at: .indexScan
        )
    }

    private func runtimeInteger(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let result = Int(exactly: value) else {
            throw FullTextReadError.invalidParameter(parameter)
        }
        return result
    }

    private func optionalInteger(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> Int? {
        guard let value = parameters[name] else {
            return nil
        }
        guard let integer = value.int64Value,
              let result = Int(exactly: integer) else {
            throw FullTextReadError.invalidParameter(name)
        }
        return result
    }

    private func facetValues(
        fieldName: String,
        from entity: PolymorphicEntity,
        context: DatabaseContext
    ) throws -> [String] {
        guard let registration = context.container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.typeName),
              let fieldSchema = registration.entity.fieldMapByName[
                fieldName
              ] else {
            throw FullTextReadError.invalidParameter(fieldName)
        }
        return try FullTextFieldValueExtractor.strings(
            from: entity.item,
            entity: entity.typeName,
            field: FieldIdentity(
                name: fieldSchema.name,
                number: fieldSchema.fieldNumber
            )
        )
    }

    private func stableKey(_ tuple: Tuple) -> ByteString {
        tuple.pack()
    }

    private func decodeMatchMode(
        from parameters: [String: FieldValue]
    ) throws -> TextMatchMode {
        let rawValue = try requireString(FullTextReadParameter.matchMode, from: parameters)
        switch rawValue {
        case "all":
            return .all
        case "any":
            return .any
        case "phrase":
            return .phrase
        default:
            throw FullTextReadError.invalidParameter(FullTextReadParameter.matchMode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [String] {
        guard let values = parameters[key]?.arrayValue else {
            throw FullTextReadError.missingParameter(key)
        }

        var strings: [String] = []
        strings.reserveCapacity(values.count)
        for value in values {
            guard let string = value.stringValue else {
                throw FullTextReadError.invalidParameter(key)
            }
            strings.append(string)
        }
        return strings
    }
}
