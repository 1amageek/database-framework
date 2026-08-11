import DatabaseEngine
import DatabaseTypes
import DatabaseKit
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

private func reserveFullTextCandidates(
    _ candidates: [[any TupleElement]],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[[any TupleElement]]>.stride)
    )
    for elements in candidates {
        let packed = Tuple(elements).pack()
        let elementStorage = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<any TupleElement>.stride + 16)
            )
        ).multiplied(by: UInt64(elements.count))
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(packed.count) + 64
            ).adding(elementStorage)
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextMapValues(
    _ valuesByKey: [ByteString: [any TupleElement]],
    keys: some Sequence<ByteString>,
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[[any TupleElement]]>.stride)
    )
    for key in keys {
        guard let elements = valuesByKey[key] else { continue }
        let elementStorage = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<any TupleElement>.stride + 16)
            )
        ).multiplied(by: UInt64(elements.count))
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(key.count) + 64
            ).adding(elementStorage)
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextKeyArray(
    count: Int,
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    let slots = try DatabaseIntermediateFootprint(
        bytes: UInt64(max(1, MemoryLayout<ByteString>.stride + 16))
    ).multiplied(by: UInt64(count))
    return try workMeter.reserveIntermediate(
        rows: UInt64(count),
        bytes: slots.bytes,
        at: .indexScan
    )
}

private func reserveFullTextTuples(
    _ tuples: [Tuple],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[Tuple]>.stride)
    )
    for tuple in tuples {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(tuple.pack().count) + 64
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextScoredTuples(
    _ tuples: [(id: Tuple, score: Double)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[(id: Tuple, score: Double)]>.stride)
    )
    for tuple in tuples {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(tuple.id.pack().count) + 80
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextModels(
    _ models: [PersistedModel],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[PersistedModel]>.stride)
    )
    for model in models {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.footprint(
                of: try QueryRowCodec.encode(model),
                workMeter: workMeter
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextEntities(
    _ entities: [PolymorphicEntity],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[PolymorphicEntity]>.stride)
    )
    for entity in entities {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.footprint(
                of: try QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName:
                            .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode:
                            .int64(entity.typeCode)
                    ]
                ),
                workMeter: workMeter
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: footprint.rows,
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextScoredModels(
    _ results: [(item: PersistedModel, score: Double)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: results.count,
        element: (item: PersistedModel, score: Double).self
    )
    for result in results {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.footprint(
                of: try QueryRowCodec.encode(result.item),
                workMeter: workMeter
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: UInt64(results.count),
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextScoredEntities(
    _ results: [(entity: PolymorphicEntity, score: Double)],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: results.count,
        element: (entity: PolymorphicEntity, score: Double).self
    )
    for result in results {
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.footprint(
                of: try QueryRowCodec.encode(result.entity.item),
                workMeter: workMeter
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(result.entity.typeName.utf8.count)
            )
        )
    }
    return try workMeter.reserveIntermediate(
        rows: UInt64(results.count),
        bytes: footprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextFacets(
    _ facets: [String: [(value: String, count: Int64)]],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(
            MemoryLayout<[String: [(value: String, count: Int64)]]>.stride
        )
    )
    for (field, buckets) in facets {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(field.utf8.count) + 96
            )
        ).adding(
            try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: buckets.count,
                element: (value: String, count: Int64).self
            )
        )
        for bucket in buckets {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(bucket.value.utf8.count) + 32
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

private func reserveFullTextArrayCopy<Element>(
    count: Int,
    element: Element.Type,
    workMeter: DatabaseWorkMeter
) throws -> DatabaseIntermediateReservation {
    let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
        count: count,
        element: element
    )
    return try workMeter.reserveIntermediate(
        rows: UInt64(count),
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

private func reserveFullTextKey(
    _ key: ByteString,
    in reservation: DatabaseIntermediateReservation
) throws {
    try reservation.reserveAdditional(
        rows: 1,
        bytes: UInt64(key.count) + 64,
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

private struct FullTextReadExecutor: IndexReadExecutor {
    let kindIdentifier = "fulltext"

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
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        if limit == 0, !includeFacets {
            return .empty
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        guard index.kindIdentifier == kindIdentifier,
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
        let configuration = try FullTextIndexConfiguration(metadata: index.kind)

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
                execution: execution,
                workMeter: options.workMeter
            )
            let itemReservation = try reserveFullTextModels(
                result.items,
                workMeter: options.workMeter
            )
            defer { itemReservation.release() }
            let facetReservation = try reserveFullTextFacets(
                result.facets,
                workMeter: options.workMeter
            )
            defer { facetReservation.release() }
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
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            let results = try await executeScoredSearch(
                context: context,
                entity: entity,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Parameters: BM25Parameters(
                    k1: Float(k1),
                    b: Float(b)
                ),
                index: index,
                partitions: partitions,
                execution: execution,
                workMeter: options.workMeter
            )
            let resultReservation = try reserveFullTextScoredModels(
                results,
                workMeter: options.workMeter
            )
            defer { resultReservation.release() }
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
            execution: execution,
            workMeter: options.workMeter
        )
        let resultReservation = try reserveFullTextModels(
            results,
            workMeter: options.workMeter
        )
        defer { resultReservation.release() }
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
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> [PersistedModel] {
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            kindIdentifier: kindIdentifier,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else { return [] }
            let identifiers: [Tuple]
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
            let identifierReservation = try reserveFullTextTuples(
                identifiers,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let limited = try limitIdentifiers(identifiers, limit: limit)
            let limitedReservation = limited.count == identifiers.count
                ? nil
                : try reserveFullTextTuples(
                    limited,
                    workMeter: workMeter
                )
            defer { limitedReservation?.release() }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: limited,
                partitions: partitions,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePersistedModels(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let modelArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PersistedModel.self,
                workMeter: workMeter
            )
            defer { modelArrayAdmission.release() }
            return try requireModels(
                identifiers: limited,
                fetched: fetched
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
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> [(item: PersistedModel, score: Double)] {
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            kindIdentifier: kindIdentifier,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else { return [] }
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
            let scoredReservation = try reserveFullTextScoredTuples(
                scored,
                workMeter: workMeter
            )
            defer { scoredReservation.release() }
            let identifiers = scored.map { $0.id }
            let identifierReservation = try reserveFullTextTuples(
                identifiers,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePersistedModels(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let modelArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PersistedModel.self,
                workMeter: workMeter
            )
            defer { modelArrayAdmission.release() }
            let models = try requireModels(
                identifiers: identifiers,
                fetched: fetched
            )
            let modelReservation = try reserveFullTextModels(
                models,
                workMeter: workMeter
            )
            defer { modelReservation.release() }
            let combinedReservation = try reserveFullTextArrayCopy(
                count: models.count,
                element: (item: PersistedModel, score: Double).self,
                workMeter: workMeter
            )
            defer { combinedReservation.release() }
            var combined: [(item: PersistedModel, score: Double)] = []
            combined.reserveCapacity(models.count)
            for (model, score) in zip(models, scored) {
                combined.append((item: model, score: score.score))
            }
            return combined
        }
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
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> (
        items: [PersistedModel],
        facets: [String: [(value: String, count: Int64)]],
        totalCount: Int
    ) {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        let search = PolymorphicFullTextReadExecutor()
        return try await context.indexQueryContext.withReadableIndex(
            named: index.name,
            kindIdentifier: kindIdentifier,
            forEntityName: entity.name,
            partitions: partitions,
            configuration: execution.transactionConfiguration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return (items: [], facets: [:], totalCount: 0)
            }
            let identifiers: [Tuple]
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
            let identifierReservation = try reserveFullTextTuples(
                identifiers,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePersistedModels(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let modelArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PersistedModel.self,
                workMeter: workMeter
            )
            defer { modelArrayAdmission.release() }
            let allModels = try requireModels(
                identifiers: identifiers,
                fetched: fetched
            )
            let modelReservation = try reserveFullTextModels(
                allModels,
                workMeter: workMeter
            )
            defer { modelReservation.release() }
            var facets: [String: [(value: String, count: Int64)]] = [:]
            let facetOutputReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[
                        String: [(value: String, count: Int64)]
                    ]>.stride
                ),
                at: .indexScan
            )
            defer { facetOutputReservation.release() }
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
            let visibleCount = min(limit ?? allModels.count, allModels.count)
            let itemCopyReservation = try reserveFullTextArrayCopy(
                count: visibleCount,
                element: PersistedModel.self,
                workMeter: workMeter
            )
            defer { itemCopyReservation.release() }
            return (
                items: try limitIdentifiers(allModels, limit: limit),
                facets: facets,
                totalCount: allModels.count
            )
        }
    }

    private func requireModels(
        identifiers: [Tuple],
        fetched: [PersistedModel?]
    ) throws -> [PersistedModel] {
        guard identifiers.count == fetched.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: fetched.count
            )
        }
        var models: [PersistedModel] = []
        models.reserveCapacity(fetched.count)
        for (identifier, model) in zip(identifiers, fetched) {
            guard let model else {
                throw FullTextReadError.missingFetchedEntity(
                    identifier.pack()
                )
            }
            models.append(model)
        }
        return models
    }

    private func limitIdentifiers<Value>(
        _ values: [Value],
        limit: Int?
    ) throws -> [Value] {
        guard let limit else { return values }
        guard limit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        return values.count > limit ? Array(values.prefix(limit)) : values
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
    let kindIdentifier = "fulltext"

    func executeRows(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        index: PolymorphicIndexMetadata,
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
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        if limit == 0, !includeFacets {
            return .empty
        }

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

        guard index.kindIdentifier == kindIdentifier,
              index.fieldNames.contains(fieldName) else {
            throw FullTextReadError.invalidParameter(index.name)
        }
        let configuration = try FullTextIndexConfiguration(
            metadata: index
        )

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
            let itemReservation = try reserveFullTextEntities(
                result.items,
                workMeter: options.workMeter
            )
            defer { itemReservation.release() }
            let facetReservation = try reserveFullTextFacets(
                result.facets,
                workMeter: options.workMeter
            )
            defer { facetReservation.release() }
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
                                    .int64(entity.typeCode)
                            ]
                        )
                    )
                }
            }
        }

        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            let results = try await executeScoredSearch(
                context: context,
                group: group,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Params: BM25Parameters(k1: Float(k1), b: Float(b)),
                index: index,
                execution: execution,
                workMeter: options.workMeter
            )
            let resultReservation = try reserveFullTextScoredEntities(
                results,
                workMeter: options.workMeter
            )
            defer { resultReservation.release() }
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
                                "score": .float64(result.score)
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
        let resultReservation = try reserveFullTextEntities(
            results,
            workMeter: options.workMeter
        )
        defer { resultReservation.release() }
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
                                .int64(entity.typeCode)
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
        index: PolymorphicIndexMetadata,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> [PolymorphicEntity] {
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction -> [PolymorphicEntity] in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return []
            }
            let matchingIDs: [Tuple]
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
            let identifierReservation = try reserveFullTextTuples(
                matchingIDs,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: matchingIDs,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePolymorphicEntities(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let entityArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PolymorphicEntity.self,
                workMeter: workMeter
            )
            defer { entityArrayAdmission.release() }
            let entities = try requireEntities(
                identifiers: matchingIDs,
                fetched: fetched
            )
            let entityReservation = try reserveFullTextEntities(
                entities,
                workMeter: workMeter
            )
            defer { entityReservation.release() }
            if let limit, entities.count > limit {
                let limitedReservation = try reserveFullTextArrayCopy(
                    count: limit,
                    element: PolymorphicEntity.self,
                    workMeter: workMeter
                )
                defer { limitedReservation.release() }
                return Array(entities.prefix(limit))
            }
            return entities
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
        index: PolymorphicIndexMetadata,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> [(entity: PolymorphicEntity, score: Double)] {
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction -> [(entity: PolymorphicEntity, score: Double)] in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return []
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
            let scoredReservation = try reserveFullTextScoredTuples(
                scoredResults,
                workMeter: workMeter
            )
            defer { scoredReservation.release() }
            let identifiers = scoredResults.map { $0.id }
            let identifierReservation = try reserveFullTextTuples(
                identifiers,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: identifiers,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePolymorphicEntities(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let entityArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PolymorphicEntity.self,
                workMeter: workMeter
            )
            defer { entityArrayAdmission.release() }
            let entities = try requireEntities(
                identifiers: identifiers,
                fetched: fetched
            )
            let entityReservation = try reserveFullTextEntities(
                entities,
                workMeter: workMeter
            )
            defer { entityReservation.release() }

            let combinedReservation = try reserveFullTextArrayCopy(
                count: scoredResults.count,
                element: (entity: PolymorphicEntity, score: Double).self,
                workMeter: workMeter
            )
            defer { combinedReservation.release() }
            var combined: [(entity: PolymorphicEntity, score: Double)] = []
            combined.reserveCapacity(scoredResults.count)
            for (result, entity) in zip(scoredResults, entities) {
                combined.append((entity: entity, score: result.score))
            }
            return combined
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
        index: PolymorphicIndexMetadata,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> (items: [PolymorphicEntity], facets: [String: [(value: String, count: Int64)]], totalCount: Int) {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        return try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) {
            transaction -> (
                items: [PolymorphicEntity],
                facets: [String: [(value: String, count: Int64)]],
                totalCount: Int
            ) in
            guard let readableIndex = try await context.container
                .readablePolymorphicIndex(
                    index,
                    in: group,
                    transaction: transaction
                ) else {
                return (items: [], facets: [:], totalCount: 0)
            }
            let matchingIDs: [Tuple]
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
            let identifierReservation = try reserveFullTextTuples(
                matchingIDs,
                workMeter: workMeter
            )
            defer { identifierReservation.release() }
            let fetched = try await context.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: matchingIDs,
                transaction: transaction,
                workMeter: workMeter
            )
            let fetchedReservation = try DatabaseIntermediateCollectionMeter
                .reservePolymorphicEntities(
                    fetched,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            defer { fetchedReservation.release() }
            let entityArrayAdmission = try reserveFullTextArrayCopy(
                count: fetched.count,
                element: PolymorphicEntity.self,
                workMeter: workMeter
            )
            defer { entityArrayAdmission.release() }
            let allEntities = try requireEntities(
                identifiers: matchingIDs,
                fetched: fetched
            )
            let entityReservation = try reserveFullTextEntities(
                allEntities,
                workMeter: workMeter
            )
            defer { entityReservation.release() }
            let totalCount = allEntities.count

            var facets: [String: [(value: String, count: Int64)]] = [:]
            let facetOutputReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[
                        String: [(value: String, count: Int64)]
                    ]>.stride
                ),
                at: .indexScan
            )
            defer { facetOutputReservation.release() }
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

            let items: [PolymorphicEntity]
            if let limit, allEntities.count > limit {
                let itemCopyReservation = try reserveFullTextArrayCopy(
                    count: limit,
                    element: PolymorphicEntity.self,
                    workMeter: workMeter
                )
                defer { itemCopyReservation.release() }
                items = Array(allEntities.prefix(limit))
            } else {
                items = allEntities
            }
            return (items: items, facets: facets, totalCount: totalCount)
        }
    }

    private func requireEntities(
        identifiers: [Tuple],
        fetched: [PolymorphicEntity?]
    ) throws -> [PolymorphicEntity] {
        guard identifiers.count == fetched.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: fetched.count
            )
        }
        var entities: [PolymorphicEntity] = []
        entities.reserveCapacity(fetched.count)
        for (identifier, entity) in zip(identifiers, fetched) {
            guard let entity else {
                throw FullTextReadError.missingFetchedEntity(
                    identifier.pack()
                )
            }
            entities.append(entity)
        }
        return entities
    }

    fileprivate func searchPhrase(
        configuration: FullTextIndexConfiguration,
        terms: [String],
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [Tuple] {
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
        guard !normalizedTerms.isEmpty else { return [] }

        let termsSubspace = indexSubspace.subspace("terms")
        let candidates = try await searchTermsAND(
            normalizedTerms,
            termsSubspace: termsSubspace,
            transaction: transaction,
            workMeter: workMeter
        )
        let candidateReservation = try reserveFullTextCandidates(
            candidates,
            workMeter: workMeter
        )
        defer { candidateReservation.release() }
        var matches: [Tuple] = []
        matches.reserveCapacity(candidates.count)
        for elements in candidates {
            try workMeter.consume(at: .indexScan)
            let identifier = Tuple(elements)
            var positionsByTerm: [[Int]] = []
            positionsByTerm.reserveCapacity(normalizedTerms.count)
            let positionReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[[Int]]>.stride),
                at: .indexScan
            )
            defer { positionReservation.release() }
            for term in normalizedTerms {
                try workMeter.consume(at: .indexScan)
                let key = termsSubspace.subspace(term).pack(identifier)
                guard let value = try await transaction.getValue(
                    for: key,
                    snapshot: true
                ) else {
                    try positionReservation.reserveAdditional(
                        rows: 1,
                        bytes: UInt64(MemoryLayout<[Int]>.stride) + 16,
                        at: .indexScan
                    )
                    positionsByTerm.append([])
                    continue
                }
                let positions = try FullTextStorageDecoder.posting(
                    from: value,
                    positionsStored: true,
                    term: term
                ).positions
                try positionReservation.reserveAdditional(
                    rows: 1,
                    bytes: try DatabaseIntermediateFootprint(
                        bytes: UInt64(max(1, MemoryLayout<Int>.stride))
                    ).multiplied(by: UInt64(positions.count)).adding(
                        DatabaseIntermediateFootprint(bytes: 32)
                    ).bytes,
                    at: .indexScan
                )
                positionsByTerm.append(positions)
            }
            if try containsConsecutivePositions(
                positionsByTerm,
                workMeter: workMeter
            ) {
                matches.append(identifier)
            }
        }
        let matchReservation = try reserveFullTextTuples(
            matches,
            workMeter: workMeter
        )
        defer { matchReservation.release() }
        return matches
    }

    fileprivate func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        bm25Params: BM25Parameters,
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        limit: Int?,
        workMeter: DatabaseWorkMeter
    ) async throws -> [(id: Tuple, score: Double)] {
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })
        let matchingIdentifiers: [Tuple]
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
        guard !matchingIdentifiers.isEmpty else { return [] }
        let matchingReservation = try reserveFullTextTuples(
            matchingIdentifiers,
            workMeter: workMeter
        )
        defer { matchingReservation.release() }

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
        var frequencies: [String: Int64] = [:]
        frequencies.reserveCapacity(normalizedTerms.count)
        let frequencyReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
            at: .indexScan
        )
        defer { frequencyReservation.release() }
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
        var scored: [(id: Tuple, score: Double)] = []
        scored.reserveCapacity(matchingIdentifiers.count)
        let scoredBuildReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[(id: Tuple, score: Double)]>.stride
            ),
            at: .indexScan
        )
        defer { scoredBuildReservation.release() }
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
                termFrequencies[term] = try FullTextStorageDecoder.posting(
                    from: postingValue,
                    positionsStored: configuration.storePositions,
                    term: term
                ).termFrequency
            }
            try scoredBuildReservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(identifier.pack().count) + 80,
                at: .indexScan
            )
            scored.append(
                (
                    id: identifier,
                    score: scorer.score(
                        termFrequencies: termFrequencies,
                        documentFrequencies: frequencies,
                        docLength: Int(metadata.docLength)
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
            let limited = Array(scored.prefix(limit))
            let limitedReservation = try reserveFullTextScoredTuples(
                limited,
                workMeter: workMeter
            )
            defer { limitedReservation.release() }
            return limited
        }
        return scored
    }

    private func statistic(
        key: ByteString,
        transaction: any TransactionAccess
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
                if !positions.contains(start + offset) {
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

    fileprivate func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [Tuple] {
        let termsSubspace = indexSubspace.subspace("terms")
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

        let matchingIDs: [[any TupleElement]]
        var matchingAdmission: DatabaseIntermediateReservation?
        switch matchMode {
        case .all:
            matchingIDs = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
        case .any:
            var idToElements: [ByteString: [any TupleElement]] = [:]
            let unionReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[ByteString: [any TupleElement]]>.stride
                ),
                at: .indexScan
            )
            defer { unionReservation.release() }
            for group in termGroups {
                let matches = try await searchTermsAND(
                    group,
                    termsSubspace: termsSubspace,
                    transaction: transaction,
                    workMeter: workMeter
                )
                let matchReservation = try reserveFullTextCandidates(
                    matches,
                    workMeter: workMeter
                )
                defer { matchReservation.release() }
                for elements in matches {
                    let key = stableKey(Tuple(elements))
                    if idToElements[key] == nil {
                        try reserveFullTextMapEntry(
                            key: key,
                            elements: elements,
                            in: unionReservation
                        )
                    }
                    idToElements[key] = elements
                }
            }
            let keyAdmission = try reserveFullTextKeyArray(
                count: idToElements.count,
                workMeter: workMeter
            )
            defer { keyAdmission.release() }
            let orderedKeys = idToElements.keys.sorted { lhs, rhs in
                lhs.lexicographicallyPrecedes(rhs)
            }
            matchingAdmission = try reserveFullTextMapValues(
                idToElements,
                keys: orderedKeys,
                workMeter: workMeter
            )
            matchingIDs = orderedKeys.compactMap { idToElements[$0] }
        case .phrase:
            throw FullTextReadError.invalidExecutionPath(
                "Phrase matching must use the position-aware search path"
            )
        }

        let matchingReservation = try matchingAdmission
            ?? reserveFullTextCandidates(
                matchingIDs,
                workMeter: workMeter
            )
        defer { matchingReservation.release() }
        let tuples = matchingIDs.map(Tuple.init)
        let tupleReservation = try reserveFullTextTuples(
            tuples,
            workMeter: workMeter
        )
        defer { tupleReservation.release() }
        return tuples
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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<ByteString>? = nil
        var intersectionReservation: DatabaseIntermediateReservation?
        var idToElements: [ByteString: [any TupleElement]] = [:]
        let mapReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[ByteString: [any TupleElement]]>.stride
            ),
            at: .indexScan
        )
        defer { mapReservation.release() }

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
            let resultReservation = try reserveFullTextCandidates(
                results,
                workMeter: workMeter
            )
            defer { resultReservation.release() }
            var currentSet: Set<ByteString> = []
            let currentReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<Set<ByteString>>.stride),
                at: .indexScan
            )

            for elements in results {
                let idKey = stableKey(Tuple(elements))
                if !currentSet.contains(idKey) {
                    try reserveFullTextKey(
                        idKey,
                        in: currentReservation
                    )
                    currentSet.insert(idKey)
                }
                if idToElements[idKey] == nil {
                    try reserveFullTextMapEntry(
                        key: idKey,
                        elements: elements,
                        in: mapReservation
                    )
                    idToElements[idKey] = elements
                }
            }

            if let existing = intersection {
                var reduced: Set<ByteString> = []
                let reducedReservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(MemoryLayout<Set<ByteString>>.stride),
                    at: .indexScan
                )
                for key in existing where currentSet.contains(key) {
                    try reserveFullTextKey(
                        key,
                        in: reducedReservation
                    )
                    reduced.insert(key)
                }
                if reduced.isEmpty {
                    return []
                }
                intersection = reduced
                intersectionReservation = reducedReservation
            } else {
                intersection = currentSet
                intersectionReservation = currentReservation
            }
        }

        guard let intersection else { return [] }
        _ = intersectionReservation
        let keyReservation = try reserveFullTextKeyArray(
            count: intersection.count,
            workMeter: workMeter
        )
        defer { keyReservation.release() }
        let orderedKeys = intersection.sorted { lhs, rhs in
            lhs.lexicographicallyPrecedes(rhs)
        }
        let outputReservation = try reserveFullTextMapValues(
            idToElements,
            keys: orderedKeys,
            workMeter: workMeter
        )
        defer { outputReservation.release() }
        let output = orderedKeys.compactMap { idToElements[$0] }
        return output
    }

    private func searchTermsOR(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [ByteString: [any TupleElement]] = [:]
        let unionReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[ByteString: [any TupleElement]]>.stride
            ),
            at: .indexScan
        )
        defer { unionReservation.release() }
        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter
            )
            let resultReservation = try reserveFullTextCandidates(
                results,
                workMeter: workMeter
            )
            defer { resultReservation.release() }
            for elements in results {
                let key = stableKey(Tuple(elements))
                if idToElements[key] == nil {
                    try reserveFullTextMapEntry(
                        key: key,
                        elements: elements,
                        in: unionReservation
                    )
                }
                idToElements[key] = elements
            }
        }
        let keyReservation = try reserveFullTextKeyArray(
            count: idToElements.count,
            workMeter: workMeter
        )
        defer { keyReservation.release() }
        let orderedKeys = idToElements.keys.sorted { lhs, rhs in
            lhs.lexicographicallyPrecedes(rhs)
        }
        let outputReservation = try reserveFullTextMapValues(
            idToElements,
            keys: orderedKeys,
            workMeter: workMeter
        )
        defer { outputReservation.release() }
        let output = orderedKeys.compactMap { idToElements[$0] }
        return output
    }

    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()
        let scanLimit = try workMeter.storageReadLimitWithSentinel()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: scanLimit,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [[any TupleElement]] = []
        let resultReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[[any TupleElement]]>.stride),
            at: .indexScan
        )
        defer { resultReservation.release() }
        do {
            while let (key, _) = try await cursor.next() {
                guard termSubspace.contains(key) else { break }
                try workMeter.consume(at: .indexScan)
                let keyTuple = try termSubspace.unpack(key)
                let elements = try keyTuple.elements()
                try reserveFullTextMapEntry(
                    key: stableKey(keyTuple),
                    elements: elements,
                    in: resultReservation
                )
                results.append(elements)
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
