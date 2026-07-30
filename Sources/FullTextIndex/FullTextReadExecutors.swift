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

    public static func register<Model: Persistable>(
        with definition: inout EntityRuntimeDefinition<Model>
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(FullTextReadExecutor())
    }
}

private enum FullTextReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidResultCount(field: String, count: Int64)
    case invalidExecutionPath(String)
    case duplicateFetchedEntity(ByteString)
    case missingFetchedEntity(ByteString)
}

private struct FullTextReadExecutor: IndexReadExecutor {
    let kindIdentifier = "fulltext"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let limit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        guard let fieldNumber = T.fieldNumber(for: fieldName) else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.fieldName
            )
        }
        var builder = FullTextQueryBuilder<T>(
            queryContext: queryContext,
            field: FieldIdentity(name: fieldName, number: fieldNumber)
        )
            .terms(terms, mode: matchMode)

        if let limit {
            builder = builder.limit(limit)
        }

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = try optionalInteger(
                FullTextReadParameter.facetLimit,
                from: indexScan.parameters
            ) ?? 10
            builder = builder.facets(
                try facetFields.map { name in
                    guard let number = T.fieldNumber(for: name) else {
                        throw FullTextReadError.invalidParameter(name)
                    }
                    return FieldIdentity(name: name, number: number)
                },
                limit: facetLimit
            )
            let result = try await builder.executeFacetedDirect(
                configuration: execution.transactionConfiguration,
                cachePolicy: execution.cachePolicy
            )
            let rows = try result.items.map { try IndexReadRow.materializing($0) }
            return IndexReadResult(
                rows: rows,
                ordering: .orderedByIndex,
                metadata: try facetMetadata(totalCount: result.totalCount, facets: result.facets)
            )
        }

        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            builder = builder.bm25(k1: Float(k1), b: Float(b))
            let results = try await builder.executeScoredDirect(
                configuration: execution.transactionConfiguration,
                cachePolicy: execution.cachePolicy
            )
            let rows = try results.map { result in
                try IndexReadRow.materializing(
                    result.item,
                    annotations: ["score": .float64(result.score)]
                )
            }
            return IndexReadResult(rows: rows, ordering: .orderedByIndex)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let rows = try results.map { try IndexReadRow.materializing($0) }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
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
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let limit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
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

        let descriptor = resolveDescriptor(
            in: group,
            indexName: indexScan.indexName,
            fieldName: fieldName
        )
        guard let descriptor else {
            throw FullTextReadError.invalidParameter(indexScan.indexName)
        }
        let configuration = try FullTextIndexConfiguration(
            metadata: descriptor
        )
        let polySubspace = try await context.container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = polySubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

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
                indexSubspace: indexSubspace,
                execution: execution
            )
            let rows = try result.items.map { entity in
                try IndexReadRow.materializing(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                    ]
                )
            }
            return IndexReadResult(
                rows: rows,
                ordering: .orderedByIndex,
                metadata: try facetMetadata(totalCount: result.totalCount, facets: result.facets)
            )
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
                indexSubspace: indexSubspace,
                execution: execution
            )
            let rows = try results.map { result in
                try IndexReadRow.materializing(
                    result.entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(result.entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(result.entity.typeCode),
                        "score": .float64(result.score)
                    ]
                )
            }
            return IndexReadResult(rows: rows, ordering: .orderedByIndex)
        }

        let results = try await executePlainSearch(
            context: context,
            group: group,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            indexSubspace: indexSubspace,
            execution: execution
        )
        let rows = try results.map { entity in
            try IndexReadRow.materializing(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
        }
        return IndexReadResult(rows: rows, ordering: .orderedByIndex)
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
        indexSubspace: Subspace,
        execution: CanonicalReadExecution
    ) async throws -> [PolymorphicEntity] {
        let matchingIDs = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            if matchMode == .phrase {
                return try await searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        var entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: matchingIDs,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        if let limit, entities.count > limit {
            entities = Array(entities.prefix(limit))
        }
        return entities
    }

    private func executeScoredSearch(
        context: DatabaseContext,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        bm25Params: BM25Parameters,
        indexSubspace: Subspace,
        execution: CanonicalReadExecution
    ) async throws -> [(entity: PolymorphicEntity, score: Double)] {
        let scoredResults = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            return try await searchWithScores(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                bm25Params: bm25Params,
                indexSubspace: indexSubspace,
                transaction: transaction,
                limit: limit
            )
        }

        let entities = try await context.fetchPolymorphicItems(
            group: group,
            ids: scoredResults.map { $0.id },
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        var entityByID: [ByteString: PolymorphicEntity] = [:]
        entityByID.reserveCapacity(entities.count)
        for entity in entities {
            let key = stableKey(entity.polymorphicIdentifier)
            guard entityByID[key] == nil else {
                throw FullTextReadError.duplicateFetchedEntity(key)
            }
            entityByID[key] = entity
        }

        var combined: [(entity: PolymorphicEntity, score: Double)] = []
        combined.reserveCapacity(scoredResults.count)
        for result in scoredResults {
            let key = stableKey(result.id)
            guard let entity = entityByID[key] else {
                throw FullTextReadError.missingFetchedEntity(key)
            }
            combined.append((entity: entity, score: result.score))
        }
        return combined
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
        indexSubspace: Subspace,
        execution: CanonicalReadExecution
    ) async throws -> (items: [PolymorphicEntity], facets: [String: [(value: String, count: Int64)]], totalCount: Int) {
        let matchingIDs = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            if matchMode == .phrase {
                return try await searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        let allEntities = try await context.fetchPolymorphicItems(
            group: group,
            ids: matchingIDs,
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        let totalCount = allEntities.count

        var facets: [String: [(value: String, count: Int64)]] = [:]
        for field in facetFields {
            var counts: [String: Int64] = [:]
            for entity in allEntities {
                let values = try facetValues(
                    fieldName: field,
                    from: entity,
                    context: context
                )
                for value in values where !value.isEmpty {
                    counts[value, default: 0] += 1
                }
            }
            facets[field] = counts
                .map { (value: $0.key, count: $0.value) }
                .sorted {
                    if $0.count == $1.count {
                        return $0.value < $1.value
                    }
                    return $0.count > $1.count
                }
                .prefix(facetLimit)
                .map { $0 }
        }

        let items: [PolymorphicEntity]
        if let limit, allEntities.count > limit {
            items = Array(allEntities.prefix(limit))
        } else {
            items = allEntities
        }
        return (items: items, facets: facets, totalCount: totalCount)
    }

    private func searchPhrase(
        configuration: FullTextIndexConfiguration,
        terms: [String],
        indexSubspace: Subspace,
        transaction: any TransactionAccess
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
            transaction: transaction
        )
        var matches: [Tuple] = []
        matches.reserveCapacity(candidates.count)
        for elements in candidates {
            let identifier = Tuple(elements)
            var positionsByTerm: [[Int]] = []
            positionsByTerm.reserveCapacity(normalizedTerms.count)
            for term in normalizedTerms {
                let key = termsSubspace.subspace(term).pack(identifier)
                guard let value = try await transaction.getValue(
                    for: key,
                    snapshot: true
                ) else {
                    positionsByTerm.append([])
                    continue
                }
                positionsByTerm.append(
                    try FullTextStorageDecoder.posting(
                        from: value,
                        positionsStored: true,
                        term: term
                    ).positions
                )
            }
            if containsConsecutivePositions(positionsByTerm) {
                matches.append(identifier)
            }
        }
        return matches
    }

    private func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        bm25Params: BM25Parameters,
        indexSubspace: Subspace,
        transaction: any TransactionAccess,
        limit: Int?
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
                transaction: transaction
            )
        case .any:
            matchingIdentifiers = try await searchFullText(
                terms: terms,
                matchMode: .any,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        case .phrase:
            matchingIdentifiers = try await searchPhrase(
                configuration: configuration,
                terms: terms,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }
        guard !matchingIdentifiers.isEmpty else { return [] }

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
        for term in normalizedTerms {
            frequencies[term] = try await statistic(
                key: documentFrequencySubspace.pack(Tuple(term)),
                transaction: transaction
            )
        }

        let termsSubspace = indexSubspace.subspace("terms")
        let documentsSubspace = indexSubspace.subspace("docs")
        var scored: [(id: Tuple, score: Double)] = []
        scored.reserveCapacity(matchingIdentifiers.count)
        for identifier in matchingIdentifiers {
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
            for term in normalizedTerms {
                let postingKey = termsSubspace.subspace(term).pack(identifier)
                guard let postingValue = try await transaction.getValue(
                    for: postingKey,
                    snapshot: true
                ) else {
                    continue
                }
                termFrequencies[term] = try FullTextStorageDecoder.posting(
                    from: postingValue,
                    positionsStored: configuration.storePositions,
                    term: term
                ).termFrequency
            }
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
            return Array(scored.prefix(limit))
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
        _ positionsByTerm: [[Int]]
    ) -> Bool {
        guard let first = positionsByTerm.first, !first.isEmpty else {
            return false
        }
        for start in first {
            var matches = true
            for (offset, positions) in positionsByTerm.enumerated()
            where !positions.contains(start + offset) {
                matches = false
                break
            }
            if matches {
                return true
            }
        }
        return false
    }

    private func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let termsSubspace = indexSubspace.subspace("terms")
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

        let matchingIDs: [[any TupleElement]]
        switch matchMode {
        case .all:
            matchingIDs = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        case .any:
            var idToElements: [ByteString: [any TupleElement]] = [:]
            for group in termGroups {
                let matches = try await searchTermsAND(
                    group,
                    termsSubspace: termsSubspace,
                    transaction: transaction
                )
                for elements in matches {
                    idToElements[stableKey(Tuple(elements))] = elements
                }
            }
            matchingIDs = Array(idToElements.values)
        case .phrase:
            throw FullTextReadError.invalidExecutionPath(
                "Phrase matching must use the position-aware search path"
            )
        }

        return matchingIDs.map(Tuple.init)
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
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<ByteString>? = nil
        var idToElements: [ByteString: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            var currentSet: Set<ByteString> = []

            for elements in results {
                let idKey = stableKey(Tuple(elements))
                currentSet.insert(idKey)
                if intersection == nil || intersection?.contains(idKey) == true {
                    idToElements[idKey] = elements
                }
            }

            if let existing = intersection {
                let reduced = existing.intersection(currentSet)
                if reduced.isEmpty {
                    return []
                }
                intersection = reduced
            } else {
                intersection = currentSet
            }
        }

        guard let intersection else { return [] }
        return intersection.compactMap { idToElements[$0] }
    }

    private func searchTermsOR(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [ByteString: [any TupleElement]] = [:]
        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            for elements in results {
                idToElements[stableKey(Tuple(elements))] = elements
            }
        }
        return Array(idToElements.values)
    }

    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [[any TupleElement]] = []
        for (key, _) in sequence {
            guard termSubspace.contains(key) else { break }
            let keyTuple = try termSubspace.unpack(key)
            let elements = try keyTuple.elements()
            results.append(elements)
        }
        return results
    }

    private func resolveDescriptor(
        in group: PolymorphicGroup,
        indexName: String,
        fieldName: String
    ) -> PolymorphicIndexMetadata? {
        if let descriptor = group.indexes.first(where: { $0.name == indexName }) {
            return descriptor
        }
        return group.indexes.first(where: {
            $0.kindIdentifier == kindIdentifier && $0.fieldNames.contains(fieldName)
        })
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
