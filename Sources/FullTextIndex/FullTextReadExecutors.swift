#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseEngine
import DatabaseValue
import Core
import QueryIR
import FullText
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
    public static var indexExecutor: any IndexReadExecutor { FullTextReadExecutor() }
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicFullTextReadExecutor()
    }
}

private enum FullTextReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidResultCount(field: String, count: Int64)
}

private struct FullTextReadExecutor: IndexReadExecutor {
    let kindIdentifier = "fulltext"

    func executeRows<T: Persistable>(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let limit = indexScan.parameters[FullTextReadParameter.limit].flatMap(\.int64Value).map(Int.init)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitions(partitions, for: T.self)
        var builder = FullTextQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )
            .terms(terms, mode: matchMode)

        if let limit {
            builder = builder.limit(limit)
        }

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = indexScan.parameters[FullTextReadParameter.facetLimit].flatMap(\.int64Value).map(Int.init) ?? 10
            builder = builder.facets(facetFields, limit: facetLimit)
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
            let k1 = indexScan.parameters[FullTextReadParameter.bm25K1]?.doubleValue ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[FullTextReadParameter.bm25B]?.doubleValue ?? Double(BM25Parameters.default.b)
            builder = builder.bm25(k1: Float(k1), b: Float(b))
            let results = try await builder.executeScoredDirect(
                configuration: execution.transactionConfiguration,
                cachePolicy: execution.cachePolicy
            )
            let rows = try results.map { result in
                try IndexReadRow.materializing(
                    result.item,
                    annotations: ["score": .double(result.score)]
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
    ) throws -> [String: DatabaseValue] {
        guard let totalCount = UInt64(exactly: totalCount) else {
            throw FullTextReadError.invalidResultCount(
                field: FullTextReadParameter.totalCount,
                count: Int64(totalCount)
            )
        }
        var metadata: [String: DatabaseValue] = [
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
        from parameters: [String: QueryParameterValue]
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
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
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

private struct PolymorphicFullTextPlaceholder: Persistable {
    typealias ID = String

    var id: String = ""

    static var persistableType: String { "_PolymorphicFullTextPlaceholder" }
    static var allFields: [String] { ["id"] }

    static func fieldNumber(for fieldName: String) -> Int? {
        fieldName == "id" ? 1 : nil
    }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<PolymorphicFullTextPlaceholder, Value>) -> String {
        if keyPath == \PolymorphicFullTextPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: PartialKeyPath<PolymorphicFullTextPlaceholder>) -> String {
        if keyPath == \PolymorphicFullTextPlaceholder.id { return "id" }
        return "\(keyPath)"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PolymorphicFullTextPlaceholder> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
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
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let limit = indexScan.parameters[FullTextReadParameter.limit].flatMap(\.int64Value).map(Int.init)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = try selectQuery.requiredOrderByColumnNames()
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: selectQuery.limit,
            offset: selectQuery.offset,
            orderBy: orderByFields
        )

        let descriptor = resolveDescriptor(
            in: group,
            indexName: indexScan.indexName,
            fieldName: fieldName
        )
        let kind = try makeKind(
            fieldName: fieldName,
            descriptor: descriptor
        )
        let polySubspace = try await context.container.resolvePolymorphicDirectory(for: group.identifier)
        let indexSubspace = polySubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = indexScan.parameters[FullTextReadParameter.facetLimit].flatMap(\.int64Value).map(Int.init) ?? 10
            let result = try await executeFacetedSearch(
                context: context,
                group: group,
                kind: kind,
                indexName: indexScan.indexName,
                fieldName: fieldName,
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
                    any: entity.item,
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
            let k1 = indexScan.parameters[FullTextReadParameter.bm25K1]?.doubleValue ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[FullTextReadParameter.bm25B]?.doubleValue ?? Double(BM25Parameters.default.b)
            let results = try await executeScoredSearch(
                context: context,
                group: group,
                kind: kind,
                indexName: indexScan.indexName,
                fieldName: fieldName,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Params: BM25Parameters(k1: Float(k1), b: Float(b)),
                indexSubspace: indexSubspace,
                execution: execution
            )
            let rows = try results.map { result in
                try IndexReadRow.materializing(
                    any: result.entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(result.entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(result.entity.typeCode),
                        "score": .double(result.score)
                    ]
                )
            }
            return IndexReadResult(rows: rows, ordering: .orderedByIndex)
        }

        let results = try await executePlainSearch(
            context: context,
            group: group,
            kind: kind,
            indexName: indexScan.indexName,
            fieldName: fieldName,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            indexSubspace: indexSubspace,
            execution: execution
        )
        let rows = try results.map { entity in
            try IndexReadRow.materializing(
                any: entity.item,
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
    ) throws -> [String: DatabaseValue] {
        guard let totalCount = UInt64(exactly: totalCount) else {
            throw FullTextReadError.invalidResultCount(
                field: FullTextReadParameter.totalCount,
                count: Int64(totalCount)
            )
        }
        var metadata: [String: DatabaseValue] = [
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
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>,
        indexName: String,
        fieldName: String,
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
                    kind: kind,
                    indexName: indexName,
                    fieldName: fieldName,
                    terms: terms,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                kind: kind,
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
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>,
        indexName: String,
        fieldName: String,
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
            let index = Index(
                name: indexName,
                kind: kind,
                rootExpression: FieldKeyExpression(fieldName: fieldName)
            )
            let maintainer = FullTextIndexMaintainer<PolymorphicFullTextPlaceholder>(
                index: index,
                tokenizer: kind.tokenizer,
                storePositions: kind.storePositions,
                ngramSize: kind.ngramSize,
                minTermLength: kind.minTermLength,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )
            return try await maintainer.searchWithScores(
                terms: terms,
                matchMode: matchMode,
                bm25Params: bm25Params,
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
        var entityByID: [String: PolymorphicEntity] = [:]
        entityByID.reserveCapacity(entities.count)
        for entity in entities {
            let identifier = try entity.item.persistableIdentifierTuple()
            let key = stableKey(
                Tuple(entity.typeCode).appending(identifier)
            )
            entityByID[key] = entity
        }

        var combined: [(entity: PolymorphicEntity, score: Double)] = []
        combined.reserveCapacity(scoredResults.count)
        for result in scoredResults {
            let key = stableKey(result.id)
            guard let entity = entityByID[key] else {
                continue
            }
            combined.append((entity: entity, score: result.score))
        }
        return combined
    }

    private func executeFacetedSearch(
        context: DatabaseContext,
        group: PolymorphicGroup,
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>,
        indexName: String,
        fieldName: String,
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
                    kind: kind,
                    indexName: indexName,
                    fieldName: fieldName,
                    terms: terms,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                kind: kind,
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
                let values = facetValues(fieldName: field, from: entity.item)
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
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>,
        indexName: String,
        fieldName: String,
        terms: [String],
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: fieldName)
        )
        let maintainer = FullTextIndexMaintainer<PolymorphicFullTextPlaceholder>(
            index: index,
            tokenizer: kind.tokenizer,
            storePositions: kind.storePositions,
            ngramSize: kind.ngramSize,
            minTermLength: kind.minTermLength,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let phrase = terms.joined(separator: " ")
        let matches = try await maintainer.searchPhrase(phrase, transaction: transaction)
        return matches.map(Tuple.init)
    }

    private func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let termsSubspace = indexSubspace.subspace("terms")
        let termGroups = normalizeQueryTermGroups(terms, kind: kind)
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
            var idToElements: [String: [any TupleElement]] = [:]
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
            matchingIDs = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        }

        return matchingIDs.map(Tuple.init)
    }

    private func normalizeQueryTermGroups(
        _ terms: [String],
        kind: FullTextIndexKind<PolymorphicFullTextPlaceholder>
    ) -> [[String]] {
        let normalizer = FullTextTermNormalizer(
            tokenizer: kind.tokenizer,
            ngramSize: kind.ngramSize,
            minTermLength: kind.minTermLength
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

        var intersection: Set<String>? = nil
        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            var currentSet: Set<String> = []

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

        var idToElements: [String: [any TupleElement]] = [:]
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
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
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

    private func makeKind(
        fieldName: String,
        descriptor: IndexDescriptorMetadata?
    ) throws -> FullTextIndexKind<PolymorphicFullTextPlaceholder> {
        guard let descriptor else {
            return FullTextIndexKind<PolymorphicFullTextPlaceholder>(
                fieldNames: [fieldName]
            )
        }

        let tokenizerRawValue = descriptor.kind.metadata["tokenizer"]?.stringValue ?? TokenizationStrategy.simple.rawValue
        guard let tokenizer = TokenizationStrategy(rawValue: tokenizerRawValue) else {
            throw FullTextReadError.invalidParameter("tokenizer")
        }
        return FullTextIndexKind<PolymorphicFullTextPlaceholder>(
            fieldNames: descriptor.fieldNames.isEmpty ? [fieldName] : descriptor.fieldNames,
            tokenizer: tokenizer,
            storePositions: descriptor.kind.metadata["storePositions"]?.boolValue ?? true,
            ngramSize: descriptor.kind.metadata["ngramSize"]?.intValue ?? 3,
            minTermLength: descriptor.kind.metadata["minTermLength"]?.intValue ?? 2
        )
    }

    private func resolveDescriptor(
        in group: PolymorphicGroup,
        indexName: String,
        fieldName: String
    ) -> IndexDescriptorMetadata? {
        if let descriptor = group.indexes.first(where: { $0.name == indexName }) {
            return descriptor
        }
        return group.indexes.first(where: {
            $0.kindIdentifier == kindIdentifier && $0.fieldNames.contains(fieldName)
        })
    }

    private func facetValues(
        fieldName: String,
        from item: any Persistable
    ) -> [String] {
        guard let raw = item[dynamicMember: fieldName] else {
            return []
        }
        if let string = raw as? String {
            return [string]
        }
        if let strings = raw as? [String] {
            return strings
        }
        if let array = raw as? [any Sendable] {
            return array.map { String(describing: $0) }
        }
        return [String(describing: raw)]
    }

    private func stableKey(_ tuple: Tuple) -> String {
        Data(tuple.pack()).base64EncodedString()
    }

    private func decodeMatchMode(
        from parameters: [String: QueryParameterValue]
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
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
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
