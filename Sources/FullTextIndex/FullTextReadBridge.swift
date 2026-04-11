import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import FullText

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

public enum FullTextReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(FullTextReadExecutor())
    }
}

private struct FullTextContinuationPayload: Codable, Sendable {
    let offset: Int
}

private enum FullTextReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct FullTextReadExecutor: IndexReadExecutor {
    let kindIdentifier = "fulltext"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
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
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
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
            return try makeFacetedResponse(result: result, selectQuery: selectQuery, options: options)
        }

        if returnScores {
            let k1 = indexScan.parameters[FullTextReadParameter.bm25K1]?.doubleValue ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[FullTextReadParameter.bm25B]?.doubleValue ?? Double(BM25Parameters.default.b)
            builder = builder.bm25(k1: Float(k1), b: Float(b))
            let results = try await builder.executeScoredDirect(
                configuration: execution.transactionConfiguration,
                cachePolicy: execution.cachePolicy
            )
            return try makeScoredResponse(results: results, selectQuery: selectQuery, options: options)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )
        return try makePlainResponse(results: results, selectQuery: selectQuery, options: options)
    }

    private func makePlainResponse<T: Persistable>(
        results: [T],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> QueryResponse {
        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for full-text access paths")
        }

        let continuationOffset = try decodeOffset(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return QueryResponse(rows: [])
        }

        let requestedPageSize: Int = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return results.count
            }
        }()

        let window = Array(results.dropFirst(baseOffset).prefix(requestedPageSize + 1))
        let hasMore = window.count > requestedPageSize
        let visible = hasMore ? Array(window.prefix(requestedPageSize)) : window
        let rows = try visible.map { QueryRow(fields: try encodeFields($0)) }
        let continuation = hasMore ? try encodeOffset(continuationOffset + visible.count) : nil
        return QueryResponse(rows: rows, continuation: continuation)
    }

    private func makeScoredResponse<T: Persistable>(
        results: [(item: T, score: Double)],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> QueryResponse {
        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for full-text access paths")
        }

        let continuationOffset = try decodeOffset(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return QueryResponse(rows: [])
        }

        let requestedPageSize: Int = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return results.count
            }
        }()

        let window = Array(results.dropFirst(baseOffset).prefix(requestedPageSize + 1))
        let hasMore = window.count > requestedPageSize
        let visible = hasMore ? Array(window.prefix(requestedPageSize)) : window
        let rows = try visible.map { result in
            QueryRow(
                fields: try encodeFields(result.item),
                annotations: ["score": .double(result.score)]
            )
        }
        let continuation = hasMore ? try encodeOffset(continuationOffset + visible.count) : nil
        return QueryResponse(rows: rows, continuation: continuation)
    }

    private func makeFacetedResponse<T: Persistable>(
        result: FacetedSearchResult<T>,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> QueryResponse {
        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for full-text access paths")
        }

        let continuationOffset = try decodeOffset(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return QueryResponse(
                rows: [],
                metadata: [FullTextReadParameter.totalCount: .int64(Int64(result.totalCount))]
            )
        }

        let requestedPageSize: Int = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return result.items.count
            }
        }()

        let window = Array(result.items.dropFirst(baseOffset).prefix(requestedPageSize + 1))
        let hasMore = window.count > requestedPageSize
        let visible = hasMore ? Array(window.prefix(requestedPageSize)) : window
        let rows = try visible.map { QueryRow(fields: try encodeFields($0)) }
        let continuation = hasMore ? try encodeOffset(continuationOffset + visible.count) : nil

        var metadata: [String: FieldValue] = [
            FullTextReadParameter.totalCount: .int64(Int64(result.totalCount))
        ]
        for (field, buckets) in result.facets {
            metadata[FullTextReadParameter.facetMetadataPrefix + field] = .array(
                buckets.map { bucket in
                    .array([.string(bucket.value), .int64(bucket.count)])
                }
            )
        }

        return QueryResponse(rows: rows, continuation: continuation, metadata: metadata)
    }

    private func isCountProjection(_ selectQuery: SelectQuery) -> Bool {
        guard case .items(let items) = selectQuery.projection,
              items.count == 1,
              case .aggregate(.count(let expression, let distinct)) = items[0].expression,
              expression == nil,
              distinct == false else {
            return false
        }
        return true
    }

    private func decodeOffset(_ continuation: QueryContinuation?) throws -> Int {
        guard let continuation else { return 0 }
        guard let data = Data(base64Encoded: continuation.token) else {
            throw CanonicalReadError.invalidContinuation
        }
        let payload = try JSONDecoder().decode(FullTextContinuationPayload.self, from: data)
        return payload.offset
    }

    private func encodeOffset(_ offset: Int) throws -> QueryContinuation {
        let data = try JSONEncoder().encode(FullTextContinuationPayload(offset: offset))
        return QueryContinuation(data.base64EncodedString())
    }

    private func encodeFields<T: Persistable>(_ item: T) throws -> [String: FieldValue] {
        let data = try JSONEncoder().encode(item)
        return try JSONDecoder().decode([String: FieldValue].self, from: data)
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
            throw FullTextReadBridgeError.invalidParameter(FullTextReadParameter.matchMode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> [String] {
        guard let values = parameters[key]?.arrayValue else {
            throw FullTextReadBridgeError.missingParameter(key)
        }

        var strings: [String] = []
        strings.reserveCapacity(values.count)
        for value in values {
            guard let string = value.stringValue else {
                throw FullTextReadBridgeError.invalidParameter(key)
            }
            strings.append(string)
        }
        return strings
    }
}
