import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import Rank
import StorageKit

enum RankReadParameter {
    static let fieldName = "fieldName"
    static let mode = "mode"
    static let count = "count"
    static let from = "from"
    static let to = "to"
    static let percentile = "percentile"

    static let topMode = "top"
    static let bottomMode = "bottom"
    static let rangeMode = "range"
    static let percentileMode = "percentile"
}

public enum RankReadBridge {
    public static func registerReadExecutors() {
        ReadExecutorRegistry.shared.register(RankReadExecutor())
        ReadExecutorRegistry.shared.registerPolymorphic(PolymorphicRankReadExecutor())
    }
}

private enum RankReadBridgeError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
}

private struct RankReadExecutor: IndexReadExecutor {
    let kindIdentifier = "rank"

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let queryContext = try context.indexQueryContext.withPartitionValues(partitionValues, for: T.self)
        var builder = RankQueryBuilder<T>(
            queryContext: queryContext,
            fieldName: fieldName
        )

        let mode = try requireString(RankReadParameter.mode, from: indexScan.parameters)
        switch mode {
        case RankReadParameter.topMode:
            builder = builder.top(try requireInt(RankReadParameter.count, from: indexScan.parameters))
        case RankReadParameter.bottomMode:
            builder = builder.bottom(try requireInt(RankReadParameter.count, from: indexScan.parameters))
        case RankReadParameter.rangeMode:
            builder = builder.range(
                from: try requireInt(RankReadParameter.from, from: indexScan.parameters),
                to: try requireInt(RankReadParameter.to, from: indexScan.parameters)
            )
        case RankReadParameter.percentileMode:
            builder = builder.percentile(try requireDouble(RankReadParameter.percentile, from: indexScan.parameters))
        default:
            throw RankReadBridgeError.invalidParameter(RankReadParameter.mode)
        }

        let results = try await builder.executeDirect(
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for rank access paths")
        }

        let page = try DatabaseEngine.CanonicalOffsetPagination.window(
            items: results,
            selectQuery: selectQuery,
            options: options
        )
        let rows = try page.items.map { result in
            let data = try JSONEncoder().encode(result.item)
            let fields = try JSONDecoder().decode([String: FieldValue].self, from: data)
            return QueryRow(
                fields: fields,
                annotations: ["rank": .int64(Int64(result.rank))]
            )
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
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

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return Int(value)
    }

    private func requireDouble(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Double {
        guard let value = parameters[key]?.doubleValue else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }
}

private struct PolymorphicRankReadExecutor: PolymorphicIndexReadExecutor {
    private static let maxScanKeys = 100_000

    let kindIdentifier = "rank"

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> QueryResponse {
        let fieldName = try requireString(RankReadParameter.fieldName, from: indexScan.parameters)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        let orderByFields = selectQuery.orderBy?.compactMap { sortKey -> String? in
            guard case .column(let column) = sortKey.expression else { return nil }
            return column.column
        }
        try context.authorizePolymorphicListAccess(
            group: group,
            limit: selectQuery.limit,
            offset: selectQuery.offset,
            orderBy: orderByFields
        )

        let scoresSubspace = try await context.container
            .resolvePolymorphicDirectory(for: group.identifier)
            .subspace(SubspaceKey.indexes)
            .subspace(indexScan.indexName)
            .subspace("scores")

        let rankedKeys = try await context.executeCanonicalRead(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let entries = try await loadEntries(
                from: scoresSubspace,
                transaction: transaction
            )
            return try rankedEntries(
                entries: entries,
                mode: requireString(RankReadParameter.mode, from: indexScan.parameters),
                parameters: indexScan.parameters
            )
        }

        let records = try await context.fetchPolymorphicItems(
            group: group,
            ids: rankedKeys.map { $0.primaryKey },
            configuration: execution.transactionConfiguration,
            cachePolicy: execution.cachePolicy
        )

        if isCountProjection(selectQuery) {
            throw CanonicalReadError.unsupportedAccessPath("count() is not supported for rank access paths")
        }

        let recordByID: [String: PolymorphicRecord] = Dictionary(
            uniqueKeysWithValues: records.map { record in
                (stableKey(Tuple([record.typeCode] + primaryKeyElements(from: record.item))), record)
            }
        )
        let orderedResults: [(record: PolymorphicRecord, rank: Int)] = rankedKeys.compactMap { result in
            let key = stableKey(result.primaryKey)
            guard let record = recordByID[key] else {
                return nil
            }
            return (record: record, rank: result.rank)
        }

        let page = try CanonicalOffsetPagination.window(
            items: orderedResults,
            selectQuery: selectQuery,
            options: options
        )
        let rows = page.items.map { result in
            QueryRowCodec.encodeAny(
                result.record.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(result.record.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(result.record.typeCode),
                    "rank": .int64(Int64(result.rank))
                ]
            )
        }
        return QueryResponse(rows: rows, continuation: page.continuation)
    }

    private func loadEntries(
        from scoresSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [(score: Double, primaryKey: Tuple)] {
        let range = scoresSubspace.range()
        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        var entries: [(score: Double, primaryKey: Tuple)] = []
        entries.reserveCapacity(128)
        var scannedKeys = 0
        for (key, _) in sequence {
            guard scoresSubspace.contains(key) else { break }
            scannedKeys += 1
            if scannedKeys >= Self.maxScanKeys { break }
            guard let entry = try parseIndexKey(key, scoresSubspace: scoresSubspace) else {
                continue
            }
            entries.append(entry)
        }
        return entries
    }

    private func rankedEntries(
        entries: [(score: Double, primaryKey: Tuple)],
        mode: String,
        parameters: [String: QueryParameterValue]
    ) throws -> [(primaryKey: Tuple, rank: Int)] {
        switch mode {
        case RankReadParameter.topMode:
            let count = try requireInt(RankReadParameter.count, from: parameters)
            let sorted = entries.sorted { $0.score > $1.score }
            return Array(sorted.prefix(count)).enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.bottomMode:
            let count = try requireInt(RankReadParameter.count, from: parameters)
            let sorted = entries.sorted { $0.score < $1.score }
            return Array(sorted.prefix(count)).enumerated().map { (primaryKey: $0.element.primaryKey, rank: $0.offset) }

        case RankReadParameter.rangeMode:
            let from = try requireInt(RankReadParameter.from, from: parameters)
            let to = try requireInt(RankReadParameter.to, from: parameters)
            let sorted = entries.sorted { $0.score > $1.score }
            return Array(sorted.dropFirst(from).prefix(max(to - from, 0))).enumerated().map {
                (primaryKey: $0.element.primaryKey, rank: from + $0.offset)
            }

        case RankReadParameter.percentileMode:
            let percentile = try requireDouble(RankReadParameter.percentile, from: parameters)
            let sorted = entries.sorted { $0.score > $1.score }
            guard !sorted.isEmpty else { return [] }
            let targetRank = Int(Double(sorted.count) * (1.0 - percentile))
            let safeRank = max(0, min(targetRank, sorted.count - 1))
            return [(primaryKey: sorted[safeRank].primaryKey, rank: safeRank)]

        default:
            throw RankReadBridgeError.invalidParameter(RankReadParameter.mode)
        }
    }

    private func parseIndexKey(
        _ key: Bytes,
        scoresSubspace: Subspace
    ) throws -> (score: Double, primaryKey: Tuple)? {
        let tuple = try scoresSubspace.unpack(key)
        guard tuple.count >= 2, let firstElement = tuple[0] else {
            return nil
        }

        let score: Double
        do {
            score = try TypeConversion.double(from: firstElement)
        } catch {
            return nil
        }

        var primaryKeyElements: [any TupleElement] = []
        for index in 1..<tuple.count {
            if let element = tuple[index] {
                primaryKeyElements.append(element)
            }
        }
        return (score: score, primaryKey: Tuple(primaryKeyElements))
    }

    private func primaryKeyElements(from item: any Persistable) -> [any TupleElement] {
        guard let raw = item[dynamicMember: "id"] as? any TupleElement else {
            return []
        }
        return [raw]
    }

    private func stableKey(_ tuple: Tuple) -> String {
        Data(tuple.pack()).base64EncodedString()
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

    private func requireString(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return Int(value)
    }

    private func requireDouble(
        _ key: String,
        from parameters: [String: QueryParameterValue]
    ) throws -> Double {
        guard let value = parameters[key]?.doubleValue else {
            throw RankReadBridgeError.missingParameter(key)
        }
        return value
    }
}
