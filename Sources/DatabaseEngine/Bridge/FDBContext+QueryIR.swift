import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

// MARK: - Error Types

public enum QueryBridgeError: Error, Sendable {
    case unsupportedSelectQuery(String)
    case unsupportedExpression
    case incompatibleLiteralType
    case invalidPartitionField(String)
}

private struct OffsetContinuationPayload: Codable, Sendable {
    let offset: Int
}

private enum OffsetContinuationCodec {
    static func decode(_ continuation: QueryContinuation?) throws -> Int {
        guard let continuation else { return 0 }
        guard let data = Data(base64Encoded: continuation.token) else {
            throw CanonicalReadError.invalidContinuation
        }
        let payload = try JSONDecoder().decode(OffsetContinuationPayload.self, from: data)
        return payload.offset
    }

    static func encode(offset: Int) throws -> QueryContinuation {
        let payload = OffsetContinuationPayload(offset: offset)
        let data = try JSONEncoder().encode(payload)
        return QueryContinuation(data.base64EncodedString())
    }
}

enum CanonicalPartitionBinding {
    static func makeBinding<T: Persistable>(
        for type: T.Type,
        partitionValues: [String: String]?
    ) throws -> DirectoryPath<T>? {
        guard let partitionValues else { return nil }

        let allowedFields = Set(T.directoryFieldNames)
        if let unexpectedField = Set(partitionValues.keys).subtracting(allowedFields).sorted().first {
            throw QueryBridgeError.invalidPartitionField(unexpectedField)
        }

        var binding = DirectoryPath<T>()
        for component in T.directoryPathComponents {
            guard let dynamicElement = component as? any DynamicDirectoryElement,
                  let keyPath = dynamicElement.anyKeyPath as? PartialKeyPath<T> else {
                continue
            }

            let fieldName = T.fieldName(for: dynamicElement.anyKeyPath)
            guard let value = partitionValues[fieldName] else { continue }
            binding.fieldValues.append((keyPath, value))
        }

        if T.hasDynamicDirectory || !binding.fieldValues.isEmpty {
            try binding.validate()
            return binding
        }
        return nil
    }
}

private struct CanonicalSortKey: Sendable {
    let fieldName: String
    let direction: SortDirection
    let nulls: NullOrdering?

    func orderedComparison<T: Persistable>(_ lhs: T, _ rhs: T) -> ComparisonResult {
        let lhsValue = FieldReader.readFieldValue(from: lhs, fieldName: fieldName)
        let rhsValue = FieldReader.readFieldValue(from: rhs, fieldName: fieldName)

        let rawResult: ComparisonResult
        switch (lhsValue, rhsValue) {
        case (.null, .null):
            rawResult = .orderedSame
        case (.null, _):
            rawResult = nulls == .last ? .orderedDescending : .orderedAscending
        case (_, .null):
            rawResult = nulls == .last ? .orderedAscending : .orderedDescending
        default:
            rawResult = lhsValue.compare(to: rhsValue) ?? .orderedSame
        }

        switch direction {
        case .ascending:
            return rawResult
        case .descending:
            switch rawResult {
            case .orderedAscending:
                return .orderedDescending
            case .orderedDescending:
                return .orderedAscending
            case .orderedSame:
                return .orderedSame
            }
        }
    }
}

// MARK: - FDBContext + SelectQuery Execution

extension FDBContext {
    /// Execute a canonical read query and return wire-level rows.
    public func query<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default,
        partitionValues: [String: String]? = nil
    ) async throws -> QueryResponse {
        if let accessPath = selectQuery.accessPath {
            switch accessPath {
            case .index(let indexScan):
                guard let executor = ReadExecutorRegistry.shared.indexExecutor(for: indexScan.kindIdentifier) else {
                    throw CanonicalReadError.executorNotRegistered(indexScan.kindIdentifier)
                }
                return try await executor.execute(
                    context: self,
                    selectQuery: selectQuery,
                    indexScan: indexScan,
                    as: type,
                    options: options,
                    partitionValues: partitionValues
                )

            case .fusion(let fusionSource):
                guard let executor = ReadExecutorRegistry.shared.fusionExecutor(for: fusionSource.strategyIdentifier) else {
                    throw CanonicalReadError.executorNotRegistered(fusionSource.strategyIdentifier)
                }
                return try await executor.execute(
                    context: self,
                    selectQuery: selectQuery,
                    fusionSource: fusionSource,
                    as: type,
                    options: options,
                    partitionValues: partitionValues
                )
            }
        }

        return try await executeTableQuery(
            selectQuery,
            as: type,
            options: options,
            partitionValues: partitionValues
        )
    }

    /// Execute a canonical read query and decode typed models from row fields.
    public func execute<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions = .default,
        partitionValues: [String: String]? = nil
    ) async throws -> [T] {
        let response = try await query(
            selectQuery,
            as: type,
            options: options,
            partitionValues: partitionValues
        )
        return try response.rows.map { try QueryRowCodec.decode($0, as: type) }
    }

    private func executeTableQuery<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        guard selectQuery.accessPath == nil else {
            throw CanonicalReadError.unsupportedAccessPath("table executor cannot handle accessPath")
        }

        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSource("Only single-table logical sources are currently supported")
        }

        guard tableRef.table == T.persistableType else {
            throw QueryBridgeError.unsupportedSelectQuery("Table '\(tableRef.table)' does not match type '\(T.persistableType)'")
        }

        guard selectQuery.groupBy == nil,
              selectQuery.having == nil,
              selectQuery.subqueries == nil,
              selectQuery.from == nil,
              selectQuery.fromNamed == nil,
              selectQuery.reduced == false else {
            throw QueryBridgeError.unsupportedSelectQuery("Table executor only supports simple SELECT queries")
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )

        if let countResponse = try await executeCountProjection(
            selectQuery,
            as: type,
            cachePolicy: execution.cachePolicy,
            partitionValues: partitionValues
        ) {
            return countResponse
        }

        let predicate = try makePredicate(from: selectQuery.filter, as: type)
        let sortKeys = try makeSortKeys(from: selectQuery.orderBy, as: type)

        let continuationOffset = try OffsetContinuationCodec.decode(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return QueryResponse(rows: [])
        }

        let effectivePageSize: Int? = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(remaining)):
                return min(pageSize, remaining)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(remaining)):
                return remaining
            case (.none, .none):
                return nil
            }
        }()

        var query = Query<T>()
        if let predicate {
            query.predicates = [predicate]
        }
        query.cachePolicy = execution.cachePolicy
        if let binding = try CanonicalPartitionBinding.makeBinding(for: type, partitionValues: partitionValues) {
            query.partitionBinding = binding
        }

        let needsMaterialization =
            !sortKeys.isEmpty ||
            selectQuery.distinct ||
            usesProjectedRows(selectQuery.projection)

        if !needsMaterialization {
            if baseOffset > 0 {
                query.fetchOffset = baseOffset
            }
            if let effectivePageSize {
                query.fetchLimit = effectivePageSize + 1
            } else if let remainingLimit {
                query.fetchLimit = remainingLimit
            }

            let models = try await fetch(query)
            let rows = try models.map { try QueryRowCodec.encode($0) }
            return try makePagedResponse(
                rows: rows,
                baseOffset: 0,
                continuationOffset: continuationOffset,
                effectivePageSize: effectivePageSize
            )
        }

        var models = try await fetch(query)
        if !sortKeys.isEmpty {
            models.sort { lhs, rhs in
                for sortKey in sortKeys {
                    let comparison = sortKey.orderedComparison(lhs, rhs)
                    if comparison != .orderedSame {
                        return comparison == .orderedAscending
                    }
                }
                return false
            }
        }

        var rows = try makeRows(
            from: models,
            projection: selectQuery.projection,
            tableRef: tableRef
        )
        if selectQuery.distinct {
            rows = uniqueRows(rows)
        }

        return try makePagedResponse(
            rows: rows,
            baseOffset: baseOffset,
            continuationOffset: continuationOffset,
            effectivePageSize: effectivePageSize
        )
    }

    private func executeCountProjection<T: Persistable>(
        _ selectQuery: QueryIR.SelectQuery,
        as type: T.Type,
        cachePolicy: CachePolicy,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse? {
        guard case .items(let projectionItems) = selectQuery.projection,
              projectionItems.count == 1,
              case .aggregate(.count(let expression, let distinct)) = projectionItems[0].expression,
              expression == nil,
              distinct == false else {
            return nil
        }

        let predicate = try makePredicate(from: selectQuery.filter, as: type)

        var query = Query<T>()
        if let predicate {
            query.predicates = [predicate]
        }
        query.cachePolicy = cachePolicy
        if let binding = try CanonicalPartitionBinding.makeBinding(for: type, partitionValues: partitionValues) {
            query.partitionBinding = binding
        }

        let count = try await fetchCount(query)
        return QueryResponse(
            rows: [
                QueryRow(fields: [
                    projectionItems[0].alias ?? "count": .int64(Int64(count))
                ])
            ]
        )
    }

    private func makePredicate<T: Persistable>(
        from expression: QueryIR.Expression?,
        as type: T.Type
    ) throws -> Predicate<T>? {
        guard let expression else { return nil }
        guard let predicate = expression.toPredicate(for: type) else {
            throw QueryBridgeError.unsupportedExpression
        }
        return predicate
    }

    private func makeSortKeys<T: Persistable>(
        from orderBy: [SortKey]?,
        as type: T.Type
    ) throws -> [CanonicalSortKey] {
        guard let orderBy else { return [] }

        return try orderBy.map { sortKey in
            guard case .column(let column) = sortKey.expression,
                  T.allFields.contains(column.column) else {
                throw QueryBridgeError.unsupportedExpression
            }

            return CanonicalSortKey(
                fieldName: column.column,
                direction: sortKey.direction,
                nulls: sortKey.nulls
            )
        }
    }

    private func usesProjectedRows(_ projection: Projection) -> Bool {
        switch projection {
        case .all, .allFrom:
            return false
        case .items, .distinctItems:
            return true
        }
    }

    private func makeRows<T: Persistable>(
        from models: [T],
        projection: Projection,
        tableRef: TableRef
    ) throws -> [QueryRow] {
        switch projection {
        case .all:
            return try models.map { try QueryRowCodec.encode($0) }

        case .allFrom(let name):
            guard name == tableRef.table || name == tableRef.alias || name == tableRef.effectiveName else {
                throw QueryBridgeError.unsupportedSelectQuery("Projection source '\(name)' does not match table '\(tableRef.effectiveName)'")
            }
            return try models.map { try QueryRowCodec.encode($0) }

        case .items(let items):
            return try models.map { try makeProjectedRow(from: $0, items: items) }

        case .distinctItems(let items):
            return uniqueRows(try models.map { try makeProjectedRow(from: $0, items: items) })
        }
    }

    private func makeProjectedRow<T: Persistable>(
        from model: T,
        items: [ProjectionItem]
    ) throws -> QueryRow {
        var fields: [String: FieldValue] = [:]

        for (index, item) in items.enumerated() {
            let fieldName = item.alias ?? defaultProjectionName(for: item.expression, index: index)
            fields[fieldName] = try evaluateProjectionExpression(item.expression, on: model)
        }

        return QueryRow(fields: fields)
    }

    private func defaultProjectionName(for expression: QueryIR.Expression, index: Int) -> String {
        switch expression {
        case .column(let column):
            return column.column
        default:
            return "column\(index)"
        }
    }

    private func evaluateProjectionExpression<T: Persistable>(
        _ expression: QueryIR.Expression,
        on model: T
    ) throws -> FieldValue {
        switch expression {
        case .column(let column):
            guard T.allFields.contains(column.column) else {
                throw QueryBridgeError.unsupportedExpression
            }
            return FieldReader.readFieldValue(from: model, fieldName: column.column)

        case .literal(let literal):
            guard let value = literal.toFieldValue() else {
                throw QueryBridgeError.incompatibleLiteralType
            }
            return value

        default:
            throw QueryBridgeError.unsupportedExpression
        }
    }

    private func uniqueRows(_ rows: [QueryRow]) -> [QueryRow] {
        var seen: Set<QueryRow> = []
        var unique: [QueryRow] = []
        unique.reserveCapacity(rows.count)

        for row in rows where seen.insert(row).inserted {
            unique.append(row)
        }
        return unique
    }

    private func makePagedResponse(
        rows: [QueryRow],
        baseOffset: Int,
        continuationOffset: Int,
        effectivePageSize: Int?
    ) throws -> QueryResponse {
        let offsetRows = baseOffset > 0 ? Array(rows.dropFirst(baseOffset)) : rows

        guard let effectivePageSize else {
            return QueryResponse(rows: offsetRows)
        }

        let window = Array(offsetRows.prefix(effectivePageSize + 1))
        let hasMore = window.count > effectivePageSize
        let visibleRows = hasMore ? Array(window.prefix(effectivePageSize)) : window
        let continuation = hasMore
            ? try OffsetContinuationCodec.encode(offset: continuationOffset + visibleRows.count)
            : nil

        return QueryResponse(rows: visibleRows, continuation: continuation)
    }
}
