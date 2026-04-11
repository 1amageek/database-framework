import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

private struct CanonicalSourceRow: Sendable {
    let fields: [String: FieldValue]
    let scopedFields: [String: [String: FieldValue]]

    init(
        fields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]] = [:]
    ) {
        self.fields = fields
        self.scopedFields = scopedFields
    }

    static func fromBaseFields(
        _ fields: [String: FieldValue],
        sourceName: String?
    ) -> CanonicalSourceRow {
        guard let sourceName else {
            return CanonicalSourceRow(fields: fields)
        }
        return CanonicalSourceRow(fields: fields, scopedFields: [sourceName: fields])
    }

    func applyingAlias(_ alias: String?) -> CanonicalSourceRow {
        guard let alias else { return self }
        return CanonicalSourceRow(fields: fields, scopedFields: [alias: fields])
    }

    func merged(with other: CanonicalSourceRow) -> CanonicalSourceRow {
        let mergedScopes = scopedFields.merging(other.scopedFields) { current, _ in current }
        return CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: mergedScopes),
            scopedFields: mergedScopes
        )
    }

    func fieldValue(for column: ColumnRef) -> FieldValue? {
        if let table = column.table {
            return scopedFields[table]?[column.column]
        }
        return fields[column.column]
    }

    func fields(for sourceName: String) -> [String: FieldValue]? {
        scopedFields[sourceName]
    }

    static func flatten(scopedFields: [String: [String: FieldValue]]) -> [String: FieldValue] {
        var counts: [String: Int] = [:]
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }

        var flattened: [String: FieldValue] = [:]
        for (sourceName, sourceFields) in scopedFields {
            for (key, value) in sourceFields {
                if counts[key] == 1 {
                    flattened[key] = value
                } else {
                    flattened["\(sourceName).\(key)"] = value
                }
            }
        }
        return flattened
    }
}

private enum CanonicalPartitionRoutingMode: Sendable {
    case strict
    case routed
}

extension FDBContext {
    public func query(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions = .default,
        partitionValues: [String: String]? = nil
    ) async throws -> QueryResponse {
        try await queryCanonical(
            selectQuery,
            options: options,
            partitionValues: partitionValues,
            partitionMode: .strict
        )
    }

    private func queryCanonical(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> QueryResponse {
        if let accessPath = selectQuery.accessPath {
            return try await executeAccessPathRows(
                selectQuery,
                accessPath: accessPath,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
        }

        if isSPARQLSource(selectQuery.source) {
            guard let executor = LogicalSourceExecutorRegistry.shared.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            return try await executor.execute(
                context: self,
                selectQuery: selectQuery,
                options: options,
                partitionValues: partitionValues
            )
        }

        if case .table = selectQuery.source,
           selectQuery.subqueries == nil,
           selectQuery.groupBy == nil,
           selectQuery.having == nil,
           selectQuery.from == nil,
           selectQuery.fromNamed == nil,
           selectQuery.reduced == false {
            return try await executeSingleTableRows(
                selectQuery,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
        }

        guard selectQuery.groupBy == nil,
              selectQuery.having == nil,
              selectQuery.from == nil,
              selectQuery.fromNamed == nil,
              selectQuery.reduced == false else {
            throw QueryBridgeError.unsupportedSelectQuery(
                "Canonical logical-source execution does not yet support grouping or SPARQL dataset clauses"
            )
        }

        let sourceRows = try await materializeRows(
            for: selectQuery.source,
            namedSubqueries: selectQuery.subqueries ?? [],
            options: options,
            partitionValues: partitionValues,
            partitionMode: .routed
        )

        let filteredRows = try applyFilter(selectQuery.filter, to: sourceRows)

        if let countResponse = try makeCountProjectionResponse(
            selectQuery,
            rows: filteredRows
        ) {
            return countResponse
        }

        let orderedRows = try applyOrder(selectQuery.orderBy, to: filteredRows)
        var projectedRows = try projectRows(orderedRows, projection: selectQuery.projection)
        if selectQuery.distinct {
            projectedRows = canonicalUniqueRows(projectedRows)
        }

        let page = try CanonicalOffsetPagination.window(
            items: projectedRows,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func executeAccessPathRows(
        _ selectQuery: SelectQuery,
        accessPath: AccessPath,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> QueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedAccessPath(
                "accessPath queries require a table source"
            )
        }

        let entity = try resolveEntity(named: tableRef.table)
        guard let type = entity.persistableType else {
            throw QueryBridgeError.unsupportedSelectQuery("Entity '\(tableRef.table)' is not loadable")
        }

        let effectivePartitionValues = scopedPartitionValues(
            partitionValues,
            for: type,
            mode: partitionMode
        )
        return try await queryUsingResolvedType(
            type,
            selectQuery: selectQuery,
            options: options,
            partitionValues: effectivePartitionValues
        )
    }

    private func executeSingleTableRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> QueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSource("Expected table source")
        }

        let entity = try resolveEntity(named: tableRef.table)
        guard let type = entity.persistableType else {
            throw QueryBridgeError.unsupportedSelectQuery("Entity '\(tableRef.table)' is not loadable")
        }

        let effectivePartitionValues = scopedPartitionValues(
            partitionValues,
            for: type,
            mode: partitionMode
        )
        return try await queryUsingResolvedType(
            type,
            selectQuery: selectQuery,
            options: options,
            partitionValues: effectivePartitionValues
        )
    }

    private func queryUsingResolvedType(
        _ type: any Persistable.Type,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        try await _queryUsingResolvedType(
            type,
            selectQuery: selectQuery,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func _queryUsingResolvedType<T: Persistable>(
        _ type: T.Type,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse {
        try await query(
            selectQuery,
            as: type,
            options: options,
            partitionValues: partitionValues
        )
    }

    private func resolveEntity(named name: String) throws -> Schema.Entity {
        guard let entity = container.schema.entity(named: name) else {
            throw ServiceError(
                code: "UNKNOWN_ENTITY",
                message: "Entity '\(name)' not found in schema"
            )
        }
        return entity
    }

    private func scopedPartitionValues(
        _ partitionValues: [String: String]?,
        for type: any Persistable.Type,
        mode: CanonicalPartitionRoutingMode
    ) -> [String: String]? {
        guard let partitionValues else { return nil }
        switch mode {
        case .strict:
            return partitionValues
        case .routed:
            let allowedFields = Set(type.directoryFieldNames)
            guard !allowedFields.isEmpty else { return nil }
            let filtered = partitionValues.filter { allowedFields.contains($0.key) }
            return filtered.isEmpty ? nil : filtered
        }
    }

    private func isSPARQLSource(_ source: DataSource) -> Bool {
        switch source {
        case .graphPattern, .namedGraph, .service:
            return true
        default:
            return false
        }
    }

    private func materializeRows(
        for source: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        switch source {
        case .table(let tableRef):
            if let named = namedSubqueries.first(where: { $0.name == tableRef.table }) {
                let response = try await queryCanonical(
                    named.query,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode
                )
                let alias = tableRef.alias ?? named.name
                return response.rows.map {
                    CanonicalSourceRow.fromBaseFields($0.fields, sourceName: alias)
                }
            }

            let select = SelectQuery(
                projection: .all,
                source: .table(TableRef(schema: tableRef.schema, table: tableRef.table))
            )
            let response = try await executeSingleTableRows(
                select,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let sourceName = tableRef.alias ?? tableRef.effectiveName
            return response.rows.map {
                CanonicalSourceRow.fromBaseFields($0.fields, sourceName: sourceName)
            }

        case .subquery(let query, let alias):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            return response.rows.map {
                CanonicalSourceRow.fromBaseFields($0.fields, sourceName: alias)
            }

        case .join(let clause):
            return try await materializeJoinRows(
                clause,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )

        case .union(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: true,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )

        case .unionAll(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: false,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )

        case .intersect(let sources):
            return try await materializeIntersectRows(
                sources,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )

        case .except(let lhs, let rhs):
            return try await materializeExceptRows(
                lhs,
                rhs,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )

        case .values(let rows, let columnNames):
            return try rows.map { values in
                let names = columnNames ?? values.indices.map { "column\($0)" }
                guard names.count == values.count else {
                    throw QueryBridgeError.unsupportedSelectQuery("VALUES column count mismatch")
                }
                let fields = try Dictionary(uniqueKeysWithValues: zip(names, values).map { name, literal in
                    guard let fieldValue = literal.toFieldValue() else {
                        throw QueryBridgeError.incompatibleLiteralType
                    }
                    return (name, fieldValue)
                })
                return CanonicalSourceRow(fields: fields)
            }

        case .graphTable(let graphTableSource):
            guard let executor = LogicalSourceExecutorRegistry.shared.graphTableExecutor else {
                throw CanonicalReadError.unsupportedSource("graphTable executor is not registered")
            }
            let rows = try await executor.execute(
                context: self,
                graphTableSource: graphTableSource,
                options: options,
                partitionValues: partitionValues
            )
            let sourceRows = rows.map {
                canonicalGraphTableSourceRow(from: $0.fields, graphName: graphTableSource.graphName)
            }
            guard let columns = graphTableSource.columns, !columns.isEmpty else {
                return sourceRows
            }
            return try sourceRows.map { row in
                var fields: [String: FieldValue] = [:]
                for column in columns {
                    fields[column.alias] = try evaluateExpression(column.expression, on: row)
                }
                return CanonicalSourceRow(fields: fields)
            }

        case .graphPattern, .namedGraph:
            guard let executor = LogicalSourceExecutorRegistry.shared.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let response = try await executor.execute(
                context: self,
                selectQuery: SelectQuery(projection: .all, source: source),
                options: options,
                partitionValues: partitionValues
            )
            return response.rows.map { CanonicalSourceRow(fields: $0.fields) }

        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        }
    }

    private func materializeJoinRows(
        _ clause: JoinClause,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        switch clause.type {
        case .lateral, .leftLateral:
            throw QueryBridgeError.unsupportedSelectQuery("LATERAL joins are not yet supported")
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            let leftRows = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let rightRows = try await materializeRows(
                for: clause.right,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let columns = inferNaturalJoinColumns(leftRows: leftRows, rightRows: rightRows)
            return try performJoin(
                leftRows: leftRows,
                rightRows: rightRows,
                type: naturalJoinBaseType(clause.type),
                condition: .using(columns)
            )
        default:
            let leftRows = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let rightRows = try await materializeRows(
                for: clause.right,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            return try performJoin(
                leftRows: leftRows,
                rightRows: rightRows,
                type: clause.type,
                condition: clause.condition
            )
        }
    }

    private func performJoin(
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow],
        type: QueryIR.JoinType,
        condition: JoinCondition?
    ) throws -> [CanonicalSourceRow] {
        if type == .cross {
            return leftRows.flatMap { left in rightRows.map { left.merged(with: $0) } }
        }

        let emptyLeft = CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: inferredEmptyScopes(from: leftRows)),
            scopedFields: inferredEmptyScopes(from: leftRows)
        )
        let emptyRight = CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: inferredEmptyScopes(from: rightRows)),
            scopedFields: inferredEmptyScopes(from: rightRows)
        )

        var matchedRightIndexes = Set<Int>()
        var results: [CanonicalSourceRow] = []

        for leftRow in leftRows {
            var matched = false
            for (rightIndex, rightRow) in rightRows.enumerated() {
                if try joinMatches(left: leftRow, right: rightRow, condition: condition, joinType: type) {
                    matched = true
                    matchedRightIndexes.insert(rightIndex)
                    results.append(leftRow.merged(with: rightRow))
                }
            }

            if !matched, type == .left || type == .full {
                results.append(leftRow.merged(with: emptyRight))
            }
        }

        if type == .right || type == .full {
            for (rightIndex, rightRow) in rightRows.enumerated() where !matchedRightIndexes.contains(rightIndex) {
                results.append(emptyLeft.merged(with: rightRow))
            }
        }

        return results
    }

    private func joinMatches(
        left: CanonicalSourceRow,
        right: CanonicalSourceRow,
        condition: JoinCondition?,
        joinType: QueryIR.JoinType
    ) throws -> Bool {
        if joinType == .cross {
            return true
        }

        guard let condition else { return true }
        switch condition {
        case .using(let columns):
            for column in columns {
                let leftValue = firstScopedFieldValue(named: column, in: left)
                let rightValue = firstScopedFieldValue(named: column, in: right)
                if leftValue != rightValue {
                    return false
                }
            }
            return true
        case .on(let expression):
            let merged = left.merged(with: right)
            return try evaluateBoolean(expression, on: merged)
        }
    }

    private func inferNaturalJoinColumns(
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow]
    ) -> [String] {
        let leftColumns = Set(leftRows.first.map { Array($0.fields.keys) } ?? [])
        let rightColumns = Set(rightRows.first.map { Array($0.fields.keys) } ?? [])
        return Array(leftColumns.intersection(rightColumns)).sorted()
    }

    private func naturalJoinBaseType(_ type: QueryIR.JoinType) -> QueryIR.JoinType {
        switch type {
        case .naturalLeft:
            return .left
        case .naturalRight:
            return .right
        case .naturalFull:
            return .full
        default:
            return .inner
        }
    }

    private func inferredEmptyScopes(from rows: [CanonicalSourceRow]) -> [String: [String: FieldValue]] {
        guard let first = rows.first else { return [:] }
        return first.scopedFields.mapValues { fields in
            Dictionary(uniqueKeysWithValues: fields.keys.map { ($0, .null) })
        }
    }

    private func firstScopedFieldValue(named column: String, in row: CanonicalSourceRow) -> FieldValue? {
        for fields in row.scopedFields.values {
            if let value = fields[column] {
                return value
            }
        }
        return row.fields[column]
    }

    private func materializeUnionRows(
        _ sources: [DataSource],
        deduplicate: Bool,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        var rows: [CanonicalSourceRow] = []
        for source in sources {
            rows.append(
                contentsOf: try await materializeRows(
                    for: source,
                    namedSubqueries: namedSubqueries,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode
                )
            )
        }
        return deduplicate ? uniqueSourceRows(rows) : rows
    }

    private func materializeIntersectRows(
        _ sources: [DataSource],
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        guard let first = sources.first else { return [] }
        var accumulator = try await materializeRows(
            for: first,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode
        )
        for source in sources.dropFirst() {
            let next = try await materializeRows(
                for: source,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let nextKeys = Set(next.map(identityRow))
            accumulator = accumulator.filter { nextKeys.contains(identityRow($0)) }
        }
        return uniqueSourceRows(accumulator)
    }

    private func materializeExceptRows(
        _ lhs: DataSource,
        _ rhs: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionOptions,
        partitionValues: [String: String]?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        let leftRows = try await materializeRows(
            for: lhs,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode
        )
        let rightRows = try await materializeRows(
            for: rhs,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode
        )
        let rightKeys = Set(rightRows.map(identityRow))
        return uniqueSourceRows(leftRows.filter { !rightKeys.contains(identityRow($0)) })
    }

    private func uniqueSourceRows(_ rows: [CanonicalSourceRow]) -> [CanonicalSourceRow] {
        var seen = Set<QueryRow>()
        var unique: [CanonicalSourceRow] = []
        for row in rows {
            let key = identityRow(row)
            if seen.insert(key).inserted {
                unique.append(row)
            }
        }
        return unique
    }

    private func identityRow(_ row: CanonicalSourceRow) -> QueryRow {
        QueryRow(fields: row.fields)
    }

    private func canonicalGraphTableSourceRow(
        from fields: [String: FieldValue],
        graphName: String
    ) -> CanonicalSourceRow {
        var baseFields: [String: FieldValue] = [:]
        var scopedFields: [String: [String: FieldValue]] = [graphName: [:]]

        for (key, value) in fields {
            if let dotIndex = key.firstIndex(of: ".") {
                let scope = String(key[..<dotIndex])
                let fieldName = String(key[key.index(after: dotIndex)...])
                scopedFields[scope, default: [:]][fieldName] = value
                baseFields[key] = value
                continue
            }

            baseFields[key] = value
            scopedFields[graphName, default: [:]][key] = value
        }

        return CanonicalSourceRow(
            fields: baseFields,
            scopedFields: scopedFields.filter { !$0.value.isEmpty }
        )
    }

    private func applyFilter(
        _ filter: QueryIR.Expression?,
        to rows: [CanonicalSourceRow]
    ) throws -> [CanonicalSourceRow] {
        guard let filter else { return rows }
        return try rows.filter { try evaluateBoolean(filter, on: $0) }
    }

    private func applyOrder(
        _ orderBy: [SortKey]?,
        to rows: [CanonicalSourceRow]
    ) throws -> [CanonicalSourceRow] {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        return try rows.sorted { lhs, rhs in
            for sortKey in orderBy {
                let lhsValue = try evaluateExpression(sortKey.expression, on: lhs)
                let rhsValue = try evaluateExpression(sortKey.expression, on: rhs)

                let comparison: ComparisonResult
                switch (lhsValue, rhsValue) {
                case (.null, .null):
                    comparison = .orderedSame
                case (.null, _):
                    comparison = sortKey.nulls == .last ? .orderedDescending : .orderedAscending
                case (_, .null):
                    comparison = sortKey.nulls == .last ? .orderedAscending : .orderedDescending
                default:
                    comparison = lhsValue.compare(to: rhsValue) ?? .orderedSame
                }

                guard comparison != .orderedSame else { continue }
                switch sortKey.direction {
                case .ascending:
                    return comparison == .orderedAscending
                case .descending:
                    return comparison == .orderedDescending
                }
            }
            return false
        }
    }

    private func projectRows(
        _ rows: [CanonicalSourceRow],
        projection: Projection
    ) throws -> [QueryRow] {
        switch projection {
        case .all:
            return rows.map { QueryRow(fields: $0.fields) }

        case .allFrom(let sourceName):
            return try rows.map { row in
                guard let fields = row.fields(for: sourceName) else {
                    throw QueryBridgeError.unsupportedSelectQuery("Projection source '\(sourceName)' not found")
                }
                return QueryRow(fields: fields)
            }

        case .items(let items):
            return try rows.map { row in
                var fields: [String: FieldValue] = [:]
                for (index, item) in items.enumerated() {
                    let fieldName = item.alias ?? canonicalDefaultProjectionName(for: item.expression, index: index)
                    fields[fieldName] = try evaluateExpression(item.expression, on: row)
                }
                return QueryRow(fields: fields)
            }

        case .distinctItems(let items):
            return canonicalUniqueRows(try projectRows(rows, projection: .items(items)))
        }
    }

    private func makeCountProjectionResponse(
        _ selectQuery: SelectQuery,
        rows: [CanonicalSourceRow]
    ) throws -> QueryResponse? {
        guard case .items(let projectionItems) = selectQuery.projection,
              projectionItems.count == 1 else {
            return nil
        }

        guard case .aggregate(.count(let expression, let distinct)) = projectionItems[0].expression else {
            return nil
        }

        guard expression == nil, distinct == false else {
            throw QueryBridgeError.unsupportedSelectQuery(
                "Canonical logical-source execution currently supports only COUNT(*) projections"
            )
        }

        return QueryResponse(
            rows: [
                QueryRow(fields: [
                    projectionItems[0].alias ?? "count": .int64(Int64(rows.count))
                ])
            ]
        )
    }

    private func evaluateBoolean(
        _ expression: QueryIR.Expression,
        on row: CanonicalSourceRow
    ) throws -> Bool {
        switch expression {
        case .column:
            let value = try evaluateExpression(expression, on: row)
            guard let boolValue = value.boolValue else {
                throw QueryBridgeError.unsupportedExpression
            }
            return boolValue
        case .literal(let literal):
            guard let value = literal.toFieldValue()?.boolValue else {
                throw QueryBridgeError.incompatibleLiteralType
            }
            return value

        case .equal(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return left == right
        case .notEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return left != right
        case .lessThan(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return left.isLessThan(right)
        case .lessThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return left == right || left.isLessThan(right)
        case .greaterThan(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return right.isLessThan(left)
        case .greaterThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return left == right || right.isLessThan(left)
        case .and(let lhs, let rhs):
            let left = try evaluateBoolean(lhs, on: row)
            let right = try evaluateBoolean(rhs, on: row)
            return left && right
        case .or(let lhs, let rhs):
            let left = try evaluateBoolean(lhs, on: row)
            let right = try evaluateBoolean(rhs, on: row)
            return left || right
        case .not(let inner):
            return try !evaluateBoolean(inner, on: row)
        case .isNull(let inner):
            return try evaluateExpression(inner, on: row) == FieldValue.null
        case .isNotNull(let inner):
            return try evaluateExpression(inner, on: row) != FieldValue.null
        case .inList(let lhs, let values):
            let left = try evaluateExpression(lhs, on: row)
            let right = try values.map { try evaluateExpression($0, on: row) }
            return right.contains(left)
        case .notInList(let lhs, let values):
            let left = try evaluateExpression(lhs, on: row)
            let right = try values.map { try evaluateExpression($0, on: row) }
            return !right.contains(left)
        default:
            throw QueryBridgeError.unsupportedExpression
        }
    }

    private func evaluateExpression(
        _ expression: QueryIR.Expression,
        on row: CanonicalSourceRow
    ) throws -> FieldValue {
        switch expression {
        case .column(let column):
            guard let value = row.fieldValue(for: column) else {
                throw QueryBridgeError.unsupportedExpression
            }
            return value
        case .literal(let literal):
            guard let value = literal.toFieldValue() else {
                throw QueryBridgeError.incompatibleLiteralType
            }
            return value
        default:
            // Canonical logical-source evaluation intentionally supports only
            // column and literal operands plus the boolean/comparison forms above.
            throw QueryBridgeError.unsupportedExpression
        }
    }

    private func canonicalDefaultProjectionName(for expression: QueryIR.Expression, index: Int) -> String {
        switch expression {
        case .column(let column):
            return column.column
        default:
            return "column\(index)"
        }
    }

    private func canonicalUniqueRows(_ rows: [QueryRow]) -> [QueryRow] {
        var seen: Set<QueryRow> = []
        var unique: [QueryRow] = []
        for row in rows where seen.insert(row).inserted {
            unique.append(row)
        }
        return unique
    }
}
