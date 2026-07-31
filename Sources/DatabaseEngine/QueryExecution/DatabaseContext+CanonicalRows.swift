import DatabaseKit
import DatabaseTypes
import DatabaseWire

struct CanonicalSourceRow: Sendable {
    let fields: [String: FieldValue]
    let scopedFields: [String: [String: FieldValue]]
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?

    init(
        fields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]] = [:],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.fields = fields
        self.scopedFields = scopedFields
        self.annotations = annotations
        self.version = version
    }

    static func fromBaseFields(
        _ fields: [String: FieldValue],
        sourceName: String?,
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) -> CanonicalSourceRow {
        guard let sourceName else {
            return CanonicalSourceRow(fields: fields, annotations: annotations, version: version)
        }
        return CanonicalSourceRow(
            fields: fields,
            scopedFields: [sourceName: fields],
            annotations: annotations,
            version: version
        )
    }

    func applyingAlias(_ alias: String?) -> CanonicalSourceRow {
        guard let alias else { return self }
        return CanonicalSourceRow(
            fields: fields,
            scopedFields: [alias: fields],
            annotations: annotations,
            version: version
        )
    }

    func merged(with other: CanonicalSourceRow) -> CanonicalSourceRow {
        let mergedScopes = scopedFields.merging(other.scopedFields) { current, _ in current }
        return CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: mergedScopes),
            scopedFields: mergedScopes,
            annotations: annotations.merging(other.annotations) { current, _ in current },
            version: nil
        )
    }

    func value(for column: ColumnRef) -> FieldValue? {
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

extension DatabaseContext {
    public func query(
        _ selectQuery: SelectQuery,
        options: ReadExecutionOptions = .default,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let response = try await query(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
        guard let rowCount = UInt32(exactly: response.rows.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: execution.workMeter.consumedRows,
                requested: UInt32.max,
                maximum: execution.workMeter.budget.maximumRows
            )
        }
        try execution.workMeter.recordOutputRows(rowCount)
        return response
    }

    package func query(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        try await queryCanonical(
            selectQuery,
            options: execution,
            partitionValues: graphPartitions,
            partitionMode: .strict
        )
    }

    private func queryCanonical(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
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

        if case .logical(let logicalSource) = selectQuery.source,
           logicalSource.kindIdentifier == LogicalSourceKind.polymorphic,
           selectQuery.subqueries == nil,
           selectQuery.groupBy == nil,
           selectQuery.having == nil,
           selectQuery.dataset == .implicit,
           selectQuery.reduced == false {
            return try await executePolymorphicRows(
                selectQuery,
                logicalSource: logicalSource,
                options: options
            )
        }

        if isSPARQLSource(selectQuery.source) {
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            return try await executor.execute(
                context: self,
                selectQuery: selectQuery,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
        }

        if case .table = selectQuery.source,
           selectQuery.subqueries == nil,
           selectQuery.groupBy == nil,
           selectQuery.having == nil,
           selectQuery.dataset == .implicit,
           selectQuery.reduced == false {
            guard partitionValues?.isEmpty != false else {
                throw CanonicalReadError.invalidPartition(
                    entity: "graph",
                    reason: "graph partitions cannot be applied to a table source"
                )
            }
            return try await executeSingleTableRows(
                selectQuery,
                options: options
            )
        }

        guard selectQuery.groupBy == nil,
              selectQuery.having == nil,
              selectQuery.dataset == .implicit,
              selectQuery.reduced == false else {
            throw CanonicalReadError.unsupportedSelectQuery(
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

        let filteredRows = try applyFilter(
            selectQuery.filter,
            to: sourceRows,
            workMeter: options.workMeter
        )

        if let countResponse = try makeCountProjectionResponse(
            selectQuery,
            rows: filteredRows,
            workMeter: options.workMeter
        ) {
            return countResponse
        }

        let orderedRows = try applyOrder(
            selectQuery.orderBy,
            to: filteredRows,
            workMeter: options.workMeter
        )
        var projectedRows = try projectRows(
            orderedRows,
            projection: selectQuery.projection,
            workMeter: options.workMeter
        )
        if selectQuery.distinct {
            projectedRows = try canonicalUniqueRows(
                projectedRows,
                workMeter: options.workMeter
            )
        }

        let page = try CanonicalQueryPagination.window(
            rows: consume projectedRows,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func executeAccessPathRows(
        _ selectQuery: SelectQuery,
        accessPath: AccessPath,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> QueryResponse {
        switch selectQuery.source {
        case .table(let tableRef):
            guard partitionValues?.isEmpty != false else {
                throw CanonicalReadError.invalidPartition(
                    entity: "graph",
                    reason: "graph partitions cannot be applied to a table source"
                )
            }
            // Non-scalar index access paths (fulltext, vector, rank, etc.) are
            // handled by kind-specific executors registered in ReadExecutorRegistry.
            // Only scalar index access is routed through SelectQueryPlanner, because
            // it maps cleanly onto Query<T>.forcedIndex + typed fetch.
            if case .index(let indexScan) = accessPath,
               indexScan.kindIdentifier != "scalar" {
                let rowSet = try await dispatchTableIndexExecutor(
                    tableRef: tableRef,
                    selectQuery: selectQuery,
                    indexScan: indexScan,
                    options: options
                )
                let sourceName = tableRef.alias ?? tableRef.effectiveName
                return try finalizeIndexReadResult(
                    rowSet,
                    sourceName: sourceName,
                    selectQuery: selectQuery,
                    options: options
                )
            }
            return try await executeSingleTableRows(
                selectQuery,
                options: options
            )

        case .logical(let logicalSource):
            guard logicalSource.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "accessPath queries do not support logical source '\(logicalSource.kindIdentifier)'"
                )
            }
            guard case .index(let indexScan) = accessPath else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "Polymorphic logical sources currently support only index access paths"
                )
            }
            let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
            guard let index = group.indexes.first(
                where: { $0.name == indexScan.indexName }
            ) else {
                throw CanonicalReadError.indexHintNotFound(
                    "Index '\(indexScan.indexName)' is not declared by polymorphic group '\(group.identifier)'"
                )
            }
            guard index.kindIdentifier == indexScan.kindIdentifier else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "Index '\(index.name)' has kind '\(index.kindIdentifier)', not '\(indexScan.kindIdentifier)'"
                )
            }
            guard let executor = container.runtimeConfiguration.readExecutors
                .polymorphicIndexExecutor(
                    for: index.kindIdentifier
                ) else {
                throw CanonicalReadError.executorNotRegistered(
                    index.kindIdentifier
                )
            }
            let rowSet = try await executor.executeRows(
                context: self,
                selectQuery: selectQuery,
                index: index,
                indexScan: indexScan,
                group: group,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            return try finalizeIndexReadResult(
                rowSet,
                sourceName: logicalSource.effectiveName,
                selectQuery: selectQuery,
                options: options
            )

        default:
            throw CanonicalReadError.unsupportedAccessPath("accessPath queries require a table or logical source")
        }
    }

    /// Apply the common SQL pipeline on top of an index executor's row set.
    ///
    /// Runs `WHERE` → `COUNT(*)` short-circuit → `ORDER BY` → projection →
    /// `DISTINCT` → `LIMIT`/`OFFSET` pagination. Index-defined ordering is
    /// preserved only when the outer `SELECT` has no `ORDER BY`.
    private func finalizeIndexReadResult(
        _ rowSet: IndexReadResult,
        sourceName: String?,
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) throws -> QueryResponse {
        let canonicalRows = try rowSet.rows.map { indexRow in
            try options.workMeter.consume(at: .projection)
            return CanonicalSourceRow.fromBaseFields(
                indexRow.fields,
                sourceName: sourceName,
                annotations: indexRow.annotations,
                version: indexRow.version
            )
        }

        let filtered = try applyFilter(
            selectQuery.filter,
            to: canonicalRows,
            workMeter: options.workMeter
        )

        if let countResponse = try makeCountProjectionResponse(
            selectQuery,
            rows: filtered,
            workMeter: options.workMeter
        ) {
            return countResponse
        }

        let ordered: [CanonicalSourceRow]
        let hasExplicitOrder = (selectQuery.orderBy?.isEmpty == false)
        if rowSet.ordering == .orderedByIndex, !hasExplicitOrder {
            ordered = filtered
        } else {
            ordered = try applyOrder(
                selectQuery.orderBy,
                to: filtered,
                workMeter: options.workMeter
            )
        }

        var projected = try projectRows(
            ordered,
            projection: selectQuery.projection,
            workMeter: options.workMeter
        )
        if selectQuery.distinct {
            projected = try canonicalUniqueRows(
                projected,
                workMeter: options.workMeter
            )
        }

        let page = try CanonicalQueryPagination.window(
            rows: consume projected,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(
            rows: page.items,
            continuation: page.continuation,
            metadata: rowSet.metadata
        )
    }

    private func executeSingleTableRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) async throws -> QueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSource("Expected table source")
        }

        let entity = try resolveEntity(named: tableRef.table)
        guard let runtime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }

        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let pushdown = try await fetchTableSourceRows(
            runtime: runtime,
            sourceName: sourceName,
            selectQuery: selectQuery,
            options: options
        )

        let filteredRows = try applyFilter(
            pushdown.residualFilter,
            to: pushdown.rows,
            workMeter: options.workMeter
        )

        if let countResponse = try makeCountProjectionResponse(
            selectQuery,
            rows: filteredRows,
            workMeter: options.workMeter
        ) {
            return countResponse
        }

        let orderedRows = try applyOrder(
            pushdown.residualOrderBy,
            to: filteredRows,
            workMeter: options.workMeter
        )
        var projectedRows = try projectRows(
            orderedRows,
            projection: selectQuery.projection,
            workMeter: options.workMeter
        )
        if selectQuery.distinct {
            projectedRows = try canonicalUniqueRows(
                projectedRows,
                workMeter: options.workMeter
            )
        }

        // When LIMIT/OFFSET are pushed down to the typed fetch, strip them from the
        // pagination input so pagination doesn't re-apply them.
        let paginationQuery: SelectQuery
        if pushdown.limitPushed || pushdown.offsetPushed {
            var modified = selectQuery
            if pushdown.limitPushed { modified = modified.replacing(limit: nil) }
            if pushdown.offsetPushed { modified = modified.replacing(offset: nil) }
            paginationQuery = modified
        } else {
            paginationQuery = selectQuery
        }

        let page = try CanonicalQueryPagination.window(
            rows: consume projectedRows,
            selectQuery: paginationQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func dispatchTableIndexExecutor(
        tableRef: TableRef,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        options: ReadExecutionContext
    ) async throws -> IndexReadResult {
        let entity = try resolveEntity(named: tableRef.table)
        guard let index = entity.indexDescriptors.first(
            where: { $0.name == indexScan.indexName }
        ) else {
            throw CanonicalReadError.indexHintNotFound(
                "Index '\(indexScan.indexName)' is not declared by entity '\(entity.name)'"
            )
        }
        guard index.kindIdentifier == indexScan.kindIdentifier else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Index '\(index.name)' has kind '\(index.kindIdentifier)', not '\(indexScan.kindIdentifier)'"
            )
        }
        guard let runtime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }
        guard let result = try await runtime.executeIndexRows(
            index: index,
            context: self,
            selectQuery: selectQuery,
            indexScan: indexScan,
            options: options,
            partitions: tableRef.partitions
        ) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(entity.name)' has no registered '\(indexScan.kindIdentifier)' index reader"
            )
        }
        return result
    }

    private func fetchTableSourceRows(
        runtime: EntityRuntimeRegistration,
        sourceName: String,
        selectQuery: SelectQuery,
        options: ReadExecutionContext
    ) async throws -> EntityTableRows {
        try await runtime.fetchTableRows(
            context: self,
            sourceName: sourceName,
            selectQuery: selectQuery,
            options: options
        )
    }

    private func resolveEntity(named name: String) throws -> Schema.Entity {
        guard let entity = container.schema.entity(named: name) else {
            throw CanonicalReadError.unsupportedSource(
                "Entity '\(name)' not found in schema"
            )
        }
        return entity
    }

    private func isSPARQLSource(_ source: DataSource) -> Bool {
        switch source {
        case .graphPattern, .namedGraph, .service:
            return true
        default:
            return false
        }
    }

    private func executePolymorphicRows(
        _ selectQuery: SelectQuery,
        logicalSource: LogicalSourceRef,
        options: ReadExecutionContext
    ) async throws -> QueryResponse {
        let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        let entities = try await scanPolymorphicItems(
            group: group,
            configuration: execution.transactionConfiguration,
            limit: try runtimeWindowValue(
                selectQuery.limit,
                name: "limit"
            ),
            offset: try runtimeWindowValue(
                selectQuery.offset,
                name: "offset"
            ),
            orderBy: canonicalOrderByFields(selectQuery.orderBy)
        )

        let sourceRows = try entities.map { entity in
            try options.workMeter.consume(at: .resultMaterialization)
            let row = try QueryRowCodec.encode(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
            return CanonicalSourceRow.fromBaseFields(
                row.fields,
                sourceName: nil,
                annotations: row.annotations,
                version: row.version
            )
        }

        if let countResponse = try makeCountProjectionResponse(
            selectQuery,
            rows: sourceRows,
            workMeter: options.workMeter
        ) {
            return countResponse
        }

        let filteredRows = try applyFilter(
            selectQuery.filter,
            to: sourceRows,
            workMeter: options.workMeter
        )
        let orderedRows = try applyOrder(
            selectQuery.orderBy,
            to: filteredRows,
            workMeter: options.workMeter
        )
        var projectedRows = try projectRows(
            orderedRows,
            projection: selectQuery.projection,
            workMeter: options.workMeter
        )
        if selectQuery.distinct {
            projectedRows = try canonicalUniqueRows(
                projectedRows,
                workMeter: options.workMeter
            )
        }
        let page = try CanonicalQueryPagination.window(
            rows: consume projectedRows,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func materializeRows(
        for source: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        switch source {
        case .table(let tableRef):
            if let named = namedSubqueries.first(where: { $0.name == tableRef.table }) {
                guard tableRef.partitions.isEmpty else {
                    throw CanonicalReadError.invalidPartition(
                        entity: tableRef.table,
                        reason: "common table expressions cannot have storage partitions"
                    )
                }
                let response = try await queryCanonical(
                    named.query,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode
                )
                let alias = tableRef.alias ?? named.name
                return response.rows.map {
                    CanonicalSourceRow.fromBaseFields(
                        $0.fields,
                        sourceName: alias,
                        annotations: $0.annotations,
                        version: $0.version
                    )
                }
            }

            let select = SelectQuery(
                projection: .all,
                source: .table(
                    TableRef(
                        schema: tableRef.schema,
                        table: tableRef.table,
                        partitions: tableRef.partitions
                    )
                )
            )
            let response = try await executeSingleTableRows(
                select,
                options: options
            )
            let sourceName = tableRef.alias ?? tableRef.effectiveName
            return response.rows.map {
                CanonicalSourceRow.fromBaseFields(
                    $0.fields,
                    sourceName: sourceName,
                    annotations: $0.annotations,
                    version: $0.version
                )
            }

        case .logical(let logicalSource):
            guard logicalSource.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Logical source '\(logicalSource.kindIdentifier)' is not supported"
                )
            }
            let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
            let execution = CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            )
            let entities = try await scanPolymorphicItems(
                group: group,
                configuration: execution.transactionConfiguration
            )
            let sourceName = logicalSource.alias ?? logicalSource.effectiveName
            return try entities.map { entity in
                let row = try QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                    ]
                )
                return CanonicalSourceRow.fromBaseFields(
                    row.fields,
                    sourceName: sourceName,
                    annotations: row.annotations,
                    version: row.version
                )
            }

        case .subquery(let query, let alias):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            return response.rows.map {
                CanonicalSourceRow.fromBaseFields(
                    $0.fields,
                    sourceName: alias,
                    annotations: $0.annotations,
                    version: $0.version
                )
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
                    throw CanonicalReadError.unsupportedSelectQuery("VALUES column count mismatch")
                }
                let fields = try Dictionary(uniqueKeysWithValues: zip(names, values).map { name, literal in
                    (name, try literal.toFieldValue())
                })
                return CanonicalSourceRow(fields: fields)
            }

        case .graphTable(let graphTableSource):
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.graphTableExecutor else {
                throw CanonicalReadError.unsupportedSource("graphTable executor is not registered")
            }
            let rows = try await executor.execute(
                context: self,
                graphTableSource: graphTableSource,
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            let sourceRows = rows.map {
                canonicalGraphTableSourceRow(from: $0.fields, graphName: graphTableSource.graphName)
            }
            guard let columns = graphTableSource.columns, !columns.isEmpty else {
                return sourceRows.map {
                    $0.applyingAlias(graphTableSource.alias)
                }
            }
            return try sourceRows.map { row in
                var fields: [String: FieldValue] = [:]
                for column in columns {
                    fields[column.alias] = try evaluateExpression(column.expression, on: row)
                }
                return CanonicalSourceRow(fields: fields)
                    .applyingAlias(graphTableSource.alias)
            }

        case .graphPattern, .namedGraph:
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let response = try await executor.execute(
                context: self,
                selectQuery: SelectQuery(projection: .all, source: source),
                options: options,
                partitions: partitionValues ?? FieldObject()
            )
            return response.rows.map { CanonicalSourceRow(fields: $0.fields, annotations: $0.annotations) }

        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        }
    }

    private func materializeJoinRows(
        _ clause: JoinClause,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode
    ) async throws -> [CanonicalSourceRow] {
        switch clause.type {
        case .lateral, .leftLateral:
            throw CanonicalReadError.unsupportedSelectQuery("LATERAL joins are not yet supported")
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
                condition: .using(columns),
                workMeter: options.workMeter
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
                condition: clause.condition,
                workMeter: options.workMeter
            )
        }
    }

    private func performJoin(
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow],
        type: JoinType,
        condition: JoinCondition?,
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        if type == .cross {
            var rows: [CanonicalSourceRow] = []
            for left in leftRows {
                for right in rightRows {
                    try workMeter.consume(at: .joinCandidate)
                    rows.append(left.merged(with: right))
                }
            }
            return rows
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
                try workMeter.consume(at: .joinCandidate)
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
        joinType: JoinType
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

    private func naturalJoinBaseType(_ type: JoinType) -> JoinType {
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
        var emptyScopes: [String: [String: FieldValue]] = [:]
        emptyScopes.reserveCapacity(first.scopedFields.count)
        for (scope, fields) in first.scopedFields {
            var emptyFields: [String: FieldValue] = [:]
            emptyFields.reserveCapacity(fields.count)
            for fieldName in fields.keys {
                emptyFields[fieldName] = .null
            }
            emptyScopes[scope] = emptyFields
        }
        return emptyScopes
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
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
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
        if deduplicate {
            return try uniqueSourceRows(rows, workMeter: options.workMeter)
        }
        try options.workMeter.consume(
            UInt64(rows.count),
            at: .bindingCandidate
        )
        return rows
    }

    private func materializeIntersectRows(
        _ sources: [DataSource],
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
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
            var nextKeys = Set<QueryRow>()
            for row in next {
                try options.workMeter.consume(at: .deduplication)
                nextKeys.insert(identityRow(row))
            }
            accumulator = try accumulator.filter { row in
                try options.workMeter.consume(at: .joinCandidate)
                return nextKeys.contains(identityRow(row))
            }
        }
        return try uniqueSourceRows(
            accumulator,
            workMeter: options.workMeter
        )
    }

    private func materializeExceptRows(
        _ lhs: DataSource,
        _ rhs: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
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
        var rightKeys = Set<QueryRow>()
        for row in rightRows {
            try options.workMeter.consume(at: .deduplication)
            rightKeys.insert(identityRow(row))
        }
        let difference = try leftRows.filter { row in
            try options.workMeter.consume(at: .joinCandidate)
            return !rightKeys.contains(identityRow(row))
        }
        return try uniqueSourceRows(
            difference,
            workMeter: options.workMeter
        )
    }

    private func uniqueSourceRows(
        _ rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        var seen = Set<QueryRow>()
        var unique: [CanonicalSourceRow] = []
        for row in rows {
            try workMeter.consume(at: .deduplication)
            let key = identityRow(row)
            if seen.insert(key).inserted {
                unique.append(row)
            }
        }
        return unique
    }

    private func identityRow(_ row: CanonicalSourceRow) -> QueryRow {
        QueryRow(fields: row.fields, annotations: row.annotations)
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

        var nonemptyScopes: [String: [String: FieldValue]] = [:]
        nonemptyScopes.reserveCapacity(scopedFields.count)
        for (scope, scopeFields) in scopedFields where !scopeFields.isEmpty {
            nonemptyScopes[scope] = scopeFields
        }

        return CanonicalSourceRow(
            fields: baseFields,
            scopedFields: nonemptyScopes
        )
    }

    private func applyFilter(
        _ filter: DatabaseKit.Expression?,
        to rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        guard let filter else { return rows }
        return try rows.filter { row in
            try workMeter.consume(at: .filterEvaluation)
            return try evaluateBoolean(filter, on: row)
        }
    }

    private func applyOrder(
        _ orderBy: [SortKey]?,
        to rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        try workMeter.consume(UInt64(rows.count), at: .sortInput)
        let fingerprinted = try rows.map { row in
            (
                row: row,
                fingerprint: try CanonicalRowFingerprint.compute(
                    QueryRow(
                        fields: row.fields,
                        annotations: row.annotations,
                        version: row.version
                    ),
                    workMeter: workMeter
                )
            )
        }
        return try fingerprinted.sorted { lhsItem, rhsItem in
            let lhs = lhsItem.row
            let rhs = rhsItem.row
            for sortKey in orderBy {
                try workMeter.consume(2, at: .sortComparison)
                let lhsValue = try evaluateExpression(sortKey.expression, on: lhs)
                let rhsValue = try evaluateExpression(sortKey.expression, on: rhs)

                let comparison: QueryComparison
                switch (lhsValue, rhsValue) {
                case (.null, .null):
                    comparison = .equal
                case (.null, _):
                    comparison = sortKey.nulls == .last
                        ? .greaterThan
                        : .lessThan
                case (_, .null):
                    comparison = sortKey.nulls == .last
                        ? .lessThan
                        : .greaterThan
                default:
                    comparison = try FieldValueComparator.compare(lhsValue, rhsValue)
                }

                guard comparison != .equal else { continue }
                switch sortKey.direction {
                case .ascending:
                    return comparison == .lessThan
                case .descending:
                    return comparison == .greaterThan
                }
            }
            try workMeter.consume(2, at: .sortComparison)
            return lhsItem.fingerprint.lexicographicallyPrecedes(
                rhsItem.fingerprint
            )
        }.map { $0.row }
    }

    private func projectRows(
        _ rows: [CanonicalSourceRow],
        projection: Projection,
        workMeter: DatabaseWorkMeter
    ) throws -> [QueryRow] {
        switch projection {
        case .all:
            return try rows.map {
                try workMeter.consume(at: .projection)
                return QueryRow(
                    fields: $0.fields,
                    annotations: $0.annotations,
                    version: $0.version
                )
            }

        case .allFrom(let sourceName):
            return try rows.map { row in
                try workMeter.consume(at: .projection)
                guard let fields = row.fields(for: sourceName) else {
                    throw CanonicalReadError.unsupportedSelectQuery("Projection source '\(sourceName)' not found")
                }
                return QueryRow(fields: fields, annotations: row.annotations, version: row.version)
            }

        case .items(let items):
            return try rows.map { row in
                try workMeter.consume(at: .projection)
                var fields: [String: FieldValue] = [:]
                for (index, item) in items.enumerated() {
                    let fieldName = item.alias ?? canonicalProjectionName(for: item.expression, index: index)
                    fields[fieldName] = try evaluateExpression(item.expression, on: row)
                }
                return QueryRow(fields: fields, annotations: row.annotations)
            }

        case .distinctItems(let items):
            return try canonicalUniqueRows(
                projectRows(
                    rows,
                    projection: .items(items),
                    workMeter: workMeter
                ),
                workMeter: workMeter
            )
        }
    }

    private func makeCountProjectionResponse(
        _ selectQuery: SelectQuery,
        rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> QueryResponse? {
        guard case .items(let projectionItems) = selectQuery.projection,
              projectionItems.count == 1 else {
            return nil
        }

        guard case .aggregate(.count(let expression, let distinct)) = projectionItems[0].expression else {
            return nil
        }

        guard expression == nil, distinct == false else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Canonical logical-source execution currently supports only COUNT(*) projections"
            )
        }

        try workMeter.consume(
            UInt64(max(1, rows.count)),
            at: .aggregateInput
        )
        try workMeter.consume(at: .resultMaterialization)

        return QueryResponse(
            rows: [
                QueryRow(fields: [
                    projectionItems[0].alias ?? "count": .int64(Int64(rows.count))
                ])
            ]
        )
    }

    private func evaluateBoolean(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow
    ) throws -> Bool {
        switch expression {
        case .column:
            let value = try evaluateExpression(expression, on: row)
            guard let boolValue = value.boolValue else {
                throw CanonicalReadError.unsupportedExpression
            }
            return boolValue
        case .literal(let literal):
            guard let value = try literal.toFieldValue().boolValue else {
                throw CanonicalReadError.incompatibleLiteralType
            }
            return value

        case .equal(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            return try FieldValueComparator.equal(left, right)
        case .notEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            if left == .null || right == .null { return false }
            return try !FieldValueComparator.equal(left, right)
        case .lessThan(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            if left == .null || right == .null { return false }
            return try FieldValueComparator.compare(left, right) == .lessThan
        case .lessThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            if left == .null || right == .null { return false }
            let comparison = try FieldValueComparator.compare(left, right)
            return comparison != .greaterThan
        case .greaterThan(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            if left == .null || right == .null { return false }
            return try FieldValueComparator.compare(left, right) == .greaterThan
        case .greaterThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, on: row)
            let right = try evaluateExpression(rhs, on: row)
            if left == .null || right == .null { return false }
            let comparison = try FieldValueComparator.compare(left, right)
            return comparison != .lessThan
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
            if left == .null { return false }
            for value in right where value != .null {
                if try FieldValueComparator.equal(left, value) { return true }
            }
            return false
        case .notInList(let lhs, let values):
            let left = try evaluateExpression(lhs, on: row)
            let right = try values.map { try evaluateExpression($0, on: row) }
            if left == .null { return false }
            for value in right {
                if value == .null { return false }
                if try FieldValueComparator.equal(left, value) { return false }
            }
            return true
        default:
            throw CanonicalReadError.unsupportedExpression
        }
    }

    private func evaluateExpression(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow
    ) throws -> FieldValue {
        switch expression {
        case .column(let column):
            guard let value = row.value(for: column) else {
                throw CanonicalReadError.unsupportedExpression
            }
            return value
        case .literal(let literal):
            return try literal.toFieldValue()
        default:
            // Canonical logical-source evaluation intentionally supports only
            // column and literal operands plus the boolean/comparison forms above.
            throw CanonicalReadError.unsupportedExpression
        }
    }

    private func canonicalProjectionName(
        for expression: DatabaseKit.Expression,
        index: Int
    ) -> String {
        switch expression {
        case .column(let column):
            return column.column
        default:
            return "column\(index)"
        }
    }

    private func canonicalOrderByFields(_ orderBy: [SortKey]?) -> [String]? {
        guard let orderBy else { return nil }
        let fields = orderBy.compactMap { sortKey -> String? in
            guard case .column(let column) = sortKey.expression else {
                return nil
            }
            return column.column
        }
        return fields.isEmpty ? nil : fields
    }

    private func runtimeWindowValue(
        _ value: UInt64?,
        name: String
    ) throws(CanonicalReadError) -> Int? {
        guard let value else {
            return nil
        }
        guard let result = Int(exactly: value) else {
            throw .paginationValueExceedsRuntimeRange(
                name: name,
                value: value
            )
        }
        return result
    }

    private func canonicalUniqueRows(
        _ rows: [QueryRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [QueryRow] {
        var seen: Set<QueryRow> = []
        var unique: [QueryRow] = []
        for row in rows {
            try workMeter.consume(at: .deduplication)
            if seen.insert(row).inserted {
                unique.append(row)
            }
        }
        return unique
    }
}
