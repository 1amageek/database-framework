import DatabaseKit
import DatabaseTypes
import DatabaseWire
import StorageKit

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
        try await withDataOperation { [self] in
            try await withFieldReadAuthorization(for: selectQuery) {
                if let binding = ActiveDatabaseTransactionContext.binding {
                    guard binding.resource == self.resource,
                          binding.authorization == self.authorization,
                          binding.grantedAccess.contains(.read) else {
                        throw DatabaseGrantAuthorizationError.denied(
                            resource: self.resource,
                            required: .read
                        )
                    }
                    return try await query(
                        selectQuery,
                        execution: execution,
                        graphPartitions: graphPartitions,
                        transaction: binding.transaction
                    )
                }
                return try await queryCanonical(
                    selectQuery,
                    options: execution,
                    partitionValues: graphPartitions,
                    partitionMode: .strict
                )
            }
        }
    }

    /// Executes a Base-local read through a caller-owned storage transaction.
    /// Relational sources and admitted index readers reuse exactly that
    /// transaction so callers cannot accidentally create a mixed snapshot.
    package func query(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        transaction: any TransactionAccess
    ) async throws -> QueryResponse {
        try await withDataOperation { [self] in
            try await withFieldReadAuthorization(for: selectQuery) {
                _ = try requireOperationDataRoot()
                return try await ActiveDatabaseTransactionContext.$binding
                    .withValue(
                        DatabaseTransactionExecutionBinding(
                            transaction: transaction,
                            resource: self.resource,
                            authorization: self.authorization,
                            grantedAccess: .read,
                            databaseTransaction: nil
                        )
                    ) {
                        if isSPARQLSource(selectQuery.source) {
                            guard let executor = container.runtimeConfiguration
                                .logicalSourceExecutors.sparqlExecutor else {
                                throw CanonicalReadError.unsupportedSource(
                                    "SPARQL source executor is not registered"
                                )
                            }
                            return try await executor.executeInTransaction(
                                context: self,
                                selectQuery: selectQuery,
                                options: execution,
                                partitions: graphPartitions,
                                transaction: transaction
                            )
                        }
                        if let accessPath = selectQuery.accessPath {
                            guard selectQuery.subqueries == nil,
                                  selectQuery.groupBy == nil,
                                  selectQuery.having == nil,
                                  selectQuery.dataset == .implicit,
                                  selectQuery.reduced == false else {
                                throw CanonicalReadError.unsupportedSelectQuery(
                                    "A transaction-bound index read does not support grouping, subqueries, or dataset clauses"
                                )
                            }
                            return try await executeAccessPathRows(
                                selectQuery,
                                accessPath: accessPath,
                                options: execution,
                                partitionValues: nil,
                                partitionMode: .strict
                            )
                        }
                        guard selectQuery.subqueries == nil,
                              selectQuery.groupBy == nil,
                              selectQuery.having == nil,
                              selectQuery.dataset == .implicit,
                              selectQuery.reduced == false,
                              isTransactionBoundRelationalSource(selectQuery.source)
                        else {
                            throw CanonicalReadError.unsupportedSelectQuery(
                                "A transaction-bound relational read requires a Base-local table or join tree without grouping or dataset clauses"
                            )
                        }
                        if case .table = selectQuery.source {
                            return try await executeSingleTableRows(
                                selectQuery,
                                options: execution,
                                transaction: transaction
                            )
                        }
                        return try await executeTransactionBoundRelationalRows(
                            selectQuery,
                            options: execution,
                            transaction: transaction
                        )
                    }
            }
        }
    }

    private func withFieldReadAuthorization<Result: Sendable>(
        for selectQuery: SelectQuery,
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: selectQuery,
            schema: container.schema
        )
        try authorizeFieldReads(plan)
        return try await RequestFieldAuthorization.$fieldsByEntity.withValue(
            plan.fieldsByEntity
        ) {
            try await operation()
        }
    }

    private func executeTransactionBoundRelationalRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> QueryResponse {
        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let sourceRows = try await materializeTransactionBoundRows(
            for: selectQuery.source,
            options: sourceOptions,
            transaction: transaction
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
        return QueryResponse(
            rows: page.items,
            continuation: page.continuation
        )
    }

    private func materializeTransactionBoundRows(
        for source: DataSource,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> [CanonicalSourceRow] {
        switch source {
        case .table(let tableRef):
            return try await materializeUnwindowedTableSourceRows(
                tableRef,
                options: options,
                transaction: transaction
            )

        case .join(let clause):
            switch clause.type {
            case .lateral, .leftLateral:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "LATERAL joins are not supported by transaction-bound reads"
                )
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                let leftRows = try await materializeTransactionBoundRows(
                    for: clause.left,
                    options: options,
                    transaction: transaction
                )
                let rightRows = try await materializeTransactionBoundRows(
                    for: clause.right,
                    options: options,
                    transaction: transaction
                )
                return try performJoin(
                    leftRows: leftRows,
                    rightRows: rightRows,
                    type: naturalJoinBaseType(clause.type),
                    condition: .using(
                        inferNaturalJoinColumns(
                            leftRows: leftRows,
                            rightRows: rightRows
                        )
                    ),
                    workMeter: options.workMeter
                )
            default:
                let leftRows = try await materializeTransactionBoundRows(
                    for: clause.left,
                    options: options,
                    transaction: transaction
                )
                let rightRows = try await materializeTransactionBoundRows(
                    for: clause.right,
                    options: options,
                    transaction: transaction
                )
                return try performJoin(
                    leftRows: leftRows,
                    rightRows: rightRows,
                    type: clause.type,
                    condition: clause.condition,
                    workMeter: options.workMeter
                )
            }

        default:
            throw CanonicalReadError.unsupportedSelectQuery(
                "The source cannot be executed inside one caller-owned transaction"
            )
        }
    }

    private func isTransactionBoundRelationalSource(
        _ source: DataSource
    ) -> Bool {
        switch source {
        case .table:
            return true
        case .join(let clause):
            return isTransactionBoundRelationalSource(clause.left)
                && isTransactionBoundRelationalSource(clause.right)
        default:
            return false
        }
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

        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let sourceRows = try await materializeRows(
            for: selectQuery.source,
            namedSubqueries: selectQuery.subqueries ?? [],
            options: sourceOptions,
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
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .projection,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: rowSet.rows.count
        )
        for indexRow in rowSet.rows {
            try options.workMeter.consume(at: .projection)
            let row = CanonicalSourceRow.fromBaseFields(
                indexRow.fields,
                sourceName: sourceName,
                annotations: indexRow.annotations,
                version: indexRow.version
            )
            try retainedRows.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: options.workMeter
                ),
                make: { row }
            )
        }
        let canonicalRows = retainedRows.finish().promoteToOutput()

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
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)? = nil
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
            options: options,
            transaction: transaction
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
            options: options,
            rowsAreContinuationRelative: pushdown.pageWindowPushed,
            continuationPosition: pushdown.continuationPosition,
            prevalidatedQueryFingerprint:
                pushdown.stableSnapshotQueryFingerprint
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
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)? = nil
    ) async throws -> EntityTableRows {
        try await runtime.fetchTableRows(
            context: self,
            sourceName: sourceName,
            selectQuery: selectQuery,
            options: options,
            transaction: transaction
        )
    }

    private func materializeUnwindowedTableSourceRows(
        _ tableRef: TableRef,
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)?
    ) async throws -> [CanonicalSourceRow] {
        let entity = try resolveEntity(named: tableRef.table)
        guard let runtime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entity.name) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Entity '\(tableRef.table)' has no registered runtime type"
            )
        }
        let sourceName = tableRef.alias ?? tableRef.effectiveName
        let select = SelectQuery(
            projection: .all,
            source: .table(tableRef)
        )
        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let rows = try await fetchTableSourceRows(
            runtime: runtime,
            sourceName: sourceName,
            selectQuery: select,
            options: sourceOptions,
            transaction: transaction
        )
        guard rows.residualFilter == nil,
              rows.residualOrderBy == nil,
              !rows.limitPushed,
              !rows.offsetPushed,
              !rows.pageWindowPushed else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A join source unexpectedly applied top-level query pushdown"
            )
        }
        return rows.rows
    }

    private func executionContextWithoutExternalPageWindow(
        _ options: ReadExecutionContext
    ) -> ReadExecutionContext {
        ReadExecutionContext(
            options: options.options.withoutExternalPageWindow(),
            monotonicClock: container.monotonicClock,
            workMeter: options.workMeter,
            queryStructuralLimits: options.queryStructuralLimits
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

        var sourceRowBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: entities.count
        )
        for entity in entities {
            try options.workMeter.consume(at: .resultMaterialization)
            let row = try QueryRowCodec.encode(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                ]
            )
            let sourceRow = CanonicalSourceRow.fromBaseFields(
                row.fields,
                sourceName: nil,
                annotations: row.annotations,
                version: row.version
            )
            try sourceRowBuilder.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: sourceRow,
                    workMeter: options.workMeter
                ),
                make: { sourceRow }
            )
        }
        let sourceRows = sourceRowBuilder.finish().promoteToOutput()

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
                return try materializeSourceRows(
                    response.rows,
                    sourceName: alias,
                    workMeter: options.workMeter
                )
            }

            return try await materializeUnwindowedTableSourceRows(
                tableRef,
                options: options,
                transaction: nil
            )

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
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self),
                expectedCount: entities.count
            )
            for entity in entities {
                try options.workMeter.consume(at: .bindingCandidate)
                let row = try QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode)
                    ]
                )
                let sourceRow = CanonicalSourceRow.fromBaseFields(
                    row.fields,
                    sourceName: sourceName,
                    annotations: row.annotations,
                    version: row.version
                )
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: sourceRow,
                        workMeter: options.workMeter
                    ),
                    make: { sourceRow }
                )
            }
            return retained.finish().promoteToOutput()

        case .subquery(let query, let alias):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            return try materializeSourceRows(
                response.rows,
                sourceName: alias,
                workMeter: options.workMeter
            )

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
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self),
                expectedCount: rows.count
            )
            for values in rows {
                try options.workMeter.consume(at: .bindingCandidate)
                let names = columnNames ?? values.indices.map { "column\($0)" }
                guard names.count == values.count else {
                    throw CanonicalReadError.unsupportedSelectQuery("VALUES column count mismatch")
                }
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(values.count)
                for (name, literal) in zip(names, values) {
                    guard fields[name] == nil else {
                        throw CanonicalReadError.unsupportedSelectQuery(
                            "VALUES contains a duplicate column name"
                        )
                    }
                    fields[name] = try literal.toFieldValue()
                }
                let sourceRow = CanonicalSourceRow(fields: fields)
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: sourceRow,
                        workMeter: options.workMeter
                    ),
                    make: { sourceRow }
                )
            }
            return retained.finish().promoteToOutput()

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
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self),
                expectedCount: rows.count
            )
            for graphRow in rows {
                try options.workMeter.consume(at: .bindingCandidate)
                let sourceRow = canonicalGraphTableSourceRow(
                    from: graphRow.fields,
                    graphName: graphTableSource.graphName
                )
                let outputRow: CanonicalSourceRow
                if let columns = graphTableSource.columns, !columns.isEmpty {
                    var fields: [String: FieldValue] = [:]
                    fields.reserveCapacity(columns.count)
                    for column in columns {
                        fields[column.alias] = try evaluateExpression(
                            column.expression,
                            on: sourceRow
                        )
                    }
                    outputRow = CanonicalSourceRow(fields: fields)
                        .applyingAlias(graphTableSource.alias)
                } else {
                    outputRow = sourceRow.applyingAlias(graphTableSource.alias)
                }
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: outputRow,
                        workMeter: options.workMeter
                    ),
                    make: { outputRow }
                )
            }
            return retained.finish().promoteToOutput()

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
            return try materializeSourceRows(
                response.rows,
                sourceName: nil,
                workMeter: options.workMeter
            )

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
            let inputReservation = try reserveIntermediateRows(
                leftRows,
                and: rightRows,
                workMeter: workMeter,
                stage: .joinCandidate
            )
            defer { inputReservation.release() }
            var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: workMeter,
                stage: .joinCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self)
            )
            for left in leftRows {
                for right in rightRows {
                    try workMeter.consume(at: .joinCandidate)
                    let merged = left.merged(with: right)
                    try rows.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .footprint(of: merged, workMeter: workMeter),
                        make: { merged }
                    )
                }
            }
            return rows.finish().promoteToOutput()
        }

        let emptyLeft = CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: inferredEmptyScopes(from: leftRows)),
            scopedFields: inferredEmptyScopes(from: leftRows)
        )
        let emptyRight = CanonicalSourceRow(
            fields: CanonicalSourceRow.flatten(scopedFields: inferredEmptyScopes(from: rightRows)),
            scopedFields: inferredEmptyScopes(from: rightRows)
        )
        let inputReservation = try reserveIntermediateRows(
            leftRows,
            and: rightRows,
            workMeter: workMeter,
            stage: .joinCandidate
        )
        defer { inputReservation.release() }

        if let hashJoined = try performHashJoin(
            leftRows: leftRows,
            rightRows: rightRows,
            type: type,
            condition: condition,
            emptyLeft: emptyLeft,
            emptyRight: emptyRight,
            workMeter: workMeter
        ) {
            return hashJoined
        }

        let matchedSetBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let matchedSetReservation = try workMeter.reserveIntermediate(
            bytes: matchedSetBytes,
            at: .joinCandidate
        )
        defer { matchedSetReservation.release() }
        var matchedRightIndexes = Set<Int>()
        matchedRightIndexes.reserveCapacity(rightRows.count)
        var results = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .joinCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self)
        )

        for leftRow in leftRows {
            var matched = false
            for (rightIndex, rightRow) in rightRows.enumerated() {
                try workMeter.consume(at: .joinCandidate)
                if try joinMatches(left: leftRow, right: rightRow, condition: condition, joinType: type) {
                    matched = true
                    matchedRightIndexes.insert(rightIndex)
                    let merged = leftRow.merged(with: rightRow)
                    try results.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .footprint(of: merged, workMeter: workMeter),
                        make: { merged }
                    )
                }
            }

            if !matched, type == .left || type == .full {
                let merged = leftRow.merged(with: emptyRight)
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }

        if type == .right || type == .full {
            for (rightIndex, rightRow) in rightRows.enumerated() where !matchedRightIndexes.contains(rightIndex) {
                let merged = emptyLeft.merged(with: rightRow)
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }

        return results.finish().promoteToOutput()
    }

    private enum CanonicalJoinKeySource: Hashable {
        case column(ColumnRef)
        case unqualified(String)
    }

    private struct CanonicalHashJoinPlan {
        let left: [CanonicalJoinKeySource]
        let right: [CanonicalJoinKeySource]
        let validatesFullCondition: Bool
    }

    private struct CanonicalJoinKey: Hashable {
        let values: [FieldValue]
    }

    private func performHashJoin(
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow],
        type: JoinType,
        condition: JoinCondition?,
        emptyLeft: CanonicalSourceRow,
        emptyRight: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow]? {
        guard let plan = canonicalHashJoinPlan(
            condition: condition,
            leftRows: leftRows,
            rightRows: rightRows
        ), hashRepresentationsAreCompatible(
            plan: plan,
            leftRows: leftRows,
            rightRows: rightRows
        ) else {
            return nil
        }

        let keySlotBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<FieldValue?>.stride + 16))
        ).multiplied(by: UInt64(plan.left.count)).bytes
        let hashEntryBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalSourceRow>.stride + 64)
            )
        ).adding(
            DatabaseIntermediateFootprint(bytes: keySlotBytes)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
            )
        ).bytes
        let hashBytes = try DatabaseIntermediateFootprint(
            bytes: hashEntryBytes
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let hashReservation = try workMeter.reserveIntermediate(
            rows: UInt64(rightRows.count),
            bytes: hashBytes,
            at: .joinCandidate
        )
        defer { hashReservation.release() }
        var buckets: [CanonicalJoinKey: [(Int, CanonicalSourceRow)]] = [:]
        buckets.reserveCapacity(rightRows.count)
        for (index, row) in rightRows.enumerated() {
            try workMeter.consume(at: .joinCandidate)
            guard let key = canonicalJoinKey(
                sources: plan.right,
                row: row
            ) else { continue }
            buckets[key, default: []].append((index, row))
        }

        let matchedSetBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<Int>.stride + 16))
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let matchedSetReservation = try workMeter.reserveIntermediate(
            bytes: matchedSetBytes,
            at: .joinCandidate
        )
        defer { matchedSetReservation.release() }
        var matchedRight = Set<Int>()
        matchedRight.reserveCapacity(rightRows.count)
        var results = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .joinCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self)
        )
        for left in leftRows {
            try workMeter.consume(at: .joinCandidate)
            let matches = canonicalJoinKey(sources: plan.left, row: left)
                .flatMap { buckets[$0] } ?? []
            var matchedLeft = false
            for (rightIndex, right) in matches {
                try workMeter.consume(at: .joinCandidate)
                if plan.validatesFullCondition,
                   try !joinMatches(
                       left: left,
                       right: right,
                       condition: condition,
                       joinType: type
                   ) {
                    continue
                }
                matchedLeft = true
                matchedRight.insert(rightIndex)
                let merged = left.merged(with: right)
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
            if !matchedLeft, type == .left || type == .full {
                let merged = left.merged(with: emptyRight)
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }
        if type == .right || type == .full {
            for (index, right) in rightRows.enumerated()
                where !matchedRight.contains(index) {
                let merged = emptyLeft.merged(with: right)
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }
        return results.finish().promoteToOutput()
    }

    private func canonicalHashJoinPlan(
        condition: JoinCondition?,
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow]
    ) -> CanonicalHashJoinPlan? {
        switch condition {
        case .using(let columns) where !columns.isEmpty:
            let sources = columns.map(CanonicalJoinKeySource.unqualified)
            return CanonicalHashJoinPlan(
                left: sources,
                right: sources,
                validatesFullCondition: false
            )
        case .on(let expression):
            guard let leftSample = leftRows.first,
                  let rightSample = rightRows.first else {
                return nil
            }
            var pairs: [(ColumnRef, ColumnRef)] = []
            collectHashJoinColumnPairs(
                from: expression,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
            guard !pairs.isEmpty else { return nil }
            return CanonicalHashJoinPlan(
                left: pairs.map { .column($0.0) },
                right: pairs.map { .column($0.1) },
                validatesFullCondition: true
            )
        default:
            return nil
        }
    }

    private func collectHashJoinColumnPairs(
        from expression: Expression,
        leftSample: CanonicalSourceRow,
        rightSample: CanonicalSourceRow,
        into pairs: inout [(ColumnRef, ColumnRef)]
    ) {
        switch expression {
        case .equal(.column(let lhs), .column(let rhs)):
            let forward = leftSample.value(for: lhs) != nil
                && rightSample.value(for: rhs) != nil
            let reverse = leftSample.value(for: rhs) != nil
                && rightSample.value(for: lhs) != nil
            if forward != reverse {
                pairs.append(forward ? (lhs, rhs) : (rhs, lhs))
            }
        case .and(let lhs, let rhs):
            collectHashJoinColumnPairs(
                from: lhs,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
            collectHashJoinColumnPairs(
                from: rhs,
                leftSample: leftSample,
                rightSample: rightSample,
                into: &pairs
            )
        default:
            break
        }
    }

    private func hashRepresentationsAreCompatible(
        plan: CanonicalHashJoinPlan,
        leftRows: [CanonicalSourceRow],
        rightRows: [CanonicalSourceRow]
    ) -> Bool {
        for (leftSource, rightSource) in zip(plan.left, plan.right) {
            guard let left = firstNonNullJoinValue(
                source: leftSource,
                rows: leftRows
            ), let right = firstNonNullJoinValue(
                source: rightSource,
                rows: rightRows
            ) else {
                continue
            }
            guard hashRepresentationTag(left) == hashRepresentationTag(right),
                  hashRepresentationTag(left) != nil else {
                return false
            }
        }
        return true
    }

    private func firstNonNullJoinValue(
        source: CanonicalJoinKeySource,
        rows: [CanonicalSourceRow]
    ) -> FieldValue? {
        for row in rows {
            if let value = joinValue(source: source, row: row), value != .null {
                return value
            }
        }
        return nil
    }

    private func hashRepresentationTag(_ value: FieldValue) -> UInt8? {
        switch value {
        case .null: return nil
        case .bool: return 1
        case .int8: return 2
        case .int16: return 3
        case .int32: return 4
        case .int64: return 5
        case .uint8: return 6
        case .uint16: return 7
        case .uint32: return 8
        case .uint64: return 9
        case .float32: return 10
        case .float64: return 11
        case .decimal: return nil
        case .string: return 12
        case .bytes: return 13
        case .date: return 14
        case .time: return 15
        case .dateTime: return 16
        case .timestamp: return 17
        case .timeSpan: return 18
        case .calendarPeriod: return 19
        case .geographicPoint: return 20
        case .geographicPosition: return 21
        case .vector: return 22
        case .uuid: return 23
        case .array: return 24
        case .object: return 25
        case .reference: return 26
        case .rdfTerm: return 27
        }
    }

    private func canonicalJoinKey(
        sources: [CanonicalJoinKeySource],
        row: CanonicalSourceRow
    ) -> CanonicalJoinKey? {
        var values: [FieldValue] = []
        values.reserveCapacity(sources.count)
        for source in sources {
            guard let value = joinValue(source: source, row: row),
                  value != .null else {
                return nil
            }
            values.append(value)
        }
        return CanonicalJoinKey(values: values)
    }

    private func joinValue(
        source: CanonicalJoinKeySource,
        row: CanonicalSourceRow
    ) -> FieldValue? {
        switch source {
        case .column(let column):
            return row.value(for: column)
        case .unqualified(let column):
            return firstScopedFieldValue(named: column, in: row)
        }
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
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self)
        )
        for source in sources {
            let sourceRows = try await materializeRows(
                for: source,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode
            )
            let sourceReservation = try reserveIntermediateRows(
                sourceRows,
                workMeter: options.workMeter,
                stage: .bindingCandidate
            )
            defer { sourceReservation.release() }
            for row in sourceRows {
                try options.workMeter.consume(at: .bindingCandidate)
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: options.workMeter
                    ),
                    make: { row }
                )
            }
        }
        let rows = retained.finish().promoteToOutput()
        if deduplicate {
            return try uniqueSourceRows(rows, workMeter: options.workMeter)
        }
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
            let inputReservation = try reserveIntermediateRows(
                accumulator,
                and: next,
                workMeter: options.workMeter,
                stage: .joinCandidate
            )
            defer { inputReservation.release() }
            let setBytes = try DatabaseIntermediateFootprint(
                bytes: UInt64(max(1, MemoryLayout<QueryRow>.stride + 32))
            ).multiplied(by: UInt64(next.count)).bytes
            let setReservation = try options.workMeter.reserveIntermediate(
                rows: UInt64(next.count),
                bytes: setBytes,
                at: .deduplication
            )
            defer { setReservation.release() }
            var nextKeys = Set<QueryRow>()
            nextKeys.reserveCapacity(next.count)
            for row in next {
                try options.workMeter.consume(at: .deduplication)
                nextKeys.insert(identityRow(row))
            }
            var intersected = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: CanonicalSourceRow.self),
                expectedCount: accumulator.count
            )
            for row in accumulator {
                try options.workMeter.consume(at: .joinCandidate)
                guard nextKeys.contains(identityRow(row)) else { continue }
                try intersected.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: options.workMeter
                    ),
                    make: { row }
                )
            }
            accumulator = intersected.finish().promoteToOutput()
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
        let inputReservation = try reserveIntermediateRows(
            leftRows,
            and: rightRows,
            workMeter: options.workMeter,
            stage: .joinCandidate
        )
        defer { inputReservation.release() }
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<QueryRow>.stride + 32))
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let setReservation = try options.workMeter.reserveIntermediate(
            rows: UInt64(rightRows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var rightKeys = Set<QueryRow>()
        rightKeys.reserveCapacity(rightRows.count)
        for row in rightRows {
            try options.workMeter.consume(at: .deduplication)
            rightKeys.insert(identityRow(row))
        }
        var difference = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        for row in leftRows {
            try options.workMeter.consume(at: .joinCandidate)
            guard !rightKeys.contains(identityRow(row)) else { continue }
            try difference.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: options.workMeter
                ),
                make: { row }
            )
        }
        return try uniqueSourceRows(
            difference.finish().promoteToOutput(),
            workMeter: options.workMeter
        )
    }

    private func uniqueSourceRows(
        _ rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        let inputReservation = try reserveIntermediateRows(
            rows,
            workMeter: workMeter,
            stage: .deduplication
        )
        defer { inputReservation.release() }
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<QueryRow>.stride + 32))
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            rows: UInt64(rows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen = Set<QueryRow>()
        seen.reserveCapacity(rows.count)
        var unique = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            let key = identityRow(row)
            if seen.insert(key).inserted {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        return unique.finish().promoteToOutput()
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
        let inputReservation = try reserveIntermediateRows(
            rows,
            workMeter: workMeter,
            stage: .filterEvaluation
        )
        defer { inputReservation.release() }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .filterEvaluation,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .filterEvaluation)
            guard try evaluateBoolean(filter, on: row) else { continue }
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return retained.finish().promoteToOutput()
    }

    private func applyOrder(
        _ orderBy: [SortKey]?,
        to rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        let inputReservation = try reserveIntermediateRows(
            rows,
            workMeter: workMeter,
            stage: .sortInput
        )
        defer { inputReservation.release() }
        try workMeter.consume(UInt64(rows.count), at: .sortInput)
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        let sorted = try retained.finish().sortingElements { lhs, rhs in
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
            let lhsFingerprint = try CanonicalRowFingerprint.compute(
                QueryRow(
                    fields: lhs.fields,
                    annotations: lhs.annotations,
                    version: lhs.version
                ),
                workMeter: workMeter
            )
            let rhsFingerprint = try CanonicalRowFingerprint.compute(
                QueryRow(
                    fields: rhs.fields,
                    annotations: rhs.annotations,
                    version: rhs.version
                ),
                workMeter: workMeter
            )
            return lhsFingerprint.lexicographicallyPrecedes(rhsFingerprint)
        }
        return sorted.promoteToOutput()
    }

    private func projectRows(
        _ rows: [CanonicalSourceRow],
        projection: Projection,
        workMeter: DatabaseWorkMeter
    ) throws -> [QueryRow] {
        let inputReservation = try reserveIntermediateRows(
            rows,
            workMeter: workMeter,
            stage: .projection
        )
        defer { inputReservation.release() }
        var retained = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: QueryRow.self),
            expectedCount: rows.count
        )
        switch projection {
        case .all:
            for row in rows {
                try workMeter.consume(at: .projection)
                let projected = QueryRow(
                    fields: row.fields,
                    annotations: row.annotations,
                    version: row.version
                )
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: projected,
                        workMeter: workMeter
                    ),
                    make: { projected }
                )
            }

        case .allFrom(let sourceName):
            for row in rows {
                try workMeter.consume(at: .projection)
                guard let fields = row.fields(for: sourceName) else {
                    throw CanonicalReadError.unsupportedSelectQuery("Projection source '\(sourceName)' not found")
                }
                let projected = QueryRow(
                    fields: fields,
                    annotations: row.annotations,
                    version: row.version
                )
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: projected,
                        workMeter: workMeter
                    ),
                    make: { projected }
                )
            }

        case .items(let items):
            for row in rows {
                try workMeter.consume(at: .projection)
                var fields: [String: FieldValue] = [:]
                for (index, item) in items.enumerated() {
                    let fieldName = item.alias ?? canonicalProjectionName(for: item.expression, index: index)
                    fields[fieldName] = try evaluateExpression(item.expression, on: row)
                }
                let projected = QueryRow(
                    fields: fields,
                    annotations: row.annotations
                )
                try retained.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: projected,
                        workMeter: workMeter
                    ),
                    make: { projected }
                )
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
        return retained.finish().promoteToOutput()
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

        let inputReservation = try reserveIntermediateRows(
            rows,
            workMeter: workMeter,
            stage: .aggregateInput
        )
        defer { inputReservation.release() }

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
            if left == .null { return false }
            for expression in values {
                let value = try evaluateExpression(expression, on: row)
                guard value != .null else { continue }
                if try FieldValueComparator.equal(left, value) { return true }
            }
            return false
        case .notInList(let lhs, let values):
            let left = try evaluateExpression(lhs, on: row)
            if left == .null { return false }
            for expression in values {
                let value = try evaluateExpression(expression, on: row)
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
        let inputFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: rows,
            workMeter: workMeter
        )
        let inputReservation = try workMeter.reserveIntermediate(
            rows: inputFootprint.rows,
            bytes: inputFootprint.bytes,
            at: .deduplication
        )
        defer { inputReservation.release() }
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<QueryRow>.stride + 32))
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen: Set<QueryRow> = []
        seen.reserveCapacity(rows.count)
        var unique = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: QueryRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            if seen.insert(row).inserted {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        return unique.finish().promoteToOutput()
    }

    private func reserveIntermediateRows(
        _ rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateReservation {
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: rows,
            workMeter: workMeter
        )
        return try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
    }

    private func materializeSourceRows(
        _ rows: [QueryRow],
        sourceName: String?,
        workMeter: DatabaseWorkMeter
    ) throws -> [CanonicalSourceRow] {
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .bindingCandidate)
            let sourceRow = CanonicalSourceRow.fromBaseFields(
                row.fields,
                sourceName: sourceName,
                annotations: row.annotations,
                version: row.version
            )
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: sourceRow,
                    workMeter: workMeter
                ),
                make: { sourceRow }
            )
        }
        return retained.finish().promoteToOutput()
    }

    private func reserveIntermediateRows(
        _ first: [CanonicalSourceRow],
        and second: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateReservation {
        let firstFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: first,
            workMeter: workMeter
        )
        let secondFootprint = try CanonicalRelationalFootprintMeter.footprint(
            of: second,
            workMeter: workMeter
        )
        let footprint = try firstFootprint.adding(secondFootprint)
        return try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
    }
}
