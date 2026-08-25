import DatabaseKit
import DatabaseTypes
import StorageKit

struct CanonicalSourceRow: Sendable {
    let fields: [String: FieldValue]
    let unscopedFields: [String: FieldValue]
    let scopedFields: [String: [String: FieldValue]]
    let coalescedColumns: Set<String>
    let annotations: [String: FieldValue]
    let version: PersistableVersionToken?
    private let ambiguityOverride: Set<String>?

    init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.fields = fields
        self.unscopedFields = fields
        self.scopedFields = [:]
        self.coalescedColumns = []
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = nil
    }

    fileprivate init(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = [],
        annotations: [String: FieldValue],
        version: PersistableVersionToken?
    ) {
        self.fields = CanonicalSourceRow.flatten(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: coalescedColumns
        )
        self.unscopedFields = unscopedFields
        self.scopedFields = scopedFields
        self.coalescedColumns = coalescedColumns
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = nil
    }

    fileprivate init(
        materializedFields: [String: FieldValue],
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = [],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil,
        ambiguityOverride: Set<String>? = nil
    ) {
        self.fields = materializedFields
        self.unscopedFields = unscopedFields
        self.scopedFields = scopedFields
        self.coalescedColumns = coalescedColumns
        self.annotations = annotations
        self.version = version
        self.ambiguityOverride = ambiguityOverride
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
            unscopedFields: [:],
            scopedFields: [sourceName: fields],
            coalescedColumns: [],
            annotations: annotations,
            version: version
        )
    }

    func applyingAlias(_ alias: String?) -> CanonicalSourceRow {
        guard let alias else { return self }
        return CanonicalSourceRow(
            unscopedFields: [:],
            scopedFields: [alias: wildcardFields],
            coalescedColumns: [],
            annotations: annotations,
            version: version
        )
    }

    func merged(with other: CanonicalSourceRow) throws -> CanonicalSourceRow {
        let duplicateUnscopedColumns = Set(unscopedFields.keys)
            .intersection(other.unscopedFields.keys)
        guard duplicateUnscopedColumns.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN inputs contain duplicate unqualified columns: \(duplicateUnscopedColumns.sorted().joined(separator: ", "))"
            )
        }
        let duplicateScopes = Set(scopedFields.keys)
            .intersection(other.scopedFields.keys)
        guard duplicateScopes.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN inputs require distinct source aliases: \(duplicateScopes.sorted().joined(separator: ", "))"
            )
        }
        return CanonicalSourceRow(
            unscopedFields: unscopedFields.merging(other.unscopedFields) {
                current, _ in current
            },
            scopedFields: scopedFields.merging(other.scopedFields) {
                current, _ in current
            },
            coalescedColumns: coalescedColumns.union(
                other.coalescedColumns
            ),
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

    var wildcardFields: [String: FieldValue] {
        CanonicalSourceRow.flattenWildcard(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: coalescedColumns
        )
    }

    var ambiguousUnqualifiedColumns: Set<String> {
        if let ambiguityOverride { return ambiguityOverride }
        var counts: [String: Int] = [:]
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }
        for key in unscopedFields.keys where !coalescedColumns.contains(key) {
            counts[key, default: 0] += 1
        }
        return Set(counts.compactMap {
            $0.value > 1 && !coalescedColumns.contains($0.key)
                ? $0.key
                : nil
        })
    }

    func overlaying(outer: CanonicalSourceRow?) -> CanonicalSourceRow {
        guard let outer else { return self }
        let localColumnNames = Set(unscopedFields.keys).union(
            scopedFields.values.reduce(into: Set<String>()) {
                $0.formUnion($1.keys)
            }
        )
        let mergedUnscopedFields = outer.unscopedFields.merging(
            unscopedFields
        ) { _, local in local }
        let mergedScopedFields = outer.scopedFields.merging(
            scopedFields
        ) { _, local in local }
        let mergedCoalescedColumns = outer.coalescedColumns
            .subtracting(localColumnNames)
            .union(coalescedColumns)
        var materializedFields = outer.fields
        materializedFields.merge(fields) { _, local in local }
        materializedFields.merge(
            CanonicalSourceRow.flatten(
                unscopedFields: mergedUnscopedFields,
                scopedFields: mergedScopedFields,
                coalescedColumns: mergedCoalescedColumns
            )
        ) { _, resolved in resolved }
        for column in localColumnNames {
            if ambiguousUnqualifiedColumns.contains(column) {
                materializedFields.removeValue(forKey: column)
            } else if let localValue = fields[column] {
                materializedFields[column] = localValue
            }
        }
        return CanonicalSourceRow(
            materializedFields: materializedFields,
            unscopedFields: mergedUnscopedFields,
            scopedFields: mergedScopedFields,
            coalescedColumns: mergedCoalescedColumns,
            annotations: outer.annotations.merging(annotations) {
                _, local in local
            },
            version: version,
            ambiguityOverride: ambiguousUnqualifiedColumns.union(
                outer.ambiguousUnqualifiedColumns.subtracting(
                    localColumnNames
                )
            )
        )
    }

    static func flatten(
        unscopedFields: [String: FieldValue] = [:],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String> = []
    ) -> [String: FieldValue] {
        var counts: [String: Int] = [:]
        for key in unscopedFields.keys {
            counts[key, default: 0] += 1
        }
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }

        var flattened: [String: FieldValue] = [:]
        for (key, value) in unscopedFields
            where counts[key] == 1 || coalescedColumns.contains(key) {
            flattened[key] = value
        }
        for (sourceName, sourceFields) in scopedFields {
            for (key, value) in sourceFields {
                flattened["\(sourceName).\(key)"] = value
                if counts[key] == 1 {
                    flattened[key] = value
                }
            }
        }
        return flattened
    }

    private static func flattenWildcard(
        unscopedFields: [String: FieldValue],
        scopedFields: [String: [String: FieldValue]],
        coalescedColumns: Set<String>
    ) -> [String: FieldValue] {
        var counts: [String: Int] = [:]
        for key in unscopedFields.keys {
            counts[key, default: 0] += 1
        }
        for fields in scopedFields.values {
            for key in fields.keys {
                counts[key, default: 0] += 1
            }
        }

        var flattened: [String: FieldValue] = [:]
        for (key, value) in unscopedFields
            where counts[key] == 1 || coalescedColumns.contains(key) {
            flattened[key] = value
        }
        for (sourceName, sourceFields) in scopedFields {
            for (key, value) in sourceFields
                where !coalescedColumns.contains(key) {
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

private struct CanonicalRelationScope: Sendable, Equatable {
    let name: String
    let columns: [String]
}

private struct CanonicalRelationSchema: Sendable, Equatable {
    let unscopedColumns: [String]
    let scopes: [CanonicalRelationScope]
    let coalescedColumns: Set<String>

    init(
        unscopedColumns: [String] = [],
        scopes: [CanonicalRelationScope] = [],
        coalescedColumns: Set<String> = []
    ) throws {
        guard Set(unscopedColumns).count == unscopedColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A relation contains duplicate unqualified column names"
            )
        }
        guard Set(scopes.map { $0.name }).count == scopes.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A relation contains duplicate source aliases"
            )
        }
        for scope in scopes where Set(scope.columns).count != scope.columns.count {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Source '\(scope.name)' contains duplicate column names"
            )
        }
        let scopedColumnNames = Set(scopes.flatMap { $0.columns })
        let unqualifiedScopeCollisions = Set(unscopedColumns)
            .intersection(scopedColumnNames)
            .subtracting(coalescedColumns)
        guard unqualifiedScopeCollisions.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A JOIN input without an alias collides with scoped columns: \(unqualifiedScopeCollisions.sorted().joined(separator: ", "))"
            )
        }
        guard coalescedColumns.isSubset(of: Set(unscopedColumns)) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Coalesced JOIN columns must be present in the unqualified schema"
            )
        }
        self.unscopedColumns = unscopedColumns
        self.scopes = scopes
        self.coalescedColumns = coalescedColumns
    }

    var visibleColumns: [String] {
        var counts: [String: Int] = [:]
        for column in unscopedColumns {
            counts[column, default: 0] += 1
        }
        for scope in scopes {
            for column in scope.columns {
                counts[column, default: 0] += 1
            }
        }

        var result = unscopedColumns.filter {
            counts[$0] == 1 || coalescedColumns.contains($0)
        }
        for scope in scopes {
            for column in scope.columns {
                if coalescedColumns.contains(column) { continue }
                result.append(counts[column] == 1 ? column : "\(scope.name).\(column)")
            }
        }
        return result
    }

    func occurrenceCount(of column: String) -> Int {
        if coalescedColumns.contains(column) { return 1 }
        return unscopedColumns.filter { $0 == column }.count
            + scopes.reduce(into: 0) { count, scope in
                count += scope.columns.filter { $0 == column }.count
            }
    }

    func applyingAlias(_ alias: String) throws -> CanonicalRelationSchema {
        return try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: alias, columns: visibleColumns)],
            coalescedColumns: []
        )
    }

    func merged(with other: CanonicalRelationSchema) throws -> CanonicalRelationSchema {
        let leftColumnNames = Set(
            unscopedColumns + scopes.flatMap { $0.columns }
        )
        let rightColumnNames = Set(
            other.unscopedColumns + other.scopes.flatMap { $0.columns }
        )
        return try CanonicalRelationSchema(
            unscopedColumns: unscopedColumns + other.unscopedColumns,
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns
                .subtracting(rightColumnNames)
                .union(
                    other.coalescedColumns.subtracting(leftColumnNames)
                )
        )
    }

    func merged(
        with other: CanonicalRelationSchema,
        coalescing columns: [String]
    ) throws -> CanonicalRelationSchema {
        let coalesced = Set(columns)
        let leftColumnNames = Set(
            unscopedColumns + scopes.flatMap { $0.columns }
        )
        let rightColumnNames = Set(
            other.unscopedColumns + other.scopes.flatMap { $0.columns }
        )
        return try CanonicalRelationSchema(
            unscopedColumns: unscopedColumns.filter {
                !coalesced.contains($0)
            } + other.unscopedColumns.filter {
                !coalesced.contains($0)
            } + columns,
            scopes: scopes + other.scopes,
            coalescedColumns: coalescedColumns
                .subtracting(rightColumnNames.subtracting(coalesced))
                .union(
                    other.coalescedColumns.subtracting(
                        leftColumnNames.subtracting(coalesced)
                    )
                )
                .union(coalesced)
        )
    }

    func nullRow() -> CanonicalSourceRow {
        let unscoped = Dictionary(
            uniqueKeysWithValues: unscopedColumns.map { ($0, FieldValue.null) }
        )
        let scoped = Dictionary(
            uniqueKeysWithValues: scopes.map { scope in
                (
                    scope.name,
                    Dictionary(
                        uniqueKeysWithValues: scope.columns.map {
                            ($0, FieldValue.null)
                        }
                    )
                )
            }
        )
        return CanonicalSourceRow(
            unscopedFields: unscoped,
            scopedFields: scoped,
            coalescedColumns: coalescedColumns,
            annotations: [:],
            version: nil
        )
    }
}

private typealias CanonicalRetainedRows =
    DatabaseSharedRetainedArray<CanonicalSourceRow>
private typealias CanonicalRetainedGroups =
    DatabaseSharedRetainedArray<CanonicalGroupedRow>
typealias CanonicalRetainedQueryRows =
    DatabaseSharedRetainedArray<QueryRow>

struct CanonicalRetainedQueryResponse: Sendable {
    let rows: CanonicalRetainedQueryRows
    let visibleRange: Range<Int>
    let continuation: QueryContinuation?
    let metadata: [String: FieldValue]
    let affectedRows: Int?

    var visibleRows: DatabaseSharedRetainedArrayView<QueryRow> {
        rows.boundedView(visibleRange)
    }

    consuming func promoteToPublicResponse() -> QueryResponse {
        let visibleRange = visibleRange
        let continuation = continuation
        let metadata = metadata
        let affectedRows = affectedRows
        guard !visibleRange.isEmpty else {
            return QueryResponse(
                rows: [],
                continuation: continuation,
                metadata: metadata,
                affectedRows: affectedRows
            )
        }

        var outputRows = rows.promoteToOutput()
        if visibleRange.upperBound < outputRows.count {
            outputRows.removeLast(outputRows.count - visibleRange.upperBound)
        }
        if visibleRange.lowerBound > 0 {
            outputRows.removeFirst(visibleRange.lowerBound)
        }
        return QueryResponse(
            rows: outputRows,
            continuation: continuation,
            metadata: metadata,
            affectedRows: affectedRows
        )
    }
}

private struct CanonicalRelation: Sendable {
    let schema: CanonicalRelationSchema
    let rows: CanonicalRetainedRows
}

private struct CanonicalGroupKey: Sendable {
    let values: [FieldValue]
    let identity: [FieldValue]
}

private struct CanonicalGroupedRow: Sendable {
    let key: CanonicalGroupKey
    let representative: CanonicalSourceRow
    let rows: CanonicalRetainedRows
}

private struct CanonicalRowValueIdentity: Sendable, Hashable {
    let fields: [String: FieldValue]
}

func canonicalQueryRequiresAggregation(_ query: SelectQuery) -> Bool {
    if query.groupBy != nil { return true }
    if query.having != nil { return true }
    if query.orderBy?.contains(
        where: { canonicalExpressionContainsAggregate($0.expression) }
    ) == true {
        return true
    }
    switch query.projection {
    case .all, .allFrom:
        return false
    case .items(let items), .distinctItems(let items):
        return items.contains {
            canonicalExpressionContainsAggregate($0.expression)
        }
    }
}

private func canonicalExpressionContainsAggregate(
    _ expression: Expression
) -> Bool {
    switch expression {
    case .aggregate:
        return true
    case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
            .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
            .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
            .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
            .lessThanOrEqual(let lhs, let rhs),
            .greaterThan(let lhs, let rhs),
            .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
            .or(let lhs, let rhs), .nullIf(let lhs, let rhs):
        return canonicalExpressionContainsAggregate(lhs)
            || canonicalExpressionContainsAggregate(rhs)
    case .negate(let nested), .not(let nested), .isNull(let nested),
            .isNotNull(let nested), .like(let nested, _),
            .regex(let nested, _, _), .cast(let nested, _),
            .isTriple(let nested), .subject(let nested),
            .predicate(let nested), .object(let nested):
        return canonicalExpressionContainsAggregate(nested)
    case .between(let value, let low, let high):
        return canonicalExpressionContainsAggregate(value)
            || canonicalExpressionContainsAggregate(low)
            || canonicalExpressionContainsAggregate(high)
    case .inList(let value, let values), .notInList(let value, let values):
        return canonicalExpressionContainsAggregate(value)
            || values.contains(where: canonicalExpressionContainsAggregate)
    case .inSubquery(let value, _):
        return canonicalExpressionContainsAggregate(value)
    case .function(let function):
        return function.arguments.contains(
            where: canonicalExpressionContainsAggregate
        )
    case .caseWhen(let pairs, let fallback):
        return pairs.contains {
            canonicalExpressionContainsAggregate($0.condition)
                || canonicalExpressionContainsAggregate($0.result)
        } || fallback.map(canonicalExpressionContainsAggregate) == true
    case .coalesce(let values):
        return values.contains(where: canonicalExpressionContainsAggregate)
    case .triple(let subject, let predicate, let object):
        return canonicalExpressionContainsAggregate(subject)
            || canonicalExpressionContainsAggregate(predicate)
            || canonicalExpressionContainsAggregate(object)
    case .literal, .column, .variable, .parameter, .bound, .subquery,
            .exists:
        return false
    }
}

private func canonicalComparisonReadError(
    _ failure: FieldValueComparisonError,
    operation: String
) -> CanonicalReadError {
    switch failure {
    case .incomparable:
        return .expressionEvaluation(.typeMismatch(operation: operation))
    case .unorderedFloatingPoint:
        return .expressionEvaluation(.numericOverflow)
    }
}

private enum CanonicalPartitionRoutingMode: Sendable {
    case strict
    case routed
}

private struct CanonicalQueryEvaluationContext: Sendable {
    let options: ReadExecutionContext
    let transaction: any TransactionAccess
    let partitionValues: FieldObject?
    let partitionMode: CanonicalPartitionRoutingMode
    let namedSubqueries: [NamedSubquery]
    let outerRow: CanonicalSourceRow?
    let preparedFusionGraph: FusionPreparedQueryGraph
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
        let response = try await queryRetained(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
        return response.promoteToPublicResponse()
    }

    func queryRetained(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> CanonicalRetainedQueryResponse {
        do {
            return try await queryRetainedUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions
            )
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
    }

    private func queryRetainedUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject
    ) async throws -> CanonicalRetainedQueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
        return try await withDataOperation { [self] in
            let resolvedFusionGraph = try FusionPreflight.resolveGraph(
                selectQuery,
                context: self,
                workMeter: execution.workMeter
            )
            let readExecution = CanonicalReadExecution.resolve(
                requested: execution.consistency,
                default: .serializable
            )
            let authorizationPlan = DatabaseFieldReadAuthorizationPlan.make(
                query: selectQuery,
                schema: container.schema
            ).merging(
                resolvedFusionGraph.authorizationPlan
            )
            return try await withFieldReadAuthorization(authorizationPlan) {
                let preparedFusionGraph = try FusionPreflight.prepareGraph(
                    selectQuery,
                    resolvedFusionGraph,
                    context: self,
                    workMeter: execution.workMeter
                )
                return try await withStorageAccess(
                    requiredAccess: .read,
                    configuration: readExecution.transactionConfiguration
                ) { [self] transaction in
                    try await queryCanonical(
                        selectQuery,
                        options: execution,
                        partitionValues: graphPartitions,
                        partitionMode: .strict,
                        transaction: transaction,
                        preparedFusionGraph: preparedFusionGraph
                    )
                }
            }
        }
    }

    /// Runs a preflighted relational Fusion input on the caller-owned
    /// transaction without promoting its retained rows to a public response.
    func executeFusionRelationalRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph
    ) async throws -> CanonicalRetainedQueryResponse {
        let internalOptions = executionContextWithoutExternalPageWindow(options)
        return try await queryCanonical(
            selectQuery,
            options: internalOptions,
            partitionValues: FieldObject(),
            partitionMode: .strict,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph
        )
    }

    /// Applies canonical relational semantics to Engine-owned candidates. The
    /// materialized row domain never crosses into a feature module.
    func executeFusionCandidateRelationalRows(
        _ candidates: FusionCandidateDomain,
        query selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph
    ) async throws -> CanonicalRetainedQueryResponse {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A Fusion candidate input must use a table source"
            )
        }
        let internalOptions = executionContextWithoutExternalPageWindow(options)
        let sourceName = tableRef.alias ?? tableRef.effectiveName
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: internalOptions.workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: candidates.count
        )
        try candidates.forEachEntry { entry in
            try internalOptions.workMeter.consume(at: .bindingCandidate)
            let rowFootprint = try CanonicalRelationalFootprintMeter
                .footprint(of: entry.row, workMeter: internalOptions.workMeter)
            let footprint = try rowFootprint.adding(rowFootprint).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(sourceName.utf8.count) + 64
                )
            )
            try builder.append(footprint: footprint) {
                CanonicalSourceRow.fromBaseFields(
                    entry.row.fields,
                    sourceName: sourceName,
                    annotations: entry.row.annotations,
                    version: entry.row.version
                )
            }
        }
        let sourceRows = try builder.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows,
            sourceSchema: try tableRelationSchema(tableRef),
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: internalOptions,
            evaluationContext: CanonicalQueryEvaluationContext(
                options: internalOptions,
                transaction: transaction,
                partitionValues: FieldObject(),
                partitionMode: .strict,
                namedSubqueries: try mergeNamedSubqueries(
                    local: selectQuery.subqueries ?? [],
                    inherited: []
                ),
                outerRow: nil,
                preparedFusionGraph: preparedFusionGraph
            )
        )
    }

    /// Resolves relational bindings before Fusion opens a physical index.
    func validateFusionRelationalInput(
        _ selectQuery: SelectQuery
    ) throws {
        guard case .table(let tableRef) = selectQuery.source else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A Fusion relational input must use a table source"
            )
        }
        try validateRelationalQueryBindings(
            selectQuery,
            sourceSchema: try tableRelationSchema(tableRef),
            outerRow: nil
        )
    }

    @_spi(DatabaseExecution)
    public func executeCanonicalQuery(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        try await query(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions
        )
    }

    /// Executes a Base-local read through the active storage transaction.
    ///
    /// The transaction argument preserves the source-executor API shape, but
    /// it is not an authority boundary. The TaskLocal execution binding is the
    /// only transaction admitted below, so an unrelated argument cannot create
    /// a mixed snapshot or rebind the read to another Base.
    @_spi(DatabaseExecution)
    public func query(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        transaction _: any TransactionAccess
    ) async throws -> QueryResponse {
        do {
            return try await queryTransactionBoundUnmapped(
                selectQuery,
                execution: execution,
                graphPartitions: graphPartitions
            )
        } catch {
            throw sanitizedFusionExecutionError(error)
        }
    }

    private func queryTransactionBoundUnmapped(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject
    ) async throws -> QueryResponse {
        try QueryStructuralValidator.validate(
            selectQuery,
            limits: execution.queryStructuralLimits
        )
        guard let binding = ActiveDatabaseTransactionContext.binding else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try binding.validate(for: self)
        #if DATABASE_MULTI_BASE
        guard binding.resource == self.resource,
              binding.authorization == self.authorization,
              binding.grantedAccess.isSuperset(of: .read) else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: self.resource,
                required: .read
            )
        }
        #endif
        let admittedTransaction = ReadAuthorizedTransactionAccess.admitted(
            binding.transaction
        )
        return try await withDataOperation { [self] in
            let resolvedFusionGraph = try FusionPreflight.resolveGraph(
                selectQuery,
                context: self,
                workMeter: execution.workMeter
            )
            let authorizationPlan = DatabaseFieldReadAuthorizationPlan.make(
                query: selectQuery,
                schema: container.schema
            ).merging(
                resolvedFusionGraph.authorizationPlan
            )
            return try await withFieldReadAuthorization(authorizationPlan) {
                let preparedFusionGraph = try FusionPreflight.prepareGraph(
                    selectQuery,
                    resolvedFusionGraph,
                    context: self,
                    workMeter: execution.workMeter
                )
                #if DATABASE_MULTI_BASE
                _ = try requireOperationDataRoot()
                let executionBinding = try DatabaseTransactionExecutionBinding(
                    context: self,
                    transaction: admittedTransaction,
                    grantedAccess: .read,
                    databaseTransaction: nil
                )
                #else
                let executionBinding = try DatabaseTransactionExecutionBinding(
                    context: self,
                    transaction: admittedTransaction,
                    databaseTransaction: nil
                )
                #endif
                return try await ActiveDatabaseTransactionContext.$binding
                    .withValue(executionBinding) {
                        try await executeTransactionBoundCanonicalQuery(
                            selectQuery,
                            options: execution,
                            graphPartitions: graphPartitions,
                            transaction: admittedTransaction,
                            preparedFusionGraph: preparedFusionGraph
                        )
                    }
            }
        }
    }

    @_spi(DatabaseExecution)
    public func executeCanonicalQuery(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject(),
        transaction: any TransactionAccess
    ) async throws -> QueryResponse {
        try await query(
            selectQuery,
            execution: execution,
            graphPartitions: graphPartitions,
            transaction: transaction
        )
    }

    private func executeTransactionBoundCanonicalQuery(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        graphPartitions: FieldObject,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph
    ) async throws -> QueryResponse {
        let response = try await queryCanonical(
            selectQuery,
            options: options,
            partitionValues: graphPartitions,
            partitionMode: .strict,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph
        )
        return response.promoteToPublicResponse()
    }

    private func mergeNamedSubqueries(
        local: [NamedSubquery],
        inherited: [NamedSubquery]
    ) throws -> [NamedSubquery] {
        guard Set(local.map { $0.name }).count == local.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A WITH clause contains duplicate common table expression names"
            )
        }
        try validateAcyclicNamedSubqueries(local)
        let localNames = Set(local.map { $0.name })
        return local + inherited.filter { !localNames.contains($0.name) }
    }

    private func validateAcyclicNamedSubqueries(
        _ subqueries: [NamedSubquery]
    ) throws {
        let names = Set(subqueries.map { $0.name })
        let dependencies = Dictionary(
            uniqueKeysWithValues: subqueries.map { subquery in
                (
                    subquery.name,
                    referencedTableNames(
                        in: subquery.query,
                        among: names
                    )
                )
            }
        )
        var visiting = Set<String>()
        var visited = Set<String>()

        func visit(_ name: String) throws {
            if visiting.contains(name) {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Recursive common table expression '\(name)' is not supported"
                )
            }
            guard !visited.contains(name) else { return }
            visiting.insert(name)
            for dependency in dependencies[name, default: []] {
                try visit(dependency)
            }
            visiting.remove(name)
            visited.insert(name)
        }

        for name in names {
            try visit(name)
        }
    }

    private func referencedTableNames(
        in query: SelectQuery,
        among candidateNames: Set<String>
    ) -> Set<String> {
        var names = Set<String>()
        let localNames = Set(query.subqueries?.map { $0.name } ?? [])
        let visibleCandidates = candidateNames.subtracting(localNames)

        func collect(_ aggregate: AggregateFunction) {
            switch aggregate {
            case .count(let expression, _):
                if let expression { collect(expression) }
            case .sum(let expression, _), .avg(let expression, _),
                    .min(let expression), .max(let expression),
                    .groupConcat(let expression, _, _),
                    .sample(let expression):
                collect(expression)
            case .arrayAgg(let expression, let orderBy, _):
                collect(expression)
                for sortKey in orderBy ?? [] {
                    collect(sortKey.expression)
                }
            }
        }

        func collect(_ expression: Expression) {
            switch expression {
            case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                    .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                    .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                    .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                    .lessThanOrEqual(let lhs, let rhs),
                    .greaterThan(let lhs, let rhs),
                    .greaterThanOrEqual(let lhs, let rhs),
                    .and(let lhs, let rhs), .or(let lhs, let rhs),
                    .nullIf(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            case .negate(let operand), .not(let operand),
                    .isNull(let operand), .isNotNull(let operand),
                    .like(let operand, _), .regex(let operand, _, _),
                    .cast(let operand, _), .isTriple(let operand),
                    .subject(let operand), .predicate(let operand),
                    .object(let operand):
                collect(operand)
            case .between(let operand, let lower, let upper):
                collect(operand)
                collect(lower)
                collect(upper)
            case .inList(let operand, let values),
                    .notInList(let operand, let values):
                collect(operand)
                values.forEach(collect)
            case .inSubquery(let operand, let subquery):
                collect(operand)
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .aggregate(let aggregate):
                collect(aggregate)
            case .function(let function):
                function.arguments.forEach(collect)
            case .caseWhen(let cases, let elseResult):
                for pair in cases {
                    collect(pair.condition)
                    collect(pair.result)
                }
                if let elseResult { collect(elseResult) }
            case .coalesce(let expressions):
                expressions.forEach(collect)
            case .triple(let subject, let predicate, let object):
                collect(subject)
                collect(predicate)
                collect(object)
            case .subquery(let subquery), .exists(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .literal, .column, .variable, .parameter, .bound:
                break
            }
        }

        func collect(_ path: PathPattern) {
            for element in path.elements {
                switch element {
                case .node(let node):
                    for property in node.properties ?? [] {
                        collect(property.value)
                    }
                case .edge(let edge):
                    for property in edge.properties ?? [] {
                        collect(property.value)
                    }
                case .quantified(let nested, _):
                    collect(nested)
                case .alternation(let alternatives):
                    alternatives.forEach(collect)
                }
            }
        }

        func collect(_ pattern: GraphPattern) {
            switch pattern {
            case .join(let lhs, let rhs), .optional(let lhs, let rhs),
                    .union(let lhs, let rhs), .minus(let lhs, let rhs),
                    .lateral(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            case .filter(let pattern, let expression):
                collect(pattern)
                collect(expression)
            case .graph(_, let pattern), .service(_, let pattern, _):
                collect(pattern)
            case .bind(let pattern, _, let expression):
                collect(pattern)
                collect(expression)
            case .subquery(let subquery):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .groupBy(let pattern, let expressions, let aggregates):
                collect(pattern)
                expressions.forEach(collect)
                aggregates.forEach { collect($0.aggregate) }
            case .basic, .values:
                break
            }
        }

        func collect(_ source: DataSource) {
            switch source {
            case .table(let table):
                if visibleCandidates.contains(table.table) {
                    names.insert(table.table)
                }
            case .subquery(let subquery, _):
                names.formUnion(
                    referencedTableNames(
                        in: subquery,
                        among: visibleCandidates
                    )
                )
            case .join(let join):
                collect(join.left)
                collect(join.right)
                if case .on(let expression) = join.condition {
                    collect(expression)
                }
            case .union(let sources), .unionAll(let sources),
                    .intersect(let sources):
                sources.forEach(collect)
            case .except(let lhs, let rhs):
                collect(lhs)
                collect(rhs)
            #if DATABASE_MULTI_BASE
            case .base(_, let source):
                collect(source)
            #endif
            case .graphTable(let graphTable):
                graphTable.matchPattern.paths.forEach(collect)
                if let filter = graphTable.matchPattern.where {
                    collect(filter)
                }
                for column in graphTable.columns ?? [] {
                    collect(column.expression)
                }
            case .graphPattern(let pattern),
                    .namedGraph(_, let pattern),
                    .service(_, let pattern, _):
                collect(pattern)
            case .logical, .values:
                break
            }
        }

        for subquery in query.subqueries ?? [] {
            names.formUnion(
                referencedTableNames(
                    in: subquery.query,
                    among: visibleCandidates
                )
            )
        }
        collect(query.source)
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            items.forEach { collect($0.expression) }
        case .all, .allFrom:
            break
        }
        if let filter = query.filter { collect(filter) }
        for expression in query.groupBy ?? [] { collect(expression) }
        if let having = query.having { collect(having) }
        for sortKey in query.orderBy ?? [] { collect(sortKey.expression) }
        return names
    }

    private func withFieldReadAuthorization<Result: Sendable>(
        _ plan: DatabaseFieldReadAuthorizationPlan,
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try authorizeFieldReads(plan)
        return try await RequestFieldAuthorization.$fieldsByEntity.withValue(
            plan.fieldsByEntity
        ) {
            try await operation()
        }
    }

    private func validateRelationalQueryBindings(
        _ query: SelectQuery,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery] = []
    ) throws {
        _ = try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        )
        switch query.projection {
        case .items(let items), .distinctItems(let items):
            for item in items {
                try validateExpressionBindings(
                    item.expression,
                    sourceSchema: sourceSchema,
                    outerRow: outerRow,
                    namedSubqueries: namedSubqueries
                )
            }
        case .all, .allFrom:
            break
        }
        if let filter = query.filter {
            try validateExpressionBindings(
                filter,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        for expression in query.groupBy ?? [] {
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        if let having = query.having {
            try validateExpressionBindings(
                having,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        for sortKey in query.orderBy ?? [] {
            let expression = groupedOrderExpression(
                sortKey.expression,
                projection: query.projection
            )
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }

        guard canonicalQueryRequiresAggregation(query) else { return }
        let groupBy = query.groupBy ?? []
        let fullSourceRow = sourceSchema.nullRow()
        let currentColumnNames = Set(sourceSchema.unscopedColumns).union(
            sourceSchema.scopes.flatMap { $0.columns }
        )
        let maskedParentRow = outerRow.map { parent in
            CanonicalSourceRow(
                materializedFields: parent.fields,
                unscopedFields: parent.unscopedFields,
                scopedFields: parent.scopedFields,
                coalescedColumns: parent.coalescedColumns,
                annotations: parent.annotations,
                version: parent.version,
                ambiguityOverride: parent.ambiguousUnqualifiedColumns.union(
                    currentColumnNames
                )
            )
        }
        let groupedOuterRow = groupedOuterScope(
            sourceRow: fullSourceRow,
            groupBy: groupBy
        ).overlaying(outer: maskedParentRow)

        func validateGrouped(_ expression: Expression) throws {
            guard !groupBy.contains(expression) else { return }
            try validateGroupedSubqueryBindings(
                expression,
                groupedOuterRow: groupedOuterRow,
                fullSourceRow: fullSourceRow,
                namedSubqueries: namedSubqueries
            )
        }

        switch query.projection {
        case .items(let items), .distinctItems(let items):
            for item in items { try validateGrouped(item.expression) }
        case .all, .allFrom:
            break
        }
        if let having = query.having {
            try validateGrouped(having)
        }
        for sortKey in query.orderBy ?? [] {
            try validateGrouped(
                groupedOrderExpression(
                    sortKey.expression,
                    projection: query.projection
                )
            )
        }
    }

    private func validateGroupedSubqueryBindings(
        _ expression: Expression,
        groupedOuterRow: CanonicalSourceRow,
        fullSourceRow: CanonicalSourceRow,
        namedSubqueries: [NamedSubquery]
    ) throws {
        func validate(_ nested: Expression) throws {
            try validateGroupedSubqueryBindings(
                nested,
                groupedOuterRow: groupedOuterRow,
                fullSourceRow: fullSourceRow,
                namedSubqueries: namedSubqueries
            )
        }

        func validateNested(_ query: SelectQuery) throws {
            do {
                _ = try validateNestedQueryBindings(
                    query,
                    outerSchema: try CanonicalRelationSchema(),
                    outerRow: groupedOuterRow,
                    namedSubqueries: namedSubqueries
                )
            } catch CanonicalReadError.expressionEvaluation(
                .missingColumn(let column)
            ) where sourceRow(fullSourceRow, containsColumnNamed: column) {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Correlated column '\(column)' is neither grouped nor aggregated"
                    )
                )
            } catch CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(let column)
            ) where sourceRow(fullSourceRow, containsColumnNamed: column) {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Correlated column '\(column)' is neither grouped nor aggregated"
                    )
                )
            }
        }

        switch expression {
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs),
                .and(let lhs, let rhs), .or(let lhs, let rhs),
                .nullIf(let lhs, let rhs):
            try validate(lhs)
            try validate(rhs)
        case .negate(let nested), .not(let nested), .isNull(let nested),
                .isNotNull(let nested), .like(let nested, _),
                .regex(let nested, _, _), .cast(let nested, _),
                .isTriple(let nested), .subject(let nested),
                .predicate(let nested), .object(let nested):
            try validate(nested)
        case .between(let nested, let lower, let upper):
            try validate(nested)
            try validate(lower)
            try validate(upper)
        case .inList(let nested, let values),
                .notInList(let nested, let values):
            try validate(nested)
            for value in values { try validate(value) }
        case .inSubquery(let nested, let query):
            try validate(nested)
            try validateNested(query)
        case .function(let function):
            for argument in function.arguments { try validate(argument) }
        case .caseWhen(let pairs, let fallback):
            for pair in pairs {
                try validate(pair.condition)
                try validate(pair.result)
            }
            if let fallback { try validate(fallback) }
        case .coalesce(let values):
            for value in values { try validate(value) }
        case .triple(let subject, let predicate, let object):
            try validate(subject)
            try validate(predicate)
            try validate(object)
        case .subquery(let query), .exists(let query):
            try validateNested(query)
        case .aggregate:
            // Aggregate arguments are evaluated against each source row, not
            // against the grouped representative.
            return
        case .literal, .column, .variable, .parameter, .bound:
            return
        }
    }

    private func sourceRow(
        _ row: CanonicalSourceRow,
        containsColumnNamed name: String
    ) -> Bool {
        if row.fields[name] != nil
            || row.ambiguousUnqualifiedColumns.contains(name) {
            return true
        }
        return row.scopedFields.contains { sourceName, fields in
            fields.keys.contains { "\(sourceName).\($0)" == name }
        }
    }

    private func groupedOuterScope(
        sourceRow: CanonicalSourceRow,
        groupBy: [Expression]
    ) -> CanonicalSourceRow {
        var unscopedFields: [String: FieldValue] = [:]
        // Preserve empty current scopes so a same-named ancestor scope cannot
        // become visible when this aggregate has no grouped column for it.
        var scopedFields = Dictionary(
            uniqueKeysWithValues: sourceRow.scopedFields.keys.map {
                ($0, [String: FieldValue]())
            }
        )

        for expression in groupBy {
            guard case .column(let column) = expression,
                  let value = sourceRow.value(for: column) else {
                continue
            }
            if let table = column.table {
                scopedFields[table, default: [:]][column.column] = value
                continue
            }
            if sourceRow.unscopedFields[column.column] != nil {
                unscopedFields[column.column] = value
                continue
            }
            let matchingScopes = sourceRow.scopedFields.compactMap {
                sourceName, fields in
                fields[column.column] == nil ? nil : sourceName
            }
            if matchingScopes.count == 1, let sourceName = matchingScopes.first {
                scopedFields[sourceName, default: [:]][column.column] = value
            }
        }

        return CanonicalSourceRow(
            unscopedFields: unscopedFields,
            scopedFields: scopedFields,
            coalescedColumns: sourceRow.coalescedColumns.intersection(
                unscopedFields.keys
            ),
            annotations: [:],
            version: nil
        )
    }

    private func validateExpressionBindings(
        _ expression: Expression,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery] = []
    ) throws {
        func validate(_ nested: Expression) throws {
            try validateExpressionBindings(
                nested,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }

        switch expression {
        case .column(let column):
            try validateColumnBinding(
                column,
                sourceSchema: sourceSchema,
                outerRow: outerRow
            )
        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
                .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
                .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs),
                .and(let lhs, let rhs), .or(let lhs, let rhs),
                .nullIf(let lhs, let rhs):
            try validate(lhs)
            try validate(rhs)
        case .negate(let operand), .not(let operand),
                .isNull(let operand), .isNotNull(let operand),
                .like(let operand, _), .regex(let operand, _, _),
                .cast(let operand, _), .isTriple(let operand),
                .subject(let operand), .predicate(let operand),
                .object(let operand):
            try validate(operand)
        case .between(let operand, let lower, let upper):
            try validate(operand)
            try validate(lower)
            try validate(upper)
        case .inList(let operand, let values),
                .notInList(let operand, let values):
            try validate(operand)
            for value in values { try validate(value) }
        case .inSubquery(let operand, let query):
            try validate(operand)
            let columnCount = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidMembershipSubquery(
                    columnCount: columnCount
                )
            }
        case .aggregate(let aggregate):
            try validateAggregateBindings(
                aggregate,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        case .function(let function):
            for argument in function.arguments { try validate(argument) }
        case .caseWhen(let cases, let elseResult):
            for pair in cases {
                try validate(pair.condition)
                try validate(pair.result)
            }
            if let elseResult { try validate(elseResult) }
        case .coalesce(let expressions):
            for expression in expressions { try validate(expression) }
        case .triple(let subject, let predicate, let object):
            try validate(subject)
            try validate(predicate)
            try validate(object)
        case .subquery(let query):
            let columnCount = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: nil,
                    columnCount: columnCount
                )
            }
        case .exists(let query):
            _ = try validateNestedQueryBindings(
                query,
                outerSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        case .literal, .variable, .parameter, .bound:
            break
        }
    }

    private func validateAggregateBindings(
        _ aggregate: AggregateFunction,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery]
    ) throws {
        func validate(_ expression: Expression) throws {
            try validateExpressionBindings(
                expression,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
        }
        switch aggregate {
        case .count(let expression, _):
            if let expression { try validate(expression) }
        case .sum(let expression, _), .avg(let expression, _),
                .min(let expression), .max(let expression),
                .groupConcat(let expression, _, _),
                .sample(let expression):
            try validate(expression)
        case .arrayAgg(let expression, let orderBy, _):
            try validate(expression)
            for sortKey in orderBy ?? [] {
                try validate(sortKey.expression)
            }
        }
    }

    private func validateNestedQueryBindings(
        _ query: SelectQuery,
        outerSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?,
        namedSubqueries: [NamedSubquery]
    ) throws -> Int {
        let visibleNamedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: namedSubqueries
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: visibleNamedSubqueries
        ) {
            switch query.projection {
            case .items, .distinctItems:
                return try canonicalProjectionColumns(
                    query.projection,
                    sourceSchema: CanonicalRelationSchema()
                ).count
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare exactly one output column"
                )
            }
        }
        let sourceSchema = try canonicalRelationSchema(
            for: query.source,
            namedSubqueries: visibleNamedSubqueries
        )
        let syntheticOuterRow = outerSchema.nullRow().overlaying(
            outer: outerRow
        )
        try validateRelationalQueryBindings(
            query,
            sourceSchema: sourceSchema,
            outerRow: syntheticOuterRow,
            namedSubqueries: visibleNamedSubqueries
        )
        try validateStaticJoinBindings(
            query.source,
            namedSubqueries: visibleNamedSubqueries,
            outerRow: syntheticOuterRow
        )
        return try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        ).count
    }

    private func validateColumnBinding(
        _ column: ColumnRef,
        sourceSchema: CanonicalRelationSchema,
        outerRow: CanonicalSourceRow?
    ) throws {
        if let table = column.table {
            if let scope = sourceSchema.scopes.first(
                where: { $0.name == table }
            ) {
                guard scope.columns.contains(column.column) else {
                    throw CanonicalReadError.expressionEvaluation(
                        .missingColumn(column.displayName)
                    )
                }
                return
            }
            guard outerRow?.scopedFields[table]?[column.column] != nil else {
                throw CanonicalReadError.expressionEvaluation(
                    .missingColumn(column.displayName)
                )
            }
            return
        }

        let occurrenceCount = sourceSchema.occurrenceCount(
            of: column.column
        )
        if occurrenceCount == 1 { return }
        if occurrenceCount > 1 {
            throw CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(column.column)
            )
        }
        if outerRow?.ambiguousUnqualifiedColumns.contains(column.column)
            == true {
            throw CanonicalReadError.expressionEvaluation(
                .ambiguousColumn(column.column)
            )
        }
        guard outerRow?.fields[column.column] != nil else {
            throw CanonicalReadError.expressionEvaluation(
                .missingColumn(column.column)
            )
        }
    }

    private func finalizeRelationalRows(
        _ selectQuery: SelectQuery,
        sourceRows: CanonicalRetainedRows,
        sourceSchema: CanonicalRelationSchema,
        residualFilter: Expression?,
        residualOrderBy: [SortKey]?,
        sourceRowsAlreadyOrdered: Bool = false,
        paginationQuery: SelectQuery? = nil,
        rowsAreContinuationRelative: Bool = false,
        continuationPosition: ByteString? = nil,
        prevalidatedQueryFingerprint: ByteString? = nil,
        metadata: [String: FieldValue] = [:],
        options: ReadExecutionContext,
        evaluationContext: CanonicalQueryEvaluationContext? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
        try validateRelationalQueryBindings(
            selectQuery,
            sourceSchema: sourceSchema,
            outerRow: evaluationContext?.outerRow,
            namedSubqueries: evaluationContext?.namedSubqueries ?? []
        )
        let filteredRows = try await applyFilter(
            residualFilter,
            to: sourceRows,
            workMeter: options.workMeter,
            evaluationContext: evaluationContext
        )

        let projectedRows: CanonicalRetainedQueryRows
        if canonicalQueryRequiresAggregation(selectQuery) {
            try validateGroupedWildcardProjection(
                selectQuery.projection,
                sourceSchema: sourceSchema,
                groupBy: selectQuery.groupBy ?? []
            )
            let groups = try await makeCanonicalGroups(
                filteredRows,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let havingGroups = try await applyHaving(
                selectQuery.having,
                to: groups,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            let orderedGroups = try await applyGroupedOrder(
                residualOrderBy,
                to: havingGroups,
                projection: selectQuery.projection,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
            projectedRows = try await projectGroupedRows(
                orderedGroups,
                projection: selectQuery.projection,
                groupBy: selectQuery.groupBy ?? [],
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        } else {
            let orderedRows: CanonicalRetainedRows
            if sourceRowsAlreadyOrdered,
               residualOrderBy?.isEmpty ?? true {
                orderedRows = filteredRows
            } else {
                orderedRows = try await applyOrder(
                    resolvedOrderBy(
                        residualOrderBy,
                        projection: selectQuery.projection
                    ),
                    to: filteredRows,
                    workMeter: options.workMeter,
                    evaluationContext: evaluationContext
                )
            }
            projectedRows = try await projectRows(
                orderedRows,
                projection: selectQuery.projection,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        }

        let distinctRows: CanonicalRetainedQueryRows
        if selectQuery.distinct {
            distinctRows = try canonicalUniqueRows(
                projectedRows,
                workMeter: options.workMeter
            )
        } else {
            distinctRows = projectedRows
        }

        let page = try CanonicalQueryPagination.retainedWindow(
            rows: distinctRows,
            selectQuery: paginationQuery ?? selectQuery,
            options: options,
            rowsAreContinuationRelative: rowsAreContinuationRelative,
            continuationPosition: continuationPosition,
            prevalidatedQueryFingerprint: prevalidatedQueryFingerprint
        )
        return CanonicalRetainedQueryResponse(
            rows: distinctRows,
            visibleRange: page.range,
            continuation: page.continuation,
            metadata: metadata,
            affectedRows: nil
        )
    }

    #if DATABASE_MULTI_BASE
    /// Applies the canonical relational pipeline to two already-authorized
    /// Base-local table inputs. Only the Composition planner may call this
    /// boundary; ordinary Base execution rejects Base-qualified sources.
    package func executeCompositionCrossBaseJoin(
        _ selectQuery: SelectQuery,
        join: JoinClause,
        leftRows: consuming DatabaseRetainedBuffer<QueryRow>,
        leftTable: TableRef,
        rightRows: consuming DatabaseRetainedBuffer<QueryRow>,
        rightTable: TableRef,
        options: ReadExecutionContext
    ) async throws -> QueryResponse {
        guard join.type == .inner else {
            throw CompositionQueryError.unsupportedPlan(
                "bounded cross-Base execution currently requires INNER JOIN"
            )
        }
        var leftBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        try leftRows.withSpan { rows in
            for index in rows.indices {
                let row = rows[index]
                let canonical = CanonicalSourceRow.fromBaseFields(
                    row.fields,
                    sourceName: leftTable.effectiveName,
                    annotations: row.annotations,
                    version: row.version
                )
                try leftBuilder.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: canonical,
                        workMeter: options.workMeter
                    ),
                    make: { canonical }
                )
            }
        }
        leftRows.discard()

        var rightBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rightRows.count
        )
        try rightRows.withSpan { rows in
            for index in rows.indices {
                let row = rows[index]
                let canonical = CanonicalSourceRow.fromBaseFields(
                    row.fields,
                    sourceName: rightTable.effectiveName,
                    annotations: row.annotations,
                    version: row.version
                )
                try rightBuilder.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: canonical,
                        workMeter: options.workMeter
                    ),
                    make: { canonical }
                )
            }
        }
        rightRows.discard()

        let left = try leftBuilder.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
        let right = try rightBuilder.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
        let leftSchema = try tableRelationSchema(leftTable)
        let rightSchema = try tableRelationSchema(rightTable)
        let joined = try await performJoin(
            left: CanonicalRelation(schema: leftSchema, rows: left),
            right: CanonicalRelation(schema: rightSchema, rows: right),
            type: join.type,
            condition: join.condition,
            workMeter: options.workMeter
        )
        let response = try await finalizeRelationalRows(
            selectQuery,
            sourceRows: joined.rows,
            sourceSchema: joined.schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options
        )
        return response.promoteToPublicResponse()
    }
    #endif

    private func queryCanonical(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        inheritedSubqueries: [NamedSubquery] = [],
        outerRow: CanonicalSourceRow? = nil,
        preparedFusionGraph: FusionPreparedQueryGraph = .empty
    ) async throws -> CanonicalRetainedQueryResponse {
        let namedSubqueries = try mergeNamedSubqueries(
            local: selectQuery.subqueries ?? [],
            inherited: inheritedSubqueries
        )
        let evaluationContext = CanonicalQueryEvaluationContext(
            options: options,
            transaction: transaction,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            namedSubqueries: namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph
        )
        if !isSPARQLSource(selectQuery.source),
           !sourceRequiresRuntimeInferredSchema(
            selectQuery.source,
            namedSubqueries: namedSubqueries
        ) {
            let sourceSchema = try canonicalRelationSchema(
                for: selectQuery.source,
                namedSubqueries: namedSubqueries
            )
            try validateRelationalQueryBindings(
                selectQuery,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: namedSubqueries
            )
            try validateStaticJoinBindings(
                selectQuery.source,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        }
        if let accessPath = selectQuery.accessPath {
            return try await executeAccessPathRows(
                selectQuery,
                accessPath: accessPath,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                evaluationContext: evaluationContext,
                preparedFusionGraph: preparedFusionGraph
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
                options: options,
                evaluationContext: evaluationContext
            )
        }

        if isSPARQLSource(selectQuery.source) {
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let response = try await executor.executeInTransaction(
                context: self,
                selectQuery: selectQuery,
                options: options,
                partitions: partitionValues ?? FieldObject(),
                transaction: transaction
            )
            return try retainExternalQueryResponse(
                response,
                workMeter: options.workMeter
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
                options: options,
                transaction: transaction,
                evaluationContext: evaluationContext
            )
        }

        guard selectQuery.dataset == .implicit,
              selectQuery.reduced == false else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Canonical relational execution does not support SPARQL dataset clauses"
            )
        }

        let sourceOptions = executionContextWithoutExternalPageWindow(options)
        let sourceRelation = try await materializeRows(
            for: selectQuery.source,
            namedSubqueries: namedSubqueries,
            options: sourceOptions,
            partitionValues: partitionValues,
            partitionMode: .routed,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow
        )

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRelation.rows,
            sourceSchema: sourceRelation.schema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func executeAccessPathRows(
        _ selectQuery: SelectQuery,
        accessPath: AccessPath,
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        evaluationContext: CanonicalQueryEvaluationContext,
        preparedFusionGraph: FusionPreparedQueryGraph
    ) async throws -> CanonicalRetainedQueryResponse {
        switch selectQuery.source {
        case .table(let tableRef):
            guard partitionValues?.isEmpty != false else {
                throw CanonicalReadError.invalidPartition(
                    entity: "graph",
                    reason: "graph partitions cannot be applied to a table source"
                )
            }
            if case .fusion(let fusionSource) = accessPath {
                let entity = try resolveEntity(named: tableRef.table)
                guard preparedFusionGraph.isValidated else {
                    throw FusionExecutionError.executionContractViolation
                }
                let preparedFusionPlan = try FusionPreflight
                    .prepareForExecution(
                        tableRef: tableRef,
                        entity: entity,
                        source: fusionSource,
                        context: self,
                        workMeter: options.workMeter
                    )
                let rowSet = try await FusionExecutor.execute(
                    context: self,
                    selectQuery: selectQuery,
                    tableRef: tableRef,
                    entity: entity,
                    source: fusionSource,
                    plan: preparedFusionPlan,
                    preparedQueryGraph: preparedFusionGraph,
                    options: options,
                    transaction: transaction
                )
                let sourceName = tableRef.alias ?? tableRef.effectiveName
                return try await finalizeIndexReadResult(
                    rowSet,
                    sourceName: sourceName,
                    sourceSchema: try tableRelationSchema(tableRef),
                    selectQuery: selectQuery,
                    options: options,
                    evaluationContext: evaluationContext
                )
            }
            // Non-scalar index access paths (fulltext, vector, rank, etc.) are
            // handled by kind-specific executors registered in ReadExecutorRegistry.
            // Only scalar index access is routed through SelectQueryPlanner, because
            // it maps cleanly onto Query<T>.forcedIndex + typed fetch.
            if case .index(let indexScan) = accessPath,
                indexScan.indexType != .ordered
            {
                let rowSet = try await dispatchTableIndexExecutor(
                    tableRef: tableRef,
                    selectQuery: selectQuery,
                    indexScan: indexScan,
                    options: options
                )
                let sourceName = tableRef.alias ?? tableRef.effectiveName
                return try await finalizeIndexReadResult(
                    rowSet,
                    sourceName: sourceName,
                    sourceSchema: try tableRelationSchema(tableRef),
                    selectQuery: selectQuery,
                    options: options,
                    evaluationContext: evaluationContext
                )
            }
            return try await executeSingleTableRows(
                selectQuery,
                options: options,
                transaction: transaction,
                evaluationContext: evaluationContext
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
            guard index.type == indexScan.indexType else {
                throw CanonicalReadError.unsupportedAccessPath(
                    "Index '\(index.name)' has type '\(index.type.diagnosticName)', not '\(indexScan.indexType.diagnosticName)'"
                )
            }
            guard let executor = container.runtimeConfiguration.readExecutors
                .polymorphicIndexExecutor(
                    for: index.type
                    )
            else {
                throw CanonicalReadError.executorNotRegistered(
                    index.type
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
            return try await finalizeIndexReadResult(
                rowSet,
                sourceName: logicalSource.effectiveName,
                sourceSchema: try polymorphicRelationSchema(
                    group,
                    sourceName: logicalSource.effectiveName
                ),
                selectQuery: selectQuery,
                options: options,
                evaluationContext: evaluationContext
            )

        default:
            throw CanonicalReadError.unsupportedAccessPath("accessPath queries require a table or logical source")
        }
    }

    /// Apply the common relational pipeline on top of an index executor's row
    /// set. Index-defined ordering is preserved only when the outer `SELECT`
    /// has no explicit ordering.
    private func finalizeIndexReadResult(
        _ rowSet: IndexReadResult,
        sourceName: String?,
        sourceSchema: CanonicalRelationSchema,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        evaluationContext: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        var retainedRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
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
        let canonicalRows = try retainedRows.finish().moveToSharedOwnership(
            at: .projection
        )

        let hasExplicitOrder = (selectQuery.orderBy?.isEmpty == false)
        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: canonicalRows,
            sourceSchema: sourceSchema,
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            sourceRowsAlreadyOrdered:
                rowSet.ordering == .orderedByIndex && !hasExplicitOrder,
            metadata: rowSet.metadata,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func executeSingleTableRows(
        _ selectQuery: SelectQuery,
        options: ReadExecutionContext,
        transaction: (any TransactionAccess)? = nil,
        evaluationContext: CanonicalQueryEvaluationContext? = nil
    ) async throws -> CanonicalRetainedQueryResponse {
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

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: pushdown.rows,
            sourceSchema: try tableRelationSchema(tableRef),
            residualFilter: pushdown.residualFilter,
            residualOrderBy: pushdown.residualOrderBy,
            paginationQuery: paginationQuery,
            rowsAreContinuationRelative: pushdown.pageWindowPushed,
            continuationPosition: pushdown.continuationPosition,
            prevalidatedQueryFingerprint:
                pushdown.stableSnapshotQueryFingerprint,
            options: options,
            evaluationContext: evaluationContext
        )
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
        guard index.type == indexScan.indexType else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Index '\(index.name)' has type '\(index.type.diagnosticName)', not '\(indexScan.indexType.diagnosticName)'"
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
                "Entity '\(entity.name)' has no registered '\(indexScan.indexType.diagnosticName)' index reader"
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
    ) async throws -> CanonicalRetainedRows {
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

    func resolveEntity(named name: String) throws -> Schema.Entity {
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
        options: ReadExecutionContext,
        evaluationContext: CanonicalQueryEvaluationContext
    ) async throws -> CanonicalRetainedQueryResponse {
        let group = try container.polymorphicGroup(identifier: logicalSource.identifier)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        let entities = try await scanPolymorphicItems(
            group: group,
            configuration: execution.transactionConfiguration,
            limit: nil,
            offset: nil,
            orderBy: nil
        )
        let sourceName = logicalSource.alias ?? logicalSource.effectiveName

        var sourceRowBuilder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: entities.count
        )
        for entity in entities {
            try options.workMeter.consume(at: .resultMaterialization)
            let row = try QueryRowCodec.encode(
                entity.item,
                annotations: [
                    PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                    PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode),
                ]
            )
            let sourceRow = CanonicalSourceRow.fromBaseFields(
                row.fields,
                sourceName: sourceName,
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
        let sourceRows = try sourceRowBuilder.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )

        return try await finalizeRelationalRows(
            selectQuery,
            sourceRows: sourceRows,
            sourceSchema: try polymorphicRelationSchema(
                group,
                sourceName: sourceName
            ),
            residualFilter: selectQuery.filter,
            residualOrderBy: selectQuery.orderBy,
            options: options,
            evaluationContext: evaluationContext
        )
    }

    private func materializeRows(
        for source: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
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
                    partitionMode: partitionMode,
                    transaction: transaction,
                    inheritedSubqueries: namedSubqueries,
                    outerRow: allowsOuterReferences ? outerRow : nil,
                    preparedFusionGraph: preparedFusionGraph
                )
                let alias = tableRef.alias ?? named.name
                return try materializeQueryRelation(
                    response.visibleRows,
                    query: named.query,
                    explicitColumns: named.columns,
                    alias: alias,
                    namedSubqueries: namedSubqueries,
                    workMeter: options.workMeter
                )
            }

            let rows = try await materializeUnwindowedTableSourceRows(
                tableRef,
                options: options,
                transaction: transaction
            )
            return CanonicalRelation(
                schema: try tableRelationSchema(tableRef),
                rows: rows
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
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: entities.count
            )
            for entity in entities {
                try options.workMeter.consume(at: .bindingCandidate)
                let row = try QueryRowCodec.encode(
                    entity.item,
                    annotations: [
                        PolymorphicRowAnnotation.typeName: .string(entity.typeName),
                        PolymorphicRowAnnotation.typeCode: .int64(entity.typeCode),
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
            return CanonicalRelation(
                schema: try polymorphicRelationSchema(
                    group,
                    sourceName: sourceName
                ),
                rows: try retained.finish().moveToSharedOwnership(
                    at: .bindingCandidate
                )
            )

        case .subquery(let query, let alias):
            let response = try await queryCanonical(
                query,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                inheritedSubqueries: namedSubqueries,
                outerRow: allowsOuterReferences ? outerRow : nil,
                preparedFusionGraph: preparedFusionGraph
            )
            return try materializeQueryRelation(
                response.visibleRows,
                query: query,
                explicitColumns: nil,
                alias: alias,
                namedSubqueries: namedSubqueries,
                workMeter: options.workMeter
            )

        case .join(let clause):
            return try await materializeJoinRows(
                clause,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )

        case .union(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: true,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )

        case .unionAll(let sources):
            return try await materializeUnionRows(
                sources,
                deduplicate: false,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )

        case .intersect(let sources):
            return try await materializeIntersectRows(
                sources,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )

        case .except(let lhs, let rhs):
            return try await materializeExceptRows(
                lhs,
                rhs,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )

        case .values(let rows, let columnNames):
            let resolvedColumnNames = columnNames
                ?? rows.first?.indices.map { "column\($0)" }
                ?? []
            guard Set(resolvedColumnNames).count == resolvedColumnNames.count else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "VALUES contains a duplicate column name"
                )
            }
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: rows.count
            )
            for values in rows {
                try options.workMeter.consume(at: .bindingCandidate)
                guard resolvedColumnNames.count == values.count else {
                    throw CanonicalReadError.unsupportedSelectQuery("VALUES column count mismatch")
                }
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(values.count)
                for (name, literal) in zip(resolvedColumnNames, values) {
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
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: resolvedColumnNames
                ),
                rows: try retained.finish().moveToSharedOwnership(
                    at: .bindingCandidate
                )
            )

        case .graphTable(let graphTableSource):
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.graphTableExecutor else {
                throw CanonicalReadError.unsupportedSource("graphTable executor is not registered")
            }
            let rows = try await executor.executeInTransaction(
                context: self,
                graphTableSource: graphTableSource,
                options: options,
                partitions: partitionValues ?? FieldObject(),
                transaction: transaction
            )
            var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .bindingCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: rows.count
            )
            for index in 0..<rows.count {
                try await rows.withElement(at: index) { graphRow in
                    try options.workMeter.consume(at: .bindingCandidate)
                    let sourceRow = canonicalGraphTableSourceRow(
                        from: graphRow.fields,
                        graphName: graphTableSource.graphName
                    )
                    let outputRow: CanonicalSourceRow
                    if let columns = graphTableSource.columns,
                       !columns.isEmpty {
                        var fields: [String: FieldValue] = [:]
                        fields.reserveCapacity(columns.count)
                        for column in columns {
                            fields[column.alias] = try await evaluateQueryExpression(
                                column.expression,
                                on: sourceRow,
                                context: CanonicalQueryEvaluationContext(
                                    options: options,
                                    transaction: transaction,
                                    partitionValues: partitionValues,
                                    partitionMode: partitionMode,
                                    namedSubqueries: namedSubqueries,
                                    outerRow: allowsOuterReferences ? outerRow : nil,
                                    preparedFusionGraph: preparedFusionGraph
                                ),
                                workMeter: options.workMeter
                            )
                        }
                        outputRow = CanonicalSourceRow(fields: fields)
                            .applyingAlias(graphTableSource.alias)
                    } else {
                        outputRow = sourceRow.applyingAlias(
                            graphTableSource.alias
                        )
                    }
                    try retained.append(
                        footprint: try CanonicalRelationalFootprintMeter.footprint(
                            of: outputRow,
                            workMeter: options.workMeter
                        ),
                        make: { outputRow }
                    )
                }
            }
            let materializedRows = try retained.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
            let schema = try graphTableRelationSchema(
                graphTableSource,
                rows: materializedRows
            )
            return CanonicalRelation(schema: schema, rows: materializedRows)

        case .graphPattern(let pattern):
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let response = try await executor.executeInTransaction(
                context: self,
                selectQuery: SelectQuery(projection: .all, source: source),
                options: options,
                partitions: partitionValues ?? FieldObject(),
                transaction: transaction
            )
            let rows = try materializeSourceRows(
                response.rows,
                sourceName: nil,
                workMeter: options.workMeter
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: sparqlVariables(in: pattern)
                ),
                rows: rows
            )

        case .namedGraph(_, let pattern):
            guard let executor = container.runtimeConfiguration.logicalSourceExecutors.sparqlExecutor else {
                throw CanonicalReadError.unsupportedSource("SPARQL source executor is not registered")
            }
            let response = try await executor.executeInTransaction(
                context: self,
                selectQuery: SelectQuery(projection: .all, source: source),
                options: options,
                partitions: partitionValues ?? FieldObject(),
                transaction: transaction
            )
            let rows = try materializeSourceRows(
                response.rows,
                sourceName: nil,
                workMeter: options.workMeter
            )
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(
                    unscopedColumns: sparqlVariables(in: pattern)
                ),
                rows: rows
            )

        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        #if DATABASE_MULTI_BASE
        case .base:
            throw CanonicalReadError.unsupportedSource(
                "Base-qualified sources require a Composition planner"
            )
        #endif
        }
    }

    private func materializeJoinRows(
        _ clause: JoinClause,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow?,
        allowsOuterReferences: Bool
    ) async throws -> CanonicalRelation {
        try validateJoinDeclaration(clause)
        if case .using(let columns) = clause.condition,
           columns.isEmpty {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN USING requires at least one column"
            )
        }
        let evaluationContext = CanonicalQueryEvaluationContext(
            options: options,
            transaction: transaction,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            namedSubqueries: namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: preparedFusionGraph
        )
        switch clause.type {
        case .lateral, .leftLateral:
            guard !sourceRequiresRuntimeInferredSchema(
                clause.right,
                namedSubqueries: namedSubqueries
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A LATERAL source must declare a stable output schema"
                )
            }
            let left = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let rightSchema = try canonicalRelationSchema(
                for: clause.right,
                namedSubqueries: namedSubqueries
            )
            try validateJoinCondition(
                clause.condition,
                leftSchema: left.schema,
                rightSchema: rightSchema,
                type: clause.type
            )
            let outputSchema = try joinOutputSchema(
                left.schema,
                rightSchema,
                condition: clause.condition
            )
            var outputRows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
            )
            for leftRow in left.rows {
                let lateralOuter = leftRow.overlaying(outer: outerRow)
                let right = try await materializeRows(
                    for: clause.right,
                    namedSubqueries: namedSubqueries,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode,
                    transaction: transaction,
                    preparedFusionGraph: preparedFusionGraph,
                    outerRow: lateralOuter,
                    allowsOuterReferences: true
                )
                guard right.schema == rightSchema else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "A LATERAL source changed its schema between outer rows"
                    )
                }
                let retainedLeftRow = try retainedCanonicalRow(
                    leftRow,
                    workMeter: options.workMeter,
                    stage: .joinCandidate
                )
                let joined = try await performJoin(
                    left: CanonicalRelation(
                        schema: left.schema,
                        rows: retainedLeftRow
                    ),
                    right: right,
                    type: clause.type == .leftLateral ? .left : .inner,
                    condition: clause.condition,
                    workMeter: options.workMeter,
                    evaluationContext: evaluationContext
                )
                for row in joined.rows {
                    try outputRows.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .footprint(
                                of: row,
                                workMeter: options.workMeter
                            ),
                        make: { row }
                    )
                }
            }
            return CanonicalRelation(
                schema: outputSchema,
                rows: try outputRows.finish().moveToSharedOwnership(
                    at: .bindingCandidate
                )
            )
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            let left = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let right = try await materializeRows(
                for: clause.right,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let columns = inferNaturalJoinColumns(
                leftSchema: left.schema,
                rightSchema: right.schema
            )
            return try await performJoin(
                left: left,
                right: right,
                type: naturalJoinBaseType(clause.type),
                condition: .using(columns),
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        default:
            let left = try await materializeRows(
                for: clause.left,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let right = try await materializeRows(
                for: clause.right,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            return try await performJoin(
                left: left,
                right: right,
                type: clause.type,
                condition: clause.condition,
                workMeter: options.workMeter,
                evaluationContext: evaluationContext
            )
        }
    }

    private func performJoin(
        left: CanonicalRelation,
        right: CanonicalRelation,
        type: JoinType,
        condition: JoinCondition?,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext? = nil
    ) async throws -> CanonicalRelation {
        try validateJoinCondition(
            condition,
            leftSchema: left.schema,
            rightSchema: right.schema,
            type: type
        )
        let outputSchema = try joinOutputSchema(
            left.schema,
            right.schema,
            condition: condition
        )
        if case .on(let expression) = condition {
            try validateExpressionBindings(
                expression,
                sourceSchema: outputSchema,
                outerRow: evaluationContext?.outerRow,
                namedSubqueries: evaluationContext?.namedSubqueries ?? []
            )
        }
        let leftRows = left.rows
        let rightRows = right.rows
        if type == .cross {
            var rows = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
            )
            for left in leftRows {
                for right in rightRows {
                    try workMeter.consume(at: .joinCandidate)
                    let merged = try mergeJoinRows(
                        left,
                        right,
                        condition: condition
                    )
                    try rows.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .footprint(of: merged, workMeter: workMeter),
                        make: { merged }
                    )
                }
            }
            return CanonicalRelation(
                schema: outputSchema,
                rows: try rows.finish().moveToSharedOwnership(
                    at: .joinCandidate
                )
            )
        }

        let emptyLeft = left.schema.nullRow()
        let emptyRight = right.schema.nullRow()
        if let hashJoined = try await performHashJoin(
            leftRows: leftRows,
            rightRows: rightRows,
            type: type,
            condition: condition,
            emptyLeft: emptyLeft,
            emptyRight: emptyRight,
            workMeter: workMeter,
            evaluationContext: evaluationContext
        ) {
            return CanonicalRelation(schema: outputSchema, rows: hashJoined)
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
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )

        for leftRow in leftRows {
            var matched = false
            for (rightIndex, rightRow) in rightRows.enumerated() {
                try workMeter.consume(at: .joinCandidate)
                if try await joinMatches(
                    left: leftRow,
                    right: rightRow,
                    condition: condition,
                    joinType: type,
                    evaluationContext: evaluationContext,
                    workMeter: workMeter
                ) {
                    matched = true
                    matchedRightIndexes.insert(rightIndex)
                    let merged = try mergeJoinRows(
                        leftRow,
                        rightRow,
                        condition: condition
                    )
                    try results.append(
                        footprint: try CanonicalRelationalFootprintMeter
                            .footprint(of: merged, workMeter: workMeter),
                        make: { merged }
                    )
                }
            }

            if !matched, type == .left || type == .full {
                let merged = try mergeJoinRows(
                    leftRow,
                    emptyRight,
                    condition: condition
                )
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }

        if type == .right || type == .full {
            for (rightIndex, rightRow) in rightRows.enumerated() where !matchedRightIndexes.contains(rightIndex) {
                let merged = try mergeJoinRows(
                    emptyLeft,
                    rightRow,
                    condition: condition
                )
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }

        return CanonicalRelation(
            schema: outputSchema,
            rows: try results.finish().moveToSharedOwnership(
                at: .joinCandidate
            )
        )
    }

    private func validateJoinCondition(
        _ condition: JoinCondition?,
        leftSchema: CanonicalRelationSchema,
        rightSchema: CanonicalRelationSchema,
        type: JoinType
    ) throws {
        guard type != .cross, case .using(let columns) = condition else {
            return
        }
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "JOIN USING contains duplicate column names"
            )
        }
        for column in columns {
            guard leftSchema.occurrenceCount(of: column) == 1,
                  rightSchema.occurrenceCount(of: column) == 1 else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "JOIN USING column '\(column)' must resolve exactly once in each input"
                )
            }
        }
    }

    private func validateJoinDeclaration(
        _ clause: JoinClause
    ) throws {
        guard clause.condition != nil else { return }
        switch clause.type {
        case .cross:
            throw CanonicalReadError.unsupportedSelectQuery(
                "CROSS JOIN cannot declare ON or USING"
            )
        case .natural, .naturalLeft, .naturalRight, .naturalFull:
            throw CanonicalReadError.unsupportedSelectQuery(
                "NATURAL JOIN cannot declare ON or USING"
            )
        default:
            return
        }
    }

    private func joinOutputSchema(
        _ left: CanonicalRelationSchema,
        _ right: CanonicalRelationSchema,
        condition: JoinCondition?
    ) throws -> CanonicalRelationSchema {
        if case .using(let columns) = condition {
            return try left.merged(with: right, coalescing: columns)
        }
        return try left.merged(with: right)
    }

    private func mergeJoinRows(
        _ left: CanonicalSourceRow,
        _ right: CanonicalSourceRow,
        condition: JoinCondition?
    ) throws -> CanonicalSourceRow {
        guard case .using(let columns) = condition else {
            return try left.merged(with: right)
        }
        let coalesced = Set(columns)
        let leftValues = Dictionary(
            uniqueKeysWithValues: columns.compactMap { column in
                firstScopedFieldValue(named: column, in: left).map {
                    (column, $0)
                }
            }
        )
        let rightValues = Dictionary(
            uniqueKeysWithValues: columns.compactMap { column in
                firstScopedFieldValue(named: column, in: right).map {
                    (column, $0)
                }
            }
        )
        var outputValues: [String: FieldValue] = [:]
        for column in columns {
            let leftValue = leftValues[column]
            let rightValue = rightValues[column]
            outputValues[column] = leftValue.flatMap {
                $0.isNull ? nil : $0
            } ?? rightValue ?? .null
        }
        let leftUnscoped = left.unscopedFields.filter {
            !coalesced.contains($0.key)
        }
        let rightUnscoped = right.unscopedFields.filter {
            !coalesced.contains($0.key)
        }
        let scopes = left.scopedFields.merging(right.scopedFields) {
            current, _ in current
        }
        return CanonicalSourceRow(
            unscopedFields: leftUnscoped
                .merging(rightUnscoped) { current, _ in current }
                .merging(outputValues) { current, _ in current },
            scopedFields: scopes,
            coalescedColumns: left.coalescedColumns
                .union(right.coalescedColumns)
                .union(coalesced),
            annotations: left.annotations.merging(right.annotations) {
                current, _ in current
            },
            version: nil
        )
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
        leftRows: CanonicalRetainedRows,
        rightRows: CanonicalRetainedRows,
        type: JoinType,
        condition: JoinCondition?,
        emptyLeft: CanonicalSourceRow,
        emptyRight: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows? {
        guard let plan = canonicalHashJoinPlan(
            condition: condition,
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
            guard let key = try canonicalJoinKey(
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
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )
        for left in leftRows {
            try workMeter.consume(at: .joinCandidate)
            let matches: [(Int, CanonicalSourceRow)]
            if let key = try canonicalJoinKey(sources: plan.left, row: left) {
                matches = buckets[key] ?? []
            } else {
                matches = []
            }
            var matchedLeft = false
            for (rightIndex, right) in matches {
                try workMeter.consume(at: .joinCandidate)
                if plan.validatesFullCondition,
                   try await joinMatches(
                       left: left,
                       right: right,
                       condition: condition,
                       joinType: type,
                       evaluationContext: evaluationContext,
                       workMeter: workMeter
                   ) == false {
                    continue
                }
                matchedLeft = true
                matchedRight.insert(rightIndex)
                let merged = try mergeJoinRows(
                    left,
                    right,
                    condition: condition
                )
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
            if !matchedLeft, type == .left || type == .full {
                let merged = try mergeJoinRows(
                    left,
                    emptyRight,
                    condition: condition
                )
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
                let merged = try mergeJoinRows(
                    emptyLeft,
                    right,
                    condition: condition
                )
                try results.append(
                    footprint: try CanonicalRelationalFootprintMeter
                        .footprint(of: merged, workMeter: workMeter),
                    make: { merged }
                )
            }
        }
        return try results.finish().moveToSharedOwnership(
            at: .joinCandidate
        )
    }

    private func canonicalHashJoinPlan(
        condition: JoinCondition?,
        leftRows: CanonicalRetainedRows,
        rightRows: CanonicalRetainedRows
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
            var leftIterator = leftRows.makeIterator()
            var rightIterator = rightRows.makeIterator()
            guard let leftSample = leftIterator.next(),
                  let rightSample = rightIterator.next() else {
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

    private func canonicalJoinKey(
        sources: [CanonicalJoinKeySource],
        row: CanonicalSourceRow
    ) throws -> CanonicalJoinKey? {
        var values: [FieldValue] = []
        values.reserveCapacity(sources.count)
        for source in sources {
            guard let value = joinValue(source: source, row: row),
                  value != .null else {
                return nil
            }
            values.append(
                try canonicalValueIdentity(value, operation: "hash JOIN")
            )
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
        joinType: JoinType,
        evaluationContext: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        if joinType == .cross {
            return true
        }

        guard let condition else { return true }
        switch condition {
        case .using(let columns):
            for column in columns {
                guard let leftValue = firstScopedFieldValue(
                    named: column,
                    in: left
                ), let rightValue = firstScopedFieldValue(
                    named: column,
                    in: right
                ) else {
                    return false
                }
                do {
                    guard try FieldValueComparator.equal(
                        leftValue,
                        rightValue
                    ) else {
                        return false
                    }
                } catch let failure {
                    throw canonicalComparisonReadError(
                        failure,
                        operation: "JOIN USING equality"
                    )
                }
            }
            return true
        case .on(let expression):
            let merged = try left.merged(with: right)
            return try await evaluateQueryBoolean(
                expression,
                on: merged,
                context: evaluationContext,
                workMeter: workMeter
            )
        }
    }

    private func inferNaturalJoinColumns(
        leftSchema: CanonicalRelationSchema,
        rightSchema: CanonicalRelationSchema
    ) -> [String] {
        let leftColumns = Set(
            leftSchema.unscopedColumns
                + leftSchema.scopes.flatMap { $0.columns }
        )
        let rightColumns = Set(
            rightSchema.unscopedColumns
                + rightSchema.scopes.flatMap { $0.columns }
        )
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

    private func firstScopedFieldValue(
        named column: String,
        in row: CanonicalSourceRow
    ) -> FieldValue? {
        if let value = row.unscopedFields[column] {
            return value
        }
        for fields in row.scopedFields.values {
            if let value = fields[column] {
                return value
            }
        }
        return nil
    }

    private func materializeUnionRows(
        _ sources: [DataSource],
        deduplicate: Bool,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        guard let firstSource = sources.first else {
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(),
                rows: try emptyCanonicalRows(
                    workMeter: options.workMeter,
                    stage: .bindingCandidate
                )
            )
        }
        let first = try await materializeRows(
            for: firstSource,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: first.schema.visibleColumns
        )
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self)
        )
        let remaining = try await materializeSetOperationInputs(
            Array(sources.dropFirst()),
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        for relation in [first] + remaining {
            let sourceRows = try alignSetOperationRows(
                relation,
                to: outputSchema,
                workMeter: options.workMeter
            )
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
        let rows = try retained.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        if deduplicate {
            return CanonicalRelation(
                schema: outputSchema,
                rows: try uniqueSourceRows(rows, workMeter: options.workMeter)
            )
        }
        return CanonicalRelation(schema: outputSchema, rows: rows)
    }

    private func materializeIntersectRows(
        _ sources: [DataSource],
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        guard let first = sources.first else {
            return CanonicalRelation(
                schema: try CanonicalRelationSchema(),
                rows: try emptyCanonicalRows(
                    workMeter: options.workMeter,
                    stage: .bindingCandidate
                )
            )
        }
        let firstRelation = try await materializeRows(
            for: first,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: firstRelation.schema.visibleColumns
        )
        var accumulator = try alignSetOperationRows(
            firstRelation,
            to: outputSchema,
            workMeter: options.workMeter
        )
        for source in sources.dropFirst() {
            let nextRelation = try await materializeRows(
                for: source,
                namedSubqueries: namedSubqueries,
                options: options,
                partitionValues: partitionValues,
                partitionMode: partitionMode,
                transaction: transaction,
                preparedFusionGraph: preparedFusionGraph,
                outerRow: outerRow,
                allowsOuterReferences: allowsOuterReferences
            )
            let next = try alignSetOperationRows(
                nextRelation,
                to: outputSchema,
                workMeter: options.workMeter
            )
            let setBytes = try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
                )
            ).multiplied(by: UInt64(next.count)).bytes
            let setReservation = try options.workMeter.reserveIntermediate(
                rows: UInt64(next.count),
                bytes: setBytes,
                at: .deduplication
            )
            defer { setReservation.release() }
            var nextKeys = Set<CanonicalRowValueIdentity>()
            nextKeys.reserveCapacity(next.count)
            for row in next {
                try options.workMeter.consume(at: .deduplication)
                nextKeys.insert(
                    try identityRow(row, operation: "INTERSECT")
                )
            }
            var intersected = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
                workMeter: options.workMeter,
                stage: .joinCandidate,
                layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
                expectedCount: accumulator.count
            )
            for row in accumulator {
                try options.workMeter.consume(at: .joinCandidate)
                guard nextKeys.contains(
                    try identityRow(row, operation: "INTERSECT")
                ) else { continue }
                try intersected.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: options.workMeter
                    ),
                    make: { row }
                )
            }
            accumulator = try intersected.finish().moveToSharedOwnership(
                at: .joinCandidate
            )
        }
        return CanonicalRelation(
            schema: outputSchema,
            rows: try uniqueSourceRows(
                accumulator,
                workMeter: options.workMeter
            )
        )
    }

    private func materializeExceptRows(
        _ lhs: DataSource,
        _ rhs: DataSource,
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> CanonicalRelation {
        let left = try await materializeRows(
            for: lhs,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let right = try await materializeRows(
            for: rhs,
            namedSubqueries: namedSubqueries,
            options: options,
            partitionValues: partitionValues,
            partitionMode: partitionMode,
            transaction: transaction,
            preparedFusionGraph: preparedFusionGraph,
            outerRow: outerRow,
            allowsOuterReferences: allowsOuterReferences
        )
        let outputSchema = try CanonicalRelationSchema(
            unscopedColumns: left.schema.visibleColumns
        )
        let leftRows = try alignSetOperationRows(
            left,
            to: outputSchema,
            workMeter: options.workMeter
        )
        let rightRows = try alignSetOperationRows(
            right,
            to: outputSchema,
            workMeter: options.workMeter
        )
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rightRows.count)).bytes
        let setReservation = try options.workMeter.reserveIntermediate(
            rows: UInt64(rightRows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var rightKeys = Set<CanonicalRowValueIdentity>()
        rightKeys.reserveCapacity(rightRows.count)
        for row in rightRows {
            try options.workMeter.consume(at: .deduplication)
            rightKeys.insert(
                try identityRow(row, operation: "EXCEPT")
            )
        }
        var difference = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: options.workMeter,
            stage: .joinCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: leftRows.count
        )
        for row in leftRows {
            try options.workMeter.consume(at: .joinCandidate)
            guard !rightKeys.contains(
                try identityRow(row, operation: "EXCEPT")
            ) else { continue }
            try difference.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: options.workMeter
                ),
                make: { row }
            )
        }
        return CanonicalRelation(
            schema: outputSchema,
            rows: try uniqueSourceRows(
                try difference.finish().moveToSharedOwnership(
                    at: .joinCandidate
                ),
                workMeter: options.workMeter
            )
        )
    }

    private func uniqueSourceRows(
        _ rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedRows {
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            rows: UInt64(rows.count),
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen = Set<CanonicalRowValueIdentity>()
        seen.reserveCapacity(rows.count)
        var unique = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            let key = try identityRow(
                row,
                operation: "relational DISTINCT"
            )
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
        return try unique.finish().moveToSharedOwnership(at: .deduplication)
    }

    private func identityRow(
        _ row: CanonicalSourceRow,
        operation: String
    ) throws -> CanonicalRowValueIdentity {
        try CanonicalRowValueIdentity(
            fields: canonicalIdentityFields(
                row.fields,
                operation: operation
            )
        )
    }

    private func canonicalIdentityFields(
        _ fields: [String: FieldValue],
        operation: String
    ) throws -> [String: FieldValue] {
        var result: [String: FieldValue] = [:]
        result.reserveCapacity(fields.count)
        for (name, value) in fields {
            result[name] = try canonicalValueIdentity(
                value,
                operation: operation
            )
        }
        return result
    }

    private func canonicalValueIdentity(
        _ value: FieldValue,
        operation: String
    ) throws -> FieldValue {
        do {
            return try RelationalValueIdentity.canonicalize(value).value
        } catch RelationalValueIdentityError.nonFiniteNumericValue {
            throw CanonicalReadError.expressionEvaluation(
                .typeMismatch(
                    operation: "\(operation) with a non-finite numeric value"
                )
            )
        } catch RelationalValueIdentityError.invalidObject {
            throw CanonicalReadError.expressionEvaluation(
                .typeMismatch(
                    operation: "\(operation) with an invalid object value"
                )
            )
        }
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

        let resolutionFields = CanonicalSourceRow.flatten(
            scopedFields: nonemptyScopes
        )
        return CanonicalSourceRow(
            materializedFields: baseFields.merging(resolutionFields) {
                current, _ in current
            },
            unscopedFields: [:],
            scopedFields: nonemptyScopes
        )
    }

    private func applyFilter(
        _ filter: DatabaseKit.Expression?,
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard let filter else { return rows }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .filterEvaluation,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .filterEvaluation)
            guard try await evaluateQueryBoolean(
                filter,
                on: row,
                context: evaluationContext,
                workMeter: workMeter
            ) else { continue }
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return try retained.finish().moveToSharedOwnership(
            at: .filterEvaluation
        )
    }

    private func applyOrder(
        _ orderBy: [SortKey]?,
        to rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedRows {
        guard let orderBy, !orderBy.isEmpty else { return rows }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: rows.count,
                element: (CanonicalSourceRow, [FieldValue], ByteString).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: FieldValue.self
            )
            .multiplied(by: UInt64(rows.count))
        let decorationFootprint = try outerArrayFootprint
            .multiplied(by: 2)
            .adding(nestedValuesFootprint)
            .adding(
                try DatabaseIntermediateFootprint(bytes: 32)
                    .multiplied(by: UInt64(rows.count))
            )
        let decorationRows = try DatabaseIntermediateFootprint(
            rows: UInt64(rows.count)
        ).multiplied(by: 2).rows
        let decorationReservation = try workMeter.reserveIntermediate(
            rows: decorationRows,
            bytes: decorationFootprint.bytes,
            at: .sortInput
        )
        defer { decorationReservation.release() }
        try workMeter.consume(UInt64(rows.count), at: .sortInput)
        var decorated: [(CanonicalSourceRow, [FieldValue], ByteString)] = []
        decorated.reserveCapacity(rows.count)
        for row in rows {
            var values: [FieldValue] = []
            values.reserveCapacity(orderBy.count)
            for key in orderBy {
                values.append(
                    try await evaluateQueryExpression(
                        key.expression,
                        on: row,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
                )
            }
            let fingerprint = try CanonicalRowFingerprint.compute(
                QueryRow(
                    fields: row.fields,
                    annotations: row.annotations,
                    version: row.version
                ),
                workMeter: workMeter
            )
            decorated.append((row, values, fingerprint))
        }
        let sorted: [(CanonicalSourceRow, [FieldValue], ByteString)]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    let comparison = try FieldValueComparator.compare(
                        lhs.1[index],
                        rhs.1[index],
                        using: sortKey
                    )
                    guard comparison != .equal else { continue }
                    return comparison == .lessThan
                }
                try workMeter.consume(2, at: .sortComparison)
                return lhs.2.lexicographicallyPrecedes(rhs.2)
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(
                failure,
                operation: "ordering"
            )
        }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: sorted.count
        )
        for item in sorted {
            let row = item.0
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .sortInput)
    }

    private func projectRows(
        _ rows: CanonicalRetainedRows,
        projection: Projection,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
        var retained = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: rows.count
        )
        switch projection {
        case .all:
            for row in rows {
                try workMeter.consume(at: .projection)
                let projected = QueryRow(
                    fields: row.wildcardFields,
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
            let names = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
            guard Set(names).count == names.count else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A projection exposes duplicate column names"
                )
            }
            for row in rows {
                try workMeter.consume(at: .projection)
                var fields: [String: FieldValue] = [:]
                for (fieldName, item) in zip(names, items) {
                    fields[fieldName] = try await evaluateQueryExpression(
                        item.expression,
                        on: row,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
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
                try await projectRows(
                    rows,
                    projection: .items(items),
                    workMeter: workMeter,
                    evaluationContext: evaluationContext
                ),
                workMeter: workMeter
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .projection)
    }

    private func makeCanonicalGroups(
        _ rows: CanonicalRetainedRows,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        let keyValueBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<FieldValue>.stride))
        ).multiplied(by: 2).multiplied(by: UInt64(groupBy.count))
        let entryFootprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(
                    1,
                    (MemoryLayout<CanonicalSourceRow>.stride * 2)
                        + MemoryLayout<CanonicalGroupKey>.stride
                        + MemoryLayout<CanonicalGroupedRow>.stride
                        + MemoryLayout<[CanonicalSourceRow]>.stride
                        + (MemoryLayout<[FieldValue]>.stride * 2)
                        + 64
                )
            )
        ).adding(keyValueBytes)
        let stateFootprint = try entryFootprint.multiplied(
            by: UInt64(max(1, rows.count))
        )
        let stateReservation = try workMeter.reserveIntermediate(
            rows: UInt64(max(1, rows.count)),
            bytes: stateFootprint.bytes,
            at: .aggregateInput
        )
        defer { stateReservation.release() }

        var groupIndexes: [[FieldValue]: Int] = [:]
        groupIndexes.reserveCapacity(max(1, rows.count))
        var keys: [CanonicalGroupKey] = []
        var representatives: [CanonicalSourceRow] = []
        var groupedRows: [[CanonicalSourceRow]] = []

        for row in rows {
            try workMeter.consume(at: .aggregateInput)
            let values = try await evaluateExpressions(
                groupBy,
                on: row,
                context: evaluationContext,
                workMeter: workMeter
            )
            let key = CanonicalGroupKey(
                values: values,
                identity: try values.map {
                    try canonicalValueIdentity($0, operation: "GROUP BY")
                }
            )
            if let index = groupIndexes[key.identity] {
                groupedRows[index].append(row)
            } else {
                groupIndexes[key.identity] = keys.count
                keys.append(key)
                representatives.append(row)
                groupedRows.append([row])
            }
        }

        if rows.isEmpty, groupBy.isEmpty {
            let key = CanonicalGroupKey(values: [], identity: [])
            keys.append(key)
            representatives.append(CanonicalSourceRow(fields: [:]))
            groupedRows.append([])
        }

        var retained = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: keys.count
        )
        for index in keys.indices {
            let retainedGroupRows = try retainedCanonicalRows(
                groupedRows[index],
                workMeter: workMeter,
                stage: .aggregateInput
            )
            let group = CanonicalGroupedRow(
                key: keys[index],
                representative: representatives[index],
                rows: retainedGroupRows
            )
            try retained.append(
                footprint: try canonicalGroupedRowFootprint(
                    group,
                    workMeter: workMeter
                ),
                make: { group }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .aggregateInput)
    }

    private func applyHaving(
        _ having: Expression?,
        to groups: CanonicalRetainedGroups,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard let having else { return groups }
        var result = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .aggregateInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: groups.count
        )
        for group in groups {
            try workMeter.consume(at: .aggregateInput)
            let value = try await evaluateGroupedExpression(
                having,
                group: group,
                groupBy: groupBy,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
            do {
                if try DatabaseExpressionEvaluator(fields: ["value": value])
                    .predicate(.column(ColumnRef("value"))) {
                    try result.append(
                        footprint: try canonicalGroupedRowFootprint(
                            group,
                            workMeter: workMeter
                        ),
                        make: { group }
                    )
                }
            } catch let error as DatabaseExpressionEvaluationError {
                throw CanonicalReadError.expressionEvaluation(error)
            }
        }
        return try result.finish().moveToSharedOwnership(at: .aggregateInput)
    }

    private func applyGroupedOrder(
        _ orderBy: [SortKey]?,
        to groups: CanonicalRetainedGroups,
        projection: Projection,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedGroups {
        guard let orderBy, !orderBy.isEmpty else { return groups }
        let outerArrayFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: groups.count,
                element: (CanonicalGroupedRow, [FieldValue]).self
            )
        let nestedValuesFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderBy.count,
                element: FieldValue.self
            )
            .multiplied(by: UInt64(groups.count))
        let decorationFootprint = try outerArrayFootprint
            .multiplied(by: 2)
            .adding(nestedValuesFootprint)
        let decorationRows = try DatabaseIntermediateFootprint(
            rows: UInt64(groups.count)
        ).multiplied(by: 2).rows
        let decorationReservation = try workMeter.reserveIntermediate(
            rows: decorationRows,
            bytes: decorationFootprint.bytes,
            at: .sortInput
        )
        defer { decorationReservation.release() }
        try workMeter.consume(UInt64(groups.count), at: .sortInput)
        var decorated: [(CanonicalGroupedRow, [FieldValue])] = []
        decorated.reserveCapacity(groups.count)
        for group in groups {
            var values: [FieldValue] = []
            values.reserveCapacity(orderBy.count)
            for sortKey in orderBy {
                let expression = groupedOrderExpression(
                    sortKey.expression,
                    projection: projection
                )
                values.append(
                    try await evaluateGroupedExpression(
                        expression,
                        group: group,
                        groupBy: groupBy,
                        workMeter: workMeter,
                        evaluationContext: evaluationContext
                    )
                )
            }
            decorated.append((group, values))
        }
        let sorted: [(CanonicalGroupedRow, [FieldValue])]
        do {
            sorted = try decorated.sorted { lhs, rhs in
                for (index, sortKey) in orderBy.enumerated() {
                    try workMeter.consume(2, at: .sortComparison)
                    let comparison = try FieldValueComparator.compare(
                        lhs.1[index],
                        rhs.1[index],
                        using: sortKey
                    )
                    guard comparison != .equal else { continue }
                    return comparison == .lessThan
                }
                try workMeter.consume(2, at: .sortComparison)
                return lhs.0.key.identity.lexicographicallyPrecedes(
                    rhs.0.key.identity
                )
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(
                failure,
                operation: "aggregate ordering"
            )
        }
        var retained = try DatabaseRetainedArrayBuilder<CanonicalGroupedRow>(
            workMeter: workMeter,
            stage: .sortInput,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalGroupedRow.self),
            expectedCount: sorted.count
        )
        for item in sorted {
            let group = item.0
            try retained.append(
                footprint: try canonicalGroupedRowFootprint(
                    group,
                    workMeter: workMeter
                ),
                make: { group }
            )
        }
        return try retained.finish().moveToSharedOwnership(at: .sortInput)
    }

    private func groupedOrderExpression(
        _ expression: Expression,
        projection: Projection
    ) -> Expression {
        guard case .column(let column) = expression,
              column.table == nil else {
            return expression
        }
        let items: [ProjectionItem]
        switch projection {
        case .items(let value), .distinctItems(let value):
            items = value
        case .all, .allFrom:
            return expression
        }
        return items.first(where: { $0.alias == column.column })?.expression
            ?? expression
    }

    private func resolvedOrderBy(
        _ orderBy: [SortKey]?,
        projection: Projection
    ) -> [SortKey]? {
        orderBy?.map { key in
            SortKey(
                groupedOrderExpression(
                    key.expression,
                    projection: projection
                ),
                direction: key.direction,
                nulls: key.nulls
            )
        }
    }

    private func projectGroupedRows(
        _ groups: CanonicalRetainedGroups,
        projection: Projection,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryRows {
        let projectedItemNames: [String]?
        switch projection {
        case .items(let items), .distinctItems(let items):
            let names = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
            guard Set(names).count == names.count else {
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Aggregate projection names must be unique"
                    )
                )
            }
            projectedItemNames = names
        case .all, .allFrom:
            projectedItemNames = nil
        }
        var rows = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: groups.count
        )
        for group in groups {
            try workMeter.consume(at: .projection)
            let projected: QueryRow
            switch projection {
            case .all:
                projected = QueryRow(
                    fields: group.representative.wildcardFields,
                    annotations: group.representative.annotations
                )
            case .allFrom(let sourceName):
                guard let fields = group.representative.fields(for: sourceName) else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Projection source '\(sourceName)' not found"
                    )
                }
                projected = QueryRow(
                    fields: fields,
                    annotations: group.representative.annotations
                )
            case .items(let items), .distinctItems(let items):
                guard let names = projectedItemNames else {
                    throw CanonicalReadError.aggregateEvaluation(
                        .invalidGroupedExpression(
                            "Grouped projection metadata is unavailable"
                        )
                    )
                }
                var fields: [String: FieldValue] = [:]
                fields.reserveCapacity(items.count)
                for (name, item) in zip(names, items) {
                    fields[name] = try await evaluateGroupedExpression(
                        item.expression,
                        group: group,
                        groupBy: groupBy,
                        workMeter: workMeter,
                        evaluationContext: evaluationContext
                    )
                }
                projected = QueryRow(fields: fields)
            }
            try rows.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: projected,
                    workMeter: workMeter
                ),
                make: { projected }
            )
        }
        let projectedRows = try rows.finish().moveToSharedOwnership(
            at: .projection
        )
        if case .distinctItems = projection {
            return try canonicalUniqueRows(
                projectedRows,
                workMeter: workMeter
            )
        }
        return projectedRows
    }

    private func validateGroupedWildcardProjection(
        _ projection: Projection,
        sourceSchema: CanonicalRelationSchema,
        groupBy: [Expression]
    ) throws {
        func requireGroupedColumn(
            _ column: String,
            sourceName: String?
        ) throws {
            let unqualified = Expression.column(ColumnRef(column))
            let isGrouped: Bool
            if let sourceName {
                let qualified = Expression.column(
                    ColumnRef(table: sourceName, column: column)
                )
                isGrouped = groupBy.contains(qualified)
                    || (sourceSchema.occurrenceCount(of: column) == 1
                        && groupBy.contains(unqualified))
            } else {
                isGrouped = groupBy.contains(unqualified)
            }
            guard isGrouped else {
                let displayName = sourceName.map { "\($0).\(column)" }
                    ?? column
                throw CanonicalReadError.aggregateEvaluation(
                    .invalidGroupedExpression(
                        "Wildcard projection contains non-grouped column '\(displayName)'"
                    )
                )
            }
        }

        switch projection {
        case .items, .distinctItems:
            return
        case .all:
            for column in sourceSchema.unscopedColumns {
                try requireGroupedColumn(column, sourceName: nil)
            }
            for scope in sourceSchema.scopes {
                for column in scope.columns
                    where !sourceSchema.coalescedColumns.contains(column) {
                    try requireGroupedColumn(
                        column,
                        sourceName: scope.name
                    )
                }
            }
        case .allFrom(let sourceName):
            guard let scope = sourceSchema.scopes.first(
                where: { $0.name == sourceName }
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Projection source '\(sourceName)' not found"
                )
            }
            for column in scope.columns {
                try requireGroupedColumn(column, sourceName: sourceName)
            }
        }
    }

    private func evaluateGroupedExpression(
        _ expression: Expression,
        group: CanonicalGroupedRow,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> FieldValue {
        let rewritten = try await rewriteGroupedExpression(
            expression,
            group: group,
            groupBy: groupBy,
            workMeter: workMeter,
            evaluationContext: evaluationContext
        )
        return try await evaluateQueryExpression(
            rewritten,
            on: group.representative,
            context: evaluationContext,
            workMeter: workMeter
        )
    }

    private func rewriteGroupedExpression(
        _ expression: Expression,
        group: CanonicalGroupedRow,
        groupBy: [Expression],
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> Expression {
        if !canonicalExpressionContainsAggregate(expression),
           groupBy.contains(
            where: {
                groupedExpression(
                    expression,
                    matches: $0,
                    on: group.representative
                )
            }
           ) {
            return expression
        }

        func rewrite(_ nested: Expression) async throws -> Expression {
            try await rewriteGroupedExpression(
                nested,
                group: group,
                groupBy: groupBy,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
        }

        switch expression {
        case .aggregate(let aggregate):
            let value = try await evaluateAggregate(
                aggregate,
                rows: group.rows,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
            return .literal(try value.toLiteral())
        case .literal:
            return expression
        case .column(let column):
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Column '\(column.displayName)' is neither grouped nor aggregated"
                )
            )
        case .variable(let variable):
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Variable '\(variable.name)' is neither grouped nor aggregated"
                )
            )
        case .parameter, .bound:
            return expression
        case .add(let lhs, let rhs): return .add(try await rewrite(lhs), try await rewrite(rhs))
        case .subtract(let lhs, let rhs): return .subtract(try await rewrite(lhs), try await rewrite(rhs))
        case .multiply(let lhs, let rhs): return .multiply(try await rewrite(lhs), try await rewrite(rhs))
        case .divide(let lhs, let rhs): return .divide(try await rewrite(lhs), try await rewrite(rhs))
        case .modulo(let lhs, let rhs): return .modulo(try await rewrite(lhs), try await rewrite(rhs))
        case .negate(let nested): return .negate(try await rewrite(nested))
        case .equal(let lhs, let rhs): return .equal(try await rewrite(lhs), try await rewrite(rhs))
        case .notEqual(let lhs, let rhs): return .notEqual(try await rewrite(lhs), try await rewrite(rhs))
        case .lessThan(let lhs, let rhs): return .lessThan(try await rewrite(lhs), try await rewrite(rhs))
        case .lessThanOrEqual(let lhs, let rhs):
            return .lessThanOrEqual(try await rewrite(lhs), try await rewrite(rhs))
        case .greaterThan(let lhs, let rhs): return .greaterThan(try await rewrite(lhs), try await rewrite(rhs))
        case .greaterThanOrEqual(let lhs, let rhs):
            return .greaterThanOrEqual(try await rewrite(lhs), try await rewrite(rhs))
        case .and(let lhs, let rhs): return .and(try await rewrite(lhs), try await rewrite(rhs))
        case .or(let lhs, let rhs): return .or(try await rewrite(lhs), try await rewrite(rhs))
        case .not(let nested): return .not(try await rewrite(nested))
        case .isNull(let nested): return .isNull(try await rewrite(nested))
        case .isNotNull(let nested): return .isNotNull(try await rewrite(nested))
        case .like(let nested, let pattern):
            return .like(try await rewrite(nested), pattern: pattern)
        case .regex(let nested, let pattern, let flags):
            return .regex(try await rewrite(nested), pattern: pattern, flags: flags)
        case .between(let nested, let low, let high):
            return .between(
                try await rewrite(nested),
                low: try await rewrite(low),
                high: try await rewrite(high)
            )
        case .inList(let nested, let values):
            return .inList(
                try await rewrite(nested),
                values: try await rewriteExpressions(values, using: rewrite)
            )
        case .notInList(let nested, let values):
            return .notInList(
                try await rewrite(nested),
                values: try await rewriteExpressions(values, using: rewrite)
            )
        case .function(let function):
            return .function(
                FunctionCall(
                    name: function.name,
                    arguments: try await rewriteExpressions(
                        function.arguments,
                        using: rewrite
                    ),
                    distinct: function.distinct
                )
            )
        case .caseWhen(let pairs, let fallback):
            return .caseWhen(
                cases: try await rewriteCasePairs(pairs, using: rewrite),
                elseResult: try await rewriteOptionalExpression(
                    fallback,
                    using: rewrite
                )
            )
        case .coalesce(let values):
            return .coalesce(
                try await rewriteExpressions(values, using: rewrite)
            )
        case .nullIf(let lhs, let rhs):
            return .nullIf(try await rewrite(lhs), try await rewrite(rhs))
        case .cast(let nested, let type):
            return .cast(try await rewrite(nested), targetType: type)
        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try await rewrite(subject),
                predicate: try await rewrite(predicate),
                object: try await rewrite(object)
            )
        case .isTriple(let nested): return .isTriple(try await rewrite(nested))
        case .subject(let nested): return .subject(try await rewrite(nested))
        case .predicate(let nested): return .predicate(try await rewrite(nested))
        case .object(let nested): return .object(try await rewrite(nested))
        case .inSubquery(let value, let query):
            return .inSubquery(try await rewrite(value), subquery: query)
        case .subquery, .exists:
            return expression
        }
    }

    private func groupedExpression(
        _ expression: Expression,
        matches groupedExpression: Expression,
        on row: CanonicalSourceRow
    ) -> Bool {
        if expression == groupedExpression {
            return true
        }
        guard case .column(let projectedColumn) = expression,
              case .column(let groupedColumn) = groupedExpression,
              projectedColumn.column == groupedColumn.column else {
            return false
        }
        if let projectedTable = projectedColumn.table,
           let groupedTable = groupedColumn.table {
            return projectedTable == groupedTable
        }
        guard !row.ambiguousUnqualifiedColumns.contains(
            projectedColumn.column
        ) else {
            return false
        }
        return row.value(for: projectedColumn) != nil
            && row.value(for: groupedColumn) != nil
    }

    private func rewriteExpressions(
        _ expressions: [Expression],
        using transform: (Expression) async throws -> Expression
    ) async throws -> [Expression] {
        var result: [Expression] = []
        result.reserveCapacity(expressions.count)
        for expression in expressions {
            result.append(try await transform(expression))
        }
        return result
    }

    private func rewriteCasePairs(
        _ pairs: [CaseWhenPair],
        using transform: (Expression) async throws -> Expression
    ) async throws -> [CaseWhenPair] {
        var result: [CaseWhenPair] = []
        result.reserveCapacity(pairs.count)
        for pair in pairs {
            result.append(
                CaseWhenPair(
                    condition: try await transform(pair.condition),
                    result: try await transform(pair.result)
                )
            )
        }
        return result
    }

    private func rewriteOptionalExpression(
        _ expression: Expression?,
        using transform: (Expression) async throws -> Expression
    ) async throws -> Expression? {
        guard let expression else { return nil }
        return try await transform(expression)
    }

    private func evaluateAggregate(
        _ aggregate: AggregateFunction,
        rows: CanonicalRetainedRows,
        workMeter: DatabaseWorkMeter,
        evaluationContext: CanonicalQueryEvaluationContext?
    ) async throws -> FieldValue {
        let functionName: String
        let expression: Expression?
        let distinct: Bool
        let orderedRows: CanonicalRetainedRows
        switch aggregate {
        case .count(let value, let isDistinct):
            functionName = "COUNT"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .sum(let value, let isDistinct):
            functionName = "SUM"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .avg(let value, let isDistinct):
            functionName = "AVG"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .min(let value):
            functionName = "MIN"
            expression = value
            distinct = false
            orderedRows = rows
        case .max(let value):
            functionName = "MAX"
            expression = value
            distinct = false
            orderedRows = rows
        case .groupConcat(let value, _, let isDistinct):
            functionName = "GROUP_CONCAT"
            expression = value
            distinct = isDistinct
            orderedRows = rows
        case .sample(let value):
            functionName = "SAMPLE"
            expression = value
            distinct = false
            orderedRows = rows
        case .arrayAgg(let value, let orderBy, let isDistinct):
            functionName = "ARRAY_AGG"
            expression = value
            distinct = isDistinct
            orderedRows = try await applyOrder(
                orderBy,
                to: rows,
                workMeter: workMeter,
                evaluationContext: evaluationContext
            )
        }

        if expression == nil, distinct {
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression("COUNT(DISTINCT *) is not valid")
            )
        }

        var aggregateScratch = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: orderedRows.count,
                element: FieldValue.self
            )
        if case .groupConcat = aggregate {
            aggregateScratch = try aggregateScratch.adding(
                DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: orderedRows.count,
                    element: String.self
                )
            )
        }
        if distinct {
            aggregateScratch = try aggregateScratch
                .adding(
                    try DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: orderedRows.count,
                        element: FieldValue.self
                    )
                )
                .adding(
                    try DatabaseIntermediateFootprint(
                        bytes: UInt64(
                            max(1, MemoryLayout<FieldValue>.stride + 32)
                        )
                    ).multiplied(by: UInt64(orderedRows.count))
                )
        }
        let aggregateScratchRows = try DatabaseIntermediateFootprint(
            rows: UInt64(orderedRows.count)
        ).multiplied(by: distinct ? 3 : 1).rows
        let aggregateScratchReservation = try workMeter.reserveIntermediate(
            rows: aggregateScratchRows,
            bytes: aggregateScratch.bytes,
            at: .aggregateInput
        )
        defer { aggregateScratchReservation.release() }
        var values: [FieldValue] = []
        values.reserveCapacity(orderedRows.count)
        for row in orderedRows {
            try workMeter.consume(at: .aggregateInput)
            if let expression {
                values.append(
                    try await evaluateQueryExpression(
                        expression,
                        on: row,
                        context: evaluationContext,
                        workMeter: workMeter
                    )
                )
            } else {
                values.append(.bool(true))
            }
        }
        if distinct {
            var seen = Set<FieldValue>()
            var distinctValues: [FieldValue] = []
            distinctValues.reserveCapacity(values.count)
            for value in values {
                let identity = try canonicalValueIdentity(
                    value,
                    operation: "\(functionName)(DISTINCT)"
                )
                if seen.insert(identity).inserted {
                    distinctValues.append(value)
                }
            }
            values = distinctValues
        }

        switch aggregate {
        case .count(let expression, _):
            let count = expression == nil
                ? values.count
                : values.lazy.filter { !$0.isNull }.count
            guard let result = Int64(exactly: count) else {
                throw CanonicalReadError.aggregateEvaluation(.countOverflow)
            }
            return .int64(result)

        case .sum, .avg:
            var accumulator = DatabaseNumericAggregateAccumulator()
            do {
                for value in values where !value.isNull {
                    try accumulator.add(value)
                }
                let result: FieldValue?
                if case .sum = aggregate {
                    result = try accumulator.sum()
                } else {
                    result = try accumulator.average()
                }
                return result ?? .null
            } catch let failure {
                throw CanonicalReadError.aggregateEvaluation(
                    aggregateNumericError(
                        function: functionName,
                        failure: failure
                    )
                )
            }

        case .min, .max:
            var result: FieldValue?
            for value in values where !value.isNull {
                guard let current = result else {
                    result = value
                    continue
                }
                do {
                    let comparison = try FieldValueComparator.compare(
                        value,
                        current
                    )
                    if (functionName == "MIN" && comparison == .lessThan)
                        || (functionName == "MAX" && comparison == .greaterThan) {
                        result = value
                    }
                } catch let failure {
                    switch failure {
                    case .incomparable(let left, let right):
                        throw CanonicalReadError.aggregateEvaluation(
                            .incomparable(
                                function: functionName,
                                left: left,
                                right: right
                            )
                        )
                    case .unorderedFloatingPoint:
                        throw CanonicalReadError.aggregateEvaluation(
                            .nonFiniteValue(function: functionName)
                        )
                    }
                }
            }
            return result ?? .null

        case .groupConcat(_, let separator, _):
            var strings: [String] = []
            strings.reserveCapacity(values.count)
            for value in values where !value.isNull {
                guard case .string(let string) = value else {
                    throw CanonicalReadError.aggregateEvaluation(
                        .invalidStringValue(function: functionName)
                    )
                }
                strings.append(string)
            }
            return strings.isEmpty
                ? .null
                : .string(strings.joined(separator: separator ?? ","))

        case .sample:
            return values.first(where: { !$0.isNull }) ?? .null

        case .arrayAgg:
            return values.isEmpty ? .null : .array(values)
        }
    }

    private func aggregateNumericError(
        function: String,
        failure: DatabaseNumericAggregateAccumulator.Failure
    ) -> DatabaseAggregateEvaluationError {
        switch failure {
        case .incompatibleNumericKinds:
            return .incompatibleNumericKinds(function: function)
        case .nonNumericValue:
            return .nonNumericValue(function: function)
        case .nonFiniteValue:
            return .nonFiniteValue(function: function)
        case .numericOverflow:
            return .numericOverflow(function: function)
        case .resultNotRepresentable:
            return .resultNotRepresentable(function: function)
        }
    }

    private func evaluateQueryBoolean(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let value = try await evaluateQueryExpression(
            expression,
            on: row,
            context: context,
            workMeter: workMeter
        )
        do {
            return try DatabaseExpressionEvaluator(fields: ["value": value])
                .predicate(.column(ColumnRef("value")))
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func evaluateQueryExpression(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> FieldValue {
        let effectiveRow = row.overlaying(outer: context?.outerRow)
        let resolved = try await resolveQueryScopedExpression(
            expression,
            on: effectiveRow,
            context: context,
            workMeter: workMeter
        )
        do {
            return try DatabaseExpressionEvaluator(
                fields: effectiveRow.fields,
                ambiguousColumns: effectiveRow.ambiguousUnqualifiedColumns,
                workMeter: workMeter
            )
                .evaluate(resolved)
        } catch let error as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(error)
        }
    }

    private func evaluateExpressions(
        _ expressions: [Expression],
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> [FieldValue] {
        var values: [FieldValue] = []
        values.reserveCapacity(expressions.count)
        for expression in expressions {
            values.append(
                try await evaluateQueryExpression(
                    expression,
                    on: row,
                    context: context,
                    workMeter: workMeter
                )
            )
        }
        return values
    }

    private func resolveQueryScopedExpression(
        _ expression: Expression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Expression {
        func resolve(_ nested: Expression) async throws -> Expression {
            try await resolveQueryScopedExpression(
                nested,
                on: row,
                context: context,
                workMeter: workMeter
            )
        }

        switch expression {
        case .subquery(let query):
            let columnCount = try nestedQueryOutputColumnCount(
                query,
                context: context
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: nil,
                    columnCount: columnCount
                )
            }
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            let visibleRows = response.visibleRows
            guard visibleRows.count <= 1 else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: visibleRows.count,
                    columnCount: nil
                )
            }
            guard !visibleRows.isEmpty else {
                return .literal(.null)
            }
            let resultRow = visibleRows[visibleRows.startIndex]
            guard resultRow.fields.count == 1,
                  let value = resultRow.fields.values.first else {
                throw CanonicalReadError.invalidScalarSubquery(
                    rowCount: 1,
                    columnCount: resultRow.fields.count
                )
            }
            return .literal(try value.toLiteral())

        case .exists(let query):
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            return .literal(.bool(!response.visibleRows.isEmpty))

        case .inSubquery(let value, let query):
            let resolvedValue = try await resolve(value)
            let candidate: FieldValue
            do {
                candidate = try DatabaseExpressionEvaluator(
                    fields: row.fields,
                    ambiguousColumns: row.ambiguousUnqualifiedColumns,
                    workMeter: workMeter
                ).evaluate(resolvedValue)
            } catch let error as DatabaseExpressionEvaluationError {
                throw CanonicalReadError.expressionEvaluation(error)
            }
            if candidate.isNull {
                return .literal(.null)
            }
            let columnCount = try nestedQueryOutputColumnCount(
                query,
                context: context
            )
            guard columnCount == 1 else {
                throw CanonicalReadError.invalidMembershipSubquery(
                    columnCount: columnCount
                )
            }
            let response = try await executeNestedQuery(
                query,
                outerRow: row,
                context: context
            )
            let visibleRows = response.visibleRows
            var sawNull = false
            for resultRow in visibleRows {
                guard resultRow.fields.count == 1,
                      let value = resultRow.fields.values.first else {
                    throw CanonicalReadError.invalidMembershipSubquery(
                        columnCount: resultRow.fields.count
                    )
                }
                if value.isNull {
                    sawNull = true
                    continue
                }
                do {
                    if try FieldValueComparator.equal(candidate, value) {
                        return .literal(.bool(true))
                    }
                } catch let failure {
                    throw canonicalComparisonReadError(
                        failure,
                        operation: "IN subquery equality"
                    )
                }
            }
            return .literal(sawNull ? .null : .bool(false))

        case .aggregate:
            throw CanonicalReadError.aggregateEvaluation(
                .invalidGroupedExpression(
                    "Aggregate expression reached scalar evaluation without a group"
                )
            )
        case .literal, .column, .variable, .parameter, .bound:
            return expression
        case .add(let lhs, let rhs): return .add(try await resolve(lhs), try await resolve(rhs))
        case .subtract(let lhs, let rhs): return .subtract(try await resolve(lhs), try await resolve(rhs))
        case .multiply(let lhs, let rhs): return .multiply(try await resolve(lhs), try await resolve(rhs))
        case .divide(let lhs, let rhs): return .divide(try await resolve(lhs), try await resolve(rhs))
        case .modulo(let lhs, let rhs): return .modulo(try await resolve(lhs), try await resolve(rhs))
        case .negate(let nested): return .negate(try await resolve(nested))
        case .equal(let lhs, let rhs): return .equal(try await resolve(lhs), try await resolve(rhs))
        case .notEqual(let lhs, let rhs): return .notEqual(try await resolve(lhs), try await resolve(rhs))
        case .lessThan(let lhs, let rhs): return .lessThan(try await resolve(lhs), try await resolve(rhs))
        case .lessThanOrEqual(let lhs, let rhs):
            return .lessThanOrEqual(try await resolve(lhs), try await resolve(rhs))
        case .greaterThan(let lhs, let rhs): return .greaterThan(try await resolve(lhs), try await resolve(rhs))
        case .greaterThanOrEqual(let lhs, let rhs):
            return .greaterThanOrEqual(try await resolve(lhs), try await resolve(rhs))
        case .and(let lhs, let rhs): return .and(try await resolve(lhs), try await resolve(rhs))
        case .or(let lhs, let rhs): return .or(try await resolve(lhs), try await resolve(rhs))
        case .not(let nested): return .not(try await resolve(nested))
        case .isNull(let nested): return .isNull(try await resolve(nested))
        case .isNotNull(let nested): return .isNotNull(try await resolve(nested))
        case .like(let nested, let pattern):
            return .like(try await resolve(nested), pattern: pattern)
        case .regex(let nested, let pattern, let flags):
            return .regex(try await resolve(nested), pattern: pattern, flags: flags)
        case .between(let nested, let low, let high):
            return .between(
                try await resolve(nested),
                low: try await resolve(low),
                high: try await resolve(high)
            )
        case .inList(let nested, let values):
            return .inList(
                try await resolve(nested),
                values: try await rewriteExpressions(values, using: resolve)
            )
        case .notInList(let nested, let values):
            return .notInList(
                try await resolve(nested),
                values: try await rewriteExpressions(values, using: resolve)
            )
        case .function(let function):
            return .function(
                FunctionCall(
                    name: function.name,
                    arguments: try await rewriteExpressions(
                        function.arguments,
                        using: resolve
                    ),
                    distinct: function.distinct
                )
            )
        case .caseWhen(let pairs, let fallback):
            return .caseWhen(
                cases: try await rewriteCasePairs(pairs, using: resolve),
                elseResult: try await rewriteOptionalExpression(
                    fallback,
                    using: resolve
                )
            )
        case .coalesce(let values):
            return .coalesce(
                try await rewriteExpressions(values, using: resolve)
            )
        case .nullIf(let lhs, let rhs):
            return .nullIf(try await resolve(lhs), try await resolve(rhs))
        case .cast(let nested, let type):
            return .cast(try await resolve(nested), targetType: type)
        case .triple(let subject, let predicate, let object):
            return .triple(
                subject: try await resolve(subject),
                predicate: try await resolve(predicate),
                object: try await resolve(object)
            )
        case .isTriple(let nested): return .isTriple(try await resolve(nested))
        case .subject(let nested): return .subject(try await resolve(nested))
        case .predicate(let nested): return .predicate(try await resolve(nested))
        case .object(let nested): return .object(try await resolve(nested))
        }
    }

    private func executeNestedQuery(
        _ query: SelectQuery,
        outerRow: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?
    ) async throws -> CanonicalRetainedQueryResponse {
        guard let context else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Subquery evaluation requires a transaction-bound query context"
            )
        }
        return try await queryCanonical(
            query,
            options: executionContextWithoutExternalPageWindow(context.options),
            partitionValues: context.partitionValues,
            partitionMode: context.partitionMode,
            transaction: context.transaction,
            inheritedSubqueries: context.namedSubqueries,
            outerRow: outerRow,
            preparedFusionGraph: context.preparedFusionGraph
        )
    }

    private func nestedQueryOutputColumnCount(
        _ query: SelectQuery,
        context: CanonicalQueryEvaluationContext?
    ) throws -> Int {
        let inherited = context?.namedSubqueries ?? []
        let namedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: inherited
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: namedSubqueries
        ) {
            switch query.projection {
            case .items, .distinctItems:
                return try canonicalProjectionColumns(
                    query.projection,
                    sourceSchema: CanonicalRelationSchema()
                ).count
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare exactly one output column"
                )
            }
        }
        let sourceSchema = try canonicalRelationSchema(
            for: query.source,
            namedSubqueries: namedSubqueries
        )
        return try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        ).count
    }

    private func canonicalProjectionName(
        for expression: DatabaseKit.Expression,
        index: Int
    ) -> String {
        switch expression {
        case .column(let column):
            return column.column
        case .aggregate(let aggregate):
            switch aggregate {
            case .count: return "count"
            case .sum: return "sum"
            case .avg: return "avg"
            case .min: return "min"
            case .max: return "max"
            case .groupConcat: return "group_concat"
            case .sample: return "sample"
            case .arrayAgg: return "array_agg"
            }
        default:
            return "column\(index)"
        }
    }

    private func canonicalUniqueRows(
        _ rows: CanonicalRetainedQueryRows,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedQueryRows {
        let setBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(
                max(1, MemoryLayout<CanonicalRowValueIdentity>.stride + 32)
            )
        ).multiplied(by: UInt64(rows.count)).bytes
        let setReservation = try workMeter.reserveIntermediate(
            bytes: setBytes,
            at: .deduplication
        )
        defer { setReservation.release() }
        var seen: Set<CanonicalRowValueIdentity> = []
        seen.reserveCapacity(rows.count)
        var unique = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .deduplication)
            if seen.insert(
                CanonicalRowValueIdentity(
                    fields: try canonicalIdentityFields(
                        row.fields,
                        operation: "SELECT DISTINCT"
                    )
                )
            ).inserted {
                try unique.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        return try unique.finish().moveToSharedOwnership(at: .deduplication)
    }

    private func canonicalGroupedRowFootprint(
        _ group: CanonicalGroupedRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: group.key.values.count,
                element: FieldValue.self
            ).adding(
            try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: group.key.identity.count,
                element: FieldValue.self
            )
        )
        footprint = try footprint.adding(
            CanonicalRelationalFootprintMeter.footprint(
                of: group.representative,
                workMeter: workMeter
            )
        )
        return footprint
    }

    private func tableRelationSchema(
        _ tableRef: TableRef
    ) throws -> CanonicalRelationSchema {
        let entity = try resolveEntity(named: tableRef.table)
        return try CanonicalRelationSchema(
            scopes: [
                CanonicalRelationScope(
                    name: tableRef.alias ?? tableRef.effectiveName,
                    columns: entity.allFields
                )
            ]
        )
    }

    private func polymorphicRelationSchema(
        _ group: PolymorphicGroup,
        sourceName: String
    ) throws -> CanonicalRelationSchema {
        var seen = Set<String>()
        var columns: [String] = []
        for entityName in group.memberTypeNames {
            let entity = try resolveEntity(named: entityName)
            for field in entity.allFields where seen.insert(field).inserted {
                columns.append(field)
            }
        }
        return try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: sourceName, columns: columns)]
        )
    }

    private func graphTableRelationSchema(
        _ source: GraphTableSource,
        rows: CanonicalRetainedRows?
    ) throws -> CanonicalRelationSchema {
        if let columns = source.columns, !columns.isEmpty {
            let names = columns.map { $0.alias }
            if let alias = source.alias {
                return try CanonicalRelationSchema(
                    scopes: [CanonicalRelationScope(name: alias, columns: names)]
                )
            }
            return try CanonicalRelationSchema(unscopedColumns: names)
        }
        guard let rows, !rows.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "An empty GRAPH_TABLE source requires an explicit COLUMNS schema"
            )
        }
        let inferred = try rows.withElement(at: 0) { row in
            try CanonicalRelationSchema(
                unscopedColumns: row.fields.keys.sorted()
            )
        }
        if let alias = source.alias {
            return try inferred.applyingAlias(alias)
        }
        return inferred
    }

    private func materializeQueryRelation<Rows: Collection>(
        _ rows: Rows,
        query: SelectQuery,
        explicitColumns: [String]?,
        alias: String,
        namedSubqueries: [NamedSubquery],
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRelation
    where Rows.Element == QueryRow {
        let visibleNamedSubqueries = try mergeNamedSubqueries(
            local: query.subqueries ?? [],
            inherited: namedSubqueries
        )
        if sourceRequiresRuntimeInferredSchema(
            query.source,
            namedSubqueries: visibleNamedSubqueries
        ) {
            switch query.projection {
            case .all, .allFrom:
                throw CanonicalReadError.unsupportedSelectQuery(
                    "A nested query over a runtime-inferred source must declare its output columns"
                )
            case .items, .distinctItems:
                break
            }
        }
        let sourceSchema: CanonicalRelationSchema
        switch query.projection {
        case .items, .distinctItems:
            sourceSchema = try CanonicalRelationSchema()
        case .all, .allFrom:
            sourceSchema = try canonicalRelationSchema(
                for: query.source,
                namedSubqueries: visibleNamedSubqueries
            )
        }
        let outputColumns = try canonicalProjectionColumns(
            query.projection,
            sourceSchema: sourceSchema
        )
        let columns = explicitColumns ?? outputColumns
        guard columns.count == outputColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A common table expression column list must match its query output"
            )
        }
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A subquery exposes duplicate column names"
            )
        }

        let schema = try CanonicalRelationSchema(
            scopes: [CanonicalRelationScope(name: alias, columns: columns)]
        )
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: .bindingCandidate)
            var fields: [String: FieldValue] = [:]
            fields.reserveCapacity(columns.count)
            for (sourceColumn, targetColumn) in zip(outputColumns, columns) {
                guard let value = row.fields[sourceColumn] else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Subquery output column '\(sourceColumn)' is missing"
                    )
                }
                fields[targetColumn] = value
            }
            let sourceRow = CanonicalSourceRow.fromBaseFields(
                fields,
                sourceName: alias,
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
        return CanonicalRelation(
            schema: schema,
            rows: try retained.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
        )
    }

    private func canonicalRelationSchema(
        for source: DataSource,
        namedSubqueries: [NamedSubquery]
    ) throws -> CanonicalRelationSchema {
        switch source {
        case .table(let tableRef):
            if let named = namedSubqueries.first(where: { $0.name == tableRef.table }) {
                let visibleNamedSubqueries = try mergeNamedSubqueries(
                    local: named.query.subqueries ?? [],
                    inherited: namedSubqueries
                )
                let sourceSchema: CanonicalRelationSchema
                switch named.query.projection {
                case .items, .distinctItems:
                    sourceSchema = try CanonicalRelationSchema()
                case .all, .allFrom:
                    sourceSchema = try canonicalRelationSchema(
                        for: named.query.source,
                        namedSubqueries: visibleNamedSubqueries
                    )
                }
                let projected = try canonicalProjectionColumns(
                    named.query.projection,
                    sourceSchema: sourceSchema
                )
                let columns = named.columns ?? projected
                guard columns.count == projected.count else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "A common table expression column list must match its query output"
                    )
                }
                return try CanonicalRelationSchema(
                    scopes: [
                        CanonicalRelationScope(
                            name: tableRef.alias ?? named.name,
                            columns: columns
                        )
                    ]
                )
            }
            return try tableRelationSchema(tableRef)

        case .logical(let source):
            guard source.kindIdentifier == LogicalSourceKind.polymorphic else {
                throw CanonicalReadError.unsupportedSource(
                    "Logical source '\(source.kindIdentifier)' has no relational schema provider"
                )
            }
            return try polymorphicRelationSchema(
                container.polymorphicGroup(identifier: source.identifier),
                sourceName: source.effectiveName
            )

        case .subquery(let query, let alias):
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: query.subqueries ?? [],
                inherited: namedSubqueries
            )
            let sourceSchema: CanonicalRelationSchema
            switch query.projection {
            case .items, .distinctItems:
                sourceSchema = try CanonicalRelationSchema()
            case .all, .allFrom:
                sourceSchema = try canonicalRelationSchema(
                    for: query.source,
                    namedSubqueries: visibleNamedSubqueries
                )
            }
            return try CanonicalRelationSchema(
                scopes: [
                    CanonicalRelationScope(
                        name: alias,
                        columns: try canonicalProjectionColumns(
                            query.projection,
                            sourceSchema: sourceSchema
                        )
                    )
                ]
            )

        case .join(let join):
            try validateJoinDeclaration(join)
            let left = try canonicalRelationSchema(
                for: join.left,
                namedSubqueries: namedSubqueries
            )
            let right = try canonicalRelationSchema(
                for: join.right,
                namedSubqueries: namedSubqueries
            )
            let condition: JoinCondition?
            switch join.type {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                condition = .using(
                    inferNaturalJoinColumns(
                        leftSchema: left,
                        rightSchema: right
                    )
                )
            default:
                condition = join.condition
            }
            try validateJoinCondition(
                condition,
                leftSchema: left,
                rightSchema: right,
                type: join.type
            )
            return try joinOutputSchema(
                left,
                right,
                condition: condition
            )

        case .values(let rows, let columnNames):
            return try CanonicalRelationSchema(
                unscopedColumns: columnNames
                    ?? rows.first?.indices.map { "column\($0)" }
                    ?? []
            )

        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            guard let first = sources.first else {
                return try CanonicalRelationSchema()
            }
            return try CanonicalRelationSchema(
                unscopedColumns: canonicalRelationSchema(
                    for: first,
                    namedSubqueries: namedSubqueries
                ).visibleColumns
            )

        case .except(let lhs, _):
            return try CanonicalRelationSchema(
                unscopedColumns: canonicalRelationSchema(
                    for: lhs,
                    namedSubqueries: namedSubqueries
                ).visibleColumns
            )

        case .graphTable(let graphTable):
            return try graphTableRelationSchema(graphTable, rows: nil)

        case .graphPattern(let pattern):
            return try CanonicalRelationSchema(
                unscopedColumns: sparqlVariables(in: pattern)
            )

        case .namedGraph(_, let pattern):
            return try CanonicalRelationSchema(
                unscopedColumns: sparqlVariables(in: pattern)
            )

        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        #if DATABASE_MULTI_BASE
        case .base:
            throw CanonicalReadError.unsupportedSource(
                "Base-qualified sources require a Composition planner"
            )
        #endif
        }
    }

    private func sourceRequiresRuntimeInferredSchema(
        _ source: DataSource,
        namedSubqueries: [NamedSubquery]
    ) -> Bool {
        switch source {
        case .table(let table):
            guard let named = namedSubqueries.first(
                where: { $0.name == table.table }
            ) else {
                return false
            }
            let nestedNames = (named.query.subqueries ?? []).map { $0.name }
            let inherited = namedSubqueries.filter {
                !nestedNames.contains($0.name)
            }
            switch named.query.projection {
            case .items, .distinctItems:
                return false
            case .all, .allFrom:
                return sourceRequiresRuntimeInferredSchema(
                    named.query.source,
                    namedSubqueries: (named.query.subqueries ?? []) + inherited
                )
            }
        case .subquery(let query, _):
            let nestedNames = (query.subqueries ?? []).map { $0.name }
            let inherited = namedSubqueries.filter {
                !nestedNames.contains($0.name)
            }
            switch query.projection {
            case .items, .distinctItems:
                return false
            case .all, .allFrom:
                return sourceRequiresRuntimeInferredSchema(
                    query.source,
                    namedSubqueries: (query.subqueries ?? []) + inherited
                )
            }
        case .join(let join):
            return sourceRequiresRuntimeInferredSchema(
                join.left,
                namedSubqueries: namedSubqueries
            ) || sourceRequiresRuntimeInferredSchema(
                join.right,
                namedSubqueries: namedSubqueries
            )
        case .union(let sources), .unionAll(let sources),
                .intersect(let sources):
            guard let first = sources.first else { return false }
            return sourceRequiresRuntimeInferredSchema(
                first,
                namedSubqueries: namedSubqueries
            )
        case .except(let lhs, _):
            return sourceRequiresRuntimeInferredSchema(
                lhs,
                namedSubqueries: namedSubqueries
            )
        case .graphTable(let graphTable):
            return graphTable.columns?.isEmpty ?? true
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            return sourceRequiresRuntimeInferredSchema(
                nested,
                namedSubqueries: namedSubqueries
            )
        #endif
        case .logical, .values, .graphPattern, .namedGraph, .service:
            return false
        }
    }

    private func validateStaticJoinBindings(
        _ source: DataSource,
        namedSubqueries: [NamedSubquery],
        outerRow: CanonicalSourceRow?
    ) throws {
        switch source {
        case .join(let join):
            let leftSchema = try canonicalRelationSchema(
                for: join.left,
                namedSubqueries: namedSubqueries
            )
            let rightSchema = try canonicalRelationSchema(
                for: join.right,
                namedSubqueries: namedSubqueries
            )
            let rightOuterRow: CanonicalSourceRow?
            switch join.type {
            case .lateral, .leftLateral:
                rightOuterRow = leftSchema.nullRow().overlaying(
                    outer: outerRow
                )
            default:
                rightOuterRow = outerRow
            }
            try validateStaticJoinBindings(
                join.left,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
            try validateStaticJoinBindings(
                join.right,
                namedSubqueries: namedSubqueries,
                outerRow: rightOuterRow
            )
            if case .on(let expression) = join.condition {
                let outputSchema = try joinOutputSchema(
                    leftSchema,
                    rightSchema,
                    condition: join.condition
                )
                try validateExpressionBindings(
                    expression,
                    sourceSchema: outputSchema,
                    outerRow: outerRow,
                    namedSubqueries: namedSubqueries
                )
            }
        case .subquery(let query, _):
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: query.subqueries ?? [],
                inherited: namedSubqueries
            )
            guard !sourceRequiresRuntimeInferredSchema(
                query.source,
                namedSubqueries: visibleNamedSubqueries
            ) else {
                return
            }
            let sourceSchema = try canonicalRelationSchema(
                for: query.source,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateRelationalQueryBindings(
                query,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateStaticJoinBindings(
                query.source,
                namedSubqueries: visibleNamedSubqueries,
                outerRow: outerRow
            )
        case .table(let table):
            guard let named = namedSubqueries.first(
                where: { $0.name == table.table }
            ) else {
                return
            }
            let visibleNamedSubqueries = try mergeNamedSubqueries(
                local: named.query.subqueries ?? [],
                inherited: namedSubqueries
            )
            guard !sourceRequiresRuntimeInferredSchema(
                named.query.source,
                namedSubqueries: visibleNamedSubqueries
            ) else {
                return
            }
            let sourceSchema = try canonicalRelationSchema(
                for: named.query.source,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateRelationalQueryBindings(
                named.query,
                sourceSchema: sourceSchema,
                outerRow: outerRow,
                namedSubqueries: visibleNamedSubqueries
            )
            try validateStaticJoinBindings(
                named.query.source,
                namedSubqueries: visibleNamedSubqueries,
                outerRow: outerRow
            )
        case .union(let sources), .unionAll(let sources),
                .intersect(let sources):
            for source in sources {
                try validateStaticJoinBindings(
                    source,
                    namedSubqueries: namedSubqueries,
                    outerRow: outerRow
                )
            }
        case .except(let lhs, let rhs):
            try validateStaticJoinBindings(
                lhs,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
            try validateStaticJoinBindings(
                rhs,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        #if DATABASE_MULTI_BASE
        case .base(_, let nested):
            try validateStaticJoinBindings(
                nested,
                namedSubqueries: namedSubqueries,
                outerRow: outerRow
            )
        #endif
        case .logical, .values, .graphTable, .graphPattern, .namedGraph,
                .service:
            break
        }
    }

    private func canonicalProjectionColumns(
        _ projection: Projection,
        sourceSchema: CanonicalRelationSchema
    ) throws -> [String] {
        let columns: [String]
        switch projection {
        case .all:
            columns = sourceSchema.visibleColumns
        case .allFrom(let sourceName):
            guard let scope = sourceSchema.scopes.first(
                where: { $0.name == sourceName }
            ) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Projection source '\(sourceName)' not found"
                )
            }
            columns = scope.columns
        case .items(let items), .distinctItems(let items):
            columns = items.enumerated().map { index, item in
                item.alias ?? canonicalProjectionName(
                    for: item.expression,
                    index: index
                )
            }
        }
        guard Set(columns).count == columns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "A projection exposes duplicate column names"
            )
        }
        return columns
    }

    private func sparqlVariables(in pattern: GraphPattern) -> [String] {
        var result: [String] = []
        var seen = Set<String>()
        func append(_ name: String) {
            if seen.insert(name).inserted { result.append(name) }
        }
        func visitTerm(_ term: SPARQLTerm) {
            switch term {
            case .variable(let name):
                append(name)
            case .tripleTerm(let subject, let predicate, let object):
                visitTerm(subject)
                visitTerm(predicate)
                visitTerm(object)
            case .reifiedTriple(let subject, let predicate, let object, let reifier):
                visitTerm(subject)
                visitTerm(predicate)
                visitTerm(object)
                visitTerm(reifier)
            default:
                break
            }
        }
        func visit(_ current: GraphPattern) {
            switch current {
            case .basic(let basic):
                for element in basic.elements {
                    switch element {
                    case .triple(let triple):
                        visitTerm(triple.subject)
                        visitTerm(triple.predicate)
                        visitTerm(triple.object)
                    case .propertyPath(let path):
                        visitTerm(path.subject)
                        visitTerm(path.object)
                    }
                }
            case .join(let lhs, let rhs), .optional(let lhs, let rhs),
                    .union(let lhs, let rhs), .minus(let lhs, let rhs),
                    .lateral(let lhs, let rhs):
                visit(lhs)
                visit(rhs)
            case .filter(let nested, _):
                visit(nested)
            case .graph(let name, let nested):
                visitTerm(name)
                visit(nested)
            case .service(_, let nested, _):
                visit(nested)
            case .bind(let nested, let variable, _):
                visit(nested)
                append(variable)
            case .values(let variables, _):
                variables.forEach(append)
            case .subquery(let query):
                switch query.projection {
                case .items(let items), .distinctItems(let items):
                    for (index, item) in items.enumerated() {
                        append(
                            item.alias ?? canonicalProjectionName(
                                for: item.expression,
                                index: index
                            )
                        )
                    }
                case .all:
                    switch query.source {
                    case .graphPattern(let nested),
                            .namedGraph(_, let nested),
                            .service(_, let nested, _):
                        visit(nested)
                    default:
                        break
                    }
                case .allFrom:
                    break
                }
            case .groupBy(let nested, _, let aggregates):
                visit(nested)
                aggregates.forEach { append($0.variable) }
            }
        }
        visit(pattern)
        return result
    }

    private func materializeSetOperationInputs(
        _ sources: [DataSource],
        namedSubqueries: [NamedSubquery],
        options: ReadExecutionContext,
        partitionValues: FieldObject?,
        partitionMode: CanonicalPartitionRoutingMode,
        transaction: any TransactionAccess,
        preparedFusionGraph: FusionPreparedQueryGraph,
        outerRow: CanonicalSourceRow? = nil,
        allowsOuterReferences: Bool = false
    ) async throws -> [CanonicalRelation] {
        var relations: [CanonicalRelation] = []
        relations.reserveCapacity(sources.count)
        for source in sources {
            relations.append(
                try await materializeRows(
                    for: source,
                    namedSubqueries: namedSubqueries,
                    options: options,
                    partitionValues: partitionValues,
                    partitionMode: partitionMode,
                    transaction: transaction,
                    preparedFusionGraph: preparedFusionGraph,
                    outerRow: outerRow,
                    allowsOuterReferences: allowsOuterReferences
                )
            )
        }
        return relations
    }

    private func alignSetOperationRows(
        _ relation: CanonicalRelation,
        to outputSchema: CanonicalRelationSchema,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedRows {
        let sourceColumns = relation.schema.visibleColumns
        let outputColumns = outputSchema.visibleColumns
        guard sourceColumns.count == outputColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Set operation inputs must expose the same number of columns"
            )
        }
        var aligned = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: relation.rows.count
        )
        for row in relation.rows {
            try workMeter.consume(at: .bindingCandidate)
            var fields: [String: FieldValue] = [:]
            fields.reserveCapacity(outputColumns.count)
            for (sourceColumn, outputColumn) in zip(sourceColumns, outputColumns) {
                guard let value = row.fields[sourceColumn] else {
                    throw CanonicalReadError.unsupportedSelectQuery(
                        "Set operation input column '\(sourceColumn)' is missing"
                    )
                }
                fields[outputColumn] = value
            }
            let sourceRow = CanonicalSourceRow(
                fields: fields,
                annotations: row.annotations,
                version: row.version
            )
            try aligned.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: sourceRow,
                    workMeter: workMeter
                ),
                make: { sourceRow }
            )
        }
        return try aligned.finish().moveToSharedOwnership(at: .bindingCandidate)
    }

    private func materializeSourceRows(
        _ rows: [QueryRow],
        sourceName: String?,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedRows {
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
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
        return try retained.finish().moveToSharedOwnership(at: .bindingCandidate)
    }

    /// Adapts a completed logical-source result back into request-accounted
    /// ownership. Logical-source executors expose `QueryResponse` as their
    /// public boundary; canonical composition must re-establish ownership
    /// before the result crosses another query operator.
    private func retainExternalQueryResponse(
        _ response: QueryResponse,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalRetainedQueryResponse {
        var retained = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: response.rows.count
        )
        for row in response.rows {
            try retained.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        let rows = try retained.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )
        return CanonicalRetainedQueryResponse(
            rows: rows,
            visibleRange: rows.startIndex..<rows.endIndex,
            continuation: response.continuation,
            metadata: response.metadata,
            affectedRows: response.affectedRows
        )
    }

    private func retainedCanonicalRow(
        _ row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            CollectionOfOne(row),
            expectedCount: 1,
            workMeter: workMeter,
            stage: stage
        )
    }

    private func retainedCanonicalRows<Rows: Sequence>(
        _ rows: Rows,
        expectedCount: Int,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows where Rows.Element == CanonicalSourceRow {
        var retained = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self),
            expectedCount: expectedCount
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
        return try retained.finish().moveToSharedOwnership(at: stage)
    }

    private func retainedCanonicalRows(
        _ rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            rows,
            expectedCount: rows.count,
            workMeter: workMeter,
            stage: stage
        )
    }

    private func emptyCanonicalRows(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> CanonicalRetainedRows {
        try retainedCanonicalRows(
            EmptyCollection<CanonicalSourceRow>(),
            expectedCount: 0,
            workMeter: workMeter,
            stage: stage
        )
    }
}
