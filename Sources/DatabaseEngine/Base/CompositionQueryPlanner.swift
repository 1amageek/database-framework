#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

/// Plans relational Composition reads and owns the domain-neutral bounded row
/// merger shared with feature-owned Base-local source executors.
@_spi(DatabaseExecution)
public struct CompositionQueryPlanner: Sendable {
    private struct RelationalMemberQueryExecutor:
        CompositionMemberQueryExecutor
    {
        func validate(_ query: SelectQuery) throws {
            try CompositionQueryPlanner.validateRelationalSource(query)
        }

        func execute(
            context: DatabaseContext,
            query: SelectQuery,
            execution: ReadExecutionContext,
            transaction: any TransactionAccess
        ) async throws -> QueryResponse {
            try await context.query(
                query,
                execution: execution,
                transaction: transaction
            )
        }

        func prepare(
            _ row: DatabaseEngine.QueryRow,
            sourceBaseID: Base.ID
        ) throws -> DatabaseEngine.QueryRow {
            row
        }
    }

    private struct OriginRow: Sendable {
        let row: DatabaseEngine.QueryRow
        var origin: CompositionOrigin
        let sequence: UInt64
        let fingerprint: ByteString
    }

    private struct MemberCursor: Sendable {
        let member: DatabaseBaseLease
        var continuation: QueryContinuation?
        var rows: [DatabaseEngine.QueryRow]
        var nextRowIndex: Int
        var reachedEnd: Bool
        var reservation: DatabaseIntermediateReservation?

        init(member: DatabaseBaseLease) {
            self.member = member
            self.continuation = nil
            self.rows = []
            self.nextRowIndex = 0
            self.reachedEnd = false
            self.reservation = nil
        }

        mutating func releaseConsumedPage() {
            guard nextRowIndex >= rows.count else { return }
            rows.removeAll(keepingCapacity: false)
            nextRowIndex = 0
            reservation?.release()
            reservation = nil
        }

        mutating func replacePage(
            _ newRows: consuming [DatabaseEngine.QueryRow],
            reservation newReservation: DatabaseIntermediateReservation?
        ) {
            precondition(rows.isEmpty && reservation == nil)
            rows = newRows
            nextRowIndex = 0
            reservation = newReservation
        }
    }

    private enum AggregateKind: Sendable {
        case countAll
        case countValue
        case sum
        case average
        case minimum
        case maximum

        var isMinimum: Bool {
            if case .minimum = self { return true }
            return false
        }
    }

    private enum MergeOrdering: Sendable {
        case query([SortKey])
        case vectorDistance
    }

    private struct AggregateDescriptor: Sendable {
        let outputName: String
        let operandName: String?
        let kind: AggregateKind
    }

    private struct CrossBaseJoinPlan: Sendable {
        let join: JoinClause
        let leftBaseID: Base.ID
        let leftTable: TableRef
        let rightBaseID: Base.ID
        let rightTable: TableRef
    }

    private enum AggregateState: Sendable {
        case count(Int64)
        case numeric(DatabaseNumericAggregateAccumulator)
        case extremum(FieldValue?)
    }

    private struct MergeHeap {
        struct Entry: Sendable {
            let memberIndex: Int
            let row: OriginRow
            let reservation: DatabaseIntermediateReservation
        }

        private(set) var rows: [Entry] = []

        mutating func insert(
            memberIndex: Int,
            row: OriginRow,
            footprint: UInt64,
            workMeter: DatabaseWorkMeter,
            orderedBefore: (OriginRow, OriginRow) throws -> Bool
        ) throws {
            let reservation = try workMeter.reserveIntermediate(
                rows: 1,
                bytes: footprint,
                at: .resultMaterialization
            )
            rows.append(
                Entry(
                    memberIndex: memberIndex,
                    row: row,
                    reservation: reservation
                )
            )
            var child = rows.count - 1
            while child > 0 {
                let parent = (child - 1) / 2
                guard try orderedBefore(rows[child].row, rows[parent].row)
                else { break }
                rows.swapAt(child, parent)
                child = parent
            }
        }

        mutating func removeFirst(
            orderedBefore: (OriginRow, OriginRow) throws -> Bool
        ) throws -> Entry? {
            guard !rows.isEmpty else { return nil }
            if rows.count == 1 { return rows.removeLast() }
            let first = rows[0]
            rows[0] = rows.removeLast()
            var parent = 0
            while true {
                let left = parent * 2 + 1
                guard left < rows.count else { break }
                let right = left + 1
                var child = left
                if right < rows.count,
                   try orderedBefore(rows[right].row, rows[left].row) {
                    child = right
                }
                guard try orderedBefore(rows[child].row, rows[parent].row)
                else { break }
                rows.swapAt(parent, child)
                parent = child
            }
            return first
        }
    }

    private struct EmissionWindow {
        var remainingOffset: Int
        var remainingLimit: Int?

        var isExhausted: Bool {
            remainingLimit == 0
        }

        mutating func acceptsNextRow() -> Bool {
            if remainingOffset > 0 {
                remainingOffset -= 1
                return false
            }
            guard let remainingLimit else { return true }
            guard remainingLimit > 0 else { return false }
            self.remainingLimit = remainingLimit - 1
            return true
        }
    }

    private let structuralLimits: QueryStructuralLimits

    public init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
    }

    public func execute(
        _ query: SelectQuery,
        source: CompositionDataSource,
        options: CompositionQueryExecutionOptions,
        emit: @Sendable @escaping (CompositionQueryEvent) async throws -> Bool
    ) async throws {
        try QueryStructuralValidator.validate(
            query,
            limits: structuralLimits
        )
        try Self.validatePageSize(options.pageSize)
        let memberExecutor = RelationalMemberQueryExecutor()
        let crossBaseJoin = try Self.crossBaseJoinPlan(query)
        try Self.validateCommon(query)
        try memberExecutor.validate(query)
        try await executeValidated(
            query,
            source: source,
            options: options,
            crossBaseJoin: crossBaseJoin,
            memberExecutor: memberExecutor,
            emit: emit
        )
    }

    /// Executes a feature-owned Base-local source through the shared global
    /// Composition row merger. The feature executor is responsible for the
    /// complete source-family validation and identity contract.
    public func executeBaseLocal(
        _ query: SelectQuery,
        source: CompositionDataSource,
        options: CompositionQueryExecutionOptions,
        memberExecutor: any CompositionMemberQueryExecutor,
        emit: @Sendable @escaping (CompositionQueryEvent) async throws -> Bool
    ) async throws {
        try QueryStructuralValidator.validate(
            query,
            limits: structuralLimits
        )
        try Self.validatePageSize(options.pageSize)
        guard !Self.containsBaseQualifier(query.source) else {
            throw CompositionQueryError.unsupportedPlan(
                "a feature-owned Base-local source cannot select another Base"
            )
        }
        try Self.validateCommon(query)
        try memberExecutor.validate(query)
        try await executeValidated(
            query,
            source: source,
            options: options,
            crossBaseJoin: nil,
            memberExecutor: memberExecutor,
            emit: emit
        )
    }

    private func executeValidated(
        _ query: SelectQuery,
        source: CompositionDataSource,
        options: CompositionQueryExecutionOptions,
        crossBaseJoin: CrossBaseJoinPlan?,
        memberExecutor: any CompositionMemberQueryExecutor,
        emit: @Sendable @escaping (CompositionQueryEvent) async throws -> Bool
    ) async throws {
        let workMeter = options.readContext.workMeter

        try await source.withReadSnapshot { snapshot in
            let metadata = CompositionQueryMetadata(
                composition: snapshot.lease.resolution,
                basePlacementGenerations: snapshot.lease
                    .basePlacementGenerations,
                schemaGeneration: source.container.schemaGeneration,
                consistency: .federated(try await snapshot.readPoints())
            )
            guard try await emit(.began(metadata)) else { return }

            if let crossBaseJoin {
                try await executeCrossBaseJoin(
                    query,
                    plan: crossBaseJoin,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor,
                    emit: emit
                )
                return
            }

            var window = try Self.emissionWindow(query)
            if !window.isExhausted,
               let aggregates = try Self.aggregateProjection(query.projection) {
                let rows = try await executeAggregates(
                    query,
                    aggregates: aggregates,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor
                )
                for row in rows where window.acceptsNextRow() {
                    guard try await emit(
                        .row(
                            CompositionQueryRow(
                                row: row.row,
                                origin: row.origin
                            )
                        )
                    ) else {
                        return
                    }
                }
            } else if !window.isExhausted,
                      query.distinct
                        || Self.isDistinctProjection(query.projection) {
                let workspace = CompositionDistinctWorkspace.create(
                    maximumIntermediateBytes: options.readContext.options
                        .budget.maximumIntermediateBytes,
                    workMeter: workMeter
                )
                do {
                    var sequence: UInt64 = 0
                    try await executeRows(
                        query,
                        options: options,
                        source: source,
                        snapshot: snapshot,
                        workMeter: workMeter,
                        memberExecutor: memberExecutor
                    ) { row in
                        try await workspace.insert(
                            row.row,
                            origin: row.origin,
                            sequence: sequence
                        )
                        let next = sequence.addingReportingOverflow(1)
                        guard !next.overflow else {
                            throw CompositionQueryError.workspaceCorrupted
                        }
                        sequence = next.partialValue
                        return true
                    }
                    let distinctWindow = Mutex(window)
                    try await workspace.forEachResult(
                        batchSize: options.pageSize
                    ) { result in
                        guard distinctWindow.withLock({
                            $0.acceptsNextRow()
                        }) else {
                            return !distinctWindow.withLock { $0.isExhausted }
                        }
                        let shouldContinue = try await emit(
                            .row(
                                CompositionQueryRow(
                                    row: result.row,
                                    origin: result.origin
                                )
                            )
                        )
                        return shouldContinue
                            && !distinctWindow.withLock { $0.isExhausted }
                    }
                    await workspace.removeAll()
                } catch {
                    let operationError = error
                    await workspace.removeAll()
                    throw operationError
                }
            } else if !window.isExhausted {
                try await executeRows(
                    query,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor
                ) { row in
                    guard window.acceptsNextRow() else {
                        return !window.isExhausted
                    }
                    return try await emit(
                        .row(
                            CompositionQueryRow(
                                row: row.row,
                                origin: row.origin
                            )
                        )
                    ) && !window.isExhausted
                }
            }
        }
    }

    private static func validateRelational(
        _ query: SelectQuery,
        crossBaseJoin: CrossBaseJoinPlan?
    ) throws {
        if crossBaseJoin != nil {
            guard query.accessPath == nil,
                  query.subqueries == nil,
                  query.groupBy == nil,
                  query.having == nil,
                  query.reduced == false,
                  query.dataset == .implicit else {
                throw CompositionQueryError.unsupportedPlan(
                    "cross-Base JOIN does not accept access paths, subqueries, grouping, or dataset clauses"
                )
            }
            return
        }
        let vectorScan = try Self.vectorIndexScan(query)
        if query.accessPath != nil, vectorScan == nil {
            throw CompositionQueryError.unsupportedPlan(
                "this index access requires a kind-specific federated planner"
            )
        }
        guard query.reduced == false else {
            throw CompositionQueryError.unsupportedPlan(
                "REDUCED is not part of relational Composition execution"
            )
        }
        guard query.dataset == .implicit else {
            throw CompositionQueryError.unsupportedPlan(
                "dataset selection is not part of relational Composition execution"
            )
        }
        guard vectorScan != nil
                ? Self.isVectorSource(query.source)
                : Self.isBaseLocalRelationalSource(query.source)
        else {
            throw CompositionQueryError.unsupportedPlan(
                "the relational source cannot be proven Base-local"
            )
        }
        let aggregateProjection = try Self.aggregateProjection(
            query.projection
        )
        if vectorScan != nil {
            guard aggregateProjection == nil,
                  !query.distinct,
                  !Self.isDistinctProjection(query.projection),
                  query.orderBy == nil || query.orderBy?.isEmpty == true,
                  query.offset == nil || query.offset == 0 else {
                throw CompositionQueryError.unsupportedPlan(
                    "vector Composition search does not accept aggregate, DISTINCT, ORDER BY, or OFFSET"
                )
            }
        }
    }

    private static func validateCommon(_ query: SelectQuery) throws {
        guard query.subqueries == nil else {
            throw CompositionQueryError.unsupportedPlan(
                "subqueries cannot be proven Base-local"
            )
        }
        guard query.groupBy == nil, query.having == nil else {
            throw CompositionQueryError.unsupportedPlan(
                "grouped aggregation is not yet a decomposable Composition plan"
            )
        }
        if let orderBy = query.orderBy {
            for sortKey in orderBy {
                switch sortKey.expression {
                case .column, .variable:
                    break
                default:
                    throw CompositionQueryError.unsupportedPlan(
                            "global ordering requires an output column or variable"
                        )
                }
            }
        }
    }

    private static func validateRelationalSource(
        _ query: SelectQuery
    ) throws {
        let crossBaseJoin = try crossBaseJoinPlan(query)
        try validateRelational(query, crossBaseJoin: crossBaseJoin)
    }

    private static func validatePageSize(_ pageSize: Int) throws {
        guard pageSize > 0 else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "pageSize must be greater than zero"
            )
        }
    }

    private func executeCrossBaseJoin(
        _ query: SelectQuery,
        plan: CrossBaseJoinPlan,
        options: CompositionQueryExecutionOptions,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        memberExecutor: any CompositionMemberQueryExecutor,
        emit: @Sendable @escaping (CompositionQueryEvent) async throws -> Bool
    ) async throws {
        guard let leftMember = snapshot.lease.member(
            identifiedBy: plan.leftBaseID
        ) else {
            throw CompositionQueryError.unsupportedPlan(
                "the left Base is not a member of the selected Composition"
            )
        }
        guard let rightMember = snapshot.lease.member(
            identifiedBy: plan.rightBaseID
        ) else {
            throw CompositionQueryError.unsupportedPlan(
                "the right Base is not a member of the selected Composition"
            )
        }

        let authorizationPlan = DatabaseFieldReadAuthorizationPlan.make(
            query: query,
            schema: source.container.schema
        )
        try Self.validateCrossBaseColumns(
            query,
            plan: plan,
            schema: source.container.schema
        )
        let leftQuery = try Self.crossBaseInputQuery(
            table: plan.leftTable,
            authorizationPlan: authorizationPlan,
            schema: source.container.schema
        )
        let rightQuery = try Self.crossBaseInputQuery(
            table: plan.rightTable,
            authorizationPlan: authorizationPlan,
            schema: source.container.schema
        )
        let leftRows = try await collectCrossBaseInput(
            query: leftQuery,
            member: leftMember,
            options: options,
            source: source,
            snapshot: snapshot,
            workMeter: workMeter,
            memberExecutor: memberExecutor
        )
        let rightRows = try await collectCrossBaseInput(
            query: rightQuery,
            member: rightMember,
            options: options,
            source: source,
            snapshot: snapshot,
            workMeter: workMeter,
            memberExecutor: memberExecutor
        )
        let joinContext = source.container.session(
            authorization: source.authorization
        ).base(plan.leftBaseID).newContext()
        let joinExecution = ReadExecutionContext(
            options: options.readContext.options.withoutExternalPageWindow(),
            monotonicClock: source.container.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
        let response: QueryResponse
        do {
            response = try await joinContext.executeCompositionCrossBaseJoin(
                query,
                join: plan.join,
                leftRows: consume leftRows,
                leftTable: plan.leftTable,
                rightRows: consume rightRows,
                rightTable: plan.rightTable,
                options: joinExecution
            )
        } catch let error as CanonicalReadError {
            switch error {
            case .unsupportedExpression,
                 .unsupportedSelectQuery,
                 .unsupportedSource,
                 .unsupportedAccessPath:
                throw CompositionQueryError.unsupportedPlan(
                    "the canonical relational executor cannot prove this cross-Base JOIN plan"
                )
            default:
                throw error
            }
        }
        guard response.continuation == nil else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let origin: CompositionOrigin
        if plan.leftBaseID == plan.rightBaseID {
            origin = .source(plan.leftBaseID)
        } else {
            origin = .derived(
                contributors: [plan.leftBaseID, plan.rightBaseID].sorted()
            )
        }
        for row in response.rows {
            guard try await emit(
                .row(CompositionQueryRow(row: row, origin: origin))
            ) else {
                return
            }
        }
    }

    private func collectCrossBaseInput(
        query: SelectQuery,
        member: DatabaseBaseLease,
        options: CompositionQueryExecutionOptions,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        memberExecutor: any CompositionMemberQueryExecutor
    ) async throws -> DatabaseRetainedBuffer<DatabaseEngine.QueryRow> {
        guard let maximumIntermediateRows = Int(
            exactly: options.readContext.options.budget.maximumIntermediateRows
        ), maximumIntermediateRows > 0 else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "maximumIntermediateRows exceeds the current runtime range"
            )
        }
        let localPageSize = max(
            1,
            min(options.pageSize, max(1, maximumIntermediateRows / 4))
        )
        var rows = try DatabaseRetainedArrayBuilder<DatabaseEngine.QueryRow>(
            workMeter: workMeter,
            stage: .joinCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: DatabaseEngine.QueryRow.self)
        )
        var cursor = MemberCursor(member: member)
        while let row = try await nextRow(
            cursor: &cursor,
            query: query,
            pageSize: localPageSize,
            options: options,
            source: source,
            snapshot: snapshot,
            workMeter: workMeter,
            memberExecutor: memberExecutor
        ) {
            try rows.append(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: row,
                    workMeter: workMeter
                ),
                make: { row }
            )
        }
        return rows.finish()
    }

    private static func crossBaseInputQuery(
        table: TableRef,
        authorizationPlan: DatabaseFieldReadAuthorizationPlan,
        schema: Schema
    ) throws -> SelectQuery {
        guard let entity = schema.entity(named: table.table) else {
            throw CompositionQueryError.unsupportedPlan(
                "a cross-Base JOIN table is not present in the active schema"
            )
        }
        guard let fieldNames = authorizationPlan.fieldsByEntity[entity.name]
        else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let projection: Projection
        if fieldNames.isEmpty {
            projection = .items([
                ProjectionItem(
                    .literal(.bool(true)),
                    alias: "__composition_row_presence"
                )
            ])
        } else {
            projection = .items(fieldNames.sorted().map { fieldName in
                ProjectionItem(
                    .column(ColumnRef(column: fieldName)),
                    alias: fieldName
                )
            })
        }
        return SelectQuery(
            projection: projection,
            source: .table(table)
        )
    }

    private static func crossBaseJoinPlan(
        _ query: SelectQuery
    ) throws -> CrossBaseJoinPlan? {
        guard containsBaseQualifier(query.source) else { return nil }
        guard case .join(let join) = query.source,
              join.type == .inner,
              join.condition != nil,
              case .base(let leftBaseID, .table(let leftTable)) = join.left,
              case .base(let rightBaseID, .table(let rightTable)) = join.right
        else {
            throw CompositionQueryError.unsupportedPlan(
                "cross-Base execution requires two explicitly Base-qualified tables joined by INNER JOIN"
            )
        }
        guard leftTable.effectiveName != rightTable.effectiveName else {
            throw CompositionQueryError.unsupportedPlan(
                "cross-Base JOIN inputs require distinct table names or aliases"
            )
        }
        if case .using(let columns)? = join.condition, columns.isEmpty {
            throw CompositionQueryError.unsupportedPlan(
                "cross-Base JOIN USING requires at least one column"
            )
        }
        return CrossBaseJoinPlan(
            join: join,
            leftBaseID: leftBaseID,
            leftTable: leftTable,
            rightBaseID: rightBaseID,
            rightTable: rightTable
        )
    }

    private static func validateCrossBaseColumns(
        _ query: SelectQuery,
        plan: CrossBaseJoinPlan,
        schema: Schema
    ) throws {
        guard let leftEntity = schema.entity(named: plan.leftTable.table),
              let rightEntity = schema.entity(named: plan.rightTable.table)
        else {
            throw CompositionQueryError.unsupportedPlan(
                "a cross-Base JOIN table is not present in the active schema"
            )
        }
        let leftQualifiers = sourceQualifiers(plan.leftTable)
        let rightQualifiers = sourceQualifiers(plan.rightTable)

        for column in query.referencedColumns {
            let leftHasField = leftEntity.fieldMapByName[column.column] != nil
            let rightHasField = rightEntity.fieldMapByName[column.column] != nil
            guard leftHasField || rightHasField else {
                throw CompositionQueryError.unsupportedPlan(
                    "cross-Base JOIN references an unknown column"
                )
            }
            guard let qualifier = column.table else {
                guard !(leftHasField && rightHasField) else {
                    throw CompositionQueryError.unsupportedPlan(
                        "ambiguous cross-Base JOIN columns require a table alias"
                    )
                }
                continue
            }
            let matchesLeft = leftQualifiers.contains(qualifier)
            let matchesRight = rightQualifiers.contains(qualifier)
            guard matchesLeft != matchesRight,
                matchesLeft ? leftHasField : rightHasField
            else {
                throw CompositionQueryError.unsupportedPlan(
                    "cross-Base JOIN column qualification is ambiguous or invalid"
                )
            }
        }

        if case .allFrom(let qualifier) = query.projection {
            let matchesLeft = leftQualifiers.contains(qualifier)
            let matchesRight = rightQualifiers.contains(qualifier)
            guard matchesLeft != matchesRight else {
                throw CompositionQueryError.unsupportedPlan(
                    "cross-Base JOIN projection qualification is ambiguous or invalid"
                )
            }
        }

        if case .using(let columns)? = plan.join.condition {
            guard columns.allSatisfy({
                leftEntity.fieldMapByName[$0] != nil
                    && rightEntity.fieldMapByName[$0] != nil
            }) else {
                throw CompositionQueryError.unsupportedPlan(
                    "cross-Base JOIN USING requires a column from both inputs"
                )
            }
        }
    }

    private static func sourceQualifiers(_ table: TableRef) -> Set<String> {
        var qualifiers: Set<String> = [table.table, table.effectiveName]
        if let alias = table.alias { qualifiers.insert(alias) }
        return qualifiers
    }

    private static func containsBaseQualifier(_ source: DataSource) -> Bool {
        switch source {
        case .base:
            return true
        case .join(let join):
            return containsBaseQualifier(join.left)
                || containsBaseQualifier(join.right)
        case .subquery(let query, _):
            return containsBaseQualifier(query.source)
        case .union(let sources), .unionAll(let sources),
             .intersect(let sources):
            return sources.contains(where: containsBaseQualifier)
        case .except(let left, let right):
            return containsBaseQualifier(left)
                || containsBaseQualifier(right)
        case .table, .logical, .values, .graphTable, .graphPattern,
             .namedGraph, .service:
            return false
        }
    }

    private func executeRows(
        _ query: SelectQuery,
        options: CompositionQueryExecutionOptions,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        memberExecutor: any CompositionMemberQueryExecutor,
        emit: (OriginRow) async throws -> Bool
    ) async throws {
        let localQuery = SelectQuery(
            projection: Self.nonDistinctProjection(query.projection),
            source: query.source,
            accessPath: query.accessPath,
            filter: query.filter,
            groupBy: nil,
            having: nil,
            orderBy: query.orderBy,
            limit: nil,
            offset: nil,
            distinct: false,
            subqueries: nil,
            reduced: false,
            dataset: query.dataset
        )
        let memberCount = max(1, snapshot.lease.members.count)
        let mergeOrdering: MergeOrdering?
        if try Self.vectorIndexScan(query) != nil {
            mergeOrdering = .vectorDistance
        } else if let orderBy = query.orderBy, !orderBy.isEmpty {
            mergeOrdering = .query(orderBy)
        } else {
            mergeOrdering = nil
        }
        guard let maximumBufferedRows = Int(
            exactly: options.readContext.options.budget.maximumIntermediateRows
        ) else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "maximumIntermediateRows exceeds the current runtime range"
            )
        }
        let requestedPageLimit = options.pageSize
        let retainedOutputRows = min(requestedPageLimit, maximumBufferedRows)
        // A Base-local page can temporarily retain its input and projected
        // output. Ordered merge also retains one cursor and one heap head for
        // every other member. Allocate one row of lookahead per local page and
        // leave the durable output page inside the same request-wide budget.
        let concurrentPipelineUnits = mergeOrdering == nil
            ? 2
            : memberCount + 1
        let availablePipelineRows = maximumBufferedRows - retainedOutputRows
        let maximumLocalPageSize = availablePipelineRows
            / concurrentPipelineUnits - 1
        guard maximumLocalPageSize >= 1 else {
            let pipelineMinimum = concurrentPipelineUnits
                .multipliedReportingOverflow(by: 2)
            let minimumRows = retainedOutputRows.addingReportingOverflow(
                pipelineMinimum.overflow
                    ? Int.max
                    : pipelineMinimum.partialValue
            )
            throw DatabaseWorkLimitError.maximumIntermediateRows(
                stage: .resultMaterialization,
                consumed: 0,
                requested: UInt64(
                    minimumRows.overflow ? Int.max : minimumRows.partialValue
                ),
                maximum: UInt64(
                    options.readContext.options.budget.maximumIntermediateRows
                )
            )
        }
        let requestedRowsPerMember: Int
        if mergeOrdering == nil {
            requestedRowsPerMember = requestedPageLimit
        } else {
            requestedRowsPerMember = requestedPageLimit / memberCount
                + (requestedPageLimit % memberCount == 0 ? 0 : 1)
        }
        let localPageSize = max(
            1,
            min(
                requestedRowsPerMember,
                maximumLocalPageSize
            )
        )
        var cursors = snapshot.lease.members.map(MemberCursor.init(member:))
        var sequence: UInt64 = 0

        func prepared(
            _ row: DatabaseEngine.QueryRow,
            member: DatabaseBaseLease
        ) throws -> OriginRow {
            let qualifiedRow = try memberExecutor.prepare(
                row,
                sourceBaseID: member.baseID
            )
            return OriginRow(
                row: qualifiedRow,
                origin: .source(member.baseID),
                sequence: sequence,
                fingerprint: try CanonicalRowFingerprint.compute(
                    qualifiedRow,
                    workMeter: workMeter
                )
            )
        }

        func advanceSequence() throws {
            let next = sequence.addingReportingOverflow(1)
            guard !next.overflow else {
                    throw DatabaseWorkLimitError.maximumIntermediateRows(
                        stage: .resultMaterialization,
                        consumed: sequence,
                    requested: UInt64.max,
                    maximum: UInt64(
                        options.readContext.options.budget
                            .maximumIntermediateRows
                    )
                )
            }
            sequence = next.partialValue
        }

        if let mergeOrdering {
            var heap = MergeHeap()
            for index in cursors.indices {
                if let row = try await nextRow(
                    cursor: &cursors[index],
                    query: localQuery,
                    pageSize: localPageSize,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor
                ) {
                    let originRow = try prepared(
                        row,
                        member: cursors[index].member
                    )
                    try heap.insert(
                        memberIndex: index,
                        row: originRow,
                        footprint: try rowFootprint(originRow.row),
                        workMeter: workMeter
                    ) { lhs, rhs in
                        try compare(
                            lhs,
                            rhs,
                            ordering: mergeOrdering,
                            workMeter: workMeter
                        )
                    }
                    try advanceSequence()
                }
            }
            while !heap.rows.isEmpty {
                let memberIndex: Int
                do {
                    guard let head = try heap.removeFirst(orderedBefore: {
                        lhs,
                        rhs in
                        try compare(
                            lhs,
                            rhs,
                            ordering: mergeOrdering,
                            workMeter: workMeter
                        )
                    }) else {
                        throw CompositionQueryError.workspaceCorrupted
                    }
                    memberIndex = head.memberIndex
                    guard try await emit(head.row) else { return }
                    // `head` and its reservation end together at this scope.
                }
                if let row = try await nextRow(
                    cursor: &cursors[memberIndex],
                    query: localQuery,
                    pageSize: localPageSize,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor
                ) {
                    let originRow = try prepared(
                        row,
                        member: cursors[memberIndex].member
                    )
                    try heap.insert(
                        memberIndex: memberIndex,
                        row: originRow,
                        footprint: try rowFootprint(originRow.row),
                        workMeter: workMeter
                    ) { lhs, rhs in
                            try compare(
                                lhs,
                                rhs,
                                ordering: mergeOrdering,
                                workMeter: workMeter
                        )
                    }
                    try advanceSequence()
                }
            }
        } else {
            for index in cursors.indices {
                while let row = try await nextRow(
                    cursor: &cursors[index],
                    query: localQuery,
                    pageSize: localPageSize,
                    options: options,
                    source: source,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    memberExecutor: memberExecutor
                ) {
                    guard try await emit(
                        prepared(row, member: cursors[index].member)
                    ) else { return }
                    try advanceSequence()
                }
            }
        }
    }

    private func nextRow(
        cursor: inout MemberCursor,
        query: SelectQuery,
        pageSize: Int,
        options: CompositionQueryExecutionOptions,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        memberExecutor: any CompositionMemberQueryExecutor
    ) async throws -> DatabaseEngine.QueryRow? {
        while cursor.nextRowIndex >= cursor.rows.count {
            cursor.releaseConsumedPage()
            guard !cursor.reachedEnd else { return nil }
            let previousContinuation = cursor.continuation
            let response = try await source.withMemberContext(
                cursor.member,
                in: snapshot
            ) { databaseContext, transaction in
                try await memberExecutor.execute(
                    context: databaseContext,
                    query: query,
                    execution: try localExecution(
                        options: options,
                        continuation: previousContinuation,
                        pageSize: pageSize,
                        workMeter: workMeter,
                        source: source
                    ),
                    transaction: transaction
                )
            }
            guard response.continuation == nil
                    || response.continuation != previousContinuation else {
                throw CompositionQueryError.unsupportedPlan(
                    "a Base-local cursor did not make progress"
                )
            }
            var retainedBytes: UInt64 = 0
            for row in response.rows {
                retainedBytes = try Self.adding(
                    retainedBytes,
                    rowFootprint(row)
                )
            }
            let reservation = response.rows.isEmpty
                ? nil
                : try workMeter.reserveIntermediate(
                    rows: UInt64(response.rows.count),
                    bytes: retainedBytes,
                    at: .resultMaterialization
                )
            cursor.replacePage(
                response.rows,
                reservation: reservation
            )
            cursor.continuation = response.continuation
            cursor.reachedEnd = response.continuation == nil
        }
        let row = cursor.rows[cursor.nextRowIndex]
        cursor.nextRowIndex += 1
        return row
    }

    private func executeAggregates(
        _ query: SelectQuery,
        aggregates: (
            descriptors: [AggregateDescriptor],
            localProjection: Projection
        ),
        options: CompositionQueryExecutionOptions,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot,
        workMeter: DatabaseWorkMeter,
        memberExecutor: any CompositionMemberQueryExecutor
    ) async throws -> [OriginRow] {
        let localQuery = SelectQuery(
            projection: aggregates.localProjection,
            source: query.source,
            accessPath: nil,
            filter: query.filter,
            groupBy: nil,
            having: nil,
            orderBy: nil,
            limit: nil,
            offset: nil,
            distinct: false,
            subqueries: nil,
            reduced: false,
            dataset: query.dataset
        )
        var states = aggregates.descriptors.map { descriptor in
            switch descriptor.kind {
            case .countAll, .countValue:
                return AggregateState.count(0)
            case .sum, .average:
                return AggregateState.numeric(
                    DatabaseNumericAggregateAccumulator()
                )
            case .minimum, .maximum:
                return AggregateState.extremum(nil)
            }
        }
        guard let pageSize = Int(
            exactly: options.readContext.options.budget.maximumIntermediateRows
        ), pageSize > 0 else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "maximumIntermediateRows exceeds the current runtime range"
            )
        }
        for member in snapshot.lease.members {
            var cursor = MemberCursor(member: member)
            while let row = try await nextRow(
                cursor: &cursor,
                query: localQuery,
                pageSize: pageSize,
                options: options,
                source: source,
                snapshot: snapshot,
                workMeter: workMeter,
                memberExecutor: memberExecutor
            ) {
                try workMeter.consume(at: .aggregateInput)
                let preparedRow = try memberExecutor.prepare(
                    row,
                    sourceBaseID: member.baseID
                )
                for index in aggregates.descriptors.indices {
                    let descriptor = aggregates.descriptors[index]
                    let value: FieldValue
                    if let operandName = descriptor.operandName {
                        guard let operand = preparedRow.fields[operandName]
                        else {
                            throw CompositionQueryError.aggregateFailure(
                                "Base-local aggregate operand is missing"
                            )
                        }
                        value = operand
                    } else {
                        value = .bool(true)
                    }
                    try Self.accumulate(
                        value,
                        descriptor: descriptor,
                        state: &states[index]
                    )
                }
            }
        }

        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(aggregates.descriptors.count)
        for index in aggregates.descriptors.indices {
            fields[aggregates.descriptors[index].outputName] = try Self.result(
                descriptor: aggregates.descriptors[index],
                state: states[index]
            )
        }
        let row = DatabaseEngine.QueryRow(fields: fields)
        return [
            OriginRow(
                row: row,
                origin: .derived(
                    contributors: snapshot.lease.resolution.bases
                ),
                sequence: 0,
                fingerprint: try CanonicalRowFingerprint.compute(
                    row,
                    workMeter: workMeter
                )
            )
        ]
    }

    private static func accumulate(
        _ value: FieldValue,
        descriptor: AggregateDescriptor,
        state: inout AggregateState
    ) throws {
        switch (descriptor.kind, state) {
        case (.countAll, .count(let count)):
            state = .count(try incrementAggregateCount(count))
        case (.countValue, .count(let count)):
            state = value == .null
                ? .count(count)
                : .count(try incrementAggregateCount(count))
        case (.sum, .numeric(var accumulator)),
             (.average, .numeric(var accumulator)):
            do {
                try accumulator.add(value)
            } catch let failure {
                throw CompositionQueryError.aggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
            state = .numeric(accumulator)
        case (.minimum, .extremum(let current)),
             (.maximum, .extremum(let current)):
            guard value != .null else { return }
            guard let current else {
                state = .extremum(value)
                return
            }
            let comparison: QueryComparison
            if value.isNumeric && current.isNumeric {
                guard let numericComparison =
                    RelationalValueIdentity.compareNumeric(value, current)
                else {
                    throw CompositionQueryError.aggregateFailure(
                        "MIN/MAX values contain a non-finite numeric value"
                    )
                }
                if numericComparison < 0 {
                    comparison = .lessThan
                } else if numericComparison > 0 {
                    comparison = .greaterThan
                } else {
                    comparison = .equal
                }
            } else {
                guard let fieldComparison = value.compare(to: current) else {
                    throw CompositionQueryError.aggregateFailure(
                        "MIN/MAX values are not mutually comparable"
                    )
                }
                comparison = fieldComparison
            }
            if descriptor.kind.isMinimum
                ? comparison == .lessThan
                : comparison == .greaterThan {
                state = .extremum(value)
            }
        default:
            throw CompositionQueryError.aggregateFailure(
                "aggregate state does not match its plan"
            )
        }
    }

    private static func result(
        descriptor: AggregateDescriptor,
        state: AggregateState
    ) throws -> FieldValue {
        switch (descriptor.kind, state) {
        case (.countAll, .count(let value)),
             (.countValue, .count(let value)):
            return .int64(value)
        case (.sum, .numeric(let accumulator)):
            do { return try accumulator.sum() ?? .null }
            catch let failure {
                throw CompositionQueryError.aggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
        case (.average, .numeric(let accumulator)):
            do { return try accumulator.average() ?? .null }
            catch let failure {
                throw CompositionQueryError.aggregateFailure(
                        aggregateFailureMessage(failure)
                    )
            }
        case (.minimum, .extremum(let value)),
             (.maximum, .extremum(let value)):
            return value ?? .null
        default:
            throw CompositionQueryError.aggregateFailure(
                "aggregate state does not match its plan"
            )
        }
    }

    private static func incrementAggregateCount(
        _ value: Int64
    ) throws -> Int64 {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw CompositionQueryError.aggregateFailure(
                "COUNT exceeds Int64"
            )
        }
        return result.partialValue
    }

    private static func aggregateFailureMessage(
        _ failure: DatabaseNumericAggregateAccumulator.Failure
    ) -> String {
        switch failure {
        case .incompatibleNumericKinds:
            return "Aggregate values use incompatible numeric kinds"
        case .nonNumericValue:
            return "Aggregate operand is not numeric"
        case .nonFiniteValue:
            return "Aggregate operand is not finite"
        case .numericOverflow:
            return "Aggregate numeric result overflowed"
        case .resultNotRepresentable:
            return "Aggregate result cannot be represented"
        }
    }

    private func localExecution(
        options: CompositionQueryExecutionOptions,
        continuation: QueryContinuation? = nil,
        pageSize: Int? = nil,
        workMeter: DatabaseWorkMeter,
        source: CompositionDataSource
    ) throws -> ReadExecutionContext {
        guard let maximumPageSize = Int(
            exactly: options.readContext.options.budget.maximumIntermediateRows
        ) else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "maximumIntermediateRows exceeds the current runtime range"
            )
        }
        return ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: pageSize ?? maximumPageSize,
                continuation: continuation,
                budget: options.readContext.options.budget,
                continuationSnapshotIsStable: true
            ),
            monotonicClock: source.container.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
    }

    private func compare(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        ordering: MergeOrdering,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        switch ordering {
        case .query(let orderBy):
            return try compare(
                lhs,
                rhs,
                orderBy: orderBy,
                workMeter: workMeter
            )
        case .vectorDistance:
            return try compareVectorDistance(
                lhs,
                rhs,
                workMeter: workMeter
            )
        }
    }

    private func compare(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        orderBy: [SortKey],
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        for key in orderBy {
            try workMeter.consume(2, at: .sortComparison)
            let name = try Self.outputName(for: key.expression)
            guard let left = lhs.row.fields[name],
                  let right = rhs.row.fields[name] else {
                throw CompositionQueryError.unsupportedPlan(
                    "ORDER BY '\(name)' is not present in the projected result"
                )
            }
            let comparison: QueryComparison
            do {
                comparison = try FieldValueComparator.compare(
                    left,
                    right,
                    using: key
                )
            } catch {
                throw CompositionQueryError.unsupportedPlan(
                    "ORDER BY values are not mutually comparable"
                )
            }
            guard comparison != .equal else { continue }
            return comparison == .lessThan
        }
        return tieBreaksBefore(lhs, rhs)
    }

    private func compareVectorDistance(
        _ lhs: OriginRow,
        _ rhs: OriginRow,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try workMeter.consume(2, at: .sortComparison)
        guard case .float64(let left)? = lhs.row.annotations["distance"],
              case .float64(let right)? = rhs.row.annotations["distance"],
              left.isFinite,
              right.isFinite else {
            throw CompositionQueryError.unsupportedPlan(
                "vector Composition members must return a finite distance annotation"
            )
        }
        if left != right { return left < right }
        return tieBreaksBefore(lhs, rhs)
    }

    private func tieBreaksBefore(
        _ lhs: OriginRow,
        _ rhs: OriginRow
    ) -> Bool {
        if lhs.fingerprint != rhs.fingerprint {
            return lhs.fingerprint.lexicographicallyPrecedes(rhs.fingerprint)
        }
        let leftBase = Self.firstContributor(lhs.origin)
        let rightBase = Self.firstContributor(rhs.origin)
        if leftBase != rightBase { return leftBase < rightBase }
        return lhs.sequence < rhs.sequence
    }

    private func rowFootprint(
        _ row: DatabaseEngine.QueryRow
    ) throws -> UInt64 {
        var bytes: UInt64 = 128
        for (key, value) in row.fields.sorted(by: { $0.key < $1.key }) {
            bytes = try Self.adding(bytes, UInt64(key.utf8.count))
            bytes = try Self.adding(
                bytes,
                UInt64(
                    try FieldValueTupleCodec.encodedByteCount(for: value)
                )
            )
        }
        for (key, value) in row.annotations.sorted(by: { $0.key < $1.key }) {
            bytes = try Self.adding(bytes, UInt64(key.utf8.count))
            bytes = try Self.adding(
                bytes,
                UInt64(
                    try FieldValueTupleCodec.encodedByteCount(for: value)
                )
            )
        }
        if let version = row.version {
            bytes = try Self.adding(bytes, UInt64(version.value.utf8.count))
        }
        return bytes
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: lhs,
                right: rhs
            )
        }
        return result.partialValue
    }

    private static func aggregateProjection(
        _ projection: Projection
    ) throws -> (
        descriptors: [AggregateDescriptor],
        localProjection: Projection
    )? {
        let items: [ProjectionItem]
        switch projection {
        case .items(let value), .distinctItems(let value):
            items = value
        case .all, .allFrom:
            return nil
        }
        let containsAggregate = items.contains {
            if case .aggregate = $0.expression { return true }
            return false
        }
        guard containsAggregate else { return nil }
        guard items.allSatisfy({ item in
            if case .aggregate = item.expression { return true }
            return false
        }) else {
            throw CompositionQueryError.unsupportedPlan(
                "aggregate and non-aggregate projection items cannot be mixed without GROUP BY"
            )
        }
        var descriptors: [AggregateDescriptor] = []
        descriptors.reserveCapacity(items.count)
        var operands: [ProjectionItem] = []
        operands.reserveCapacity(items.count)
        var outputNames = Set<String>()
        for (index, item) in items.enumerated() {
            guard case .aggregate(let aggregate) = item.expression else {
                throw CompositionQueryError.unsupportedPlan(
                    "aggregate projection is malformed"
                )
            }
            let operandName = "__composition_aggregate_operand_\(index)"
            let kind: AggregateKind
            let expression: Expression?
            let defaultName: String
            switch aggregate {
            case .count(let value, distinct: false):
                kind = value == nil ? .countAll : .countValue
                expression = value
                defaultName = "count"
            case .sum(let value, distinct: false):
                kind = .sum
                expression = value
                defaultName = "sum"
            case .avg(let value, distinct: false):
                kind = .average
                expression = value
                defaultName = "avg"
            case .min(let value):
                kind = .minimum
                expression = value
                defaultName = "min"
            case .max(let value):
                kind = .maximum
                expression = value
                defaultName = "max"
            case .count(_, distinct: true), .sum(_, distinct: true),
                 .avg(_, distinct: true), .groupConcat, .sample, .arrayAgg:
                throw CompositionQueryError.unsupportedPlan(
                        "this aggregate is not decomposable across Base boundaries"
                    )
            }
            let outputName = item.alias ?? defaultName
            guard outputNames.insert(outputName).inserted else {
                throw CompositionQueryError.unsupportedPlan(
                    "aggregate output names must be unique"
                )
            }
            if let expression {
                operands.append(
                    ProjectionItem(expression, alias: operandName)
                )
            }
            descriptors.append(
                AggregateDescriptor(
                    outputName: outputName,
                    operandName: expression == nil ? nil : operandName,
                    kind: kind
                )
            )
        }
        if operands.isEmpty {
            operands.append(
                ProjectionItem(
                    .literal(.bool(true)),
                    alias: "__composition_aggregate_marker"
                )
            )
        }
        return (descriptors, .items(operands))
    }

    private static func isBaseLocalRelationalSource(
        _ source: DataSource
    ) -> Bool {
        switch source {
        case .table:
            return true
        case .logical:
            return false
        case .join(let join):
            return isBaseLocalRelationalSource(join.left)
                && isBaseLocalRelationalSource(join.right)
        case .base:
            return false
        case .subquery, .values, .graphTable, .graphPattern, .namedGraph,
             .service, .union, .unionAll, .intersect, .except:
            return false
        }
    }

    private static func isVectorSource(_ source: DataSource) -> Bool {
        switch source {
        case .table:
            return true
        case .logical(let source):
            return source.kindIdentifier == LogicalSourceKind.polymorphic
        case .join, .base, .subquery, .values, .graphTable, .graphPattern,
             .namedGraph, .service, .union, .unionAll, .intersect, .except:
            return false
        }
    }

    /// Validates the complete score-comparability contract used by the
    /// federated vector merge. Every member executes the same immutable schema
    /// generation and the same canonical access-path parameters; the merger
    /// therefore compares only distances produced under one metric contract.
    private static func vectorIndexScan(
        _ query: SelectQuery
    ) throws -> IndexScanSource? {
        guard let accessPath = query.accessPath else { return nil }
        guard case .index(let scan) = accessPath,
            scan.indexType == .vector
        else {
            return nil
        }
        guard !scan.indexName.isEmpty,
              case .int64(let dimensions)? = scan.parameters["dimensions"],
              dimensions > 0,
              let dimensionCount = Int(exactly: dimensions),
              case .vector(let queryVector)? =
                scan.parameters["queryVector"],
              queryVector.elementType == .float32,
              queryVector.count == dimensionCount,
              case .string(let metric)? = scan.parameters["metric"],
              !metric.isEmpty,
              case .int64(let k)? = scan.parameters["k"],
              k > 0,
              let resultLimit = UInt64(exactly: k),
              query.limit == resultLimit else {
            throw CompositionQueryError.unsupportedPlan(
                "vector Composition search requires one valid vector index contract and LIMIT equal to k"
            )
        }
        return scan
    }

    private static func nonDistinctProjection(
        _ projection: Projection
    ) -> Projection {
        if case .distinctItems(let items) = projection {
            return .items(items)
        }
        return projection
    }

    private static func isDistinctProjection(_ projection: Projection) -> Bool {
        if case .distinctItems = projection { return true }
        return false
    }

    private static func outputName(for expression: Expression) throws -> String {
        switch expression {
        case .column(let column):
            return column.column
        case .variable(let variable):
            return variable.name
        default:
            throw CompositionQueryError.unsupportedPlan(
                "global ordering requires an output column or variable"
            )
        }
    }

    private static func firstContributor(
        _ origin: CompositionOrigin
    ) -> Base.ID {
        switch origin {
        case .source(let baseID):
            return baseID
        case .derived(let contributors):
            return contributors[0]
        }
    }

    private static func runtimeCount(
        _ value: UInt64?,
        name: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let count = Int(exactly: value) else {
            throw CompositionQueryError.unsupportedPlan(
                "\(name) exceeds the current runtime range"
            )
        }
        return count
    }

    private static func emissionWindow(
        _ query: SelectQuery
    ) throws -> EmissionWindow {
        EmissionWindow(
            remainingOffset: try runtimeCount(
                query.offset,
                name: "OFFSET"
            ) ?? 0,
            remainingLimit: try runtimeCount(
                query.limit,
                name: "LIMIT"
            )
        )
    }
}

#endif
