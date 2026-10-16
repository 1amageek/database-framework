import DatabaseKit
import StorageKit

/// One authorized prepared Fusion plan bound to one active read session.
///
/// Construction is centralized here so the executor cannot receive or combine
/// an unrelated query, context, transaction, schema generation, or work meter.
struct FusionExecution: Sendable {
    let plan: FusionPreparedPlan
    let source: FusionSource
    let options: ReadExecutionContext
    let maximumResultCount: Int?

    private let session: DatabaseReadSession
    private let preparedQueryGraph: FusionPreparedQueryGraph
    private let authorization: DatabaseReadAuthorization
    private let parameters: [QueryParameter]
    private let relationalInputs: DatabaseSharedRetainedArray<RelationalInput>

    private struct RelationalInput: Sendable {
        let stageIndex: Int
        let inputIndex: Int
        let query: SelectQuery?
        let boundPlan: CanonicalBoundSelect?
    }

    private init(
        plan: FusionPreparedPlan,
        source: FusionSource,
        options: ReadExecutionContext,
        maximumResultCount: Int?,
        session: DatabaseReadSession,
        preparedQueryGraph: FusionPreparedQueryGraph,
        authorization: DatabaseReadAuthorization,
        parameters: [QueryParameter],
        relationalInputs: DatabaseSharedRetainedArray<RelationalInput>
    ) {
        self.plan = plan
        self.source = source
        self.options = options
        self.maximumResultCount = maximumResultCount
        self.session = session
        self.preparedQueryGraph = preparedQueryGraph
        self.authorization = authorization
        self.parameters = parameters
        self.relationalInputs = relationalInputs
    }

    static func make(
        query: SelectQuery,
        entry: FusionPreparedQueryGraph.Entry,
        graph: FusionPreparedQueryGraph,
        session: DatabaseReadSession,
        options: ReadExecutionContext,
        parameters: [QueryParameter] = []
    ) throws -> FusionExecution {
        guard let authorization = graph.authorization else {
            throw FusionExecutionError.executionContractViolation
        }
        let listAuthorizationRequirement = try DatabaseReadPolicy
            .listRequirement(
                entityName: entry.plan.entity.name,
                selectQuery: query, schema: session.schema
            )
        guard listAuthorizationRequirement
                == entry.plan.listAuthorizationRequirement,
              authorization.covers(
                listRequirement: listAuthorizationRequirement
              ) else {
            throw FusionExecutionError.executionContractViolation
        }
        try session.validatePreparedExecution(
            authorization: authorization,
            workMeter: options.workMeter
        )
        var queryCount = 0
        for stage in entry.plan.stages {
            for input in stage.inputs {
                if case .index = input.operation { continue }
                let (next, overflow) = queryCount.addingReportingOverflow(1)
                guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                queryCount = next
            }
        }
        let queryLayout = try DatabaseRetainedArrayLayout.forElement(SelectQuery.self)
        let queryGrowth = try queryLayout.growth(from: 0, toFit: queryCount)
        let queryFootprint = try DatabaseIntermediateFootprint(bytes: queryLayout.containerByteCount)
            .adding(DatabaseIntermediateFootprint(bytes: queryGrowth.additionalByteCount))
        let queryReservation = try options.workMeter.reserveIntermediate(
            bytes: queryFootprint.bytes, at: .bindingCandidate)
        defer { withExtendedLifetime(queryReservation) {} }
        var queries: [SelectQuery] = []
        queries.reserveCapacity(queryGrowth.capacity)
        for stage in entry.plan.stages {
            for input in stage.inputs {
                if let query = relationalQuery(for: input, table: entry.plan.tableRef) {
                    queries.append(query)
                }
            }
        }
        let boundPlans = try session.boundFusionRelationalQueries(
            queries,
            parameters: parameters,
            options: options
        )
        guard boundPlans.count == queries.count else {
            throw FusionExecutionError.executionContractViolation
        }
        var relationalInputs = try DatabaseRetainedArrayBuilder<RelationalInput>(
            workMeter: options.workMeter,
            stage: .bindingCandidate,
            layout: DatabaseRetainedArrayLayout.forElement(RelationalInput.self)
        )
        var relationalIndex = 0
        for stage in entry.plan.stages {
            for input in stage.inputs {
                let isRelational: Bool
                switch input.operation {
                case .filter, .order, .connected: isRelational = true
                case .index: isRelational = false
                }
                try relationalInputs.append(footprint: DatabaseIntermediateFootprint()) {
                    RelationalInput(
                        stageIndex: input.stageIndex,
                        inputIndex: input.inputIndex,
                        query: isRelational ? queries[relationalIndex] : nil,
                        boundPlan: isRelational ? boundPlans[relationalIndex] : nil
                    )
                }
                if isRelational { relationalIndex += 1 }
            }
        }
        return FusionExecution(
            plan: entry.plan,
            source: entry.source,
            options: options,
            maximumResultCount: try scoringOutputPrefixLimit(query),
            session: session,
            preparedQueryGraph: graph,
            authorization: authorization,
            parameters: parameters,
            relationalInputs: try relationalInputs.finish()
                .moveToSharedOwnership(at: .bindingCandidate)
        )
    }

    var wallClock: any WallClock { session.wallClock }

    func executeRelationalRows(
        for input: FusionPreparedPlan.Input,
        at ordinal: Int
    ) async throws -> CanonicalRetainedQueryResponse {
        let (query, boundPlan) = try relationalInput(for: input, at: ordinal)
        return try await session.executeFusionRelationalRows(
            query,
            options: options,
            preparedFusionGraph: preparedQueryGraph,
            authorization: authorization,
            listAuthorizationRequirement:
                plan.listAuthorizationRequirement,
            parameters: parameters,
            boundPlan: boundPlan
        )
    }

    func executeCandidateRelationalRows(
        _ candidates: FusionCandidateDomain,
        for input: FusionPreparedPlan.Input,
        at ordinal: Int
    ) async throws -> CanonicalRetainedQueryResponse {
        let (query, boundPlan) = try relationalInput(for: input, at: ordinal)
        return try await session.executeFusionCandidateRelationalRows(
            candidates,
            query: query,
            options: options,
            preparedFusionGraph: preparedQueryGraph,
            authorization: authorization,
            parameters: parameters,
            boundPlan: boundPlan
        )
    }

    private func relationalInput(
        for input: FusionPreparedPlan.Input,
        at ordinal: Int
    ) throws -> (SelectQuery, CanonicalBoundSelect) {
        guard relationalInputs.indices.contains(ordinal) else {
            throw FusionExecutionError.executionContractViolation
        }
        let selected = relationalInputs[ordinal]
        guard selected.stageIndex == input.stageIndex,
              selected.inputIndex == input.inputIndex,
              let query = selected.query,
              let boundPlan = selected.boundPlan else {
            throw FusionExecutionError.executionContractViolation
        }
        return (query, boundPlan)
    }

    private static func relationalQuery(
        for input: FusionPreparedPlan.Input,
        table: TableRef
    ) -> SelectQuery? {
        switch input.operation {
        case .filter(let expression):
            return SelectQuery(projection: [.wildcard()], source: .table(table),
                filter: expression, pagination: QueryPagination(limit: input.limit.map { .count(.fixed(UInt64($0)), withTies: false) }))
        case .order(let keys):
            return SelectQuery(projection: [.wildcard()], source: .table(table),
                orderBy: keys, pagination: QueryPagination(limit: input.limit.map { .count(.fixed(UInt64($0)), withTies: false) }))
        case .connected:
            return SelectQuery(projection: [.wildcard()], source: .table(table))
        case .index:
            return nil
        }
    }

    func readableIndex(
        descriptor: IndexDescriptor,
        entity: Schema.Entity,
        partitions: FieldObject
    ) async throws -> ReadableIndex? {
        try await session.readableFusionIndex(
            descriptor: descriptor,
            entity: entity,
            partitions: partitions,
            authorization: authorization
        )
    }

    func withIndexReadLease<Result: Sendable>(
        index: ReadableIndex,
        _ operation: @Sendable (
            FusionIndexReadSession
        ) async throws -> Result
    ) async throws -> Result {
        try await session.withFusionIndexReadLease(
            index: index,
            snapshot: CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            ).consistency == .snapshot,
            workMeter: options.workMeter,
            operation
        )
    }

    func materializeCandidates(
        _ result: FusionIndexReadResult
    ) async throws -> FusionCandidateDomain {
        let primaryKeys = try Self.makeRetainedPrimaryKeys(
            from: result,
            workMeter: options.workMeter
        )
        let snapshot = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        ).consistency == .snapshot
        let models = try await session
            .fetchRetainedFusionCandidateModelsPreservingOrder(
                entity: plan.entity,
                primaryKeys: primaryKeys,
                partitions: plan.tableRef.partitions,
                snapshot: snapshot,
                authorization: authorization
            )
        guard let admittedFieldNames = authorization.admittedFieldNames(forEntityName: plan.entity.name) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        return try FusionCandidateDomain.make(
            models: models,
            primaryKeys: primaryKeys,
            entity: plan.entity,
            admittedFieldNames: admittedFieldNames,
            workMeter: options.workMeter
        )
    }

    static func makeRetainedPrimaryKeys(
        from result: borrowing FusionIndexReadResult,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPrimaryKeys {
        var builder = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPrimaryKey
        >(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPrimaryKey.self
            ),
            expectedCount: result.matches.count
        )
        for match in result.matches {
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .storageRow
            )
            let primaryKey = try makeRetainedPrimaryKey(
                match.primaryKey,
                workMeter: workMeter
            )
            builder.append(primaryKey, using: admission)
        }
        return try DatabaseRetainedPrimaryKeys(buffer: builder.finish())
    }

    private static func makeRetainedPrimaryKey(
        _ packedKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPrimaryKey {
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(packedKey.count),
            at: .storageRow
        )
        do {
            let retainedKey = try DatabaseRetainedByteString.make(
                packedKey,
                reservation: reservation,
                at: .storageRow
            )
            let tuple = try Tuple(packed: retainedKey) {
                additionalByteCount in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalByteCount),
                    at: .storageRow
                )
            }
            return DatabaseRetainedPrimaryKey(
                value: tuple,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private static func scoringOutputPrefixLimit(
        _ query: SelectQuery
    ) throws -> Int? {
        guard case .count(.fixed(let limit), withTies: false) = query.pagination.limit,
              query.filter == nil,
              query.groupBy == nil,
              query.having == nil,
              query.orderBy?.isEmpty != false,
              query.quantifier == .all,
              !query.reduced else {
            return nil
        }
        let offset: UInt64
        switch query.pagination.offset {
        case nil: offset = 0
        case .fixed(let value): offset = value
        case .expression: return nil
        }
        let (prefix, overflow) = limit.addingReportingOverflow(offset)
        guard !overflow else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: "limit + offset",
                value: UInt64.max
            )
        }
        guard let result = Int(exactly: prefix) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: "limit + offset",
                value: prefix
            )
        }
        return result
    }
}
