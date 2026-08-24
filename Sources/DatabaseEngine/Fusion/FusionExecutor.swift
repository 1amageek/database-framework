import DatabaseKit
import DatabaseTypes
import StorageKit

/// Canonical staged Fusion runtime. It alone owns relational candidates,
/// transaction lifetime, feature admission, stage algebra, and score output.
enum FusionExecutor {
    static let scoreAnnotation = "fusion.score"

    static func execute(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        tableRef: TableRef,
        entity: Schema.Entity,
        source: FusionSource,
        plan: FusionPreparedPlan,
        preparedQueryGraph: FusionPreparedQueryGraph,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> IndexReadResult {
        var candidates: FusionCandidateDomain?
        var scoredInputs = try DatabaseRetainedArrayBuilder<FusionScoredInput>(
            workMeter: options.workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionScoredInput.self
            ),
            expectedCount: source.scoredInputCount
        )

        for stage in plan.stages {
            let incoming = candidates
            var stageCandidates: FusionCandidateDomain?
            for input in stage.inputs {
                let result: InputExecutionResult
                if incoming?.isEmpty == true {
                    result = try emptyResult(
                        for: input,
                        workMeter: options.workMeter
                    )
                } else {
                    result = try await execute(
                        input,
                        incoming: incoming,
                        context: context,
                        tableRef: tableRef,
                        entity: entity,
                        preparedQueryGraph: preparedQueryGraph,
                        options: options,
                        transaction: transaction
                    )
                }
                if let existing = stageCandidates {
                    stageCandidates = try existing.union(
                        result.candidates,
                        workMeter: options.workMeter
                    )
                } else {
                    stageCandidates = result.candidates
                }
                if let scored = result.scored {
                    try scoredInputs.append(
                        footprint: DatabaseIntermediateFootprint(rows: 1)
                    ) {
                        scored
                    }
                }
            }
            guard var resolvedStage = stageCandidates else {
                throw FusionExecutionError.unsupportedSource
            }
            if let incoming {
                resolvedStage = try resolvedStage.intersection(
                    incoming,
                    workMeter: options.workMeter
                )
            }
            candidates = resolvedStage
        }

        guard let candidates else {
            throw FusionExecutionError.unsupportedSource
        }
        let retainedScoredInputs = try scoredInputs.finish()
            .moveToSharedOwnership(at: .projection)
        return try FusionScoreComposer.compose(
            candidates: candidates,
            scoredInputs: retainedScoredInputs,
            strategy: source.strategy,
            maximumResultCount: try scoringOutputPrefixLimit(selectQuery),
            workMeter: options.workMeter
        )
    }

    private static func scoringOutputPrefixLimit(
        _ query: SelectQuery
    ) throws -> Int? {
        guard let limit = query.limit,
              query.filter == nil,
              query.groupBy == nil,
              query.having == nil,
              query.orderBy?.isEmpty != false,
              !query.distinct,
              !query.reduced else {
            return nil
        }
        let offset = query.offset ?? 0
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

    private struct InputExecutionResult: Sendable {
        let candidates: FusionCandidateDomain
        let scored: FusionScoredInput?
    }

    private static func execute(
        _ input: FusionPreparedPlan.Input,
        incoming: FusionCandidateDomain?,
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity,
        preparedQueryGraph: FusionPreparedQueryGraph,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> InputExecutionResult {
        if input.requirement == .candidates, incoming == nil {
            throw FusionExecutionError.requiredCandidates(
                stage: input.stageIndex,
                input: input.inputIndex
            )
        }
        switch input.operation {
        case .filter(let expression):
            let query = SelectQuery(
                projection: .all,
                source: .table(tableRef),
                filter: expression,
                limit: input.limit.map(UInt64.init)
            )
            let response: CanonicalRetainedQueryResponse
            if let incoming {
                response = try await context
                    .executeFusionCandidateRelationalRows(
                        incoming,
                        query: query,
                        options: options,
                        transaction: transaction,
                        preparedFusionGraph: preparedQueryGraph
                    )
            } else {
                response = try await context.executeFusionRelationalRows(
                    query,
                    options: options,
                    transaction: transaction,
                    preparedFusionGraph: preparedQueryGraph
                )
            }
            return InputExecutionResult(
                candidates: try FusionCandidateDomain.make(
                    rows: response.visibleRows,
                    entity: entity,
                    workMeter: options.workMeter
                ),
                scored: nil
            )
        case .order(let sortKeys):
            guard let incoming, let scoring = input.scoring else {
                throw FusionExecutionError.invalidInputScoring(input.scoring)
            }
            let query = SelectQuery(
                projection: .all,
                source: .table(tableRef),
                orderBy: sortKeys,
                limit: input.limit.map(UInt64.init)
            )
            let response = try await context
                .executeFusionCandidateRelationalRows(
                    incoming,
                    query: query,
                    options: options,
                    transaction: transaction,
                    preparedFusionGraph: preparedQueryGraph
                )
            let limit = input.limit ?? incoming.count
            let sink = try FusionMatchSink(
                candidates: incoming,
                scoring: scoring,
                limit: limit,
                workMeter: options.workMeter
            )
            do {
                for row in response.visibleRows {
                    guard let identity = row.fields["id"] else {
                        throw FusionExecutionContractError.missingIdentity(field: "id")
                    }
                    let primaryKey = try PersistableIdentifierKeyCodec.tuple(
                        forPersistedIdentifier: identity
                    ).pack()
                    try sink.submit(
                        primaryKey: primaryKey,
                        numericSignal: nil
                    )
                }
                let result = try sink.freeze(coverage: .exhausted)
                return InputExecutionResult(
                    candidates: try FusionCandidateDomain.make(
                        rows: response.visibleRows,
                        entity: entity,
                        workMeter: options.workMeter
                    ),
                    scored: FusionScoredInput(
                        scoring: scoring,
                        result: result
                    )
                )
            } catch {
                sink.invalidate()
                throw error
            }
        case .index(let source, let descriptor, let executor):
            return try await executeIndex(
                source: source,
                descriptor: descriptor,
                executor: executor,
                input: input,
                incoming: incoming,
                context: context,
                tableRef: tableRef,
                entity: entity,
                options: options,
                transaction: transaction
            )
        case .connected(
            let source,
            let edgeEntity,
            let descriptor,
            let executor
        ):
            return try await executeConnected(
                source: source,
                edgeEntity: edgeEntity,
                descriptor: descriptor,
                executor: executor,
                input: input,
                incoming: incoming,
                context: context,
                tableRef: tableRef,
                entity: entity,
                preparedQueryGraph: preparedQueryGraph,
                options: options,
                transaction: transaction
            )
        }
    }

    private static func executeConnected(
        source: FusionConnectedSource,
        edgeEntity: Schema.Entity,
        descriptor: IndexDescriptor,
        executor: any FusionConnectedReadExecutor,
        input: FusionPreparedPlan.Input,
        incoming: FusionCandidateDomain?,
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity,
        preparedQueryGraph: FusionPreparedQueryGraph,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> InputExecutionResult {
        let candidatePool: FusionCandidateDomain
        if let incoming {
            candidatePool = incoming
        } else {
            let response = try await context.executeFusionRelationalRows(
                SelectQuery(
                    projection: .all,
                    source: .table(tableRef)
                ),
                options: options,
                transaction: transaction,
                preparedFusionGraph: preparedQueryGraph
            )
            candidatePool = try FusionCandidateDomain.make(
                rows: response.visibleRows,
                entity: entity,
                workMeter: options.workMeter
            )
        }

        let limit = input.limit ?? candidatePool.count
        let matchSink = try FusionMatchSink(
            candidates: candidatePool,
            scoring: input.scoring,
            limit: limit,
            workMeter: options.workMeter
        )
        let connectedSink = try FusionConnectedMatchSink(
            candidates: candidatePool,
            resultFieldName: source.resultField.name,
            limit: limit,
            workMeter: options.workMeter
        )
        guard let readableIndex = try await context.indexQueryContext
            .readableIndex(
                named: descriptor.name,
                indexType: descriptor.type,
                forEntityName: edgeEntity.name,
                partitions: source.edgePartitions,
                transaction: transaction
            ) else {
            connectedSink.invalidate()
            matchSink.invalidate()
            throw FusionExecutionError.indexNotReadable(
                entity: edgeEntity.name,
                name: descriptor.name
            )
        }
        let session = try FusionIndexReadSession(
            index: readableIndex,
            transaction: transaction,
            snapshot: CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            ).consistency == .snapshot,
            workMeter: options.workMeter
        )
        let request = FusionConnectedReadRequest(
            source: source,
            access: session,
            workMeter: options.workMeter
        )

        let coverage: FusionInputCoverage
        let emittedCount: Int
        do {
            coverage = try await executor.execute(
                request,
                output: connectedSink
            )
            emittedCount = try connectedSink.freeze(into: matchSink)
        } catch {
            let executionError = error
            connectedSink.invalidate()
            matchSink.invalidate()
            do {
                try await session.invalidate()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: executionError,
                    cleanupError: error
                )
            }
            throw executionError
        }

        let result: FusionIndexReadResult
        do {
            result = try matchSink.freeze(coverage: coverage)
        } catch {
            let freezeError = error
            matchSink.invalidate()
            do {
                try await session.invalidate()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: freezeError,
                    cleanupError: error
                )
            }
            throw freezeError
        }
        try await session.invalidate()
        guard emittedCount == result.matches.count else {
            throw FusionExecutionContractError.executionContractViolation
        }
        if coverage == .satisfiedLimit, emittedCount != limit {
            throw FusionExecutionContractError.invalidInputCoverage
        }
        return InputExecutionResult(
            candidates: try FusionCandidateDomain.make(
                selecting: result.matches.lazy.map(\.primaryKey),
                from: candidatePool,
                workMeter: options.workMeter
            ),
            scored: input.scoring.map {
                FusionScoredInput(scoring: $0, result: result)
            }
        )
    }

    private static func executeIndex(
        source: FusionIndexSource,
        descriptor: IndexDescriptor,
        executor: any FusionIndexReadExecutor,
        input: FusionPreparedPlan.Input,
        incoming: FusionCandidateDomain?,
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> InputExecutionResult {
        let limit = try input.limit
            ?? incoming?.count
            ?? options.workMeter.storageReadLimitWithSentinel()
        let sink = try FusionMatchSink(
            candidates: incoming,
            scoring: input.scoring,
            limit: limit,
            workMeter: options.workMeter
        )
        guard let readableIndex = try await context.indexQueryContext
            .readableIndex(
                named: descriptor.name,
                indexType: descriptor.type,
                forEntityName: entity.name,
                partitions: tableRef.partitions,
                transaction: transaction
            ) else {
            sink.invalidate()
            throw FusionExecutionError.indexNotReadable(
                entity: entity.name,
                name: descriptor.name
            )
        }

        let session = try FusionIndexReadSession(
            index: readableIndex,
            transaction: transaction,
            snapshot: CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            ).consistency == .snapshot,
            workMeter: options.workMeter
        )
        let request = FusionIndexReadRequest(
            source: source,
            scoring: input.scoring,
            limit: limit,
            access: session,
            workMeter: options.workMeter,
            timestamp: context.container.wallClock.now
        )
        let coverage: FusionInputCoverage
        do {
            if let incoming {
                coverage = try await executor.executeRestricted(
                    request,
                    candidates: incoming,
                    output: sink
                )
            } else {
                coverage = try await executor.executeUnrestricted(
                    request,
                    output: sink
                )
            }
        } catch {
            let executionError = error
            sink.invalidate()
            do {
                try await session.invalidate()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: executionError,
                    cleanupError: error
                )
            }
            throw executionError
        }
        // Revoke the feature's output authority before the first cleanup
        // suspension. The immutable result alone retains admitted matches.
        let result: FusionIndexReadResult
        do {
            result = try sink.freeze(coverage: coverage)
        } catch {
            let freezeError = error
            sink.invalidate()
            do {
                try await session.invalidate()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: freezeError,
                    cleanupError: error
                )
            }
            throw freezeError
        }
        do {
            try await session.invalidate()
        } catch {
            throw error
        }
        if coverage == .satisfiedLimit,
           result.matches.count != limit {
            throw FusionExecutionContractError.invalidInputCoverage
        }
        let domain: FusionCandidateDomain
        if let incoming {
            domain = try FusionCandidateDomain.make(
                selecting: result.matches.lazy.map(\.primaryKey),
                from: incoming,
                workMeter: options.workMeter
            )
        } else {
            domain = try await materializeUnrestrictedCandidates(
                result.matches,
                context: context,
                tableRef: tableRef,
                entity: entity,
                options: options,
                transaction: transaction
            )
        }
        return InputExecutionResult(
            candidates: domain,
            scored: input.scoring.map {
                FusionScoredInput(scoring: $0, result: result)
            }
        )
    }

    private static func materializeUnrestrictedCandidates(
        _ matches: [FusionIndexMatch],
        context: DatabaseContext,
        tableRef: TableRef,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        transaction: any TransactionAccess
    ) async throws -> FusionCandidateDomain {
        var builder = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: options.workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(Tuple.self),
            expectedCount: matches.count
        )
        for match in matches {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(match.primaryKey.count) + 64
                )
            ) {
                try Tuple(packed: match.primaryKey)
            }
        }
        let primaryKeys = try builder.finish().moveToSharedOwnership(
            at: .storageRow
        )
        return try await primaryKeys.withElements { primaryKeys in
            let snapshot = CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            ).consistency == .snapshot
            let models = try await context.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: tableRef.partitions,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: options.workMeter
            )
            return try FusionCandidateDomain.make(
                models: models,
                primaryKeys: primaryKeys,
                entity: entity,
                workMeter: options.workMeter
            )
        }
    }

    private static func emptyResult(
        for input: FusionPreparedPlan.Input,
        workMeter: DatabaseWorkMeter
    ) throws -> InputExecutionResult {
        let domain = try FusionCandidateDomain.empty(workMeter: workMeter)
        guard let scoring = input.scoring else {
            return InputExecutionResult(candidates: domain, scored: nil)
        }
        let sink = try FusionMatchSink(
            candidates: domain,
            scoring: scoring,
            limit: input.limit ?? 0,
            workMeter: workMeter
        )
        return InputExecutionResult(
            candidates: domain,
            scored: FusionScoredInput(
                scoring: scoring,
                result: try sink.freeze(coverage: .exhausted)
            )
        )
    }
}

private extension FusionSource {
    var scoredInputCount: Int {
        stages.reduce(into: 0) { count, stage in
            count += stage.inputs.reduce(into: 0) { inputCount, input in
                if input.scoring != nil { inputCount += 1 }
            }
        }
    }
}
