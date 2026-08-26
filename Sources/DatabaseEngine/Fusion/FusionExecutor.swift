import DatabaseKit
import DatabaseTypes
import StorageKit

/// Canonical staged Fusion runtime. It alone owns relational candidates,
/// transaction lifetime, feature admission, stage algebra, and score output.
enum FusionExecutor {
    static let scoreAnnotation = "fusion.score"

    static func execute(
        _ execution: FusionExecution
    ) async throws -> IndexReadResult {
        var candidates: FusionCandidateDomain?
        var scoredInputs = try DatabaseRetainedArrayBuilder<FusionScoredInput>(
            workMeter: execution.options.workMeter,
            stage: .projection,
            layout: try DatabaseRetainedArrayLayout.forElement(
                FusionScoredInput.self
            ),
            expectedCount: execution.source.scoredInputCount
        )

        for stage in execution.plan.stages {
            let incoming = candidates
            var stageCandidates: FusionCandidateDomain?
            for input in stage.inputs {
                let result: InputExecutionResult
                if incoming?.isEmpty == true {
                    result = try emptyResult(
                        for: input,
                        workMeter: execution.options.workMeter
                    )
                } else {
                    result = try await execute(
                        input,
                        incoming: incoming,
                        execution: execution
                    )
                }
                if let existing = stageCandidates {
                    stageCandidates = try existing.union(
                        result.candidates,
                        workMeter: execution.options.workMeter
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
                    workMeter: execution.options.workMeter
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
            strategy: execution.source.strategy,
            maximumResultCount: execution.maximumResultCount,
            workMeter: execution.options.workMeter
        )
    }

    private struct InputExecutionResult: Sendable {
        let candidates: FusionCandidateDomain
        let scored: FusionScoredInput?
    }

    private static func execute(
        _ input: FusionPreparedPlan.Input,
        incoming: FusionCandidateDomain?,
        execution: FusionExecution
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
                source: .table(execution.plan.tableRef),
                filter: expression,
                limit: input.limit.map(UInt64.init)
            )
            let response: CanonicalRetainedQueryResponse
            if let incoming {
                response = try await execution.executeCandidateRelationalRows(
                    incoming,
                    query: query
                )
            } else {
                response = try await execution.executeRelationalRows(query)
            }
            return InputExecutionResult(
                candidates: try FusionCandidateDomain.make(
                    rows: response.visibleRows,
                    entity: execution.plan.entity,
                    workMeter: execution.options.workMeter
                ),
                scored: nil
            )
        case .order(let sortKeys):
            guard let incoming, let scoring = input.scoring else {
                throw FusionExecutionError.invalidInputScoring(input.scoring)
            }
            let query = SelectQuery(
                projection: .all,
                source: .table(execution.plan.tableRef),
                orderBy: sortKeys,
                limit: input.limit.map(UInt64.init)
            )
            let response = try await execution.executeCandidateRelationalRows(
                incoming,
                query: query
            )
            let limit = input.limit ?? incoming.count
            let sink = try FusionMatchSink(
                candidates: incoming,
                scoring: scoring,
                limit: limit,
                workMeter: execution.options.workMeter
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
                        entity: execution.plan.entity,
                        workMeter: execution.options.workMeter
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
                execution: execution
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
                execution: execution
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
        execution: FusionExecution
    ) async throws -> InputExecutionResult {
        let candidatePool: FusionCandidateDomain
        if let incoming {
            candidatePool = incoming
        } else {
            let response = try await execution.executeRelationalRows(
                SelectQuery(
                    projection: .all,
                    source: .table(execution.plan.tableRef)
                )
            )
            candidatePool = try FusionCandidateDomain.make(
                rows: response.visibleRows,
                entity: execution.plan.entity,
                workMeter: execution.options.workMeter
            )
        }

        let limit = input.limit ?? candidatePool.count
        let matchSink = try FusionMatchSink(
            candidates: candidatePool,
            scoring: input.scoring,
            limit: limit,
            workMeter: execution.options.workMeter
        )
        let connectedSink = try FusionConnectedMatchSink(
            candidates: candidatePool,
            resultFieldName: source.resultField.name,
            limit: limit,
            workMeter: execution.options.workMeter
        )
        guard let readableIndex = try await execution.readableIndex(
            descriptor: descriptor,
            entity: edgeEntity,
            partitions: source.edgePartitions
        ) else {
            connectedSink.invalidate()
            matchSink.invalidate()
            throw FusionExecutionError.indexNotReadable(
                entity: edgeEntity.name,
                name: descriptor.name
            )
        }
        return try await execution.withIndexReadLease(
            index: readableIndex
        ) { lease in
            let request = FusionConnectedReadRequest(
                source: source,
                access: lease,
                workMeter: execution.options.workMeter
            )
            do {
                let coverage = try await executor.execute(
                    request,
                    output: connectedSink
                )
                let emittedCount = try connectedSink.freeze(into: matchSink)
                let result = try matchSink.freeze(coverage: coverage)
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
                        workMeter: execution.options.workMeter
                    ),
                    scored: input.scoring.map {
                        FusionScoredInput(scoring: $0, result: result)
                    }
                )
            } catch {
                connectedSink.invalidate()
                matchSink.invalidate()
                throw error
            }
        }
    }

    private static func executeIndex(
        source: FusionIndexSource,
        descriptor: IndexDescriptor,
        executor: any FusionIndexReadExecutor,
        input: FusionPreparedPlan.Input,
        incoming: FusionCandidateDomain?,
        execution: FusionExecution
    ) async throws -> InputExecutionResult {
        let limit = try input.limit
            ?? incoming?.count
            ?? execution.options.workMeter.storageReadLimitWithSentinel()
        let sink = try FusionMatchSink(
            candidates: incoming,
            scoring: input.scoring,
            limit: limit,
            workMeter: execution.options.workMeter
        )
        guard let readableIndex = try await execution.readableIndex(
            descriptor: descriptor,
            entity: execution.plan.entity,
            partitions: execution.plan.tableRef.partitions
        ) else {
            sink.invalidate()
            throw FusionExecutionError.indexNotReadable(
                entity: execution.plan.entity.name,
                name: descriptor.name
            )
        }
        let result = try await execution.withIndexReadLease(
            index: readableIndex
        ) { lease in
            let request = FusionIndexReadRequest(
                source: source,
                scoring: input.scoring,
                limit: limit,
                access: lease,
                workMeter: execution.options.workMeter,
                timestamp: execution.wallClock.now
            )
            do {
                let coverage: FusionInputCoverage
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
                let result = try sink.freeze(coverage: coverage)
                if coverage == .satisfiedLimit,
                   result.matches.count != limit {
                    throw FusionExecutionContractError.invalidInputCoverage
                }
                return result
            } catch {
                sink.invalidate()
                throw error
            }
        }
        let domain: FusionCandidateDomain
        if let incoming {
            domain = try FusionCandidateDomain.make(
                selecting: result.matches.lazy.map(\.primaryKey),
                from: incoming,
                workMeter: execution.options.workMeter
            )
        } else {
            domain = try await execution.materializeCandidates(result)
        }
        return InputExecutionResult(
            candidates: domain,
            scored: input.scoring.map {
                FusionScoredInput(scoring: $0, result: result)
            }
        )
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
