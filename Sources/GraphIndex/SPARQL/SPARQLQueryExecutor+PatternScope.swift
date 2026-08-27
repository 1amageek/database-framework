import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateGroupPattern(
        sourcePattern: ExecutionPattern,
        grouping: SPARQLGroupingPlan,
        aggregates aggs: [AggregateExpression],
        having havingExpr: FilterExpression?,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        let sourceResult = try await evaluate(
            pattern: sourcePattern,
            transaction: transaction,
            activeGraph: activeGraph,
            seed: seed
        )
        let sourceStatistics = sourceResult.stats
        guard let expressionContext else {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "query-scoped expression context is unavailable"
            )
        }
        let workMeter = try requiredWorkMeter()
        let groupKeys = grouping.keys
        let partition = try await SPARQLGroupPartitionBuilder.build(
            source: consume sourceResult.bindings,
            grouping: grouping,
            expressionContext: expressionContext,
            workMeter: workMeter,
            evaluateKey: { plan, binding in
                try await self.evaluateCanonicalExpression(
                    plan,
                    binding: binding,
                    transaction: transaction,
                    activeGraph: activeGraph
                )
            }
        )

        var resultBindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: workMeter,
            stage: .aggregateInput,
            expectedCount: partition.groupCount
        )
        let outputFootprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .aggregateInput
        )
        defer { outputFootprintMeter.shutdown() }
        for groupIndex in 0..<partition.groupCount {
            let memberRange = partition.memberRange(at: groupIndex)
            try workMeter.consume(
                UInt64(max(1, memberRange.count)),
                at: .aggregateInput
            )
            var binding = VariableBinding()
            var bindingFootprint = try outputFootprintMeter.footprint(
                of: binding
            )
            let candidateReservation = try workMeter.reserveIntermediate(
                rows: bindingFootprint.rows,
                bytes: bindingFootprint.bytes,
                at: .aggregateInput
            )
            defer { candidateReservation.release() }
            for index in groupKeys.indices {
                let groupValue = try partition.withGroupKeyValue(
                    groupIndex: groupIndex,
                    keyIndex: index,
                    { copy $0 }
                )
                if let fieldValue = groupValue.fieldValue {
                    let variable = groupKeys[index].outputVariable
                    bindingFootprint = try extendCandidate(
                        &binding,
                        currentFootprint: bindingFootprint,
                        variable: variable,
                        value: .borrowing(fieldValue),
                        footprintMeter: outputFootprintMeter,
                        reservation: candidateReservation
                    )
                }
            }
            for agg in aggs {
                let aggregateValue: DatabaseQueryScopedFieldValue?
                switch try agg.resultOwnership() {
                case .borrowed:
                    aggregateValue = try await evaluateAggregateValue(
                        agg,
                        groupIndex: groupIndex,
                        in: partition,
                        workMeter: workMeter,
                        transaction: transaction,
                        activeGraph: activeGraph
                    ).map(DatabaseQueryScopedFieldValue.borrowing)
                case .produced(let maximumFootprint):
                    aggregateValue = try await DatabaseQueryScopedFieldValue
                        .producingOptional(
                            maximumFootprint: maximumFootprint,
                            workMeter: workMeter,
                            stage: .aggregateInput
                        ) {
                            try await evaluateAggregateValue(
                                agg,
                                groupIndex: groupIndex,
                                in: partition,
                                workMeter: workMeter,
                                transaction: transaction,
                                activeGraph: activeGraph
                            )
                        }
                }
                if let aggregateValue {
                    bindingFootprint = try extendCandidate(
                        &binding,
                        currentFootprint: bindingFootprint,
                        variable: agg.alias,
                        value: aggregateValue,
                        footprintMeter: outputFootprintMeter,
                        reservation: candidateReservation
                    )
                }
            }
            let admission = try resultBindings.prepareAppend(
                footprint: bindingFootprint,
                at: .aggregateInput
            )
            if let having = havingExpr {
                try workMeter.consume(at: .filterEvaluation)
                guard try await evaluateFilterExpression(
                    having,
                    binding: binding,
                    transaction: transaction,
                    activeGraph: activeGraph
                ) else {
                    continue
                }
            }
            resultBindings.append(binding, using: admission)
            if let resultLimit,
               resultBindings.count >= resultLimit {
                break
            }
        }

        return EvaluationResult(
            bindings: resultBindings.finish(),
            stats: stats
        )
            .mergedStats(with: sourceStatistics)
    }

    private func extendCandidate(
        _ binding: inout VariableBinding,
        currentFootprint: DatabaseIntermediateFootprint,
        variable: String,
        value: DatabaseQueryScopedFieldValue,
        footprintMeter: SPARQLBindingFootprintMeter,
        reservation: DatabaseIntermediateReservation
    ) throws -> DatabaseIntermediateFootprint {
        let nextFootprint = try value.withValue { borrowedValue in
            switch try footprintMeter.footprint(
                extending: binding,
                variable: variable,
                value: borrowedValue
            ) {
            case .incompatible:
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "group output variable is already bound"
                )
            case .compatible(let footprint):
                return footprint
            }
        }
        guard nextFootprint.rows == currentFootprint.rows,
              nextFootprint.bytes >= currentFootprint.bytes else {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "group binding footprint did not grow monotonically"
            )
        }
        try reservation.reserveAdditional(
            bytes: nextFootprint.bytes - currentFootprint.bytes,
            at: .aggregateInput
        )
        try value.withValue { borrowedValue in
            guard binding.merge(
                variable: variable,
                value: copy borrowedValue
            ) else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "group output changed after prospective admission"
                )
            }
        }
        return nextFootprint
    }

    private func evaluateAggregateValue(
        _ aggregate: AggregateExpression,
        groupIndex: Int,
        in partition: borrowing SPARQLGroupPartition,
        workMeter: DatabaseWorkMeter,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph
    ) async throws -> FieldValue? {
        let outcome = try await aggregate.evaluate(
            groupIndex: groupIndex,
            in: partition,
            workMeter: workMeter,
            evaluateExpression: { plan, solution in
                try await self.evaluateCanonicalExpression(
                    plan,
                    binding: solution,
                    transaction: transaction,
                    activeGraph: activeGraph
                )
            }
        )
        switch outcome {
        case .value(let value):
            return value
        case .expressionError(let error):
            if error.isSPARQLEvaluationError {
                return nil
            }
            throw error
        }
    }

}
