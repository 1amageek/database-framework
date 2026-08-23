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
        transaction: any TransactionAccess,
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
        for groupIndex in 0..<partition.groupCount {
            let memberRange = partition.memberRange(at: groupIndex)
            try workMeter.consume(
                UInt64(max(1, memberRange.count)),
                at: .aggregateInput
            )
            var binding = VariableBinding()
            for index in groupKeys.indices {
                let groupValue = try partition.withGroupKeyValue(
                    groupIndex: groupIndex,
                    keyIndex: index,
                    { copy $0 }
                )
                if let fieldValue = groupValue.fieldValue {
                    binding = binding.binding(
                        groupKeys[index].outputVariable,
                        to: fieldValue
                    )
                }
            }
            for agg in aggs {
                let aggregateOutcome = try await agg.evaluate(
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
                switch aggregateOutcome {
                case .value(.some(let aggregateValue)):
                    binding = binding.binding(
                        agg.alias,
                        to: aggregateValue
                    )
                case .value(.none):
                    break
                case .expressionError(let error):
                    if error.isSPARQLEvaluationError {
                        break
                    }
                    throw error
                }
            }
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
            try resultBindings.append(
                binding,
                at: .aggregateInput
            )
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

}
