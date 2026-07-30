import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateGraphPattern(
        selector: ExecutionGraphSelector,
        innerPattern: ExecutionPattern,
        transaction: any TransactionAccess,
        filter: FilterExpression?,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        switch selector {
        case .named(let graph):
            guard datasetScope.contains(namedGraph: graph) else {
                return .empty(stats: stats)
            }
            if datasetScope.selectedNamedGraphs == nil {
                let exists = try await datasetScanner.containsNamedGraph(
                    graph,
                    readMode: readMode,
                    transaction: transaction,
                    workMeter: try requiredWorkMeter()
                )
                guard exists else {
                    return .empty(stats: stats)
                }
            }
            let result = try await evaluate(
                pattern: innerPattern,
                transaction: transaction,
                activeGraph: .named(graph),
                filter: filter,
                seed: seed,
                resultLimit: resultLimit
            )
            return EvaluationResult(
                bindings: consume result.bindings,
                stats: stats
            )
                .mergedStats(with: result.stats)

        case .variable(let variable):
            let graphNames: [RDFGraphName]
            if let selectedGraphs = datasetScope.selectedNamedGraphs {
                graphNames = selectedGraphs
            } else {
                graphNames = try await datasetScanner.namedGraphs(
                    limit: nil,
                    readMode: readMode,
                    transaction: transaction,
                    workMeter: try requiredWorkMeter()
                )
            }

            var bindings = try SPARQLRetainedBindingBuilder.make(
                workMeter: try requiredWorkMeter(),
                stage: .joinCandidate,
                expectedCount: 0
            )
            var mergedStats = stats
            for graph in graphNames {
                let remainingLimit = resultLimit.map {
                    max(0, $0 - bindings.count)
                }
                let graphBinding = VariableBinding().binding(
                    variable,
                    to: .rdfTerm(graph.term)
                )
                guard let graphSeed = seed.merged(with: graphBinding) else {
                    continue
                }
                let result = try await evaluate(
                    pattern: innerPattern,
                    transaction: transaction,
                    activeGraph: .named(graph),
                    filter: filter,
                    seed: graphSeed,
                    resultLimit: remainingLimit
                )
                mergedStats = mergedStats.merged(with: result.stats)
                for resultIndex in 0..<result.bindings.count {
                    if let resultLimit,
                       bindings.count >= resultLimit {
                        break
                    }
                    try requiredWorkMeter().consume(at: .joinCandidate)
                    try result.bindings.withElement(
                        at: resultIndex
                    ) { binding in
                        _ = try bindings.appendMerged(
                            graphSeed,
                            with: binding,
                            at: .joinCandidate
                        )
                    }
                }
                if let resultLimit, bindings.count >= resultLimit {
                    break
                }
            }
            return EvaluationResult(
                bindings: bindings.finish(),
                stats: mergedStats
            )
        }
    }

}
