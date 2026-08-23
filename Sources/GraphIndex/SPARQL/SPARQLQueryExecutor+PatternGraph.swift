import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateGraphPattern(
        selector: ExecutionGraphSelector,
        innerPattern: ExecutionPattern,
        transaction: any TransactionReadAccess,
        filter: FilterExpression?,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics stats: ExecutionStatistics
    ) async throws -> EvaluationResult {
        switch selector {
        case .named(let graph):
            guard dataset.contains(namedGraph: graph) else {
                return .empty(stats: stats)
            }
            if dataset.selectedNamedGraphs == nil {
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
            if let selectedGraphs = dataset.selectedNamedGraphs {
                return try await evaluateVariableGraphPattern(
                    graphNames: selectedGraphs,
                    graphName: { $0 },
                    variable: variable,
                    innerPattern: innerPattern,
                    transaction: transaction,
                    filter: filter,
                    seed: seed,
                    resultLimit: resultLimit,
                    statistics: stats
                )
            }
            let discoveredGraphs = try await datasetScanner.namedGraphs(
                limit: nil,
                readMode: readMode,
                transaction: transaction,
                workMeter: try requiredWorkMeter()
            )
            return try await evaluateVariableGraphPattern(
                graphNames: discoveredGraphs,
                graphName: { $0.graph },
                variable: variable,
                innerPattern: innerPattern,
                transaction: transaction,
                filter: filter,
                seed: seed,
                resultLimit: resultLimit,
                statistics: stats
            )
        }
    }

    private func evaluateVariableGraphPattern<GraphNames: Sequence & Sendable>(
        graphNames: GraphNames,
        graphName: @Sendable (GraphNames.Element) -> RDFGraphName,
        variable: String,
        innerPattern: ExecutionPattern,
        transaction: any TransactionReadAccess,
        filter: FilterExpression?,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics: ExecutionStatistics
    ) async throws -> EvaluationResult {
        var bindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .joinCandidate,
            expectedCount: 0
        )
        var mergedStats = statistics
        for element in graphNames {
            let graph = graphName(element)
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
