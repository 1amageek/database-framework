import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    func evaluateValuesPattern(
        _ table: SPARQLValuesTable,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression?,
        seed: VariableBinding,
        resultLimit: Int?,
        statistics initialStatistics: ExecutionStatistics
    ) async throws -> EvaluationResult {
        var stats = initialStatistics
        let meter = try requiredWorkMeter()
        let capacity = resultLimit.map {
            min(table.rowCount, max(0, $0))
        } ?? table.rowCount
        guard capacity > 0 else {
            return .empty(stats: stats)
        }
        var bindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: meter,
            stage: .bindingCandidate,
            expectedCount: 0
        )

        for row in 0..<table.rowCount {
            try meter.consume(at: .bindingCandidate)
            for column in table.variables.indices {
                guard table.value(row: row, column: column) != nil else {
                    continue
                }
                try meter.consume(at: .joinCandidate)
            }

            if let filter {
                guard let binding = valuesBinding(
                    extending: seed,
                    with: table,
                    row: row
                ) else {
                    continue
                }
                try meter.consume(at: .filterEvaluation)
                guard try await evaluateFilterExpression(
                    filter,
                    binding: binding,
                    transaction: transaction,
                    activeGraph: activeGraph
                ) else {
                    continue
                }
                switch try bindings.prepareAppend(
                    extending: seed,
                    with: table,
                    row: row,
                    at: .bindingCandidate
                ) {
                case .incompatible:
                    throw SPARQLQueryError.executionFailed(
                        "VALUES filter candidate disagrees with retained footprint preflight"
                    )
                case .admitted(let admission):
                    bindings.append(binding, using: admission)
                }
                if let resultLimit, bindings.count >= resultLimit {
                    break
                }
                continue
            }

            switch try bindings.prepareAppend(
                extending: seed,
                with: table,
                row: row,
                at: .bindingCandidate
            ) {
            case .incompatible:
                continue
            case .admitted(let admission):
                guard let binding = valuesBinding(
                    extending: seed,
                    with: table,
                    row: row
                ) else {
                    throw SPARQLQueryError.executionFailed(
                        "VALUES preflight disagrees with row construction"
                    )
                }
                bindings.append(binding, using: admission)
            }
            if let resultLimit, bindings.count >= resultLimit {
                break
            }
        }
        stats.intermediateResults = bindings.count
        return EvaluationResult(
            bindings: bindings.finish(),
            stats: stats
        )
    }

    private func valuesBinding(
        extending seed: borrowing VariableBinding,
        with table: borrowing SPARQLValuesTable,
        row: Int
    ) -> VariableBinding? {
        var binding = copy seed
        for column in table.variables.indices {
            guard let value = table.value(row: row, column: column) else {
                continue
            }
            guard binding.merge(
                variable: table.variables[column],
                value: value
            ) else {
                return nil
            }
        }
        return binding
    }

}
