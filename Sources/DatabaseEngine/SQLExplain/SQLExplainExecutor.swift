import DatabaseKit
import StorageKit

/// EXPLAIN shares admission and execution with ordinary statements.
struct SQLExplainExecutor: Sendable {
    let context: DatabaseContext
    let transaction: DatabaseTransaction
    let options: SQLBlockExecutionOptions
    let temporal: SQLTemporalEnvironment
    let workMeter: DatabaseWorkMeter

    func execute(_ explain: SQLExplain, parameters: [QueryParameter],
                 occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil,
                 execute: @Sendable (QueryStatement, [QueryParameter]) async throws -> SQLBlockStatementOutcome
    ) async throws -> SQLBlockStatementOutcome {
        try workMeter.checkpoint(at: .bindingCandidate)
        let plan = try await SQLPreparedAnalysis.analyze(explain.statement, parameters: parameters,
            context: context, transaction: transaction, options: options, temporal: temporal, workMeter: workMeter,
            occurrences: occurrences, occurrence: occurrence)
        let measurement: SQLExplainMeasurement?
        if explain.options.analyze {
            let clock = context.container.monotonicClock
            let started = clock.now
            let workBefore = workMeter.consumedWorkUnits
            // The enclosing SQLBlock retains transaction/checkpoint authority.
            let outcome = try await execute(explain.statement, parameters)
            let ended = clock.now
            let consumedWork = workMeter.consumedWorkUnits - workBefore
            let count: UInt64
            switch outcome {
            case .rows(let rows): count = UInt64(rows.affectedRows ?? rows.visibleRange.count)
            case .command(_, let affected): count = affected ?? 0
            case .copy(let result, _): count = result.affectedRows
            }
            measurement = SQLExplainMeasurement(rows: count, workUnits: consumedWork, elapsed: started.duration(to: ended))
        } else {
            measurement = nil
        }
        try workMeter.checkpoint(at: .resultMaterialization)
        return .rows(try SQLExplainRenderer.render(plan, options: explain.options,
            measurement: measurement, meter: workMeter))
    }
}
