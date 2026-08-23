#if DATABASE_GRAPH_INDEXES
import DatabaseKit
import DatabaseEngine
@_spi(DatabaseExecution) import GraphIndex
import QueryAST
import StorageKit

extension DatabaseContext {
    public func executeSPARQL<T: Persistable>(
        _ sparql: String,
        on type: T.Type,
        compilationLimits: SPARQLExpressionCompilationLimits = .default,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SPARQLResult {
        let statement = try SPARQLParser(
            structuralLimits: compilationLimits.structuralLimits
        ).parse(sparql)
        guard case .select(let query) = statement else {
            throw SPARQLStringError.unsupportedQueryForm(statement)
        }
        let plan = try SPARQLSelectPlanCompiler.compile(
            query,
            expressionLimits: compilationLimits
        )
        return try await executeSPARQLSelectPlan(
            plan,
            on: type,
            budget: budget
        )
    }
}

func _executeRetainedSPARQLSelectPlan(
    _ plan: SPARQLSelectExecutionPlan,
    sources: [RDFDatasetSource],
    monotonicClock: any StorageMonotonicClock,
    wallClock: any WallClock,
    transaction: any TransactionReadAccess,
    workMeter: DatabaseWorkMeter
) async throws -> SPARQLRetainedResult {
    let executor = SPARQLQueryExecutor(
        monotonicClock: monotonicClock,
        wallClock: wallClock,
        sources: sources
    )
    let startTime = monotonicClock.now
    let result = try await executor.executeRetainedInTransaction(
        selectPlan: plan,
        transaction: transaction,
        workMeter: workMeter
    )
    return result.recordingDuration(
        nanoseconds: DatabaseMonotonicMeasurement.nanoseconds(
            from: startTime,
            to: monotonicClock.now
        )
    )
}

func compileSPARQLSelectPlan(
    _ sparql: String,
    compilationLimits: SPARQLExpressionCompilationLimits
) throws -> SPARQLSelectExecutionPlan {
    let statement = try SPARQLParser(
        structuralLimits: compilationLimits.structuralLimits
    ).parse(sparql)
    guard case .select(let query) = statement else {
        throw SPARQLStringError.unsupportedQueryForm(statement)
    }
    return try SPARQLSelectPlanCompiler.compile(
        query,
        expressionLimits: compilationLimits
    )
}

public enum SPARQLStringError: Error, CustomStringConvertible {
    case unsupportedQueryForm(QueryStatement)

    public var description: String {
        switch self {
        case .unsupportedQueryForm(let statement):
            return "Unsupported query form: expected SELECT, got \(statement)"
        }
    }
}
#endif
