import Core
import DatabaseEngine
import DatabaseWire
import Graph
import GraphIndex
import QueryAST
import QueryIR
import StorageKit

extension FDBContext {
    public func executeSPARQL<T: Persistable>(
        _ sparql: String,
        on type: T.Type,
        budget: DatabaseExecutionBudget = DatabaseExecutionBudget()
    ) async throws -> SPARQLResult {
        let statement = try SPARQLParser().parse(sparql)
        guard case .select(let query) = statement else {
            throw SPARQLStringError.unsupportedQueryForm(statement)
        }
        let plan = try SPARQLSelectPlanCompiler.compile(query)
        return try await executeSPARQLSelectPlan(
            plan,
            on: type,
            datasetScope: try SPARQLDatasetExecutionScope(query.dataset),
            budget: budget
        )
    }
}

public func executeSPARQLString(
    _ sparql: String,
    database: any StorageEngine,
    sources: [RDFDatasetSource],
    transaction: (any Transaction)? = nil,
    budget: DatabaseExecutionBudget
) async throws -> SPARQLResult {
    let workMeter = DatabaseWorkMeter(budget: budget)
    let result = try await _executeSPARQLString(
        sparql,
        database: database,
        sources: sources,
        transaction: transaction,
        workMeter: workMeter
    )
    guard let rowCount = UInt32(exactly: result.bindings.count) else {
        throw DatabaseWorkLimitError.maximumRows(
            stage: .resultMaterialization,
            consumed: workMeter.consumedRows,
            requested: UInt32.max,
            maximum: budget.maximumRows
        )
    }
    try workMeter.recordOutputRows(rowCount)
    return result
}

func _executeSPARQLString(
    _ sparql: String,
    database: any StorageEngine,
    sources: [RDFDatasetSource],
    transaction: (any Transaction)? = nil,
    workMeter: DatabaseWorkMeter
) async throws -> SPARQLResult {
    let statement = try SPARQLParser().parse(sparql)
    guard case .select(let query) = statement else {
        throw SPARQLStringError.unsupportedQueryForm(statement)
    }
    let plan = try SPARQLSelectPlanCompiler.compile(query)
    let executor = SPARQLQueryExecutor(
        database: database,
        sources: sources,
        datasetScope: try SPARQLDatasetExecutionScope(query.dataset)
    )
    let startTime = MonotonicClock.now()
    let executionResult: ([VariableBinding], ExecutionStatistics)
    if let transaction {
        executionResult = try await executor.executeInTransaction(
            selectPlan: plan,
            transaction: transaction,
            workMeter: workMeter
        )
    } else {
        executionResult = try await executor.execute(
            selectPlan: plan,
            workMeter: workMeter
        )
    }
    var (bindings, statistics) = executionResult
    statistics.durationNs = MonotonicClock.now().uptimeNanoseconds
        - startTime.uptimeNanoseconds
    let reachedLimit = plan.slice.limit.map {
        bindings.count >= $0
    } ?? false
    return SPARQLResult(
        bindings: consume bindings,
        projectedVariables: plan.projectionVariables,
        isComplete: !reachedLimit,
        limitReason: reachedLimit ? .explicitLimit : nil,
        statistics: statistics
    )
}

public enum SPARQLStringError: Error, CustomStringConvertible {
    case unsupportedQueryForm(QueryIR.QueryStatement)

    public var description: String {
        switch self {
        case .unsupportedQueryForm(let statement):
            return "Unsupported query form: expected SELECT, got \(statement)"
        }
    }
}
