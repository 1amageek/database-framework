import DatabaseKit

extension DatabaseContext {
    /// Binds the ordinary query without constructing or pulling a row producer.
    func boundSQLExplainPlan(_ query: SelectQuery, parameters: [QueryParameter],
        options: ReadExecutionContext, occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil) throws -> SQLExplainPlanNode {
        let plan = try boundFusionRelationalQueries([query], parameters: parameters, options: options,
            parameterState: SQLPreparedAnalysisRuntime.parameters, occurrences: occurrences, occurrence: occurrence)[0]
        return try SQLExplainPlanBuilder(context: self, options: options).select(query, plan: plan)
    }
}
