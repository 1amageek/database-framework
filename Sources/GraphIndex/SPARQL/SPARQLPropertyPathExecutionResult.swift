/// Completed property-path query result with linear binding ownership.
struct SPARQLPropertyPathExecutionResult: ~Copyable, Sendable {
    var bindings: SPARQLRetainedBindings
    var statistics: ExecutionStatistics
}
