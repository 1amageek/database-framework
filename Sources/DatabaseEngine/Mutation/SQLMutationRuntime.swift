/// Dynamic binding supplied by the SQL request owner across transaction retries.
package enum SQLMutationRuntime {
    @TaskLocal package static var request: SQLMutationRequest?
    @TaskLocal package static var statementIndex: Int = 0
}
