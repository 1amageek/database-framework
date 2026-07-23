public protocol DatabaseUnsuccessfulOutcomeIndependentOperation:
    DatabaseResumableOperation {}

public extension DatabaseUnsuccessfulOutcomeIndependentOperation {
    func applyUnsuccessfulOutcome(
        plan: Plan,
        state: State,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = outcome
        _ = context
    }
}
