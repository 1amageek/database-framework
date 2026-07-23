import DatabaseWire

public protocol DatabaseResumableOperation: Sendable {
    associatedtype Job: DatabaseJobDescriptor
    associatedtype Plan: DatabaseWireValue
    associatedtype State: DatabaseWireValue

    func compile(
        _ request: Job.Request,
        context: DatabaseResumableOperationStartContext
    ) async throws -> DatabasePreparedResumableJob<Plan, State>

    func commitModel(
        for plan: Plan
    ) -> DatabaseResumableOperationCommitModel

    func runSlice(
        plan: Plan,
        state: State,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<State, Job.Response>

    func runCheckpointedSlice(
        plan: Plan,
        state: State,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<State, Job.Response>

    /// Applies operation-owned state in the same transaction that publishes
    /// the job's unsuccessful outcome. The transaction may retry this invocation.
    func applyUnsuccessfulOutcome(
        plan: Plan,
        state: State,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws
}

public extension DatabaseResumableOperation {
    func commitModel(
        for plan: Plan
    ) -> DatabaseResumableOperationCommitModel {
        _ = plan
        return .atomicWithJobState
    }

    func runCheckpointedSlice(
        plan: Plan,
        state: State,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }
}
