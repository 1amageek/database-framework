@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseResumableOperation: Sendable {
    associatedtype Request: Sendable
    associatedtype Response: Sendable
    associatedtype Plan: PersistentJobPayload
    associatedtype State: PersistentJobPayload

    static func job()
        throws(DatabaseWireError) -> JobOperation<Request, Response>

    /// Declares which Base lifecycle state the operation requires while each
    /// durable slice runs. Data maintenance uses the default active admission;
    /// lifecycle operations override this with `.lifecycleJob`.
    var baseAdmission: DatabaseBaseAdmissionKind { get }

    func compile(
        _ request: Request,
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
    ) async throws -> sending DatabaseResumableOperationSlice<State, Response>

    func runCheckpointedSlice(
        plan: Plan,
        state: State,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws -> sending DatabaseResumableOperationSlice<State, Response>

    /// Applies operation-owned state in the same control-domain transaction
    /// that publishes a successful job result. A checkpointed operation must
    /// retain durable ownership evidence until this hook commits.
    func applySuccessfulOutcome(
        plan: Plan,
        state: State,
        context: DatabaseResumableOperationContext
    ) async throws

    /// Completes idempotent operation-owned recovery before the job outcome
    /// is published. This hook runs without a control-domain transaction and
    /// may be repeated after a crash or lease expiry. Implementations must
    /// persist enough ownership evidence for `applyUnsuccessfulOutcome` to
    /// validate the recovery in the final control-domain transaction.
    func prepareUnsuccessfulOutcomeCommit(
        plan: Plan,
        state: State,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws

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
    var baseAdmission: DatabaseBaseAdmissionKind { .activeData }

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
    ) async throws -> sending DatabaseResumableOperationSlice<State, Response> {
        _ = plan
        _ = state
        _ = maximumWorkUnits
        _ = context
        throw DatabaseJobRuntimeError.commitModelMismatch
    }

    func prepareUnsuccessfulOutcomeCommit(
        plan: Plan,
        state: State,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = outcome
        _ = context
    }

    func applySuccessfulOutcome(
        plan: Plan,
        state: State,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = context
    }
}
