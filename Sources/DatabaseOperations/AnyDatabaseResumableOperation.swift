import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct AnyDatabaseResumableOperation: Sendable {
    struct PreparedJob: Sendable {
        let planPayload: ByteString
        let initialStatePayload: ByteString
        let sliceTimeoutMilliseconds: UInt32
    }

    struct Slice: Sendable {
        enum Outcome: Sendable {
            case incomplete(ByteString)
            case complete(ByteString)
        }

        let completedWorkUnits: UInt64
        let totalWorkUnits: UInt64?
        let outcome: Outcome
    }

    public let operation: JobOperationIdentifier
    public let baseAdmission: DatabaseBaseAdmissionKind

    private let prepareJob: @Sendable (
        ByteString,
        DatabaseResumableOperationStartContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> PreparedJob
    private let resolveCommitModel: @Sendable (
        ByteString,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) throws -> DatabaseResumableOperationCommitModel
    private let executeSlice: @Sendable (
        ByteString,
        ByteString,
        UInt64,
        DatabaseResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Slice
    private let executeCheckpointedSlice: @Sendable (
        ByteString,
        ByteString,
        UInt64,
        DatabaseCheckpointedResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Slice
    private let applySuccessfulOutcome: @Sendable (
        ByteString,
        ByteString,
        DatabaseResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Void
    private let prepareUnsuccessfulOutcomeCommit: @Sendable (
        ByteString,
        ByteString,
        DatabaseJobUnsuccessfulOutcome,
        DatabaseCheckpointedResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Void
    private let applyUnsuccessfulOutcome: @Sendable (
        ByteString,
        ByteString,
        DatabaseJobUnsuccessfulOutcome,
        DatabaseResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Void

    public init<Operation: DatabaseResumableOperation>(
        _ operation: Operation
    ) throws {
        let job = try Operation.job()
        self.operation = job.identifier
        self.baseAdmission = operation.baseAdmission
        self.prepareJob = { payload, context, limits, storageLimits in
            let request = try job.decodeStartRequest(
                payload,
                limits: limits
            )
            let prepared = try await operation.compile(request, context: context)
            return PreparedJob(
                planPayload: try encodePersistentJobPayload(
                    prepared.plan,
                    limits: try storageLimits.planWireLimits(basedOn: limits),
                    maximum: storageLimits.maximumPlanPayloadBytes,
                    kind: .plan
                ),
                initialStatePayload: try encodePersistentJobPayload(
                    prepared.initialState,
                    limits: try storageLimits.stateWireLimits(basedOn: limits),
                    maximum: storageLimits.maximumOperationStateBytes,
                    kind: .state
                ),
                sliceTimeoutMilliseconds: prepared.sliceTimeoutMilliseconds
            )
        }
        self.resolveCommitModel = { planPayload, limits, storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            return operation.commitModel(for: plan)
        }
        self.executeSlice = {
            planPayload,
            statePayload,
            workUnits,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try decodePersistentJobPayload(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            let slice = try await operation.runSlice(
                plan: plan,
                state: state,
                maximumWorkUnits: workUnits,
                context: context
            )
            let outcome: Slice.Outcome
            switch slice.outcome {
            case .incomplete(let nextState):
                outcome = .incomplete(
                    try encodePersistentJobPayload(
                        nextState,
                        limits: try storageLimits.stateWireLimits(
                            basedOn: limits
                        ),
                        maximum: storageLimits.maximumOperationStateBytes,
                        kind: .state
                    )
                )
            case .complete(let result):
                outcome = .complete(
                    try encodePersistentJobResult(
                        result,
                        job: job,
                        limits: try storageLimits.resultWireLimits(
                            basedOn: limits
                        ),
                        maximum: storageLimits.maximumResultBytes,
                        kind: .result
                    )
                )
            }
            return Slice(
                completedWorkUnits: slice.completedWorkUnits,
                totalWorkUnits: slice.totalWorkUnits,
                outcome: outcome
            )
        }
        self.executeCheckpointedSlice = {
            planPayload,
            statePayload,
            workUnits,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try decodePersistentJobPayload(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            let slice = try await operation.runCheckpointedSlice(
                plan: plan,
                state: state,
                maximumWorkUnits: workUnits,
                context: context
            )
            let outcome: Slice.Outcome
            switch slice.outcome {
            case .incomplete(let nextState):
                outcome = .incomplete(
                    try encodePersistentJobPayload(
                        nextState,
                        limits: try storageLimits.stateWireLimits(
                            basedOn: limits
                        ),
                        maximum: storageLimits.maximumOperationStateBytes,
                        kind: .state
                    )
                )
            case .complete(let result):
                outcome = .complete(
                    try encodePersistentJobResult(
                        result,
                        job: job,
                        limits: try storageLimits.resultWireLimits(
                            basedOn: limits
                        ),
                        maximum: storageLimits.maximumResultBytes,
                        kind: .result
                    )
                )
            }
            return Slice(
                completedWorkUnits: slice.completedWorkUnits,
                totalWorkUnits: slice.totalWorkUnits,
                outcome: outcome
            )
        }
        self.applySuccessfulOutcome = {
            planPayload,
            statePayload,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try decodePersistentJobPayload(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await operation.applySuccessfulOutcome(
                plan: plan,
                state: state,
                context: context
            )
        }
        self.prepareUnsuccessfulOutcomeCommit = {
            planPayload,
            statePayload,
            outcome,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try decodePersistentJobPayload(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await operation.prepareUnsuccessfulOutcomeCommit(
                plan: plan,
                state: state,
                outcome: outcome,
                context: context
            )
        }
        self.applyUnsuccessfulOutcome = {
            planPayload,
            statePayload,
            outcome,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try decodePersistentJobPayload(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try decodePersistentJobPayload(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await operation.applyUnsuccessfulOutcome(
                plan: plan,
                state: state,
                outcome: outcome,
                context: context
            )
        }
    }

    func compile(
        requestPayload: ByteString,
        context: DatabaseResumableOperationStartContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws -> PreparedJob {
        try await prepareJob(
            requestPayload,
            context,
            limits,
            storageLimits
        )
    }

    func runSlice(
        planPayload: ByteString,
        statePayload: ByteString,
        maximumWorkUnits: UInt64,
        context: DatabaseResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws -> Slice {
        try await executeSlice(
            planPayload,
            statePayload,
            maximumWorkUnits,
            context,
            limits,
            storageLimits
        )
    }

    func commitModel(
        planPayload: ByteString,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) throws -> DatabaseResumableOperationCommitModel {
        try resolveCommitModel(planPayload, limits, storageLimits)
    }

    func runCheckpointedSlice(
        planPayload: ByteString,
        statePayload: ByteString,
        maximumWorkUnits: UInt64,
        context: DatabaseCheckpointedResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws -> Slice {
        try await executeCheckpointedSlice(
            planPayload,
            statePayload,
            maximumWorkUnits,
            context,
            limits,
            storageLimits
        )
    }

    func applySuccessfulOutcome(
        planPayload: ByteString,
        statePayload: ByteString,
        context: DatabaseResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws {
        try await applySuccessfulOutcome(
            planPayload,
            statePayload,
            context,
            limits,
            storageLimits
        )
    }

    func applyUnsuccessfulOutcome(
        planPayload: ByteString,
        statePayload: ByteString,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws {
        try await applyUnsuccessfulOutcome(
            planPayload,
            statePayload,
            outcome,
            context,
            limits,
            storageLimits
        )
    }

    func prepareUnsuccessfulOutcomeCommit(
        planPayload: ByteString,
        statePayload: ByteString,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseCheckpointedResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws {
        try await prepareUnsuccessfulOutcomeCommit(
            planPayload,
            statePayload,
            outcome,
            context,
            limits,
            storageLimits
        )
    }
}

private enum DatabasePersistentJobPayloadKind {
    case plan
    case state
    case result

    func limitError(actual: Int, maximum: Int) -> DatabaseJobRuntimeError {
        switch self {
        case .plan:
            return .planTooLarge(actual: actual, maximum: maximum)
        case .state:
            return .stateTooLarge(actual: actual, maximum: maximum)
        case .result:
            return .responseTooLarge(actual: actual, maximum: maximum)
        }
    }
}

private func encodePersistentJobPayload<Value: PersistentJobPayload>(
    _ value: Value,
    limits: DatabaseWireLimits,
    maximum: Int,
    kind: DatabasePersistentJobPayloadKind
) throws -> ByteString {
    do {
        return try PersistentJobPayloadStorage.encode(
            value,
            limits: limits
        )
    } catch let error as DatabaseWireError {
        let actual: Int
        switch error {
        case .frameTooLarge(let value, _),
             .stringTooLarge(let value, _),
             .byteStringTooLarge(let value, _),
             .collectionTooLarge(let value, _),
             .nestingTooDeep(let value, _),
             .objectBudgetExceeded(let value, _):
            actual = value
        default:
            throw error
        }
        throw kind.limitError(actual: actual, maximum: maximum)
    } catch {
        throw error
    }
}

private func decodePersistentJobPayload<Value: PersistentJobPayload>(
    _ type: Value.Type,
    from bytes: ByteString,
    limits: DatabaseWireLimits
) throws -> Value {
    try PersistentJobPayloadStorage.decode(
        type,
        from: bytes,
        limits: limits
    )
}

private func encodePersistentJobResult<Request, Response>(
    _ response: Response,
    job: JobOperation<Request, Response>,
    limits: DatabaseWireLimits,
    maximum: Int,
    kind: DatabasePersistentJobPayloadKind
) throws -> ByteString
where Request: Sendable, Response: Sendable {
    do {
        return try job.encodeCompletedResponse(response, limits: limits)
    } catch let error {
        let actual: Int
        switch error {
        case .frameTooLarge(let value, _),
             .stringTooLarge(let value, _),
             .byteStringTooLarge(let value, _),
             .collectionTooLarge(let value, _),
             .nestingTooDeep(let value, _),
             .objectBudgetExceeded(let value, _):
            actual = value
        default:
            throw error
        }
        throw kind.limitError(actual: actual, maximum: maximum)
    }
}
