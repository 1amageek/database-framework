import DatabaseValue
import DatabaseWire

public struct AnyDatabaseResumableOperation: Sendable {
    struct PreparedJob: Sendable {
        let planPayload: DatabaseBytes
        let initialStatePayload: DatabaseBytes
        let sliceTimeoutMilliseconds: UInt32
    }

    struct Slice: Sendable {
        enum Outcome: Sendable {
            case incomplete(DatabaseBytes)
            case complete(DatabaseBytes)
        }

        let completedWorkUnits: UInt64
        let totalWorkUnits: UInt64?
        let outcome: Outcome
    }

    public let operation: DatabaseJobOperationIdentifier

    private let prepareJob: @Sendable (
        DatabaseBytes,
        DatabaseResumableOperationStartContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> PreparedJob
    private let resolveCommitModel: @Sendable (
        DatabaseBytes,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) throws -> DatabaseResumableOperationCommitModel
    private let executeSlice: @Sendable (
        DatabaseBytes,
        DatabaseBytes,
        UInt64,
        DatabaseResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Slice
    private let executeCheckpointedSlice: @Sendable (
        DatabaseBytes,
        DatabaseBytes,
        UInt64,
        DatabaseCheckpointedResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Slice
    private let handleTerminalState: @Sendable (
        DatabaseBytes,
        DatabaseBytes,
        DatabaseResumableOperationTerminalState,
        DatabaseResumableOperationContext,
        DatabaseWireLimits,
        DatabasePersistentJobStorageLimits
    ) async throws -> Void

    public init<Operation: DatabaseResumableOperation>(
        _ operation: Operation
    ) throws {
        self.operation = try Operation.Job.jobOperationIdentifier()
        self.prepareJob = { payload, context, limits, storageLimits in
            let request = try DatabaseEnvelopeCodec.decode(
                Operation.Job.Request.self,
                from: payload,
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
                plan = try DatabaseEnvelopeCodec.decode(
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
                plan = try DatabaseEnvelopeCodec.decode(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try DatabaseEnvelopeCodec.decode(
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
                    try encodePersistentJobPayload(
                        result,
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
                plan = try DatabaseEnvelopeCodec.decode(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try DatabaseEnvelopeCodec.decode(
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
                    try encodePersistentJobPayload(
                        result,
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
        self.handleTerminalState = {
            planPayload,
            statePayload,
            terminalState,
            context,
            limits,
            storageLimits in
            let plan: Operation.Plan
            do {
                plan = try DatabaseEnvelopeCodec.decode(
                    Operation.Plan.self,
                    from: planPayload,
                    limits: try storageLimits.planWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            let state: Operation.State
            do {
                state = try DatabaseEnvelopeCodec.decode(
                    Operation.State.self,
                    from: statePayload,
                    limits: try storageLimits.stateWireLimits(basedOn: limits)
                )
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            try await operation.handleTerminalState(
                plan: plan,
                state: state,
                terminalState: terminalState,
                context: context
            )
        }
    }

    func compile(
        requestPayload: DatabaseBytes,
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
        planPayload: DatabaseBytes,
        statePayload: DatabaseBytes,
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
        planPayload: DatabaseBytes,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) throws -> DatabaseResumableOperationCommitModel {
        try resolveCommitModel(planPayload, limits, storageLimits)
    }

    func runCheckpointedSlice(
        planPayload: DatabaseBytes,
        statePayload: DatabaseBytes,
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

    func handleTerminalState(
        planPayload: DatabaseBytes,
        statePayload: DatabaseBytes,
        state: DatabaseResumableOperationTerminalState,
        context: DatabaseResumableOperationContext,
        limits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws {
        try await handleTerminalState(
            planPayload,
            statePayload,
            state,
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

private func encodePersistentJobPayload<Value: DatabaseWireValue>(
    _ value: Value,
    limits: DatabaseWireLimits,
    maximum: Int,
    kind: DatabasePersistentJobPayloadKind
) throws -> DatabaseBytes {
    do {
        return try DatabaseEnvelopeCodec.encode(value, limits: limits)
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
