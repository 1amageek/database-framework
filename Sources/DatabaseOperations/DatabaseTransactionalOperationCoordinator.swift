import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit

public struct DatabaseTransactionalOperationCoordinator: Sendable {
    private enum TransactionScope: Sendable, Equatable {
        case requestTarget
        case controlMetadata
        case authorizedControlMetadata
    }

    private let stateStore: DatabaseMutationStateStore
    private let controlContainer: DBContainer
    private let runtimeLimits: DatabaseOperationLimits

    public init(
        stateStore: DatabaseMutationStateStore,
        runtimeLimits: DatabaseOperationLimits = .default
    ) {
        self.stateStore = stateStore
        self.controlContainer = stateStore.boundContainer
        self.runtimeLimits = runtimeLimits
    }

    public func execute<Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.executor.monotonicClock
        )
        return try await executeAtomically(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: .requestTarget,
            stateTarget: context.target,
            deadline: deadline,
            body: body,
            makeResponse: { value, logicalVersion in
                try makeResponse(value, logicalVersion)
            }
        )
    }

    /// Executes a control-catalog mutation while retaining the request target
    /// in its request digest. The database Grant is evaluated in the same
    /// control-domain transaction as the catalog and idempotency writes.
    public func executeControlMetadata<Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.executor.monotonicClock
        )
        return try await executeAtomically(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: .controlMetadata,
            stateTarget: .database,
            deadline: deadline,
            body: body,
            makeResponse: makeResponse
        )
    }

    /// Persists control metadata after the caller has authorized a non-control
    /// target in its own transaction domain. This is the explicit federated
    /// boundary used for Base-scoped job creation; it never substitutes a
    /// database Grant for the already-evaluated Base Grant.
    public func executeControlMetadataAfterTargetAuthorization<Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.executor.monotonicClock
        )
        return try await executeAtomically(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: .authorizedControlMetadata,
            stateTarget: .database,
            deadline: deadline,
            body: body,
            makeResponse: makeResponse
        )
    }

    /// Prepares non-transactional input only after an idempotency preflight.
    /// The prepared value is captured once and reused by every storage retry;
    /// external I/O therefore never runs inside a retryable transaction body.
    public func executeStaged<Preparation: Sendable, Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        prepare: @Sendable @escaping () async throws -> Preparation,
        body: @Sendable @escaping (
            Preparation,
            DatabaseTransaction
        ) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        try await executeStaged(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: .requestTarget,
            stateTarget: context.target,
            timeoutMilliseconds: timeoutMilliseconds,
            prepare: prepare,
            body: body,
            makeResponse: makeResponse
        )
    }

    /// Performs idempotency preflight in the control domain, prepares a
    /// target-authorized value without any control transaction being active,
    /// and then persists the control metadata. This is the federated creation
    /// boundary for Base-scoped jobs: Base validation and control persistence
    /// never overlap as an implicit cross-domain transaction.
    public func executeControlMetadataAfterTargetAuthorizationStaged<
        Preparation: Sendable,
        Value: Sendable
    >(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        prepare: @Sendable @escaping () async throws -> Preparation,
        body: @Sendable @escaping (
            Preparation,
            DatabaseTransaction
        ) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        try await executeStaged(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: .authorizedControlMetadata,
            stateTarget: .database,
            timeoutMilliseconds: timeoutMilliseconds,
            prepare: prepare,
            body: body,
            makeResponse: makeResponse
        )
    }

    private func executeStaged<Preparation: Sendable, Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        transactionScope: TransactionScope,
        stateTarget: DatabaseOperationTarget,
        timeoutMilliseconds: UInt32,
        prepare: @Sendable @escaping () async throws -> Preparation,
        body: @Sendable @escaping (
            Preparation,
            DatabaseTransaction
        ) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.executor.monotonicClock
        )
        guard context.executor.containerIdentity
                == stateStore.containerIdentity else {
            throw DatabaseMutationError.stateStoreContainerMismatch
        }
        let idempotencyKey = try validatedIdempotencyKey(
            context.metadata.idempotencyKey
        )
        let requestDigest = DatabaseRequestDigest.computeRequest(
            operation: operation,
            target: context.target,
            payload: requestPayload
        )

        if let replay = try await storedResponseIfPresent(
            operation: operation,
            idempotencyKey: idempotencyKey,
            requestDigest: requestDigest,
            context: context,
            transactionScope: transactionScope,
            stateTarget: stateTarget,
            deadline: deadline
        ) {
            return replay
        }

        let preparation = try await deadline.run(prepare)
        return try await executeAtomically(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            transactionScope: transactionScope,
            stateTarget: stateTarget,
            deadline: deadline,
            body: { transactionContext in
                try await body(preparation, transactionContext)
            },
            makeResponse: { value, logicalVersion in
                try makeResponse(value, logicalVersion)
            }
        )
    }

    private func executeAtomically<Value: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        transactionScope: TransactionScope,
        stateTarget: DatabaseOperationTarget,
        deadline: DatabaseExecutionDeadline,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> DatabaseOperationResponseEncoder
    ) async throws -> DatabaseCoordinatedOperationResponse {
        guard context.executor.containerIdentity
                == stateStore.containerIdentity else {
            throw DatabaseMutationError.stateStoreContainerMismatch
        }
        let wireLimits = context.wireLimits
        let idempotencyKey = try validatedIdempotencyKey(
            context.metadata.idempotencyKey
        )
        let requestDigest = DatabaseRequestDigest.computeRequest(
            operation: operation,
            target: context.target,
            payload: requestPayload
        )

        let configuration = TransactionConfiguration.batch
            .replacing(timeout: nil)
            .limitingMutationAggregateBytes(
                to: runtimeLimits.maximumMutationAggregateBytes
            )
        do {
            return try await withTransaction(
                context: context,
                scope: transactionScope,
                configuration: configuration,
                executionDeadline: deadline.transactionExecutionDeadline
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                if let stored = try await stateStore.idempotencyEntry(
                    for: idempotencyKey,
                    target: stateTarget,
                    transaction: transaction,
                    limits: wireLimits
                ) {
                    guard stored.operation == operation,
                          stored.requestDigest == requestDigest else {
                        throw DatabaseMutationError.idempotencyKeyConflict
                    }
                    do {
                        let successPayload = try DatabaseSuccessPayload(
                            operation: operation,
                            bytes: stored.responsePayload,
                            limits: wireLimits
                        )
                        let frame = try DatabaseWireEncoder(
                            limits: wireLimits
                        ).encodeSuccessPayload(
                            requestID: context.requestID,
                            operation: operation,
                            payload: successPayload.bytes
                        )
                        return DatabaseCoordinatedOperationResponse(
                            result: DatabaseOperationResult(
                                operation: operation,
                                requestID: context.requestID,
                                frame: frame
                            ),
                            successPayload: successPayload
                        )
                    } catch {
                        throw DatabaseMutationError.idempotencyEntryCorrupted
                    }
                }

                let value = try await body(transactionContext)
                let logicalVersion = try await stateStore.nextLogicalVersion(
                    for: stateTarget,
                    transaction: transaction
                )
                let encoder = try makeResponse(
                    value,
                    logicalVersion
                )
                let encodedResponse: DatabaseWireEncodedResponse
                do {
                    encodedResponse = try encoder.encode(
                        requestID: context.requestID,
                        limits: wireLimits
                    )
                } catch let wireError as DatabaseWireError {
                    throw DatabaseResponsePreparationError(
                        wireError: wireError
                    )
                } catch {
                    throw error
                }
                let payload: DatabaseSuccessPayload
                do {
                    payload = try DatabaseSuccessPayload(
                        operation: operation,
                        bytes: encodedResponse.payload,
                        limits: wireLimits
                    )
                } catch let wireError as DatabaseWireError {
                    throw DatabaseResponsePreparationError(
                        wireError: wireError
                    )
                } catch {
                    throw error
                }
                try stateStore.store(
                    DatabaseIdempotencyEntry(
                        operation: operation,
                        requestDigest: requestDigest,
                        responseDigest: DatabaseRequestDigest.compute(
                            operation: operation,
                            payload: payload.bytes
                        ),
                        responsePayload: payload.bytes
                    ),
                    for: idempotencyKey,
                    target: stateTarget,
                    transaction: transaction,
                    limits: wireLimits
                )
                return DatabaseCoordinatedOperationResponse(
                    result: DatabaseOperationResult(
                        operation: operation,
                        requestID: context.requestID,
                        frame: encodedResponse.frame
                    ),
                    successPayload: payload
                )
            }
        } catch let error as TransactionExecutionDeadlineExceeded
            where error.source == .inheritedOperation {
            guard let timeoutMilliseconds = UInt32(
                exactly: error.timeoutMilliseconds
            ) else {
                throw error
            }
            throw DatabaseOperationLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        }
    }

    private func validatedIdempotencyKey(_ key: String?) throws -> String {
        guard let key, !key.isEmpty else {
            throw DatabaseMutationError.idempotencyKeyRequired
        }
        let count = key.utf8.count
        guard count <= runtimeLimits.maximumIdempotencyKeyBytes else {
            throw DatabaseMutationError.idempotencyKeyTooLarge(
                actual: count,
                maximum: runtimeLimits.maximumIdempotencyKeyBytes
            )
        }
        return key
    }

    private func storedResponseIfPresent(
        operation: DatabaseOperationIdentifier,
        idempotencyKey: String,
        requestDigest: ByteString,
        context: DatabaseOperationContext,
        transactionScope: TransactionScope,
        stateTarget: DatabaseOperationTarget,
        deadline: DatabaseExecutionDeadline
    ) async throws -> DatabaseCoordinatedOperationResponse? {
        let wireLimits = context.wireLimits
        do {
            return try await withTransaction(
                context: context,
                scope: transactionScope,
                configuration: .readOnly.replacing(timeout: nil),
                executionDeadline: deadline.transactionExecutionDeadline
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                guard let stored = try await stateStore.idempotencyEntry(
                    for: idempotencyKey,
                    target: stateTarget,
                    transaction: transaction,
                    limits: wireLimits
                ) else {
                    return nil
                }
                guard stored.operation == operation,
                      stored.requestDigest == requestDigest else {
                    throw DatabaseMutationError.idempotencyKeyConflict
                }
                do {
                    let successPayload = try DatabaseSuccessPayload(
                        operation: operation,
                        bytes: stored.responsePayload,
                        limits: wireLimits
                    )
                    let frame = try DatabaseWireEncoder(
                        limits: wireLimits
                    ).encodeSuccessPayload(
                        requestID: context.requestID,
                        operation: operation,
                        payload: successPayload.bytes
                    )
                    return DatabaseCoordinatedOperationResponse(
                        result: DatabaseOperationResult(
                            operation: operation,
                            requestID: context.requestID,
                            frame: frame
                        ),
                        successPayload: successPayload
                    )
                } catch {
                    throw DatabaseMutationError.idempotencyEntryCorrupted
                }
            }
        } catch let error as TransactionExecutionDeadlineExceeded
            where error.source == .inheritedOperation {
            guard let timeoutMilliseconds = UInt32(
                exactly: error.timeoutMilliseconds
            ) else {
                throw error
            }
            throw DatabaseOperationLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        }
    }

    private func withTransaction<Result: Sendable>(
        context: DatabaseOperationContext,
        scope: TransactionScope,
        configuration: TransactionConfiguration,
        executionDeadline: TransactionExecutionDeadline?,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        if scope == .controlMetadata {
            return try await context.requireControlExecutor().withTransaction(
                requiredAccess: .administer,
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
        }
        if scope == .authorizedControlMetadata {
            return try await controlContainer.withControlMetadataTransaction(
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
        }
        switch context.target {
        case .database:
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            return try await context.requireControlExecutor().withTransaction(
                requiredAccess: context.requirement.access,
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
            #else
            return try await context.requireDataExecutor().withDataTransaction(
                requiredAccess: context.requirement.access,
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
            #endif
        case .base:
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            if context.requirement.baseAdmission == .administration
                || context.requirement.baseAdmission == .lifecycleJob {
                return try await context.requireBaseExecutor()
                    .withAdministrationTransaction(
                        requiredAccess: context.requirement.access,
                        configuration: configuration,
                        executionDeadline: executionDeadline,
                        operation
                    )
            }
            return try await context.requireDataContext().withTransaction(
                requiredAccess: context.requirement.access,
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
            #else
            throw DatabaseOperationError.targetKindNotAccepted(context.target)
            #endif
        case .composition:
            throw DatabaseOperationError.targetKindNotAccepted(context.target)
        }
    }
}
