import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public struct DatabaseTransactionalOperationCoordinator: Sendable {
    private let stateStore: DatabaseMutationStateStore
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits

    public init(
        stateStore: DatabaseMutationStateStore,
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.stateStore = stateStore
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
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
            clock: context.container.monotonicClock
        )
        let executed = try await executePrepared(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            deadline: deadline,
            body: body,
            decodeStoredResponse: { _ in () },
            makeResponse: { value, logicalVersion in
                (try makeResponse(value, logicalVersion), ())
            }
        )
        return executed.coordinated
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
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.container.monotonicClock
        )
        try stateStore.validate(container: context.container)
        let idempotencyKey = try validatedIdempotencyKey(
            context.metadata.idempotencyKey
        )
        let requestDigest = DatabaseRequestDigest.compute(
            operation: operation,
            payload: requestPayload
        )

        if let replay = try await storedResponseIfPresent(
            operation: operation,
            idempotencyKey: idempotencyKey,
            requestDigest: requestDigest,
            context: context,
            deadline: deadline
        ) {
            return replay
        }

        let preparation = try await deadline.run(prepare)
        let executed = try await executePrepared(
            operation: operation,
            requestPayload: requestPayload,
            context: context,
            deadline: deadline,
            body: { transactionContext in
                try await body(preparation, transactionContext)
            },
            decodeStoredResponse: { _ in () },
            makeResponse: { value, logicalVersion in
                (try makeResponse(value, logicalVersion), ())
            }
        )
        return executed.coordinated
    }

    private func executePrepared<Value: Sendable, Prepared: Sendable>(
        operation: DatabaseOperationIdentifier,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        deadline: DatabaseExecutionDeadline,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        decodeStoredResponse: @Sendable @escaping (
            ByteString
        ) throws -> Prepared,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> (DatabaseOperationResponseEncoder, Prepared)
    ) async throws -> (
        coordinated: DatabaseCoordinatedOperationResponse,
        prepared: Prepared
    ) {
        try stateStore.validate(container: context.container)
        let idempotencyKey = try validatedIdempotencyKey(
            context.metadata.idempotencyKey
        )
        let requestDigest = DatabaseRequestDigest.compute(
            operation: operation,
            payload: requestPayload
        )

        let databaseContext = context.container.newContext()
        let configuration = TransactionConfiguration.batch
            .replacing(timeout: nil)
            .limitingMutationAggregateBytes(
                to: runtimeLimits.maximumMutationAggregateBytes
            )
        do {
            return try await databaseContext.withTransaction(
                configuration: configuration,
                executionDeadline: deadline.transactionExecutionDeadline
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                if let stored = try await stateStore.idempotencyEntry(
                    for: idempotencyKey,
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
                        let prepared = try decodeStoredResponse(
                            successPayload.bytes
                        )
                        return (
                            DatabaseCoordinatedOperationResponse(
                                result: DatabaseOperationResult(
                                    operation: operation,
                                    requestID: context.requestID,
                                    frame: frame
                                ),
                                successPayload: successPayload
                            ),
                            prepared
                        )
                    } catch {
                        throw DatabaseMutationError.idempotencyEntryCorrupted
                    }
                }

                let value = try await body(transactionContext)
                let logicalVersion = try await stateStore.nextLogicalVersion(
                    transaction: transaction
                )
                let (encoder, prepared) = try makeResponse(
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
                    transaction: transaction,
                    limits: wireLimits
                )
                return (
                    DatabaseCoordinatedOperationResponse(
                        result: DatabaseOperationResult(
                            operation: operation,
                            requestID: context.requestID,
                            frame: encodedResponse.frame
                        ),
                        successPayload: payload
                    ),
                    prepared
                )
            }
        } catch let error as TransactionExecutionDeadlineExceeded
            where error.source == .inheritedOperation {
            guard let timeoutMilliseconds = UInt32(
                exactly: error.timeoutMilliseconds
            ) else {
                throw error
            }
            throw DatabaseRuntimeLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        }
    }

    public func execute<
        Operation: ServerOperationDeclaration,
        Value: Sendable
    >(
        _ operation: Operation.Type,
        requestPayload: ByteString,
        context: DatabaseOperationContext,
        timeoutMilliseconds: UInt32,
        body: @Sendable @escaping (DatabaseTransaction) async throws -> Value,
        makeResponse: @Sendable @escaping (
            Value,
            UInt64
        ) throws -> Operation.Response
    ) async throws -> DatabasePreparedOperationResponse<Operation> {
        let deadline = DatabaseExecutionDeadline(
            timeoutMilliseconds: timeoutMilliseconds,
            clock: context.container.monotonicClock
        )
        let executed = try await executePrepared(
            operation: Operation.operation.identifier,
            requestPayload: requestPayload,
            context: context,
            deadline: deadline,
            body: body,
            decodeStoredResponse: { bytes in
                try DatabaseWireDecoder(
                    limits: wireLimits
                ).decodeResponsePayload(
                    Operation.operation,
                    from: bytes,
                )
            },
            makeResponse: { value, logicalVersion in
                let response = try makeResponse(value, logicalVersion)
                return (
                    DatabaseOperationResponseEncoder(
                        Operation.self,
                        response: response
                    ),
                    response
                )
            }
        )
        return DatabasePreparedOperationResponse(
            response: executed.prepared,
            operationResult: executed.coordinated.result
        )
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
        deadline: DatabaseExecutionDeadline
    ) async throws -> DatabaseCoordinatedOperationResponse? {
        let databaseContext = context.container.newContext()
        do {
            return try await databaseContext.withTransaction(
                configuration: .readOnly.replacing(timeout: nil),
                executionDeadline: deadline.transactionExecutionDeadline
            ) { transactionContext in
                let transaction = transactionContext.storageAccess
                guard let stored = try await stateStore.idempotencyEntry(
                    for: idempotencyKey,
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
            throw DatabaseRuntimeLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        }
    }
}
