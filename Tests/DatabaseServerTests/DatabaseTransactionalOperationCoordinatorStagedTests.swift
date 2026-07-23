import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseValue
import DatabaseWire
import StorageKit
import Synchronization
import Testing
@testable import DatabaseServer

@Suite("Staged transactional operation coordinator")
struct DatabaseTransactionalOperationCoordinatorStagedTests {
    @Test("Preparation runs once and an idempotent replay skips it")
    func replaySkipsPreparation() async throws {
        let mutationContext = try await CoordinatedMutationContext()
        let preparations = Counter()
        let bodies = Counter()

        _ = try await mutationContext.execute(
            payload: [1],
            requestID: 1,
            prepare: {
                preparations.increment()
                return 7
            },
            body: { value, _ in
                bodies.increment()
                return value
            }
        )
        _ = try await mutationContext.execute(
            payload: [1],
            requestID: 2,
            prepare: {
                preparations.increment()
                return 99
            },
            body: { value, _ in
                bodies.increment()
                return value
            }
        )

        #expect(preparations.value == 1)
        #expect(bodies.value == 1)
    }

    @Test("A conflicting idempotency payload is rejected before preparation")
    func conflictPrecedesPreparation() async throws {
        let mutationContext = try await CoordinatedMutationContext()
        let preparations = Counter()

        _ = try await mutationContext.execute(
            payload: [1],
            requestID: 1,
            prepare: {
                preparations.increment()
                return 1
            },
            body: { value, _ in value }
        )

        do {
            _ = try await mutationContext.execute(
                payload: [2],
                requestID: 2,
                prepare: {
                    preparations.increment()
                    return 2
                },
                body: { value, _ in value }
            )
            Issue.record("Expected an idempotency conflict")
        } catch DatabaseMutationError.idempotencyKeyConflict {
            #expect(preparations.value == 1)
        }
    }

    @Test("Preparation failure leaves no mutation or idempotency state")
    func preparationFailureDoesNotCommit() async throws {
        let mutationContext = try await CoordinatedMutationContext()
        let bodies = Counter()

        do {
            _ = try await mutationContext.execute(
                payload: [3],
                requestID: 1,
                prepare: { () async throws -> Int in
                    throw PreparationFailure.rejected
                },
                body: { value, _ in
                    bodies.increment()
                    return value
                }
            )
            Issue.record("Expected preparation to fail")
        } catch PreparationFailure.rejected {
            #expect(bodies.value == 0)
        }

        let state = try await mutationContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            (
                try await mutationContext.stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await mutationContext.stateStore.idempotencyRecord(
                    for: CoordinatedMutationContext.idempotencyKey,
                    transaction: transaction,
                    limits: .default
                )
            )
        }
        #expect(state.0 == 0)
        #expect(state.1 == nil)
    }

    @Test("Aggregate mutation overflow rolls back body, version, and replay state")
    func aggregateMutationOverflowRollsBackEntireOperation() async throws {
        let mutationContext = try await CoordinatedMutationContext(
            maximumMutationAggregateBytes: 64
        )
        let key: Bytes = [0xF0]

        do {
            _ = try await mutationContext.execute(
                payload: [4],
                requestID: 1,
                prepare: { 1 },
                body: { value, context in
                    try context.rawTransaction.setValue(
                        Bytes(repeating: 0, count: 64),
                        for: key
                    )
                    return value
                }
            )
            Issue.record("Expected aggregate mutation byte rejection")
        } catch let error as TransactionMutationByteLimitError {
            guard case .exceeded(let actual, let maximum) = error else {
                Issue.record("Expected an aggregate overflow")
                return
            }
            #expect(actual == 82)
            #expect(maximum == 64)
        }

        let state = try await mutationContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            (
                try await transaction.getValue(for: key),
                try await mutationContext.stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await mutationContext.stateStore.idempotencyRecord(
                    for: CoordinatedMutationContext.idempotencyKey,
                    transaction: transaction,
                    limits: .default
                )
            )
        }
        #expect(state.0 == nil)
        #expect(state.1 == 0)
        #expect(state.2 == nil)
    }

    @Test("The request deadline covers the complete transaction body")
    func transactionBodyUsesOriginalDeadline() async throws {
        let mutationContext = try await CoordinatedMutationContext()

        do {
            _ = try await mutationContext.execute(
                payload: [5],
                requestID: 1,
                timeoutMilliseconds: 1,
                prepare: { 1 },
                body: { value, context in
                    try await ContinuousClock().sleep(for: .milliseconds(100))
                    try context.rawTransaction.setValue(
                        [UInt8(value)],
                        for: [0xF1]
                    )
                    return value
                }
            )
            Issue.record("Expected the transaction body to time out")
        } catch DatabaseRuntimeLimitError.executionTimedOut(
            let timeoutMilliseconds
        ) {
            #expect(timeoutMilliseconds == 1)
        }

        let state = try await mutationContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            (
                try await transaction.getValue(for: [0xF1]),
                try await mutationContext.stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await mutationContext.stateStore.idempotencyRecord(
                    for: CoordinatedMutationContext.idempotencyKey,
                    transaction: transaction,
                    limits: .default
                )
            )
        }
        #expect(state.0 == nil)
        #expect(state.1 == 0)
        #expect(state.2 == nil)
    }

    @Test(
        "A deadline after commit dispatch preserves authoritative success",
        .timeLimit(.minutes(1))
    )
    func deadlineAfterCommitDispatchPreservesSuccess() async throws {
        let engine = CommitGatedInMemoryEngine()
        let mutationContext = try await CoordinatedMutationContext(engine: engine)
        let bodies = Counter()
        let commitGate = engine.suspendNextMutatingCommit()

        let operation = Task {
            try await mutationContext.execute(
                payload: [6],
                requestID: 1,
                timeoutMilliseconds: 1_000,
                prepare: { 1 },
                body: { value, _ in
                    bodies.increment()
                    return value
                }
            )
        }
        await commitGate.waitUntilStarted()
        do {
            try await ContinuousClock().sleep(for: .milliseconds(1_100))
        } catch {
            await commitGate.release()
            operation.cancel()
            _ = await operation.result
            throw error
        }
        await commitGate.release()
        _ = try await operation.value

        let committedState = try await mutationContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            (
                try await mutationContext.stateStore.currentLogicalVersion(
                    transaction: transaction
                ),
                try await mutationContext.stateStore.idempotencyRecord(
                    for: CoordinatedMutationContext.idempotencyKey,
                    transaction: transaction,
                    limits: .default
                )
            )
        }
        #expect(committedState.0 == 1)
        #expect(committedState.1 != nil)

        _ = try await mutationContext.execute(
            payload: [6],
            requestID: 2,
            timeoutMilliseconds: 5_000,
            prepare: { 2 },
            body: { value, _ in
                bodies.increment()
                return value
            }
        )

        #expect(bodies.value == 1)
    }
}

private extension DatabaseTransactionalOperationCoordinatorStagedTests {
    enum PreparationFailure: Error {
        case rejected
    }

    final class Counter: Sendable {
        private let storage = Mutex(0)

        var value: Int {
            storage.withLock { $0 }
        }

        func increment() {
            storage.withLock { value in
                value += 1
            }
        }
    }

    struct CoordinatedMutationContext {
        static let idempotencyKey = "staged-operation"

        let container: DBContainer
        let stateStore: DatabaseMutationStateStore
        let coordinator: DatabaseTransactionalOperationCoordinator

        init(
            maximumMutationAggregateBytes: Int = 8 * 1_024 * 1_024,
            engine: any StorageEngine = InMemoryEngine()
        ) async throws {
            let container = try await DBContainer(
                for: Schema(
                    [DatabaseEndpointRecord.self],
                    version: Schema.Version(1, 0, 0)
                ),
                configuration: DBConfiguration(
                    backend: .custom(engine)
                ),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
                security: .disabled
            )
            let stateStore = try await DatabaseMutationStateStore(
                container: container
            )
            self.container = container
            self.stateStore = stateStore
            self.coordinator = DatabaseTransactionalOperationCoordinator(
                stateStore: stateStore,
                runtimeLimits: try DatabaseRuntimeLimits(
                    maximumRows: 10_000,
                    maximumWorkUnits: 1_000_000,
                    maximumTimeoutMilliseconds: 30_000,
                    maximumMutationAggregateBytes: maximumMutationAggregateBytes
                )
            )
        }

        func execute<Preparation: Sendable>(
            payload: DatabaseBytes,
            requestID: UInt64,
            timeoutMilliseconds: UInt32 = 5_000,
            prepare: @Sendable @escaping () async throws -> Preparation,
            body: @Sendable @escaping (
                Preparation,
                TransactionContext
            ) async throws -> Int
        ) async throws -> DatabaseCoordinatedOperationResponse {
            try await coordinator.executeStaged(
                operation: .mutationExecute,
                requestPayload: payload,
                context: DatabaseOperationContext(
                    container: container,
                    requestID: requestID,
                    metadata: DatabaseRequestMetadata(
                        idempotencyKey: Self.idempotencyKey
                    ),
                    requestPayload: payload
                ),
                timeoutMilliseconds: timeoutMilliseconds,
                prepare: prepare,
                body: body,
                makeResponse: { value, commitVersion in
                    DatabaseOperationResponseEncoder(
                        MutationExecuteOperation.Response(
                            commitVersion: commitVersion,
                            result: .rdf(
                                MutationExecuteOperation.RDFEffect(
                                    insertedQuads: UInt64(value)
                                )
                            )
                        )
                    )
                }
            )
        }
    }
}
