import DatabaseKit
import DatabaseRuntime
import TestSupport
import DatabaseTypes
@testable import DatabaseEngine
import StorageKit
import Synchronization
import Testing

@Persistable
private struct TransactionLifecycleParent: Equatable {
    #Directory<TransactionLifecycleParent>("transaction_lifecycle_parents")

    var id: String = ""
    var value: String = ""
}

@Persistable
private struct TransactionLifecycleChild: Equatable {
    #Directory<TransactionLifecycleChild>("transaction_lifecycle_children")

    var id: String = ""
    var parentID: String = ""
}

@Suite("Database transaction lifecycle")
struct DatabaseTransactionLifecycleTests {
    @Test(
        "Scan continuation remains valid after nested byte framing",
        .timeLimit(.minutes(1))
    )
    func scanContinuationSurvivesNestedByteFraming() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        for identifier in ["a", "b", "c"] {
            try await context.withTransaction { transaction in
                try await transaction.save(
                    TransactionLifecycleParent(
                        id: identifier,
                        value: identifier
                    ),
                    precondition: .notExists
                )
            }
        }

        let firstContinuation = try await context.withTransaction {
            transaction in
            let page = try await transaction.scan(
                TransactionLifecycleParent.self,
                in: DirectoryPath<TransactionLifecycleParent>(),
                after: nil,
                limit: 2,
                consistency: .serializable
            )
            let identifiers = page.items.map(\.id)
            let continuation = page.continuation
            #expect(identifiers == ["a", "b"])
            return try #require(continuation).encodedBytes
        }
        let framedBytes = ByteString([0xA5])
            .appending(contentsOf: firstContinuation)
            .appending(0x5A)
        let nestedContinuation = try DatabaseScanContinuation(
            encodedBytes: framedBytes[1..<(framedBytes.count - 1)]
        )

        try await context.withTransaction { transaction in
            let page = try await transaction.scan(
                TransactionLifecycleParent.self,
                in: DirectoryPath<TransactionLifecycleParent>(),
                after: nestedContinuation,
                limit: 2,
                consistency: .serializable
            )
            let identifiers = page.items.map(\.id)
            let continuation = page.continuation
            #expect(identifiers == ["c"])
            #expect(continuation == nil)
        }
    }

    @Test(
        "External overlap is rejected while maintainer reentry completes",
        .timeLimit(.minutes(1))
    )
    func externalOverlapIsRejected() async throws {
        let suspension = MutationSuspension()
        let capture = TransactionLifecycleCapture()
        let container = try await makeContainer(
            maintainer: SuspendingPersistableMaintainer(
                suspension: suspension
            )
        )
        let context = container.newContext()
        let model = TransactionLifecycleParent(
            id: "overlap-parent",
            value: "pending"
        )

        let operation = Task {
            try await context.withTransaction { transaction in
                await capture.append(transaction: transaction)
                try await transaction.save(
                    model,
                    precondition: .notExists
                )
                return transaction
            }
        }

        await suspension.waitUntilEntered()
        let activeTransaction = try #require(
            await capture.transactions().first
        )
        await #expect(
            throws: DatabaseTransactionError.concurrentOperation
        ) {
            try await activeTransaction.fetch(
                TransactionLifecycleParent.self,
                identifiedBy: [model.id]
            )
        }

        await suspension.release()
        let escapedTransaction = try await operation.value
        await #expect(throws: DatabaseTransactionError.closed) {
            try await escapedTransaction.fetch(
                TransactionLifecycleParent.self,
                identifiedBy: [model.id]
            )
        }

        let verification = container.newContext()
        #expect(
            try await verification.model(
                for: model.id,
                as: TransactionLifecycleParent.self
            ) == model
        )
    }

    @Test(
        "Commit drains a child operation admitted by the transaction scope",
        .timeLimit(.minutes(1))
    )
    func commitDrainsAdmittedChildOperation() async throws {
        let suspension = MutationSuspension()
        let closureReturnGate = AsyncGate()
        let capture = TransactionLifecycleCapture()
        let container = try await makeContainer(
            maintainer: SuspendingPersistableMaintainer(
                suspension: suspension
            )
        )
        let context = container.newContext()
        let model = TransactionLifecycleParent(
            id: "admitted-child",
            value: "committed"
        )

        let transactionAttempt = Task {
            try await context.withTransaction { transaction in
                await capture.append(transaction: transaction)
                let childOperation = Task {
                    try await transaction.save(
                        model,
                        precondition: .notExists
                    )
                }
                await capture.append(operation: childOperation)
                await suspension.waitUntilEntered()
                await closureReturnGate.waitUntilOpen()
            }
        }

        await suspension.waitUntilEntered()
        let transaction = try #require(
            await capture.transactions().first
        )
        await closureReturnGate.open()

        var observedClosedAdmission = false
        for _ in 0..<1_000 {
            do {
                _ = try await transaction.fetch(
                    TransactionLifecycleParent.self,
                    identifiedBy: [model.id]
                )
            } catch DatabaseTransactionError.closed {
                observedClosedAdmission = true
                break
            } catch DatabaseTransactionError.concurrentOperation {
                await Task.yield()
            }
        }
        #expect(observedClosedAdmission)

        await suspension.release()
        let childOperation = try #require(
            await capture.operations().first
        )
        try await childOperation.value
        try await transactionAttempt.value

        #expect(
            try await container.newContext().model(
                for: model.id,
                as: TransactionLifecycleParent.self
            ) == model
        )
    }

    @Test(
        "Retry isolates transaction owners and derived mutations",
        .timeLimit(.minutes(1))
    )
    func retryIsolatesTransactionOwners() async throws {
        let capture = TransactionLifecycleCapture()
        let container = try await makeContainer(
            maintainer: DerivedChildMaintainer()
        )
        let context = container.newContext()
        let attempts = Mutex(0)
        let parent = TransactionLifecycleParent(
            id: "retry-parent",
            value: "committed"
        )

        try await context.withTransaction(
            configuration: TransactionConfiguration(
                maximumAttempts: 2,
                maxRetryDelay: 0,
                initialRetryDelay: 0
            )
        ) { transaction in
            await capture.append(transaction: transaction)
            try await transaction.save(
                parent,
                precondition: .notExists
            )
            let attempt = attempts.withLock { value in
                value += 1
                return value
            }
            if attempt == 1 {
                throw StorageError.transactionConflict
            }
        }

        #expect(attempts.withLock { $0 } == 2)
        let transactions = await capture.transactions()
        #expect(transactions.count == 2)
        if transactions.count == 2 {
            #expect(
                ObjectIdentifier(transactions[0])
                    != ObjectIdentifier(transactions[1])
            )
        }

        for transaction in transactions {
            await #expect(throws: DatabaseTransactionError.closed) {
                try await transaction.fetch(
                    TransactionLifecycleParent.self,
                    identifiedBy: [parent.id]
                )
            }
        }

        let verification = container.newContext()
        #expect(
            try await verification.model(
                for: parent.id,
                as: TransactionLifecycleParent.self
            ) == parent
        )
        #expect(
            try await verification.model(
                for: parent.id,
                as: TransactionLifecycleChild.self
            ) == TransactionLifecycleChild(
                id: parent.id,
                parentID: parent.id
            )
        )
    }

    @Test(
        "Failed save preserves follow-up intent and rejects rollback in flight",
        .timeLimit(.minutes(1))
    )
    func failedSavePreservesFollowupIntent() async throws {
        let suspension = MutationSuspension()
        let container = try await makeContainer(
            maintainer: FailingOncePersistableMaintainer(
                suspension: suspension
            )
        )
        let context = container.newContext()
        let initial = TransactionLifecycleParent(
            id: "followup-parent",
            value: "initial"
        )
        let latest = TransactionLifecycleParent(
            id: initial.id,
            value: "latest"
        )
        try context.insert(initial)

        let firstSave = Task {
            try await context.save()
        }
        await suspension.waitUntilEntered()

        #expect(
            try await context.model(
                for: initial.id,
                as: TransactionLifecycleParent.self
            ) == initial
        )
        try context.update(latest)
        #expect(
            try await context.model(
                for: latest.id,
                as: TransactionLifecycleParent.self
            ) == latest
        )
        #expect(throws: DatabaseContextError.rollbackDuringSaveNotAllowed) {
            try context.rollback()
        }

        await suspension.release()
        await #expect(throws: TransactionLifecycleFailure.expected) {
            try await firstSave.value
        }

        #expect(context.hasChanges)
        #expect(
            try await context.model(
                for: latest.id,
                as: TransactionLifecycleParent.self
            ) == latest
        )

        try await context.save()
        let verification = container.newContext()
        #expect(
            try await verification.model(
                for: latest.id,
                as: TransactionLifecycleParent.self
            ) == latest
        )
    }

    @Test("Deleting a missing identity is a typed failure")
    func deletingMissingIdentityFails() async throws {
        let container = try await makeContainer()
        let context = container.newContext()
        let identity = try EntityReference(
            entity: TransactionLifecycleParent.persistableType,
            id: .string("missing-parent")
        )

        do {
            try await context.withTransaction { transaction in
                try await transaction.delete(
                    TransactionLifecycleParent.self,
                    identifiedBy: "missing-parent"
                )
            }
            Issue.record("Expected a missing persisted model failure")
        } catch let error as DatabaseTransactionError {
            #expect(error == .persistedModelNotFound(identity))
        }
    }

    @Test("Commit-unknown outcome poisons every context entry point")
    func commitUnknownPoisonsContext() async throws {
        let engine = CommitOutcomeUnknownEngine()
        let container = try await makeContainer(engine: engine)
        let context = container.newContext()
        let committedModel = TransactionLifecycleParent(
            id: "ambiguous-commit",
            value: "possibly-committed"
        )
        try context.insert(committedModel)
        engine.reportNextCommitAsUnknown()

        do {
            try await context.save()
            Issue.record("Expected an ambiguous commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }

        let followupModel = TransactionLifecycleParent(
            id: "poisoned-followup",
            value: "must-not-stage"
        )
        #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try context.insert(followupModel)
        }
        await #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try await context.model(
                for: committedModel.id,
                as: TransactionLifecycleParent.self
            )
        }
        await #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try await context.withTransaction { _ in () }
        }
        #expect(throws: DatabaseContextError.commitOutcomeUnknown) {
            try context.rollback()
        }

        #expect(
            try await container.newContext().model(
                for: committedModel.id,
                as: TransactionLifecycleParent.self
            ) == committedModel
        )
    }

    private func makeContainer() async throws -> DBContainer {
        try await makeContainer(maintainers: [])
    }

    private func makeContainer(
        engine: any StorageEngine
    ) async throws -> DBContainer {
        try await makeContainer(
            engine: engine,
            maintainers: []
        )
    }

    private func makeContainer(
        maintainer: any PersistableMutationMaintainer
    ) async throws -> DBContainer {
        try await makeContainer(maintainers: [maintainer])
    }

    private func makeContainer(
        maintainers: [any PersistableMutationMaintainer]
    ) async throws -> DBContainer {
        try await makeContainer(
            engine: InMemoryEngine(),
            maintainers: maintainers
        )
    }

    private func makeContainer(
        engine: any StorageEngine,
        maintainers: [any PersistableMutationMaintainer]
    ) async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try TransactionLifecycleParent.schemaEntity,
                    try TransactionLifecycleChild.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(
                backend: .custom(engine)
            ),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                persistableMutationMaintainers: maintainers,
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(TransactionLifecycleParent.self), try DatabaseFrameworkRuntime.entity(TransactionLifecycleChild.self)]
            ),
            security: .disabled
        )
    }
}

private struct SuspendingPersistableMaintainer:
    PersistableMutationMaintainer {
    let identifier = "test.transaction.suspension"
    let suspension: MutationSuspension

    func validate(schema: Schema) throws {}

    func update(
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        guard newModel?.entity == TransactionLifecycleParent.persistableType else {
            return
        }
        await suspension.suspendUntilReleased()
    }

    func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws {}
}

private struct DerivedChildMaintainer: PersistableMutationMaintainer {
    let identifier = "test.transaction.derived-child"

    func validate(schema: Schema) throws {}

    func update(
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        guard let newModel,
              newModel.entity == TransactionLifecycleParent.persistableType else {
            return
        }
        let parent = try newModel.decode(as: TransactionLifecycleParent.self)
        try await context.save(
            TransactionLifecycleChild(
                id: parent.id,
                parentID: parent.id
            ),
            precondition: .notExists
        )
    }

    func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws {}
}

private final class FailingOncePersistableMaintainer:
    PersistableMutationMaintainer,
    Sendable {
    let identifier = "test.transaction.fail-once"

    private let suspension: MutationSuspension
    private let failurePending = Mutex(true)

    init(suspension: MutationSuspension) {
        self.suspension = suspension
    }

    func validate(schema: Schema) throws {}

    func update(
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        guard newModel?.entity == TransactionLifecycleParent.persistableType else {
            return
        }
        let shouldFail = failurePending.withLock { pending in
            guard pending else {
                return false
            }
            pending = false
            return true
        }
        guard shouldFail else {
            return
        }
        await suspension.suspendUntilReleased()
        throw TransactionLifecycleFailure.expected
    }

    func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws {}
}

private enum TransactionLifecycleFailure: Error, Sendable, Equatable {
    case expected
}

private actor MutationSuspension {
    private var entered = false
    private var enteredWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func waitUntilEntered() async {
        guard !entered else {
            return
        }
        await withCheckedContinuation { continuation in
            enteredWaiters.append(continuation)
        }
    }

    func suspendUntilReleased() async {
        entered = true
        let waiters = enteredWaiters
        enteredWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

private actor TransactionLifecycleCapture {
    private var capturedTransactions: [DatabaseTransaction] = []
    private var capturedOperations: [Task<Void, any Error>] = []

    func append(transaction: DatabaseTransaction) {
        capturedTransactions.append(transaction)
    }

    func append(operation: Task<Void, any Error>) {
        capturedOperations.append(operation)
    }

    func transactions() -> [DatabaseTransaction] {
        capturedTransactions
    }

    func operations() -> [Task<Void, any Error>] {
        capturedOperations
    }
}

private actor AsyncGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func waitUntilOpen() async {
        guard !isOpen else {
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func open() {
        isOpen = true
        let continuations = waiters
        waiters.removeAll(keepingCapacity: false)
        for continuation in continuations {
            continuation.resume()
        }
    }
}
