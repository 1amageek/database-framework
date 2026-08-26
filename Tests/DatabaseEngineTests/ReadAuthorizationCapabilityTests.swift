import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import ScalarIndex
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

private enum ReadCapabilityProbeError: Error, Equatable, Sendable {
    case observedReadOnlyCapability
    case recoveredTransactionCapability
    case didNotObserveCallerSnapshot
    case missingAuthorizationEvidence
}

private let readCapabilityProbeKey = ByteString(
    utf8: "read-capability-probe-snapshot"
)
private let readCapabilityProbeValue = ByteString([0x01])

private final class SynchronizedValue<Value: Sendable>: Sendable {
    private let storage: Mutex<Value>

    init(_ value: Value) {
        storage = Mutex(value)
    }

    func withLock<Result: ~Copyable, Failure: Error>(
        _ operation: (inout sending Value) throws(Failure) -> sending Result
    ) throws(Failure) -> sending Result {
        try storage.withLock(operation)
    }

    func value() -> Value {
        storage.withLock { $0 }
    }
}

private final class HistoricalReadPositionProbeTransaction:
    TransactionAccess,
    Sendable
{
    private let domain = StorageTransactionDomain()
    private let readVersion = Mutex<Int64>(0)
    private let captureOffset: Int64

    init(captureOffset: Int64 = 0) {
        self.captureOffset = captureOffset
    }

    var transactionDomain: StorageTransactionDomain { domain }

    var capabilities: TransactionCapabilities {
        TransactionCapabilities(
            historicalReadVersion: true,
            readVersion: true
        )
    }

    var compaction: StorageCompactionAccess? { nil }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? { nil }

    func getValue(for key: ByteString) async throws -> ByteString? { nil }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? { nil }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(validatingScope: {})
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        throw unsupportedMutation()
    }

    func clear(key: ByteString) throws {
        throw unsupportedMutation()
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        throw unsupportedMutation()
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        throw unsupportedMutation()
    }

    func setReadVersion(_ version: Int64) throws {
        readVersion.withLock { $0 = version }
    }

    func getReadVersion() async throws -> Int64 {
        let version = readVersion.withLock { $0 }
        let result = version.addingReportingOverflow(captureOffset)
        guard !result.overflow else {
            throw StorageError.invalidOperation(
                "The read-position probe overflowed"
            )
        }
        return result.partialValue
    }

    private func unsupportedMutation() -> StorageError {
        StorageError.invalidOperation(
            "The read-position probe does not support mutations"
        )
    }
}

@Persistable(type: "ReadPolicyAuthorizationAnchor")
private struct ReadPolicyAuthorizationAnchor: SecurityPolicy {
    var id: String = ""

    static func permitsRead(
        of resource: borrowing ReadPolicyAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "test-runner"
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "test-runner"
    }

    static func permitsCreate(
        _ newResource: borrowing ReadPolicyAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing ReadPolicyAuthorizationAnchor,
        to newResource: borrowing ReadPolicyAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing ReadPolicyAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Persistable(type: "ReadPolicyAuthorizationAnchor")
private struct ReadPolicyDenyingAuthorizationAnchor: SecurityPolicy {
    var id: String = ""

    static func permitsRead(
        of resource: borrowing ReadPolicyDenyingAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { false }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { false }

    static func permitsCreate(
        _ newResource: borrowing ReadPolicyDenyingAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing ReadPolicyDenyingAuthorizationAnchor,
        to newResource: borrowing ReadPolicyDenyingAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing ReadPolicyDenyingAuthorizationAnchor,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Polymorphable
@PolymorphicDirectory("read-authorization-polymorphic-probe")
private protocol ReadAuthorizationPolymorphicProbe:
    Polymorphable<ReadAuthorizationPolymorphicProbePolymorphicGroup>
{
    var id: String { get }
}

@Persistable
private struct ReadAuthorizationPolymorphicMember:
    ReadAuthorizationPolymorphicProbe,
    SecurityPolicy
{
    #Directory<ReadAuthorizationPolymorphicMember>(
        "read-authorization-polymorphic-member"
    )

    @Restricted(read: .roles(["polymorphic-secret-reader"]))
    var id: String = ""
    var title: String = ""

    static func permitsRead(
        of resource: borrowing ReadAuthorizationPolymorphicMember,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier != "polymorphic-list-denied"
    }

    static func permitsCreate(
        _ newResource: borrowing ReadAuthorizationPolymorphicMember,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing ReadAuthorizationPolymorphicMember,
        to newResource: borrowing ReadAuthorizationPolymorphicMember,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing ReadAuthorizationPolymorphicMember,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Persistable
private struct ReadIndexAuthorizationProbe: SecurityPolicy {
    #Directory<ReadIndexAuthorizationProbe>(
        "read-index-authorization-probe"
    )
    #Index(
        .ordered(
            name: "read_index_authorization_probe_id",
            keys: [.ascending(\ReadIndexAuthorizationProbe.id)],
            unique: true
        )
    )

    @Restricted(read: .roles(["index-reader"]))
    var id: String = ""

    static func permitsRead(
        of resource: borrowing ReadIndexAuthorizationProbe,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsCreate(
        _ newResource: borrowing ReadIndexAuthorizationProbe,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing ReadIndexAuthorizationProbe,
        to newResource: borrowing ReadIndexAuthorizationProbe,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing ReadIndexAuthorizationProbe,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

private struct ReadCapabilityProbeGraphTableExecutor:
    GraphTableSourceExecutor
{
    func executeInTransaction(
        session: DatabaseReadSession,
        graphTableSource: GraphTableSource,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        if session.transaction is any TransactionAccess {
            throw ReadCapabilityProbeError.recoveredTransactionCapability
        }
        guard try await session.transaction.getValue(
            for: readCapabilityProbeKey,
            snapshot: false
        ) == readCapabilityProbeValue else {
            throw ReadCapabilityProbeError.didNotObserveCallerSnapshot
        }
        throw ReadCapabilityProbeError.observedReadOnlyCapability
    }
}

private struct ReadCapabilityProbeSPARQLExecutor: SPARQLSourceExecutor {
    let expectedSnapshot: SynchronizedValue<DatabaseReadTransaction?>?

    init(
        expectedSnapshot: SynchronizedValue<DatabaseReadTransaction?>? = nil
    ) {
        self.expectedSnapshot = expectedSnapshot
    }

    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        try await observe(session)
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        try await observe(session)
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try await observe(session)
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try await observe(session)
    }

    private func observe<Result: ~Copyable>(
        _ session: DatabaseReadSession
    ) async throws -> Result {
        if session.transaction is any TransactionAccess {
            throw ReadCapabilityProbeError.recoveredTransactionCapability
        }
        guard session.transaction.authorization != nil else {
            throw ReadCapabilityProbeError.missingAuthorizationEvidence
        }
        if let expectedSnapshot {
            guard let expected = expectedSnapshot.value(),
                  session.transaction.sharesSnapshot(with: expected) else {
                throw ReadCapabilityProbeError.didNotObserveCallerSnapshot
            }
        }
        guard try await session.transaction.getValue(
            for: readCapabilityProbeKey,
            snapshot: false
        ) == readCapabilityProbeValue else {
            throw ReadCapabilityProbeError.didNotObserveCallerSnapshot
        }
        throw ReadCapabilityProbeError.observedReadOnlyCapability
    }
}

private struct AdmittedOperationSPARQLExecutor: SPARQLSourceExecutor {
    let barrier: StorageOperationBarrier
    let cursorBarrier: StorageOperationBarrier

    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the admitted-operation probe"
        )
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        await barrier.enterAndWait()
        try session.requireRDFDatasetReadAuthorization()
        guard try await session.transaction.getValue(
            for: readCapabilityProbeKey,
            snapshot: false
        ) == readCapabilityProbeValue else {
            return false
        }
        var cursor = session.transaction.rangeCursor(
            from: .firstGreaterOrEqual(readCapabilityProbeKey),
            to: .firstGreaterOrEqual(ByteString([0xFF])),
            limit: 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        await cursorBarrier.enterAndWait()
        guard let row = try await cursor.next() else { return false }
        return row.0 == readCapabilityProbeKey
            && row.1 == readCapabilityProbeValue
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the admitted-operation probe"
        )
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the admitted-operation probe"
        )
    }
}

private struct BorrowedOperationSPARQLExecutor: SPARQLSourceExecutor {
    let readBarrier: StorageOperationBarrier
    let readTask: SynchronizedValue<Task<ByteString?, any Error>?>

    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the borrowed-operation probe"
        )
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        let task = Task {
            try await session.transaction.getValue(
                for: readCapabilityProbeKey,
                snapshot: false
            )
        }
        readTask.withLock { $0 = task }
        await readBarrier.waitUntilEntered()
        return true
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the borrowed-operation probe"
        )
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only ASK is used by the borrowed-operation probe"
        )
    }
}

@Suite("Read authorization capability")
struct ReadAuthorizationCapabilityTests {
    @Persistable
    struct Anchor {
        #Directory<Anchor>("read-authorization-capability", "anchors")
        #Index(
            .ordered(
                name: "read_authorization_anchor_id",
                keys: [.ascending(\Anchor.id)],
                unique: true
            )
        )

        var id: String = ""
    }

    @Persistable
    struct LogicalSourceAnchor {
        #Directory<LogicalSourceAnchor>(
            "read-authorization-capability",
            "logical-source-anchors"
        )

        var id: String = ""
    }

    @Test("Cursor drain eligibility follows the root operation lifetime")
    func cursorDrainEligibilityFollowsRootOperationLifetime() throws {
        let identityOwner = SynchronizedValue(())
        let rootEndCount = SynchronizedValue(0)
        let operationEndCount = SynchronizedValue(0)
        let scopeIdentity = ObjectIdentifier(identityOwner)
        let root = DatabaseReadScopeOperationLease(
            scopeIdentity: scopeIdentity,
            rootDidEnd: {
                rootEndCount.withLock { $0 += 1 }
            },
            endOperation: {
                operationEndCount.withLock { $0 += 1 }
            }
        )

        let completedBorrow = try root.borrowed(by: scopeIdentity)
        let completedBorrowEligibility = DatabaseReadCursorDrainEligibility(
            parentOperation: completedBorrow
        )
        #expect(root.isRootActive)
        #expect(!completedBorrowEligibility.allowsInvalidation)

        completedBorrow.end()
        #expect(root.isRootActive)
        #expect(!completedBorrowEligibility.allowsInvalidation)
        #expect(rootEndCount.value() == 0)
        #expect(operationEndCount.value() == 0)

        let finalBorrow = try root.borrowed(by: scopeIdentity)
        let finalBorrowEligibility = DatabaseReadCursorDrainEligibility(
            parentOperation: finalBorrow
        )
        root.end()
        #expect(!root.isRootActive)
        #expect(finalBorrowEligibility.allowsInvalidation)
        #expect(rootEndCount.value() == 1)
        #expect(operationEndCount.value() == 0)

        root.end()
        finalBorrow.end()
        finalBorrow.end()
        #expect(rootEndCount.value() == 1)
        #expect(operationEndCount.value() == 1)
    }

    @Test("Read transactions cannot recover mutation capability by casting")
    func readTransactionCannotRecoverMutationCapability() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let key = ByteString(utf8: "read-authorization-capability")

        try await context.indexQueryContext.withTransaction { transaction in
            let exposedAccess: any TransactionReadAccess = transaction
            #expect(exposedAccess as? any TransactionAccess == nil)
            let value = try await transaction.getValue(
                for: key,
                snapshot: true
            )
            #expect(value == nil)
        }
    }

    @Test("Read transactions forward the caller's bounded point-read maximum")
    func readTransactionForwardsBoundedPointReadMaximum() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let key = ByteString(utf8: "bounded-read-forwarding")

        try await context.indexQueryContext.withTransaction { transaction in
            let value = try await transaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: 7
            )
            #expect(value == nil)
        }

        #expect(control.boundedValueReadMaximums == [7])
    }

    @Test("Read snapshot metadata does not expose configuration authority")
    func readSnapshotMetadataPreservesAttenuation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )

        let position = try await context.withReadSnapshot(
            workMeter: meter
        ) { snapshot in
            #expect(
                snapshot.transaction as? any TransactionAccess == nil
            )
            #expect(snapshot.supportsPositionRestoration == false)
            guard case .version = snapshot.position else {
                Issue.record("InMemoryEngine must expose its read version")
                return snapshot.position
            }
            return snapshot.position
        }

        await #expect(
            throws: DatabaseReadPositionError.positionIsNotRestorable
        ) {
            _ = try await context.withReadSnapshot(
                restoring: position,
                workMeter: meter
            ) { _ in () }
        }
    }

    @Test("Historical read selection restores and verifies the exact version")
    func historicalReadSelectionRestoresExactVersion() async throws {
        let rawTransaction = HistoricalReadPositionProbeTransaction()
        let admitted = ReadAuthorizedTransactionAccess.admittedReadAccess(
            rawTransaction
        )

        let position = try await admitted.selectReadPosition(
            restoring: .version(42)
        )

        #expect(position == .version(42))
    }

    @Test("Historical read selection rejects a changed captured version")
    func historicalReadSelectionRejectsChangedVersion() async {
        let rawTransaction = HistoricalReadPositionProbeTransaction(
            captureOffset: 1
        )
        let admitted = ReadAuthorizedTransactionAccess.admittedReadAccess(
            rawTransaction
        )

        await #expect(
            throws: DatabaseReadPositionError.restoredPositionChanged(
                expected: .version(42),
                actual: .version(43)
            )
        ) {
            _ = try await admitted.selectReadPosition(
                restoring: .version(42)
            )
        }
    }

    @Test("Escaped read transaction is revoked after its public scope")
    func escapedReadTransactionIsRevoked() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        let transaction = try await context.indexQueryContext.withTransaction {
            transaction in
            transaction
        }

        await #expect(throws: DatabaseTransactionError.invalidOperationContext) {
            _ = try await transaction.getValue(
                for: ByteString(utf8: "revoked-read-capability"),
                snapshot: true
            )
        }
    }

    @Test("Escaped cursor is closed and cannot outlive its public scope")
    func escapedCursorIsClosedAndRevoked() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let rowKey = ByteString([0x80])
        let rowValue = ByteString([0x01])
        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(rowValue, for: rowKey)
        }
        let openedBefore = control.openedRangeCursorCount
        let finishedBefore = control.finishedRangeCursorCount

        var cursor = try await context.indexQueryContext.withTransaction {
            transaction in
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(rowKey),
                to: .firstGreaterOrEqual([0x81]),
                limit: 1,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            let row = try #require(await cursor.next())
            #expect(row.0 == rowKey)
            #expect(row.1 == rowValue)
            return cursor
        }

        #expect(control.openedRangeCursorCount == openedBefore + 1)
        #expect(control.finishedRangeCursorCount == finishedBefore + 1)
        await #expect(throws: DatabaseTransactionError.invalidOperationContext) {
            _ = try await cursor.next()
        }
    }

    @Test("Read scope waits for an admitted operation before transaction exit")
    func scopeWaitsForInFlightRead() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let readKey = ByteString(utf8: "in-flight-read")
        let readBarrier = control.suspendNextValueRead(for: readKey)
        let readTask = SynchronizedValue<Task<ByteString?, any Error>?>(nil)
        let readCompletionMonitor = SynchronizedValue<Task<Void, Never>?>(nil)
        let escapedTransaction = SynchronizedValue<DatabaseReadTransaction?>(nil)
        let didComplete = SynchronizedValue(false)

        let scopedOperation = Task {
            defer { didComplete.withLock { $0 = true } }
            try await context.indexQueryContext.withTransaction {
                transaction in
                escapedTransaction.withLock { $0 = transaction }
                let task = Task {
                    try await transaction.getValue(
                        for: readKey,
                        snapshot: true
                    )
                }
                readTask.withLock { $0 = task }
                let monitor = try await readBarrier.waitUntilEntered(
                    beforeCompletionOf: task
                )
                readCompletionMonitor.withLock { $0 = monitor }
            }
        }

        let scopeCompletionMonitor = try await readBarrier.waitUntilEntered(
            beforeCompletionOf: scopedOperation
        )
        let transaction = try #require(escapedTransaction.value())
        while true {
            do {
                _ = try await transaction.getValue(
                    for: ByteString(utf8: "closing-probe"),
                    snapshot: true
                )
                await Task.yield()
            } catch DatabaseTransactionError.invalidOperationContext {
                break
            }
        }
        #expect(!didComplete.withLock { $0 })

        readBarrier.release()
        try await scopedOperation.value
        await scopeCompletionMonitor.value
        let admittedRead = try #require(readTask.withLock { $0 })
        _ = try await admittedRead.value
        let readMonitor = try #require(readCompletionMonitor.withLock { $0 })
        await readMonitor.value
        #expect(didComplete.withLock { $0 })
    }

    @Test("Admitted feature operations can open and advance a cursor while closing")
    func admittedFeatureOperationRemainsTransitiveDuringClose() async throws {
        let barrier = StorageOperationBarrier()
        let cursorBarrier = StorageOperationBarrier()
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeAdmittedOperationContainer(
            storage: storage,
            barrier: barrier,
            cursorBarrier: cursorBarrier
        )
        defer { await container.shutdown() }
        let finishedBefore = storage.control.finishedRangeCursorCount
        let context = container.testBaseContext()
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
        let escapedSession = SynchronizedValue<DatabaseReadSession?>(nil)
        let featureTask = SynchronizedValue<Task<Bool, any Error>?>(nil)
        let didComplete = SynchronizedValue(false)

        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(
                readCapabilityProbeValue,
                for: readCapabilityProbeKey
            )
        }

        let scopedTask = Task {
            defer { didComplete.withLock { $0 = true } }
            try await context.withReadSnapshot(
                workMeter: workMeter
            ) { snapshot in
                escapedSession.withLock { $0 = snapshot.session }
                let task = Task {
                    try await snapshot.session.executeSPARQLAsk(
                        AskQuery(pattern: .basic([])),
                        options: ReadExecutionContext(
                            monotonicClock: container.monotonicClock,
                            workMeter: workMeter
                        )
                    )
                }
                featureTask.withLock { $0 = task }
                await barrier.waitUntilEntered()
            }
        }

        await barrier.waitUntilEntered()
        do {
            let session = try #require(escapedSession.value())
            while true {
                do {
                    _ = try session.admittingRDFDatasetRead()
                    await Task.yield()
                } catch DatabaseTransactionError.invalidOperationContext {
                    break
                }
            }

            barrier.release()
            await cursorBarrier.waitUntilEntered()
            #expect(!didComplete.value())
            #expect(
                storage.control.finishedRangeCursorCount == finishedBefore
            )
            cursorBarrier.release()
            let admittedFeature = try #require(featureTask.withLock { $0 })
            #expect(try await admittedFeature.value)
            try await scopedTask.value
            #expect(
                storage.control.finishedRangeCursorCount == finishedBefore + 1
            )
        } catch {
            barrier.release()
            cursorBarrier.release()
            scopedTask.cancel()
            _ = await scopedTask.result
            if let admittedFeature = featureTask.withLock({ $0 }) {
                admittedFeature.cancel()
                _ = await admittedFeature.result
            }
            throw error
        }
    }

    @Test("Read scope drains a borrowed child operation after feature return")
    func scopeDrainsBorrowedFeatureChildOperation() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let readBarrier = storage.control.suspendNextValueRead(
            for: readCapabilityProbeKey
        )
        let childRead = SynchronizedValue<Task<ByteString?, any Error>?>(nil)
        let container = try await makeBorrowedOperationContainer(
            storage: storage,
            readBarrier: readBarrier,
            readTask: childRead
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
        let escapedSession = SynchronizedValue<DatabaseReadSession?>(nil)
        let didComplete = SynchronizedValue(false)

        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(
                readCapabilityProbeValue,
                for: readCapabilityProbeKey
            )
        }

        let scopedTask = Task {
            defer { didComplete.withLock { $0 = true } }
            try await context.withReadSnapshot(workMeter: meter) { snapshot in
                escapedSession.withLock { $0 = snapshot.session }
                let result = try await snapshot.session.executeSPARQLAsk(
                    AskQuery(pattern: .basic([])),
                    options: ReadExecutionContext(
                        monotonicClock: container.monotonicClock,
                        workMeter: meter
                    )
                )
                #expect(result)
            }
        }

        await readBarrier.waitUntilEntered()
        do {
            let session = try #require(escapedSession.value())
            while true {
                do {
                    _ = try session.admittingRDFDatasetRead()
                    await Task.yield()
                } catch DatabaseTransactionError.invalidOperationContext {
                    break
                }
            }
            #expect(!didComplete.value())

            readBarrier.release()
            let read = try #require(childRead.value())
            #expect(try await read.value == readCapabilityProbeValue)
            try await scopedTask.value
            #expect(didComplete.value())
        } catch {
            readBarrier.release()
            scopedTask.cancel()
            _ = await scopedTask.result
            if let read = childRead.value() {
                read.cancel()
                _ = await read.result
            }
            throw error
        }
    }

    @Test("Cancellation drains an admitted child before read scope exit")
    func cancellationDrainsAdmittedChildOperation() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let readBarrier = storage.control.suspendNextValueRead(
            for: readCapabilityProbeKey
        )
        let childRead = SynchronizedValue<Task<ByteString?, any Error>?>(nil)
        let container = try await makeBorrowedOperationContainer(
            storage: storage,
            readBarrier: readBarrier,
            readTask: childRead
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
        let escapedSession = SynchronizedValue<DatabaseReadSession?>(nil)
        let didComplete = SynchronizedValue(false)

        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(
                readCapabilityProbeValue,
                for: readCapabilityProbeKey
            )
        }

        let scopedTask = Task {
            defer { didComplete.withLock { $0 = true } }
            try await context.withReadSnapshot(workMeter: meter) { snapshot in
                escapedSession.withLock { $0 = snapshot.session }
                _ = try await snapshot.session.executeSPARQLAsk(
                    AskQuery(pattern: .basic([])),
                    options: ReadExecutionContext(
                        monotonicClock: container.monotonicClock,
                        workMeter: meter
                    )
                )
            }
        }

        await readBarrier.waitUntilEntered()
        do {
            let session = try #require(escapedSession.value())
            while true {
                do {
                    _ = try session.admittingRDFDatasetRead()
                    await Task.yield()
                } catch DatabaseTransactionError.invalidOperationContext {
                    break
                }
            }

            scopedTask.cancel()
            await Task.yield()
            #expect(!didComplete.value())

            readBarrier.release()
            let read = try #require(childRead.value())
            #expect(try await read.value == readCapabilityProbeValue)
            await #expect(throws: CancellationError.self) {
                try await scopedTask.value
            }
            #expect(didComplete.value())
        } catch {
            readBarrier.release()
            scopedTask.cancel()
            _ = await scopedTask.result
            if let read = childRead.value() {
                read.cancel()
                _ = await read.result
            }
            throw error
        }
    }

    @Test("Transactions from another live snapshot are rejected")
    func crossSnapshotTransactionIsRejectedBeforeExecution() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let firstSnapshotReady = StorageOperationBarrier()
        let releaseFirstSnapshot = StorageOperationBarrier()
        let firstSession = SynchronizedValue<DatabaseReadSession?>(nil)
        let firstMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
        let secondMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )

        let firstTask = Task {
            try await context.withReadSnapshot(
                workMeter: firstMeter
            ) { snapshot in
                firstSession.withLock { $0 = snapshot.session }
                firstSnapshotReady.signalEntry()
                await releaseFirstSnapshot.enterAndWait()
            }
        }
        await firstSnapshotReady.waitUntilEntered()

        do {
            let foreignSession = try #require(
                firstSession.value()
            )
            _ = try await context.withReadSnapshot(
                workMeter: secondMeter
            ) { _ in
                await #expect(
                    throws: DatabaseTransactionError.invalidOperationContext
                ) {
                    _ = try await context.querySessionBound(
                        SelectQuery(
                            projection: .all,
                            source: .table(TableRef(Anchor.persistableType))
                        ),
                        execution: ReadExecutionContext(
                            monotonicClock: container.monotonicClock
                        ),
                        session: foreignSession
                    )
                }
            }
            releaseFirstSnapshot.release()
            try await firstTask.value
        } catch {
            releaseFirstSnapshot.release()
            firstTask.cancel()
            _ = await firstTask.result
            throw error
        }
    }

    @Test("Read scope drains an admitted cursor advance before transaction exit")
    func scopeDrainsInFlightCursorAdvance() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let rowKey = ByteString([0x90])
        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue([0x01], for: rowKey)
        }
        let rangeBarrier = control.suspendNextRangeAdvance()
        let readTask = SynchronizedValue<Task<KeyValueCursor.Element?, any Error>?>(nil)
        let readCompletionMonitor = SynchronizedValue<Task<Void, Never>?>(nil)
        let escapedTransaction = SynchronizedValue<DatabaseReadTransaction?>(nil)
        let didComplete = SynchronizedValue(false)
        let finishedBefore = control.finishedRangeCursorCount

        let scopedOperation = Task {
            defer { didComplete.withLock { $0 = true } }
            try await context.indexQueryContext.withTransaction {
                transaction in
                escapedTransaction.withLock { $0 = transaction }
                var cursor = transaction.rangeCursor(
                    from: .firstGreaterOrEqual(rowKey),
                    to: .firstGreaterOrEqual([0x91]),
                    limit: 1,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )
                let task = Task { try await cursor.next() }
                readTask.withLock { $0 = task }
                let monitor = try await rangeBarrier.waitUntilEntered(
                    beforeCompletionOf: task
                )
                readCompletionMonitor.withLock { $0 = monitor }
            }
        }

        let scopeCompletionMonitor = try await rangeBarrier.waitUntilEntered(
            beforeCompletionOf: scopedOperation
        )
        let transaction = try #require(escapedTransaction.value())
        while true {
            do {
                _ = try await transaction.getValue(
                    for: ByteString(utf8: "cursor-closing-probe"),
                    snapshot: true
                )
                await Task.yield()
            } catch DatabaseTransactionError.invalidOperationContext {
                break
            }
        }
        #expect(!didComplete.withLock { $0 })

        rangeBarrier.release()
        try await scopedOperation.value
        await scopeCompletionMonitor.value
        let admittedRead = try #require(readTask.withLock { $0 })
        let row = try #require(await admittedRead.value)
        #expect(row.0 == rowKey)
        let readMonitor = try #require(readCompletionMonitor.withLock { $0 })
        await readMonitor.value
        #expect(didComplete.withLock { $0 })
        #expect(control.finishedRangeCursorCount == finishedBefore + 1)
    }

    @Test("Cursor admission failure does not open backend state")
    func cursorBudgetFailureDoesNotOpenBackendCursor() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 0,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: container.monotonicClock
        )
        let openedBefore = control.openedRangeCursorCount

        try await context.indexQueryContext.withTransaction(
            workMeter: meter
        ) { transaction in
            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual([0x00]),
                to: .firstGreaterOrEqual([0xFF]),
                limit: 1,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            await #expect(throws: DatabaseWorkLimitError.self) {
                _ = try await cursor.next()
            }
        }

        #expect(control.openedRangeCursorCount == openedBefore)
    }

    @Test("Read snapshot charges cursors to the request meter")
    func readSnapshotUsesRequestMeter() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 0,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: container.monotonicClock
        )
        let openedBefore = control.openedRangeCursorCount

        try await context.withReadSnapshot(workMeter: meter) { snapshot in
            var cursor = snapshot.transaction.rangeCursor(
                from: .firstGreaterOrEqual([0x00]),
                to: .firstGreaterOrEqual([0xFF]),
                limit: 1,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            await #expect(throws: DatabaseWorkLimitError.self) {
                _ = try await cursor.next()
            }
        }

        #expect(control.openedRangeCursorCount == openedBefore)
    }

    @Test("Read session rejects a split execution meter")
    func readSessionRejectsSplitExecutionMeter() async throws {
        let container = try await makeLogicalSourceContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let sessionMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
        let foreignMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )

        _ = try await context.withReadSnapshot(
            workMeter: sessionMeter
        ) { snapshot in
            await #expect(
                throws: DatabaseReadSessionError.workMeterMismatch
            ) {
                _ = try await snapshot.session.executeSPARQLAsk(
                    AskQuery(pattern: .basic([])),
                    options: ReadExecutionContext(
                        monotonicClock: container.monotonicClock,
                        workMeter: foreignMeter
                    )
                )
            }
        }
    }

    @Test("Context-owned reads ignore a forged ambient authorization")
    func readPolicyUsesContextCapturedAuthorization() async throws {
        let container = try await makeAuthorizationContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let anchor = ReadPolicyAuthorizationAnchor(id: "captured-reader")
        try context.insert(anchor)
        try await context.save()
        let identifier = try anchor.persistableIdentifierTuple()
        let forged = AuthorizationContext.authenticated(
            Principal(identifier: "forged-reader")
        )

        try await RequestAuthorization.$context.withValue(forged) {
            try await context.withStorageAccess(requiredAccess: .read) { _ in
                try await DatabaseReadSession.withSession(
                    context: context,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: container.monotonicClock
                    )
                ) { session in
                    try session.authorizeCanonicalListAccess(
                        entity: try ReadPolicyAuthorizationAnchor.schemaEntity,
                        selectQuery: SelectQuery(
                            projection: .all,
                            source: .table(
                                TableRef(
                                    ReadPolicyAuthorizationAnchor
                                        .persistableType
                                )
                            )
                        )
                    )
                }
            }

            let fetched = try await context.indexQueryContext.fetchItems(
                ids: [identifier],
                type: ReadPolicyAuthorizationAnchor.self
            )
            #expect(fetched.map(\.id) == [anchor.id])

            // A raw container store has no context owner and must retain the
            // ambient request authorization instead of borrowing this context.
            await #expect(throws: SecurityError.self) {
                #if MultiBase
                let forgedContext = container.testBaseContext(
                    authorization: forged
                )
                _ = try await forgedContext.withExecutionDataOperation {
                    let store = try await container.store(
                        for: ReadPolicyAuthorizationAnchor.self
                    )
                    return try await store.fetchAll(
                        ReadPolicyAuthorizationAnchor.self
                    )
                }
                #else
                let store = try await container.store(
                    for: ReadPolicyAuthorizationAnchor.self
                )
                _ = try await store.fetchAll(
                    ReadPolicyAuthorizationAnchor.self
                )
                #endif
            }

            try await context.withTransaction(
                requiredAccess: .read
            ) { transaction in
                let scanned = try await transaction.scan(
                    ReadPolicyAuthorizationAnchor.self,
                    in: DirectoryPath(),
                    after: nil,
                    limit: 1,
                    consistency: .snapshot
                )
                #expect(scanned.items.map(\.id) == [anchor.id])
            }
        }
    }

    @Test("Read policy rejects captured denial despite authorized ambient context")
    func readPolicyDoesNotReplaceCapturedDenialWithAmbientAuthorization() async throws {
        let container = try await makeAuthorizationContainer()
        defer { await container.shutdown() }
        let authorizedContext = container.testBaseContext()
        let anchor = ReadPolicyAuthorizationAnchor(id: "captured-denial")
        try authorizedContext.insert(anchor)
        try await authorizedContext.save()
        let identifier = try anchor.persistableIdentifierTuple()
        let deniedPrincipal = Principal(identifier: "forged-reader")
        let deniedAuthorization = AuthorizationContext.authenticated(
            deniedPrincipal
        )
        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal(deniedPrincipal.identifier),
            access: .read
        )
        #endif
        let context = container.testBaseContext(
            authorization: deniedAuthorization
        )
        let query = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(ReadPolicyAuthorizationAnchor.persistableType)
            )
        )

        do {
            _ = try await RequestAuthorization.$context.withValue(
                TestBaseEnvironment.authorization
            ) {
                try await context.query(query)
            }
            Issue.record("Expected the captured authorization to deny the query")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
            #expect(
                error.targetType
                    == ReadPolicyAuthorizationAnchor.persistableType
            )
            #expect(error.userID == deniedPrincipal.identifier)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        await #expect(throws: SecurityError.self) {
            _ = try await RequestAuthorization.$context.withValue(
                TestBaseEnvironment.authorization
            ) {
                try await context.indexQueryContext.fetchItems(
                    ids: [identifier],
                    type: ReadPolicyAuthorizationAnchor.self
                )
            }
        }
    }

    @Test("Public polymorphic scans authorize every member before storage")
    func publicPolymorphicScanAuthorizesListBeforeStorage() async throws {
        let (container, control) = try await makePolymorphicAuthorizationContainer()
        defer { await container.shutdown() }
        let principal = Principal(
            identifier: "polymorphic-list-denied",
            roles: ["admin", "polymorphic-secret-reader"]
        )
        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal(principal.identifier),
            access: .read
        )
        #endif
        let context = container.testBaseContext(
            authorization: .authenticated(principal)
        )
        let readsBeforeScan = control.dataReadOperationCount

        await #expect(throws: SecurityError.self) {
            _ = try await context.fetchPolymorphic(
                ReadAuthorizationPolymorphicMember.self
            )
        }
        #expect(control.dataReadOperationCount == readsBeforeScan)
    }

    @Test("Public polymorphic scans authorize every returned field before storage")
    func publicPolymorphicScanAuthorizesFieldsBeforeStorage() async throws {
        let (container, control) = try await makePolymorphicAuthorizationContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let readsBeforeScan = control.dataReadOperationCount

        await #expect(throws: FieldSecurityError.self) {
            _ = try await context.fetchPolymorphic(
                ReadAuthorizationPolymorphicMember.self
            )
        }
        #expect(control.dataReadOperationCount == readsBeforeScan)
    }

    @Test("Polymorphic projections do not authorize unselected fields after storage")
    func polymorphicProjectionPreservesSealedFieldSet() async throws {
        let (container, _) = try await makePolymorphicAuthorizationContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            ReadAuthorizationPolymorphicMember(
                id: "polymorphic-visible",
                title: "Visible"
            )
        )
        try await context.save()

        let response = try await context.query(
            SelectQuery(
                projection: .items([
                    ProjectionItem(
                        .column(ColumnRef(column: "title"))
                    )
                ]),
                source: .logical(
                    LogicalSourceRef(
                        kindIdentifier: LogicalSourceKind.polymorphic,
                        identifier:
                            ReadAuthorizationPolymorphicMember.polymorphableType
                    )
                )
            )
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields == ["title": .string("Visible")])
    }

    @Test("Batch materialization retains one schema generation")
    func batchMaterializationRetainsSchemaGeneration() async throws {
        let (container, control) = try await makeControlledAuthorizationContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let first = ReadPolicyAuthorizationAnchor(id: "generation-first")
        let second = ReadPolicyAuthorizationAnchor(id: "generation-second")
        try context.insert(first)
        try context.insert(second)
        try await context.save()
        let identifiers = try [first, second].map {
            try $0.persistableIdentifierTuple()
        }
        let readKey: ByteString
        do {
            let store = try await container.testBaseStore(
                for: ReadPolicyAuthorizationAnchor.self
            )
            readKey = store.itemSubspace
                .subspace(ReadPolicyAuthorizationAnchor.persistableType)
                .pack(identifiers[0])
        }

        let readBarrier = control.suspendNextValueRead(for: readKey)
        let fetch = Task {
            try await context.indexQueryContext.fetchItems(
                ids: identifiers,
                type: ReadPolicyAuthorizationAnchor.self
            )
        }
        let fetchCompletionMonitor: Task<Void, Never>
        do {
            fetchCompletionMonitor = try await readBarrier.waitUntilEntered(
                beforeCompletionOf: fetch
            )
        } catch {
            readBarrier.release()
            fetch.cancel()
            _ = await fetch.result
            throw error
        }

        let drain = SynchronizedValue<Task<Void, any Error>?>(nil)
        do {
            let schema = container.schema
            let targetRuntime = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "read-policy-authorization-tests",
                revision: 2
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    ReadPolicyDenyingAuthorizationAnchor.self
                )
            ],
            authorizationPolicies: [
                AuthorizationPolicyHandler(
                    ReadPolicyDenyingAuthorizationAnchor.self
                )
            ]
            )
            let prepared = try container.prepareSchemaGeneration(
                schema,
                runtimeConfiguration: targetRuntime
            )
            let targetGeneration = container.schemaGeneration + 1
            container.publishSchemaGeneration(
                schema,
                fingerprint: try SchemaManifest(schema: schema).fingerprint(),
                indexPhysicalFingerprint: prepared.indexPhysicalFingerprint,
                executionRuntimeFingerprint: prepared
                    .executionRuntimeFingerprint,
                runtimeConfiguration: targetRuntime,
                indexPhysicalLayouts: prepared.indexPhysicalLayouts,
                generation: targetGeneration
            )

            let drainCompleted = SynchronizedValue(false)
            let drainTask = Task {
                try await container.waitForSchemaLeases(
                    olderThan: targetGeneration
                )
                drainCompleted.withLock { $0 = true }
            }
            drain.withLock { $0 = drainTask }
            while container.pendingSchemaDrainWaiterCount == 0 {
                await Task.yield()
            }
            #expect(container.pendingSchemaDrainWaiterCount == 1)
            #expect(!drainCompleted.withLock { $0 })

            readBarrier.release()
            let items = try await fetch.value
            await fetchCompletionMonitor.value
            #expect(items.map(\.id) == [first.id, second.id])
            try await drainTask.value
            #expect(drainCompleted.withLock { $0 })

            await #expect(throws: SecurityError.self) {
                _ = try await context.indexQueryContext.fetchItems(
                    ids: identifiers,
                    type: ReadPolicyAuthorizationAnchor.self
                )
            }
        } catch {
            readBarrier.release()
            fetch.cancel()
            _ = await fetch.result
            await fetchCompletionMonitor.value
            if let drainTask = drain.withLock({ $0 }) {
                drainTask.cancel()
                _ = await drainTask.result
            }
            throw error
        }
    }

    @Test("Container-owned read handle materializes on the admitted snapshot")
    func containerOwnedReadHandleMaterializesModel() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let anchor = Anchor(id: "materialized")
        let identifier = try anchor.persistableIdentifierTuple()
        let expectedID = anchor.id

        try await context.withTransaction { writeTransaction in
            try await writeTransaction.save(
                anchor,
                precondition: .notExists
            )
            try await context.indexQueryContext.withTransaction {
                readTransaction in
                let exposedAccess: any TransactionReadAccess = readTransaction
                #expect(exposedAccess as? any TransactionAccess == nil)
                let fetched = try await context.indexQueryContext
                    .fetchItemsPreservingOrder(
                        ids: [identifier],
                        type: Anchor.self,
                        transaction: readTransaction
                    )

                #expect(fetched.count == 1)
                #expect(fetched[0]?.id == expectedID)
            }
        }
    }

    @Test("Readable index resolution requires a container-owned read handle")
    func readableIndexRequiresContainerOwnedReadHandle() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        try await context.indexQueryContext.withTransaction { transaction in
            let readable = try await context.indexQueryContext.readableIndex(
                named: "read_authorization_anchor_id",
                indexType: .ordered,
                for: Anchor.self,
                transaction: transaction
            )
            #expect(readable?.descriptor.name == "read_authorization_anchor_id")
        }
    }

    @Test("Authorized index resolution uses direct session evidence")
    func authorizedIndexResolutionUsesSessionEvidence() async throws {
        let local = try await makeControlledIndexAuthorizationContainer()
        defer { await local.container.shutdown() }
        #if MultiBase
        try await local.container.grantTestBaseAccess(
            to: .principal("local-index-reader"),
            access: .read
        )
        #endif
        let context = local.container.testBaseContext(
            authorization: .authenticated(
                Principal(
                    identifier: "local-index-reader",
                    roles: ["index-reader"]
                )
            )
        )
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: local.container.monotonicClock
        )

        try await context.withStorageAccess(requiredAccess: .read) { _ in
            let evidence = try context.readPolicy().authorizeRead(
                listRequirements: [],
                fields: try indexAuthorizationPlan(context: context)
            )
            try await DatabaseReadSession.withSession(
                context: context,
                workMeter: workMeter
            ) { session in
                let authorizedSession = try session.authorizedSession(evidence)
                let readable = try await authorizedSession.readableIndex(
                    named: "read_index_authorization_probe_id",
                    indexType: .ordered,
                    forEntityName:
                        ReadIndexAuthorizationProbe.persistableType,
                    partitions: FieldObject()
                )
                #expect(
                    readable?.descriptor.name
                        == "read_index_authorization_probe_id"
                )
            }
        }
    }

    @Test("Direct tokenless index resolution evaluates field policy")
    func directTokenlessIndexResolutionPreservesPolicy() async throws {
        let scenario = try await makeControlledIndexAuthorizationContainer()
        defer { await scenario.container.shutdown() }
        #if MultiBase
        try await scenario.container.grantTestBaseAccess(
            to: .principal("unauthorized-index-reader"),
            access: .read
        )
        #endif
        let context = scenario.container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "unauthorized-index-reader")
            )
        )

        try await context.indexQueryContext.withTransaction { transaction in
            let readAccess: any TransactionReadAccess = transaction
            let readsBefore = scenario.control.dataReadOperationCount
            await #expect(throws: FieldSecurityError.self) {
                _ = try await context.indexQueryContext.readableIndex(
                    named: "read_index_authorization_probe_id",
                    indexType: .ordered,
                    forEntityName:
                        ReadIndexAuthorizationProbe.persistableType,
                    partitions: FieldObject(),
                    transaction: readAccess
                )
            }
            #expect(scenario.control.dataReadOperationCount == readsBefore)
        }
    }

    @Test("A read session requires direct sealed evidence")
    func ambientEvidenceCannotGrantOrWidenReadSession() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let entity = try Anchor.schemaEntity
        let index = try #require(
            entity.indexDescriptors.first {
                $0.name == "read_authorization_anchor_id"
            }
        )
        let admittedQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef(Anchor.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: index.name,
                    indexType: index.type
                )
            )
        )
        try await context.withStorageAccess(requiredAccess: .read) { _ in
            let evidence = try context.readPolicy().authorizeRead(
                listRequirements: [
                    try DatabaseReadPolicy.listRequirement(
                        entityName: entity.name,
                        selectQuery: admittedQuery
                    )
                ],
                fields: DatabaseFieldReadAuthorizationPlan.index(
                    entity: entity,
                    descriptor: index
                )
            )
            try await DatabaseReadSession.withSession(
                context: context,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget(),
                    monotonicClock: container.monotonicClock
                )
            ) { session in
                #expect(throws: DatabaseReadSessionError.authorizationMismatch) {
                    try session.requireCanonicalIndexReadAuthorization(
                        entity: entity,
                        index: index,
                        selectQuery: admittedQuery,
                        additionalFieldNames: []
                    )
                }

                let authorizedSession = try session.authorizedSession(evidence)
                try authorizedSession.requireCanonicalIndexReadAuthorization(
                    entity: entity,
                    index: index,
                    selectQuery: admittedQuery,
                    additionalFieldNames: []
                )

                let widenedQuery = SelectQuery(
                    projection: admittedQuery.projection,
                    source: admittedQuery.source,
                    accessPath: admittedQuery.accessPath,
                    limit: 1
                )
                #expect(throws: DatabaseReadSessionError.authorizationMismatch) {
                    try authorizedSession
                        .requireCanonicalIndexReadAuthorization(
                            entity: entity,
                            index: index,
                            selectQuery: widenedQuery,
                            additionalFieldNames: []
                        )
                }

                let readsBeforeScan = control.dataReadOperationCount
                await #expect(
                    throws: DatabaseReadSessionError.authorizationMismatch
                ) {
                    _ = try await context.scanPersistedModels(
                        entity: entity,
                        partitions: FieldObject(),
                        limit: 10,
                        transaction: authorizedSession.transaction,
                        authorizationRequirement: try DatabaseReadPolicy
                            .listRequirement(
                                entityName: entity.name,
                                selectQuery: widenedQuery
                            )
                    )
                }
                #expect(control.dataReadOperationCount == readsBeforeScan)
            }
        }
    }

    @Test("Logical source executors preserve a read-only caller snapshot")
    func logicalSourceExecutorsReceiveReadOnlyCapability() async throws {
        let container = try await makeLogicalSourceContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let graphTable = GraphTableSource.match(
            graph: "authority_probe",
            from: NodePattern(variable: "source"),
            via: EdgePattern(direction: .outgoing),
            to: NodePattern(variable: "target")
        )

        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(
                readCapabilityProbeValue,
                for: readCapabilityProbeKey
            )

            await #expect(
                throws: ReadCapabilityProbeError.observedReadOnlyCapability
            ) {
                _ = try await context.query(
                    SelectQuery(
                        projection: .all,
                        source: .graphTable(graphTable)
                    )
                )
            }
            await #expect(
                throws: ReadCapabilityProbeError.observedReadOnlyCapability
            ) {
                _ = try await context.query(
                    SelectQuery(
                        projection: .all,
                        source: .graphPattern(.basic([]))
                    )
                )
            }
            await #expect(
                throws: ReadCapabilityProbeError.observedReadOnlyCapability
            ) {
                _ = try await context.query(
                    SelectQuery(
                        projection: .all,
                        source: .union([
                            .graphPattern(.basic([]))
                        ])
                    )
                )
            }
            await #expect(
                throws: ReadCapabilityProbeError.observedReadOnlyCapability
            ) {
                _ = try await context.query(
                    SelectQuery(
                        projection: .all,
                        source: .union([
                            .namedGraph(
                                name: "authority_probe",
                                pattern: .basic([])
                            )
                        ])
                    )
                )
            }
        }
    }

    @Test("SPARQL executors receive sealed session authorization")
    func sparqlExecutorReceivesSealedSessionAuthorization() async throws {
        let expectedSnapshot = SynchronizedValue<DatabaseReadTransaction?>(nil)
        let container = try await makeLogicalSourceContainer(
            sparqlExecutor: ReadCapabilityProbeSPARQLExecutor(
                expectedSnapshot: expectedSnapshot
            )
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )

        try await context.withTransaction { transaction in
            try transaction.storageAccess.setValue(
                readCapabilityProbeValue,
                for: readCapabilityProbeKey
            )
        }

        try await context.withReadSnapshot(
            workMeter: meter
        ) { snapshot in
            expectedSnapshot.withLock { $0 = snapshot.transaction }
            await #expect(
                throws: ReadCapabilityProbeError.observedReadOnlyCapability
            ) {
                _ = try await snapshot.session.executeSPARQLAsk(
                    AskQuery(pattern: .basic([])),
                    options: ReadExecutionContext(
                        monotonicClock: container.monotonicClock,
                        workMeter: meter
                    )
                )
            }
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try makeAnchorRuntimeConfiguration(),
            security: .testingDisabled
        )
    }

    private func makeControlledContainer() async throws -> (
        DBContainer,
        StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try makeAnchorRuntimeConfiguration(),
            security: .testingDisabled
        )
        return (container, storage.control)
    }

    private func makeAnchorRuntimeConfiguration() throws
        -> DatabaseRuntimeConfiguration {
        let scalarProvider = ScalarIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(Anchor.self)
        try entityRuntime.register(scalarProvider)
        return try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: scalarProvider)
            ],
            entityRuntimes: [entityRuntime.registration()]
        )
    }

    private func makeLogicalSourceContainer(
        sparqlExecutor: any SPARQLSourceExecutor =
            ReadCapabilityProbeSPARQLExecutor()
    ) async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try LogicalSourceAnchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-capability-logical-source-tests",
                    revision: 1
                ),
                graphTableSourceExecutor:
                    ReadCapabilityProbeGraphTableExecutor(),
                sparqlSourceExecutor: sparqlExecutor,
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        LogicalSourceAnchor.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeAdmittedOperationContainer(
        storage: any StorageEngine,
        barrier: StorageOperationBarrier,
        cursorBarrier: StorageOperationBarrier
    ) async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try LogicalSourceAnchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-capability-admitted-operation-tests",
                    revision: 1
                ),
                sparqlSourceExecutor: AdmittedOperationSPARQLExecutor(
                    barrier: barrier,
                    cursorBarrier: cursorBarrier
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        LogicalSourceAnchor.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeBorrowedOperationContainer(
        storage: ControlledStorageEngine<InMemoryEngine>,
        readBarrier: StorageOperationBarrier,
        readTask: SynchronizedValue<Task<ByteString?, any Error>?>
    ) async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try LogicalSourceAnchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-capability-borrowed-operation-tests",
                    revision: 1
                ),
                sparqlSourceExecutor: BorrowedOperationSPARQLExecutor(
                    readBarrier: readBarrier,
                    readTask: readTask
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        LogicalSourceAnchor.self
                    ).registration()
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeAuthorizationContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try ReadPolicyAuthorizationAnchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-policy-authorization-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        ReadPolicyAuthorizationAnchor.self
                    ).registration()
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        ReadPolicyAuthorizationAnchor.self
                    )
                ]
            ),
            security: .enabled()
        )
    }

    private func makeControlledAuthorizationContainer() async throws -> (
        DBContainer,
        StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try ReadPolicyAuthorizationAnchor.schemaEntity
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-policy-authorization-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        ReadPolicyAuthorizationAnchor.self
                    ).registration()
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        ReadPolicyAuthorizationAnchor.self
                    )
                ]
            ),
            security: .enabled()
        )
        return (container, storage.control)
    }

    private func makePolymorphicAuthorizationContainer() async throws -> (
        DBContainer,
        StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try ReadAuthorizationPolymorphicMember.schemaEntity
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-polymorphic-authorization-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        ReadAuthorizationPolymorphicMember.self
                    ).registration()
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        ReadAuthorizationPolymorphicMember.self
                    )
                ]
            ),
            security: .enabled()
        )
        return (container, storage.control)
    }

    private func makeControlledIndexAuthorizationContainer() async throws -> (
        container: DBContainer,
        control: StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let scalarProvider = ScalarIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(
            ReadIndexAuthorizationProbe.self
        )
        try runtime.register(scalarProvider)
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try ReadIndexAuthorizationProbe.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "read-index-authorization-probe-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: scalarProvider)
                ],
                entityRuntimes: [runtime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        ReadIndexAuthorizationProbe.self
                    )
                ]
            ),
            security: .enabled()
        )
        return (container, storage.control)
    }

    private func makeIndexAuthorizationEvidence(
        context: DatabaseContext
    ) async throws -> DatabaseReadAuthorization {
        try await context.withDataOperation {
            try context.readPolicy().authorizeRead(
                listRequirements: [],
                fields: try indexAuthorizationPlan(context: context)
            )
        }
    }

    private func indexAuthorizationPlan(
        context: DatabaseContext
    ) throws -> DatabaseFieldReadAuthorizationPlan {
        let entity = try #require(
            context.readPolicy().schema.entity(
                named: ReadIndexAuthorizationProbe.persistableType
            )
        )
        let descriptor = try #require(
            entity.indexDescriptors.first {
                $0.name == "read_index_authorization_probe_id"
            }
        )
        return DatabaseFieldReadAuthorizationPlan.index(
            entity: entity,
            descriptor: descriptor
        )
    }
}
