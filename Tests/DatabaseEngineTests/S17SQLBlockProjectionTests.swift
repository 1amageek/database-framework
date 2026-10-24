import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S17 SQL complete-response projection", .timeLimit(.minutes(1)))
struct S17SQLBlockProjectionTests {
    @Test("Measurement and encoding rejection undo actual earlier writes")
    func projectionRejectionRollsBack() async throws {
        for refusal in [S17ProjectionRefusal.measure, .encode] {
            let container = try await makeContainer(InMemoryEngine())
            defer { await container.shutdown() }
            let options = try options()
            let meter = meter(container, options)
            let encoder = S17ProjectionEncoder(refusal: refusal)
            await #expect { try await execute(container, block: writeAndRead("rejected"),
                options: options, meter: meter, encoder: encoder) } throws: {
                ($0 as? S17ProjectionRefusal) == refusal
            }
            #expect(encoder.measureCount == 1)
            #expect(encoder.encodeCount == (refusal == .encode ? 1 : 0))
            #expect(meter.retainedIntermediateBytes == 0)
            #expect(try await container.testBaseContext().model(for: "rejected", as: EntityMutationFixture.self) == nil)
        }
    }

    @Test("An encoded frame stays private through real commit and owns exactly its frame claim until promotion")
    func commitGatesFrameAndClaim() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine)
        defer { await container.shutdown() }
        let options = try options()
        let meter = meter(container, options)
        let encoder = S17ProjectionEncoder()
        let returned = S17ProjectionCompletion()
        let barrier = engine.control.suspendNextMutatingCommit()
        let task = Task {
            let response = try await execute(container, block: writeAndRead("held"), options: options,
                meter: meter, encoder: encoder)
            returned.markReturned()
            return response
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: task)
        #expect(encoder.encodeCount == 1)
        #expect(!returned.didReturn)
        #expect(meter.retainedIntermediateBytes > S17ProjectionEncoder.frameBytes)
        barrier.release()
        let response = try await task.value
        await monitor.value
        #expect(returned.didReturn)
        #expect(response.byteCount == Int(S17ProjectionEncoder.frameBytes))
        #expect(meter.retainedIntermediateBytes == S17ProjectionEncoder.frameBytes)
        #expect(try response.promoteToOutput() == ByteString(repeating: 0x53, count: response.byteCount))
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(throws: SQLBlockProjectionError.alreadyPromoted) { try response.promoteToOutput() }
        #expect(try await container.testBaseContext().model(for: "held", as: EntityMutationFixture.self)?.title == "value")
        #expect(engine.control.openedRangeCursorCount == engine.control.finishedRangeCursorCount)
    }

    @Test("Caller cancellation during dispatched commit destroys the encoded frame after authoritative close")
    func cancellationDiscardsFrame() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine)
        defer { await container.shutdown() }
        let options = try options()
        let meter = meter(container, options)
        let encoder = S17ProjectionEncoder()
        let returned = S17ProjectionCompletion()
        let barrier = engine.control.suspendNextMutatingCommit()
        let task = Task {
            let response = try await execute(container, block: writeAndRead("cancelled"), options: options,
                meter: meter, encoder: encoder)
            returned.markReturned()
            return response
        }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: task)
        #expect(encoder.encodeCount == 1)
        task.cancel()
        barrier.release()
        await #expect(throws: CancellationError.self) { try await task.value }
        await monitor.value
        #expect(!returned.didReturn)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(try await container.testBaseContext().model(for: "cancelled", as: EntityMutationFixture.self)?.title == "value")
    }

    @Test("A later commit failure discards the complete frame and its earlier mutation")
    func commitFailureDiscardsFrame() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine)
        defer { await container.shutdown() }
        let options = try options(configuration: TransactionConfiguration(maximumAttempts: 1))
        let meter = meter(container, options)
        let encoder = S17ProjectionEncoder()
        engine.control.failNextMutatingCommit(with: .transactionConflict)
        await #expect { try await execute(container, block: writeAndRead("failed"), options: options,
            meter: meter, encoder: encoder) } throws: { ($0 as? StorageError)?.code == .transactionConflict }
        #expect(encoder.encodeCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(try await container.testBaseContext().model(for: "failed", as: EntityMutationFixture.self) == nil)
    }

    @Test("A recovery-close failure after the producing read commit still discards the encoded frame")
    func sequenceCloseFailureDiscardsFrame() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeContainer(engine)
        defer { await container.shutdown() }
        let options = try options(configuration: TransactionConfiguration(maximumAttempts: 1))
        let executor = SQLBlockExecutor(context: container.testBaseContext())
        let sequence = try SQLSequence(id: "projected", namespaceID: "public", name: "projected",
            start: 1, increment: 1, minimum: 1, maximum: 100)
        _ = try await executor.execute(SQLBlock(steps: [.sequence(.create(sequence))]), options: options)
        let block = try SQLBlock(steps: [.statement(.select(SelectQuery(projection: [.expression(.function(
            FunctionCall(name: "nextval", arguments: [.string("projected")])))], source: .values([[]]))))])
        let meter = meter(container, options)
        // Allocation has completed before encoding. The producing transaction is
        // read-only; only mandatory recovery-marker close performs the next write.
        let encoder = S17ProjectionEncoder(onEncoding: {
            engine.control.failNextMutatingCommit(with: .transactionConflict)
        })
        await #expect { try await execute(container, block: block, options: options, meter: meter, encoder: encoder) }
            throws: { ($0 as? StorageError)?.code == .transactionConflict }
        #expect(encoder.encodeCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
        let next = try await executor.execute(block, options: options)
        guard case .rows(let rows) = next.results[0] else { Issue.record("Expected sequence observation"); return }
        #expect(rows.rows.first?.values == [.primitive(.int64(2))])
    }

    @Test("Discarding an unpromoted ready response releases its exact private reservation")
    func abandonedReadyReleasesClaim() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let options = try options()
        let meter = meter(container, options)
        func discardResponse() async throws {
            let response = try await execute(container, block: writeAndRead("abandoned"), options: options,
                meter: meter, encoder: S17ProjectionEncoder())
            #expect(meter.retainedIntermediateBytes == S17ProjectionEncoder.frameBytes)
            withExtendedLifetime(response) {}
        }
        try await discardResponse()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Exact full-frame admission refuses oversized output before allocation and rolls back")
    func frameBudgetRefusal() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        // Use the same collection budget as the successful projection cases so
        // this refusal occurs at the measured frame reservation, after SQL work.
        let options = try options()
        let meter = meter(container, options)
        let measuredBytes = options.budget.maximumIntermediateBytes + 1
        let encoder = S17ProjectionEncoder(measuredBytes: measuredBytes)
        await #expect {
            try await execute(container, block: writeAndRead("oversized"), options: options, meter: meter, encoder: encoder)
        } throws: { error in
            guard case DatabaseWorkLimitError.maximumIntermediateBytes(let stage, _, let requested, let maximum) = error else {
                return false
            }
            return stage == .resultMaterialization && requested == measuredBytes
                && maximum == options.budget.maximumIntermediateBytes
        }
        #expect(encoder.measureCount == 1)
        #expect(encoder.encodeCount == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(try await container.testBaseContext().model(for: "oversized", as: EntityMutationFixture.self) == nil)
    }

    @Test("Successful explicit rollback encodes ordered observations while undoing the write")
    func rollbackKeepsEncodedObservations() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let options = try options()
        let meter = meter(container, options)
        let encoder = S17ProjectionEncoder()
        let block = try SQLBlock(steps: [.transaction(.begin), .statement(insert("undone")),
            .statement(select("undone")), .transaction(.rollback)])
        let response = try await execute(container, block: block, options: options, meter: meter, encoder: encoder)
        #expect(encoder.lastStatements == ["command", "command", "rows", "command"])
        #expect(encoder.lastValues == [.primitive(.string("value"))])
        #expect(try await container.testBaseContext().model(for: "undone", as: EntityMutationFixture.self) == nil)
        _ = try response.promoteToOutput()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Scoped visitors preserve row schema, values, metadata and COPY without bulk result materialization")
    func scopedOutputVisitors() async throws {
        let container = try await makeContainer(InMemoryEngine())
        defer { await container.shutdown() }
        let options = try options()
        let meter = meter(container, options)
        let encoder = S17ProjectionEncoder()
        let copy = SQLCopyCommand(target: .query(SelectQuery(projection: [.expression(.string("copied"))],
            source: .values([[]]))), direction: .to)
        let response = try await execute(container, block: SQLBlock(steps: [.statement(insert("visible")),
            .statement(select("visible")), .copy(copy)]), options: options, meter: meter, encoder: encoder)
        #expect(encoder.lastStatements == ["command", "rows", "copy"])
        #expect(encoder.lastValues == [.primitive(.string("value"))])
        #expect(encoder.lastCopy == ByteString(utf8: "copied\n"))
        #expect(encoder.lastColumnCount == 1)
        #expect(encoder.lastRowCount == 1)
        #expect(meter.retainedIntermediateBytes == S17ProjectionEncoder.frameBytes)
        _ = try response.promoteToOutput()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func execute(_ container: DBContainer, block: SQLBlock, options: SQLBlockExecutionOptions,
        meter: DatabaseWorkMeter, encoder: any SQLBlockResponseEncoder) async throws -> EncodedQueryResponse {
        try await SQLBlockExecutor(context: container.testBaseContext()).executeEncoded(block, options: options,
            workMeter: meter, executionDeadline: TransactionExecutionDeadline(
                instant: container.monotonicClock.now.advanced(by: .milliseconds(Int64(options.budget.timeoutMilliseconds))),
                timeoutMilliseconds: options.budget.timeoutMilliseconds), encoder: encoder)
    }
    private func meter(_ container: DBContainer, _ options: SQLBlockExecutionOptions) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: options.budget, monotonicClock: container.monotonicClock)
    }
    private func makeContainer(_ engine: any StorageEngine) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [EntityMutationFixture.schemaEntity]),
            configuration: .testing(storageEngine: engine), runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s17-projection", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }
    private func options(budget: ExecutionBudget = ExecutionBudget(),
        configuration: TransactionConfiguration = .default) throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 10_000,
            maximumDepth: 32, maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 128), budget: budget, configuration: configuration)
    }
    private func writeAndRead(_ id: String) throws -> SQLBlock {
        try SQLBlock(steps: [.statement(insert(id)), .statement(select(id))])
    }
    private func insert(_ id: String) -> QueryStatement {
        .insert(InsertQuery(target: TableRef(EntityMutationFixture.persistableType), columns: ["id", "title"],
            source: .values([[.string(id), .string("value")]])))
    }
    private func select(_ id: String) -> QueryStatement {
        .select(SelectQuery(projection: [.column("title")], source: .table(TableRef(EntityMutationFixture.persistableType)),
            filter: .equal(.col("id"), .string(id))))
    }
}

private enum S17ProjectionRefusal: Error, Equatable { case measure, encode }

private final class S17ProjectionCompletion: Sendable {
    private let returned = Mutex(false)
    var didReturn: Bool { returned.withLock { $0 } }
    func markReturned() { returned.withLock { $0 = true } }
}

/// A deliberately fixed frame for testing the Framework ownership boundary, not a Wire codec.
private final class S17ProjectionEncoder: SQLBlockResponseEncoder, Sendable {
    static let frameBytes: UInt64 = 32
    private struct State: Sendable {
        var measurements = 0
        var encodings = 0
        var statements: [String] = []
        var values: [QueryValue] = []
        var copy = ByteString()
        var columns = 0
        var rows = 0
    }
    private let state = Mutex(State())
    private let refusal: S17ProjectionRefusal?
    private let measuredBytes: UInt64
    private let onEncoding: (@Sendable () -> Void)?
    init(refusal: S17ProjectionRefusal? = nil, measuredBytes: UInt64 = frameBytes,
        onEncoding: (@Sendable () -> Void)? = nil) {
        self.refusal = refusal; self.measuredBytes = measuredBytes; self.onEncoding = onEncoding
    }
    var measureCount: Int { state.withLock { $0.measurements } }
    var encodeCount: Int { state.withLock { $0.encodings } }
    var lastStatements: [String] { state.withLock { $0.statements } }
    var lastValues: [QueryValue] { state.withLock { $0.values } }
    var lastCopy: ByteString { state.withLock { $0.copy } }
    var lastColumnCount: Int { state.withLock { $0.columns } }
    var lastRowCount: Int { state.withLock { $0.rows } }
    func measure(_ output: borrowing SQLBlockOutputView) throws -> UInt64 {
        state.withLock { $0.measurements += 1 }
        if refusal == .measure { throw S17ProjectionRefusal.measure }
        try inspect(output)
        return measuredBytes
    }
    func encode(_ output: borrowing SQLBlockOutputView, into destination: inout MutableSpan<UInt8>) throws {
        state.withLock { $0.encodings += 1 }
        if refusal == .encode { throw S17ProjectionRefusal.encode }
        try inspect(output)
        for index in 0..<destination.count { destination[index] = 0x53 }
        onEncoding?()
    }
    private func inspect(_ output: borrowing SQLBlockOutputView) throws {
        var observed = State()
        for index in 0..<output.statementCount {
            try output.withStatement(at: index, command: { _, _ in observed.statements.append("command") }, rows: { rows in
                observed.statements.append("rows")
                observed.columns += rows.columnCount
                observed.rows += rows.rowCount
                for column in 0..<rows.columnCount {
                    try rows.withColumn(at: column) { _ in }
                }
                for index in 0..<rows.rowCount {
                    try rows.withRow(at: index) { row in
                        for value in 0..<row.valueCount {
                            try row.withValue(at: value) { observed.values.append($0) }
                        }
                        try row.withAnnotations { _ in }
                        try row.withVersion { _ in }
                    }
                }
            }, copy: { copy in
                observed.statements.append("copy")
                try copy.withBytes { observed.copy = $0 }
            })
        }
        state.withLock {
            $0.statements = observed.statements; $0.values = observed.values; $0.copy = observed.copy
            $0.columns = observed.columns; $0.rows = observed.rows
        }
    }
}
