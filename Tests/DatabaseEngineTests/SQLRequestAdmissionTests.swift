import DatabaseKit
import DatabaseRuntime
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("SQL request admission", .timeLimit(.minutes(1)))
struct SQLRequestAdmissionTests {
    @Test("A read completes while an earlier read is held at authoritative commit")
    func sharedReadsOverlap() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await open(engine)
        defer { await container.shutdown() }
        let barrier = engine.control.suspendNextReadOnlyCommit()
        let first = Task { try await execute(container, block: read()) }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: first)
        do {
            // The held commit proves overlap; the deadline only bounds a hang.
            let second = try await execute(container, block: read())
            #expect(second.results.count == 1)
            barrier.release()
            _ = try await first.value
            await monitor.value
        } catch {
            barrier.release(); first.cancel()
            _ = await first.result; await monitor.value
            throw error
        }
    }

    @Test("Exclusive requests block reads until their authoritative close")
    func exclusiveOwnerBlocksReaders() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await open(engine)
        defer { await container.shutdown() }
        let barrier = engine.control.suspendNextReadOnlyCommit()
        let owner = Task { try await execute(container, block: SQLBlock(steps: [.savepoint("held")])) }
        let monitor = try await barrier.waitUntilEntered(beforeCompletionOf: owner)
        await #expect(throws: TransactionExecutionDeadlineExceeded.self) {
            try await execute(container, block: read(), timeout: 100)
        }
        barrier.release()
        _ = try await owner.value
        await monitor.value
        _ = try await execute(container, block: read())
    }

    @Test("An earlier writer is not overtaken and queued readers share the next cohort")
    func fairReadWriteAdmission() async throws {
        let gate = SQLRequestAdmission(), meter = makeMeter(), clock = SystemStorageClock()
        let deadline = TransactionExecutionDeadline(instant: clock.now.advanced(by: .seconds(10)), timeoutMilliseconds: 10000)
        let first = try await gate.enter(.shared, meter: meter, clock: clock, deadline: deadline)
        let writerQueued = StorageOperationBarrier(), readerQueued = StorageOperationBarrier()
        let writer = Task { try await gate.enter(.exclusive, meter: meter,
            clock: AdmissionClock(queued: writerQueued), deadline: deadline) }
        let writerMonitor = try await writerQueued.waitUntilEntered(beforeCompletionOf: writer)
        let reader = Task { try await gate.enter(.shared, meter: meter,
            clock: AdmissionClock(queued: readerQueued), deadline: deadline) }
        var firstHeld = true, writerReturned = false
        do {
            let readerMonitor = try await readerQueued.waitUntilEntered(beforeCompletionOf: reader)
            await gate.leave(first); firstHeld = false
            let exclusive = try await writer.value
            // If a queued reader could overtake, it would complete without its timer handshake.
            await writerMonitor.value
            await gate.leave(exclusive); writerReturned = true
            let shared = try await reader.value
            await readerMonitor.value
            let peer = try await gate.enter(.shared, meter: meter, clock: clock, deadline: deadline)
            await gate.leave(shared); await gate.leave(peer)
            #expect(meter.retainedIntermediateBytes == 0)
        } catch {
            writer.cancel(); reader.cancel()
            if firstHeld { await gate.leave(first) }
            if !writerReturned, case .success(let permit) = await writer.result { await gate.leave(permit) }
            if case .success(let permit) = await reader.result { await gate.leave(permit) }
            await writerMonitor.value
            throw error
        }
    }

    @Test("Sequence resolution consumes its explicit request meter without an ambient duplicate")
    func sequenceMeterOwnership() async throws {
        let container = try await open(InMemoryEngine())
        defer { await container.shutdown() }
        let context = container.testBaseContext(), meter = makeMeter(), clock = SystemStorageClock()
        let request = SQLSequenceRequest(context: context, searchPath: .default, meter: meter,
            configuration: .default, deadline: .init(instant: clock.now.advanced(by: .seconds(30)),
                timeoutMilliseconds: 30000), exclusiveAdmission: true)
        #expect(request.meter === meter)
        _ = try await context.withDataOperation {
            #expect(throws: SQLSequenceExecutionError.unknownSequence("missing")) {
                try SQLSequenceRuntime.resolve(name: "missing", request: request)
            }
        }
        let limited = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 0), monotonicClock: clock)
        let restricted = SQLSequenceRequest(context: context, searchPath: .default, meter: limited,
            configuration: .default, deadline: .init(instant: clock.now.advanced(by: .seconds(30)),
                timeoutMilliseconds: 30000), exclusiveAdmission: true)
        _ = try await context.withDataOperation {
            #expect(throws: DatabaseWorkLimitError.self) {
                try SQLSequenceRuntime.resolve(name: "missing", request: restricted)
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(limited.retainedIntermediateBytes == 0)
    }

    private struct AdmissionClock: StorageMonotonicClock {
        let queued: StorageOperationBarrier
        var now: StorageInstant { SystemStorageClock().now }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) {
            queued.signalEntry()
            try await SystemStorageClock().sleep(until: deadline)
        }
        func sleep(for duration: Duration) async throws(StorageClockError) {
            try await sleep(until: now.advanced(by: duration))
        }
    }
    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 1024 * 1024), monotonicClock: SystemStorageClock())
    }

    private func read() throws -> SQLBlock {
        try SQLBlock(steps: [.statement(.select(SelectQuery(projection: [.expression(.int(7))], source: .values([[]]))))])
    }
    private func execute(_ container: DBContainer, block: SQLBlock, timeout: UInt32 = 30000) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(block,
            options: SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 1000,
                maximumDepth: 16, maximumRetainedBytes: 1024 * 1024, maximumWorkUnits: 100000,
                maximumReadPageRows: 128), budget: ExecutionBudget(timeoutMilliseconds: timeout)))
    }
    private func open(_ engine: any StorageEngine) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [EntityMutationFixture.schemaEntity]),
            configuration: .testing(storageEngine: engine), runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "sql-admission", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(EntityMutationFixture.self)]), security: .testingDisabled)
    }
}
