import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S10 recursive producer lifecycle", .timeLimit(.minutes(1)))
struct S10RecursiveProducerTests {
    @Test("Unused recursion starts no child")
    func unusedIsLazy() async throws {
        let meter = makeMeter(); let witness = S10RecursiveWitness(); let schema = try makeSchema()
        let producer = try makeRecursive(schema: schema, meter: meter, witness: witness)
        #expect(await witness.starts == 0)
        try await producer.close()
        #expect(await witness.starts == 0)
        expectReleased(meter)
    }

    @Test("Seed and successive working frontiers are evaluated iteratively")
    func workingFrontierOrder() async throws {
        let meter = makeMeter(); let witness = S10RecursiveWitness(); let schema = try makeSchema()
        let producer = try makeRecursive(schema: schema, meter: meter, witness: witness)
        #expect(try await collect(producer) == [[number(1)], [number(2)], [number(3)]])
        #expect(await witness.starts == 1)
        #expect(await witness.steps == 3)
        try await producer.close(); expectReleased(meter)
    }

    @Test("UNION suppresses seed, iteration and prior-output duplicates including NULL")
    func unionIdentity() async throws {
        let meter = makeMeter(); let schema = try makeSchema()
        let producer = try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: false,
            generated: nil, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { rows([[.primitive(.null)], [.primitive(.null)], [number(1)], [number(1)]], schema: schema, meter: meter) },
            stepFactory: { frontier in
                mapped(frontier, schema: schema, meter: meter) { $0.values }
            })
        #expect(try await collect(producer) == [[.primitive(.null)], [number(1)]])
        try await producer.close(); expectReleased(meter)
    }

    @Test("UNION ALL preserves duplicate multiplicity in the next frontier")
    func unionAllMultiplicity() async throws {
        let meter = makeMeter(); let schema = try makeSchema()
        let producer = try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: true,
            generated: nil, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { rows([[number(1)], [number(1)]], schema: schema, meter: meter) },
            stepFactory: { frontier in mapped(frontier, schema: schema, meter: meter) { row in
                row.values[0] == number(1) ? [number(2)] : nil
            } })
        #expect(try await collect(producer) == [[number(1)], [number(1)], [number(2)], [number(2)]])
        try await producer.close(); expectReleased(meter)
    }

    @Test("Depth SEARCH paths participate in UNION identity and retain anonymous fields")
    func depthSearchIdentity() async throws {
        let meter = makeMeter(); let record = try keyType(); let user = try makeSchema()
        let output = try makeSchema(names: ["n", "search"], types: [.sql(.bigint), .sql(.array(.composite(record)))])
        let step = try makeSchema(names: ["n", "parent"], types: [.sql(.bigint), .sql(.array(.composite(record)))])
        let generated = SQLRecursiveGeneratedColumns(search: .init(keySlots: [0], recordType: record, breadthFirst: false), cycle: nil)
        let producer = try SQLRecursiveRowProducer(schema: output, userColumnCount: 1, unionAll: false,
            generated: generated, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { rows([[number(1)]], schema: user, meter: meter) },
            stepFactory: { frontier in mapped(frontier, schema: step, meter: meter) { [$0.values[0], $0.values[1]] } })
        for length in 1...3 {
            let result = try #require(await producer.next())
            guard case .array(let path) = result.rows[0].values[1] else { Issue.record("Expected a record array"); return }
            #expect(path.elements.count == length)
            guard case .composite(let key) = path.elements[length - 1] else { Issue.record("Expected a record"); return }
            #expect(key.type.columns[0].name == "f1")
            #expect(key.fields[0] == number(1))
        }
        try await producer.close(); expectReleased(meter)
    }

    @Test("Breadth SEARCH starts at bigint depth zero and preserves selected keys")
    func breadthSearchDepth() async throws {
        let meter = makeMeter(); let record = try keyType(breadth: true); let user = try makeSchema()
        let output = try makeSchema(names: ["n", "search"], types: [.sql(.bigint), .sql(.composite(record))])
        let step = try makeSchema(names: ["n", "parent"], types: [.sql(.bigint), .sql(.composite(record))])
        let producer = try SQLRecursiveRowProducer(schema: output, userColumnCount: 1, unionAll: true,
            generated: .init(search: .init(keySlots: [0], recordType: record, breadthFirst: true), cycle: nil),
            temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { rows([[number(7)]], schema: user, meter: meter) },
            stepFactory: { frontier in mapped(frontier, schema: step, meter: meter) { [$0.values[0], $0.values[1]] } })
        for depth in 0...2 {
            let result = try #require(await producer.next())
            guard case .composite(let search) = result.rows[0].values[1] else { Issue.record("Expected a breadth record"); return }
            #expect(search.fields[0] == number(depth))
            #expect(search.fields[1] == number(7))
            #expect(search.type.columns[0].name == "f1")
            #expect(search.type.columns[1].name == "f2")
        }
        try await producer.close(); expectReleased(meter)
    }

    @Test("Repeated NULL cycle keys emit the cyclic row then stop expansion")
    func nullCycleKey() async throws {
        let meter = makeMeter(); let producer = try cycleProducer(meter: meter, key: .primitive(.null),
            cycle: .primitive(.bool(true)), default: .primitive(.bool(false)), markType: .sql(.boolean))
        let output = try await collect(producer)
        #expect(output.count == 2)
        #expect(output[0][1] == .primitive(.bool(false)))
        #expect(output[1][1] == .primitive(.bool(true)))
        if case .array(let path) = output[1][2] { #expect(path.elements.count == 2) }
        else { Issue.record("Expected the complete cycle path") }
        try await producer.close(); expectReleased(meter)
    }

    @Test("Custom cycle constants and NULL or identical defaults use SQL expansion truth")
    func selectedCycleMarks() async throws {
        for (cycle, initial, markType, expectedCount) in [
            (QueryValue.primitive(.string("stop")), QueryValue.primitive(.string("go")), QueryColumnType.sql(.text), 2),
            (.primitive(.null), .primitive(.null), .sql(.text), 1),
            (.primitive(.bool(true)), .primitive(.bool(true)), .sql(.boolean), 1)
        ] {
            let meter = makeMeter()
            let producer = try cycleProducer(meter: meter, key: number(1), cycle: cycle, default: initial, markType: markType)
            let output = try await collect(producer)
            #expect(output.count == expectedCount)
            #expect(output[0][1] == initial)
            if expectedCount == 2 { #expect(output[1][1] == cycle) }
            try await producer.close(); expectReleased(meter)
        }
    }

    @Test("Independent CTE consumers stop and resume one recursive invocation")
    func sharedRecursivePrefix() async throws {
        let meter = makeMeter(); let witness = S10RecursiveWitness(); let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            try makeRecursive(schema: schema, meter: meter, witness: witness)
        }
        let first = cache.consumer(); let second = cache.consumer()
        #expect(try await first.next()?.rows[0].values == [number(1)])
        try await first.close()
        #expect(try await collect(second) == [[number(1)], [number(2)], [number(3)]])
        #expect(await witness.starts == 1)
        try await second.close(); try await cache.close(); expectReleased(meter)
    }

    @Test("Early close awaits child cleanup and preserves cleanup failure")
    func awaitedCloseAndFailure() async throws {
        let meter = makeMeter(); let witness = S10RecursiveWitness(); let schema = try makeSchema()
        let producer = try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: true,
            generated: nil, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: {
                SQLRelationPullProducer(schema: schema, meter: meter, operation: { index in
                    try row([number(index)], schema: schema, meter: meter)
                }, cleanup: { await Task.yield(); await witness.closed(); throw S10RecursiveFailure.cleanup })
            }, stepFactory: { _ in throw S10RecursiveFailure.operation })
        _ = try await producer.next()
        await #expect(throws: S10RecursiveFailure.self) { try await producer.close() }
        #expect(await witness.closes == 1)
        try await producer.close(); expectReleased(meter)
    }

    @Test("A retained generated row outlives close without retaining its producer")
    func retainedOutputHasNoBackpointer() async throws {
        let meter = makeMeter()
        var producer: SQLRecursiveRowProducer? = try cycleProducer(meter: meter, key: number(1),
            cycle: .primitive(.bool(true)), default: .primitive(.bool(false)), markType: .sql(.boolean))
        let releasedProducer = { [weak producer] in producer }
        var output = try await producer?.next()
        try await producer?.close(); producer = nil
        #expect(releasedProducer() == nil)
        #expect(meter.retainedIntermediateBytes > 0)
        #expect(output?.rows[0].values[0] == number(1))
        output = nil
        expectReleased(meter)
    }

    @Test("Operation and child cleanup failures remain failures on repeated pulls")
    func operationAndCleanupFailure() async throws {
        let meter = makeMeter(); let schema = try makeSchema()
        let producer = try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: true,
            generated: nil, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: {
                SQLRelationPullProducer(schema: schema, meter: meter,
                    operation: { _ in throw S10RecursiveFailure.operation },
                    cleanup: { throw S10RecursiveFailure.cleanup })
            }, stepFactory: { _ in throw S10RecursiveFailure.operation })
        for _ in 0..<2 {
            do { _ = try await producer.next(); Issue.record("Failure became a successful pull") }
            catch let error as SQLRelationCleanupError {
                #expect(error.operationError as? S10RecursiveFailure == .operation)
                #expect(error.cleanupError as? S10RecursiveFailure == .cleanup)
            }
        }
        try await producer.close(); expectReleased(meter)
    }

    @Test("Close racing a suspended child waits for its serialized advance")
    func closeRacingAdvance() async throws {
        let meter = makeMeter(); let schema = try makeSchema(); let barrier = S10RecursiveBarrier()
        let witness = S10RecursiveWitness()
        let producer = try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: true,
            generated: nil, temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: {
                SQLRelationPullProducer(schema: schema, meter: meter, operation: { _ in
                    await barrier.suspend()
                    return try row([number(1)], schema: schema, meter: meter)
                }, cleanup: { await witness.closed() })
            }, stepFactory: { _ in throw S10RecursiveFailure.operation })
        let advance = Task { _ = try await producer.next() }
        await barrier.waitUntilEntered()
        let closing = Task { try await producer.close() }
        await Task.yield()
        #expect(await witness.closes == 0)
        await barrier.resume()
        try await advance.value; try await closing.value
        #expect(await witness.closes == 1)
        #expect(try await producer.next() == nil)
        expectReleased(meter)
    }

    @Test("Cancellation and deadline terminate children and release the same meter")
    func cancelAndDeadline() async throws {
        let meter = makeMeter(); let witness = S10RecursiveWitness(); let schema = try makeSchema()
        let producer = try makeRecursive(schema: schema, meter: meter, witness: witness)
        _ = try await producer.next()
        let task = Task { withUnsafeCurrentTask { $0?.cancel() }; return try await producer.next() }
        await #expect(throws: (any Error).self) { _ = try await task.value }
        try await producer.close(); expectReleased(meter)
        let clock = S10RecursiveClock()
        let timedMeter = DatabaseWorkMeter(budget: ExecutionBudget(timeoutMilliseconds: 1), monotonicClock: clock)
        let timed = try makeRecursive(schema: schema, meter: timedMeter, witness: witness)
        _ = try await timed.next()
        clock.advance()
        await #expect(throws: DatabaseWorkLimitError.self) { _ = try await timed.next() }
        try await timed.close(); expectReleased(timedMeter)
    }

    @Test("Exact and one-past work, frontier rows and bytes are admitted before publication")
    func exactResourceBoundaries() async throws {
        let baseline = makeMeter()
        try await finiteRun(baseline)
        let work = baseline.consumedWorkUnits, bytes = baseline.peakIntermediateBytes
        let retainedRows = UInt32(baseline.peakIntermediateRows)
        try await finiteRun(makeMeter(work: work, bytes: bytes, rows: retainedRows))
        for budget in [ExecutionBudget(maximumWorkUnits: work - 1),
                       ExecutionBudget(maximumIntermediateBytes: bytes - 1),
                       ExecutionBudget(maximumIntermediateRows: retainedRows - 1)] {
            let meter = DatabaseWorkMeter(budget: budget, monotonicClock: SystemStorageClock())
            await #expect(throws: DatabaseWorkLimitError.self) { try await finiteRun(meter) }
            expectReleased(meter)
            let claim = try meter.reserveIntermediate(bytes: 1, at: .bindingCandidate)
            claim.release()
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    private func finiteRun(_ meter: DatabaseWorkMeter) async throws {
        let producer = try makeRecursive(schema: makeSchema(), meter: meter, witness: S10RecursiveWitness())
        do { _ = try await collect(producer); try await producer.close() }
        catch { try await producer.close(); throw error }
        expectReleased(meter)
    }

    private func cycleProducer(meter: DatabaseWorkMeter, key: QueryValue, cycle: QueryValue,
        default initial: QueryValue, markType: QueryColumnType) throws -> SQLRecursiveRowProducer {
        let record = try keyType(), user = try makeSchema()
        let output = try makeSchema(names: ["n", "cyclic", "path"], types: [.sql(.bigint), markType, .sql(.array(.composite(record)))])
        let step = try makeSchema(names: ["n", "parent"], types: [.sql(.bigint), .sql(.array(.composite(record)))])
        return try SQLRecursiveRowProducer(schema: output, userColumnCount: 1, unionAll: true,
            generated: .init(search: nil, cycle: .init(keySlots: [0], recordType: record, markType: markType,
                cycleValue: cycle, defaultValue: initial)), temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { rows([[key]], schema: user, meter: meter) },
            stepFactory: { frontier in mapped(frontier, schema: step, meter: meter) { [$0.values[0], $0.values[2]] } })
    }

    private func makeRecursive(schema: CanonicalRelationSchema, meter: DatabaseWorkMeter,
        witness: S10RecursiveWitness) throws -> SQLRecursiveRowProducer {
        try SQLRecursiveRowProducer(schema: schema, userColumnCount: 1, unionAll: true, generated: nil,
            temporalEnvironment: nil, limits: limits, meter: meter,
            seedFactory: { await witness.started(); return rows([[number(1)]], schema: schema, meter: meter) },
            stepFactory: { frontier in
                await witness.stepped()
                return mapped(frontier, schema: schema, meter: meter) { row in
                    guard case .primitive(.int64(let value)) = row.values[0], value < 3 else { return nil }
                    return [.primitive(.int64(value + 1))]
                }
            })
    }

    private func rows(_ values: [[QueryValue]], schema: CanonicalRelationSchema,
        meter: DatabaseWorkMeter) -> any SQLRelationProducer {
        SQLRelationPullProducer(schema: schema, meter: meter) { index in
            index < values.count ? try row(values[index], schema: schema, meter: meter) : nil
        }
    }
    private func mapped(_ frontier: SQLRecursiveFrontier, schema: CanonicalRelationSchema,
        meter: DatabaseWorkMeter, projection: @escaping @Sendable (CanonicalSourceRow) throws -> [QueryValue]?) -> any SQLRelationProducer {
        let source = frontier.consumer()
        return SQLRelationPullProducer(schema: schema, meter: meter, operation: { _ in
            while let input = try await source.next() {
                if let values = try projection(input.rows[0]) { return try row(values, schema: schema, meter: meter) }
            }
            return nil
        }, cleanup: { try await source.close() })
    }
    private func row(_ values: [QueryValue], schema: CanonicalRelationSchema, meter: DatabaseWorkMeter) throws -> CanonicalRelation {
        try sqlSingleRow(CanonicalSourceRow(values: values, layout: schema.layout), schema: schema, meter: meter)
    }
    private func collect(_ source: any SQLRelationProducer) async throws -> [[QueryValue]] {
        var result: [[QueryValue]] = []
        while let input = try await source.next() { result.append(input.rows[0].values) }
        return result
    }
    private func number(_ value: Int) -> QueryValue { .primitive(.int64(Int64(value))) }
    private func keyType(breadth: Bool = false) throws -> QueryCompositeType {
        try QueryCompositeType(columns: (1...(breadth ? 2 : 1)).map {
            try QueryColumn(number: UInt32($0), name: "f\($0)", type: .sql(.bigint), isNullable: true)
        }, limits: limits)
    }
    private func makeSchema(names: [String] = ["n"], types: [QueryColumnType] = [.sql(.bigint)]) throws -> CanonicalRelationSchema {
        try CanonicalRelationSchema(unscopedColumns: names, types: types.map(Optional.some))
    }
    private func makeMeter(work: UInt64 = 1_000_000, bytes: UInt64 = 16 * 1024 * 1024,
        rows: UInt32 = 10_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work,
            maximumIntermediateRows: rows, maximumIntermediateBytes: bytes), monotonicClock: SystemStorageClock())
    }
    private var limits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: 16 * 1024 * 1024, maximumTotalNodes: 100_000,
            maximumNestingDepth: 64, maximumCollectionElements: 100_000)
    }
    private func expectReleased(_ meter: DatabaseWorkMeter) {
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
    }
}

private actor S10RecursiveWitness {
    private(set) var starts = 0
    private(set) var steps = 0
    private(set) var closes = 0
    func started() { starts += 1 }
    func stepped() { steps += 1 }
    func closed() { closes += 1 }
}
private enum S10RecursiveFailure: Error { case operation, cleanup }
private actor S10RecursiveBarrier {
    private var continuation: CheckedContinuation<Void, Never>?
    private var entered = false
    func suspend() async {
        await withCheckedContinuation { continuation in self.continuation = continuation; entered = true }
    }
    func waitUntilEntered() async { while !entered { await Task.yield() } }
    func resume() { continuation?.resume(); continuation = nil }
}
private final class S10RecursiveClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))
    var now: StorageInstant { instant.withLock { $0 } }
    func advance() { instant.withLock { $0 = $0.advanced(by: .seconds(1)) } }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { instant.withLock { $0 = deadline } }
}
