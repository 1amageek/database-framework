import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S08 relation producer lifecycle", .timeLimit(.minutes(1)))
struct S08RelationProducerTests {
    @Test("Independent consumers replay and grow one suspended prefix")
    func prefixFrontierIsSharedWithoutEarlyEOF() async throws {
        let meter = makeMeter()
        let witness = S08ProducerWitness()
        let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            await witness.started()
            return SQLRelationPullProducer(schema: schema, meter: meter, operation: { index in
                guard index < 3 else { return nil }
                await witness.produced()
                return try makeRow(index, schema: schema, meter: meter)
            }, cleanup: { await witness.closed() })
        }
        let first = cache.consumer()
        let second = cache.consumer()
        #expect(try await first.next()?.value.values == [.primitive(.int64(0))])
        try await first.close()
        #expect(await witness.productions == 1)
        #expect(try await second.next()?.value.values == [.primitive(.int64(0))])
        #expect(await witness.productions == 1)
        #expect(try await second.next()?.value.values == [.primitive(.int64(1))])
        #expect(try await second.next()?.value.values == [.primitive(.int64(2))])
        #expect(try await second.next() == nil)
        #expect(await witness.starts == 1)
        #expect(await witness.productions == 3)
        #expect(await witness.closes == 1)
        try await second.close(); try await cache.close()
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
    }

    @Test("Concurrent consumers serialize a suspended producer advance")
    func concurrentFrontierAdvancesOnce() async throws {
        let meter = makeMeter(); let witness = S08ProducerWitness(); let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            await witness.started()
            await Task.yield()
            return SQLRelationPullProducer(schema: schema, meter: meter) { index in
                guard index == 0 else { return nil }
                await witness.produced(); await Task.yield()
                return try makeRow(index, schema: schema, meter: meter)
            }
        }
        let first = cache.consumer(); let second = cache.consumer()
        async let lhs = first.next()
        async let rhs = second.next()
        let values = try await (lhs, rhs)
        #expect(values.0?.value.values == values.1?.value.values)
        #expect(await witness.starts == 1)
        #expect(await witness.productions == 1)
        try await first.close(); try await second.close(); try await cache.close()
        withExtendedLifetime(values) {}
    }

    @Test("Failure stays a failure for every consumer and closes exactly once")
    func failureNeverBecomesEOF() async throws {
        let meter = makeMeter(); let witness = S08ProducerWitness(); let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            SQLRelationPullProducer(schema: schema, meter: meter, operation: { _ in
                throw S08ProducerFailure.selected
            }, cleanup: { await witness.closed(); throw S08ProducerFailure.cleanup })
        }
        let first = cache.consumer(); let second = cache.consumer()
        await #expect(throws: SQLRelationCleanupError.self) { _ = try await first.next() }
        await #expect(throws: SQLRelationCleanupError.self) { _ = try await first.next() }
        await #expect(throws: SQLRelationCleanupError.self) { _ = try await second.next() }
        try await first.close(); try await second.close(); try await cache.close()
        #expect(await witness.closes == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Unused cache never starts and cancellation releases its prefix")
    func unusedAndCancelledCacheOwnership() async throws {
        let meter = makeMeter(); let witness = S08ProducerWitness(); let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            await witness.started()
            return SQLRelationPullProducer(schema: schema, meter: meter) { index in
                try makeRow(index, schema: schema, meter: meter)
            }
        }
        try await cache.close()
        #expect(await witness.starts == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        let cancelled = try SQLCTECache(schema: schema, meter: meter) {
            await witness.started()
            return SQLRelationPullProducer(schema: schema, meter: meter) { index in
                try makeRow(index, schema: schema, meter: meter)
            }
        }
        let consumer = cancelled.consumer()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await consumer.next()
        }
        await #expect(throws: (any Error).self) { _ = try await task.value }
        try await consumer.close(); try await cancelled.close()
        #expect(await witness.starts == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Admission failure does not publish a cached row")
    func cacheGrowthBudgetFailureIsTerminal() async throws {
        let meter = makeMeter(bytes: 256); let schema = try makeSchema()
        let cache = try SQLCTECache(schema: schema, meter: meter) {
            SQLRelationPullProducer(schema: schema, meter: meter) { index in
                try makeRow(index, schema: schema, meter: meter)
            }
        }
        let consumer = cache.consumer()
        await #expect(throws: DatabaseWorkLimitError.self) { _ = try await consumer.next() }
        await #expect(throws: DatabaseWorkLimitError.self) { _ = try await consumer.next() }
        try await consumer.close(); try await cache.close()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(bytes: UInt64 = 16 * 1024 * 1024) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: 100_000,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
    }
    private func makeSchema() throws -> CanonicalRelationSchema {
        try CanonicalRelationSchema(unscopedColumns: ["n"], types: [.sql(.bigint)], nullability: [false])
    }
    private func makeRow(_ index: Int, schema: CanonicalRelationSchema, meter: DatabaseWorkMeter) throws -> SQLRelationRow {
        try SQLRelationRow.producing(CanonicalSourceRow(values: [.primitive(.int64(Int64(index)))], layout: schema.layout), schema: schema, meter: meter)
    }
}

private actor S08ProducerWitness {
    private(set) var starts = 0
    private(set) var productions = 0
    private(set) var closes = 0
    func started() { starts += 1 }
    func produced() { productions += 1 }
    func closed() { closes += 1 }
}
private enum S08ProducerFailure: Error { case selected, cleanup }
