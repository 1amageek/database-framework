import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL collection identity")
struct CanonicalSQLCollectionIdentityTests {
    private struct Monotonic: StorageMonotonicClock {
        var now: StorageInstant { StorageInstant(durationSinceReference: .zero) }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
    }
    private struct Clock: WallClock { var now: Timestamp { Timestamp(secondsSinceUnixEpoch: 0) } }
    private var environment: SQLTemporalEnvironment {
        SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0),
            statementInstant: Timestamp(secondsSinceUnixEpoch: 0), wallClock: Clock())
    }
    private var limits: QueryValueLimits {
        .init(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 10_000,
            maximumNestingDepth: 100, maximumCollectionElements: 10_000)
    }
    private func meter(work: UInt64 = 1_000_000, bytes: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: Monotonic())
    }
    private func array(_ values: [QueryValue], element: DataType, lower: Int32 = 1,
        dimensions: [QueryArrayDimension]? = nil) throws -> QueryValue {
        .array(try QueryArrayValue(elementType: element,
            dimensions: dimensions ?? (values.isEmpty ? [] : [QueryArrayDimension(lowerBound: lower, length: Int32(values.count))]),
            elements: values, limits: limits))
    }
    private func numeric(_ value: Int128, display: Int32) throws -> QueryValue {
        .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: value, scale: 0), displayScale: display))
    }
    private func json(_ root: QueryJSONBNode) throws -> QueryValue {
        .jsonb(try QueryJSONBValue(root: root, limits: limits))
    }
    private func key(_ value: QueryValue, _ type: DataType, meter: DatabaseWorkMeter,
        digest: UInt64? = nil) throws -> CanonicalSQLValueIdentity {
        .collection(try CanonicalSQLCollectionIdentity.prepare(value, type: .sql(type),
            environment: environment, workMeter: meter, forcedDigest: digest))
    }
    private func equal(_ left: CanonicalSQLValueIdentity, _ right: CanonicalSQLValueIdentity,
        _ meter: DatabaseWorkMeter) throws -> Bool {
        try CanonicalSQLValueIdentity.equal(left, right, workMeter: meter, stage: .deduplication)
    }

    @Test("NUMERIC display scale is representation-only for array and JSONB identities")
    func numericDisplayScale() throws {
        let meter = meter()
        let decimal = DataType.decimal(precision: nil, scale: nil)
        let a = try array([numeric(1, display: 0)], element: decimal)
        let b = try array([numeric(1, display: 4)], element: decimal)
        #expect(a != b)
        let ka = try key(a, .array(decimal), meter: meter), kb = try key(b, .array(decimal), meter: meter)
        let same = try equal(ka, kb, meter)
        let ha = try CanonicalSQLValueIdentity.hash([ka], workMeter: meter, stage: .deduplication)
        let hb = try CanonicalSQLValueIdentity.hash([kb], workMeter: meter, stage: .deduplication)
        #expect(same)
        #expect(ha == hb)
        let n1 = try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 0)
        let n2 = try QueryNumericValue(value: n1.value, displayScale: 5)
        let j1 = try json(.number(n1)), j2 = try json(.number(n2))
        #expect(j1 != j2)
        let jk1 = try key(j1, .jsonb, meter: meter), jk2 = try key(j2, .jsonb, meter: meter)
        let jsonSame = try equal(jk1, jk2, meter)
        #expect(jsonSame)
    }

    @Test("Selected CHAR trims only trailing spaces while TEXT keeps byte identity")
    func characterCells() throws {
        let meter = meter()
        let a = try array([.primitive(.string("x "))], element: .char(length: nil))
        let b = try array([.primitive(.string("x"))], element: .char(length: nil))
        let ak = try key(a, .array(.char(length: nil)), meter: meter)
        let bk = try key(b, .array(.char(length: nil)), meter: meter)
        let same = try equal(ak, bk, meter)
        #expect(same)
        let ta = try key(array([.primitive(.string("x "))], element: .text), .array(.text), meter: meter)
        let tb = try key(array([.primitive(.string("x"))], element: .text), .array(.text), meter: meter)
        let distinct = try equal(ta, tb, meter)
        #expect(!distinct)
        let composed = try key(array([.primitive(.string("é"))], element: .text), .array(.text), meter: meter)
        let decomposed = try key(array([.primitive(.string("e\u{301}"))], element: .text), .array(.text), meter: meter)
        let unicodeSame = try equal(composed, decomposed, meter)
        #expect(!unicodeSame)
    }

    @Test("Interval equivalence and TIMETZ tie fields use selected temporal identity")
    func temporalCells() throws {
        let meter = meter()
        let month = try QueryIntervalValue(period: CalendarPeriod(months: 1), time: TimeSpan(seconds: 0))
        let days = try QueryIntervalValue(period: CalendarPeriod(days: 30), time: TimeSpan(seconds: 0))
        #expect(month != days)
        let a = try key(array([.interval(month)], element: .interval()), .array(.interval()), meter: meter)
        let b = try key(array([.interval(days)], element: .interval()), .array(.interval()), meter: meter)
        let same = try equal(a, b, meter)
        #expect(same)
        let timetz = DataType.time(withTimeZone: true)
        let first = QueryValue.timeWithTimeZone(QueryTimeWithTimeZoneValue(time: try QueryTimeValue(microsecondsSinceMidnight: 3_600_000_000), utcOffsetSeconds: 3600))
        let second = QueryValue.timeWithTimeZone(QueryTimeWithTimeZoneValue(time: try QueryTimeValue(microsecondsSinceMidnight: 0), utcOffsetSeconds: 0))
        let tk1 = try key(array([first], element: timetz), .array(timetz), meter: meter)
        let tk2 = try key(array([second], element: timetz), .array(timetz), meter: meter)
        let tied = try equal(tk1, tk2, meter)
        #expect(!tied)
    }

    @Test("Array dimensions, bounds, NULL cells and kind boundaries remain unambiguous")
    func arrayShapeAndNulls() throws {
        let meter = meter()
        let cells = [QueryValue.primitive(.int32(1)), .primitive(.null)]
        let a = try key(array(cells, element: .integer), .array(.integer), meter: meter)
        let b = try key(array(cells, element: .integer, lower: 0), .array(.integer), meter: meter)
        let c = try key(array(cells, element: .integer,
            dimensions: [.init(lowerBound: 1, length: 1), .init(lowerBound: 1, length: 2)]), .array(.integer), meter: meter)
        let ab = try equal(a, b, meter), ac = try equal(a, c, meter)
        #expect(!ab)
        #expect(!ac)
        let copy = try key(array(cells, element: .integer), .array(.integer), meter: meter)
        let same = try equal(a, copy, meter)
        #expect(same)
        let empty = try key(array([], element: .integer), .array(.integer), meter: meter)
        let nullCell = try key(array([.primitive(.null)], element: .integer), .array(.integer), meter: meter)
        let emptyNull = try equal(empty, nullCell, meter)
        #expect(!emptyNull)
    }

    @Test("JSONB member boundaries, scalar kinds and nested values produce semantic tokens")
    func jsonStructures() throws {
        let meter = meter()
        let first = try QueryJSONBNode.object([.init(key: "a", value: .string("bc"))], limits: limits)
        let second = try QueryJSONBNode.object([.init(key: "ab", value: .string("c"))], limits: limits)
        let a = try key(json(first), .jsonb, meter: meter), b = try key(json(second), .jsonb, meter: meter)
        let same = try equal(a, b, meter)
        #expect(!same)
        let sqlNull = CanonicalSQLValueIdentity.value(.primitive(.null))
        let jsonNull = try key(json(.null), .jsonb, meter: meter)
        let nullSame = try equal(sqlNull, jsonNull, meter)
        #expect(!nullSame)
        let one = try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 3)
        let nested = try QueryJSONBNode.array([.null, .bool(true), .number(one)], limits: limits)
        let original = try json(nested)
        let k1 = try key(original, .jsonb, meter: meter), k2 = try key(original, .jsonb, meter: meter)
        let identical = try equal(k1, k2, meter)
        #expect(identical)
    }

    @Test("Forced digest and bucket collisions preserve exact keys and row-array identity")
    func collisionSet() throws {
        let meter = meter()
        let a = try key(array([.primitive(.int32(1))], element: .integer), .array(.integer), meter: meter, digest: 7)
        let b = try key(array([.primitive(.int32(2))], element: .integer), .array(.integer), meter: meter, digest: 7)
        let equalDigest = try equal(a, b, meter)
        #expect(!equalDigest)
        let limitedComparison = self.meter(work: 3)
        #expect(throws: DatabaseWorkLimitError.self) {
            try CanonicalSQLValueIdentity.equal(a, b, workMeter: limitedComparison, stage: .deduplication)
        }
        #expect(limitedComparison.retainedIntermediateBytes == 0)
        var set = try CanonicalSQLIdentitySet(workMeter: meter, stage: .deduplication, expectedCount: 1, forcedHash: 0)
        let insertA = try set.insert(a), insertB = try set.insert(b), duplicate = try set.insert(a)
        #expect(insertA)
        #expect(insertB)
        #expect(!duplicate)
        let hasA = try set.contains(a), hasB = try set.contains(b)
        #expect(hasA && hasB)
        let row = [a, .value(.primitive(.string("tail")))]
        let insertedRow = try set.insert(row), containsRow = try set.contains(row)
        #expect(insertedRow && containsRow)
        let reverse = try set.contains([.value(.primitive(.string("tail"))), a])
        #expect(!reverse)
    }

    @Test("GROUP ordering uses SQL comparators instead of identity byte order")
    func meteredOrdering() throws {
        let meter = meter()
        let small = try key(array([.primitive(.int32(2))], element: .integer), .array(.integer), meter: meter)
        let large = try key(array([.primitive(.int32(10))], element: .integer), .array(.integer), meter: meter)
        let before = meter.consumedWorkUnits
        let arrayOrder = try CanonicalSQLValueIdentity.precedes(small, large, environment: environment, workMeter: meter)
        #expect(arrayOrder)
        #expect(meter.consumedWorkUnits > before)
        let empty = try key(json(QueryJSONBNode.array([], limits: limits)), .jsonb, meter: meter)
        let null = try key(json(.null), .jsonb, meter: meter)
        let jsonOrder = try CanonicalSQLValueIdentity.precedes(empty, null, environment: environment, workMeter: meter)
        #expect(jsonOrder)
    }

    @Test("Collection preparation rejects JSON identity and preserves primitive/temporal routing")
    func selectedCapability() throws {
        let meter = meter()
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL JSON identity")) {
            try CanonicalSQLValueIdentity.prepareCollection(.primitive(.null), type: .sql(.json), environment: environment, workMeter: meter)
        }
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL JSON identity")) {
            try CanonicalSQLValueIdentity.prepareCollection(.primitive(.null), type: .sql(.array(.json)), environment: environment, workMeter: meter)
        }
        let primitive = try CanonicalSQLValueIdentity.prepareCollection(.primitive(.int32(1)), type: .primitive(.int32), environment: nil, workMeter: meter)
        let null = try CanonicalSQLValueIdentity.prepareCollection(.primitive(.null), type: .sql(.array(.integer)), environment: nil, workMeter: meter)
        #expect(primitive == nil)
        #expect(null == nil)
    }

    @Test("Key and set storage admit exact boundaries and release on failure and last owner")
    func resourcesAndRelease() throws {
        let source = try json(QueryJSONBNode.array([.string("payload"), .null], limits: limits))
        let probe = meter()
        do { _ = try key(source, .jsonb, meter: probe) }
        #expect(probe.retainedIntermediateBytes == 0)
        let exact = meter(bytes: probe.peakIntermediateBytes)
        do { _ = try key(source, .jsonb, meter: exact) }
        #expect(exact.retainedIntermediateBytes == 0)
        let short = meter(bytes: probe.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try key(source, .jsonb, meter: short) }
        #expect(short.retainedIntermediateBytes == 0)
        let initialProbe = meter()
        do {
            let set = try CanonicalSQLIdentitySet(workMeter: initialProbe, stage: .deduplication, expectedCount: 4)
            let found = try set.contains([CanonicalSQLValueIdentity]())
            #expect(!found)
            #expect(initialProbe.retainedIntermediateBytes > 0)
        }
        #expect(initialProbe.retainedIntermediateBytes == 0)
        let initialShort = meter(bytes: initialProbe.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try CanonicalSQLIdentitySet(workMeter: initialShort, stage: .deduplication, expectedCount: 4)
        }
        #expect(initialShort.retainedIntermediateBytes == 0)
        let tableMeter = meter()
        do {
            var set = try CanonicalSQLIdentitySet(workMeter: tableMeter, stage: .deduplication, expectedCount: 0)
            for index in 0..<20 {
                let inserted = try set.insert(.value(.primitive(.string(String(index)))))
                #expect(inserted)
            }
            #expect(tableMeter.retainedIntermediateBytes > 0)
        }
        #expect(tableMeter.retainedIntermediateBytes == 0)
    }

    @Test("Work, cancellation and deadline failures remain typed and release prepared scratch")
    func cancellationAndWork() async throws {
        let source = try json(QueryJSONBNode.array([.string("scan"), .bool(true)], limits: limits))
        let limited = meter(work: 1)
        #expect(throws: DatabaseWorkLimitError.self) { try key(source, .jsonb, meter: limited) }
        #expect(limited.retainedIntermediateBytes == 0)
        let cancelled = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try key(source, .jsonb, meter: cancelled)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(cancelled.retainedIntermediateBytes == 0)
        let deadline = DatabaseWorkMeter(budget: ExecutionBudget(timeoutMilliseconds: 0), monotonicClock: Expired())
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .expressionEvaluation)) { try key(source, .jsonb, meter: deadline) }
        #expect(deadline.retainedIntermediateBytes == 0)
    }
    private final class Expired: StorageMonotonicClock, Sendable {
        private let ticks = Mutex(Int64(0))
        var now: StorageInstant {
            ticks.withLock { value in
                defer { value += 1 }
                return StorageInstant(durationSinceReference: .milliseconds(value))
            }
        }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
    }
}
