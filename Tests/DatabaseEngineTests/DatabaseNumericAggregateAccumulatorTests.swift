import DatabaseKit
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL numeric aggregation")
struct DatabaseNumericAggregateAccumulatorTests {
    private let numericType = QueryColumnType.sql(.decimal(precision: nil, scale: nil))

    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 1_000_000,
                       clock: AggregateClock = AggregateClock()) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100,
            maximumWorkUnits: work, maximumIntermediateRows: 100,
            maximumIntermediateBytes: bytes, timeoutMilliseconds: 1_000), monotonicClock: clock)
    }

    private func numeric(_ coefficient: Int128, scale: Int32 = 0, display: Int32? = nil) throws -> QueryValue {
        .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale),
                                      displayScale: display ?? max(0, scale)))
    }

    @Test("SUM and AVG preserve the six selected input signatures")
    func typeMatrix() throws {
        let numericSum = try numeric(4)
        let numericAverage = try numeric(2, display: 16)
        let inputs: [(QueryColumnType, QueryValue, QueryValue, QueryValue)] = [
            (.sql(.smallint), .primitive(.int16(2)), .primitive(.int64(4)), numericAverage),
            (.sql(.integer), .primitive(.int32(2)), .primitive(.int64(4)), numericAverage),
            (.sql(.bigint), .primitive(.int64(2)), numericSum, numericAverage),
            (numericType, try numeric(2), numericSum, numericAverage),
            (.sql(.real), .primitive(.float32(2)), .primitive(.float32(4)), .primitive(.float64(2))),
            (.sql(.doublePrecision), .primitive(.float64(2)), .primitive(.float64(4)), .primitive(.float64(2))),
            (.primitive(.int8), .primitive(.int8(2)), numericSum, numericAverage),
            (.primitive(.int16), .primitive(.int16(2)), .primitive(.int64(4)), numericAverage),
            (.primitive(.int32), .primitive(.int32(2)), .primitive(.int64(4)), numericAverage),
            (.primitive(.int64), .primitive(.int64(2)), numericSum, numericAverage),
            (.primitive(.uint8), .primitive(.uint8(2)), numericSum, numericAverage),
            (.primitive(.uint16), .primitive(.uint16(2)), numericSum, numericAverage),
            (.primitive(.uint32), .primitive(.uint32(2)), numericSum, numericAverage),
            (.primitive(.uint64), .primitive(.uint64(2)), numericSum, numericAverage),
            (.primitive(.decimal), .primitive(.decimal(ExactDecimal(coefficient: 2, scale: 0))), numericSum, numericAverage),
            (.primitive(.float32), .primitive(.float32(2)), .primitive(.float32(4)), .primitive(.float64(2))),
            (.primitive(.float64), .primitive(.float64(2)), .primitive(.float64(4)), .primitive(.float64(2))),
        ]
        for entry in inputs {
            let workMeter = meter()
            do {
                var accumulator = DatabaseNumericAggregateAccumulator(inputType: entry.0, workMeter: workMeter)
                try accumulator.add(entry.1)
                try accumulator.add(entry.1)
                #expect(try accumulator.sum() == entry.2)
                #expect(try accumulator.average() == entry.3)
            }
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
        var unsigned = DatabaseNumericAggregateAccumulator(inputType: .primitive(.uint64), workMeter: meter())
        try unsigned.add(.primitive(.uint64(.max)))
        try unsigned.add(.primitive(.uint64(.max)))
        #expect(try unsigned.sum() == numeric(Int128(UInt64.max) * 2))
        #expect(try unsigned.average() == numeric(Int128(UInt64.max)))
    }

    @Test("Empty and all-NULL accumulators return no value for their bound nullable column")
    func emptyAndNull() throws {
        for type: QueryColumnType in [.sql(.smallint), .sql(.integer), .sql(.bigint), numericType,
                                      .sql(.real), .sql(.doublePrecision),
                                      .primitive(.int8), .primitive(.int16), .primitive(.int32), .primitive(.int64),
                                      .primitive(.uint8), .primitive(.uint16), .primitive(.uint32), .primitive(.uint64),
                                      .primitive(.decimal), .primitive(.float32), .primitive(.float64)] {
            let workMeter = meter()
            var accumulator = DatabaseNumericAggregateAccumulator(inputType: type, workMeter: workMeter)
            #expect(try accumulator.sum() == nil)
            #expect(try accumulator.average() == nil)
            try accumulator.add(.primitive(.null))
            #expect(try accumulator.sum() == nil)
            #expect(try accumulator.average() == nil)
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Numeric SUM retains maximum input display scale and ignores NULL")
    func displayScale() throws {
        var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        try accumulator.add(numeric(125, scale: 2, display: 5))
        try accumulator.add(.primitive(.null))
        try accumulator.add(.primitive(.decimal(ExactDecimal(coefficient: -125, scale: 2))))
        #expect(try accumulator.sum() == numeric(0, display: 5))
        #expect(try accumulator.average() == numeric(0, display: 20))
        var primitive = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        for coefficient: Int128 in [1, 3] {
            let value = QueryValue.primitive(.decimal(ExactDecimal(coefficient: coefficient, scale: 0)))
            #expect(QueryValueValidator.accepts(value, type: numericType))
            try primitive.add(value)
        }
        #expect(try primitive.sum() == numeric(4))
        #expect(try primitive.average() == numeric(2, display: 16))
    }

    @Test("Recurring AVG rounds with PostgreSQL division scale from widened sum and count")
    func recurringAverage() throws {
        var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        for value: Int128 in [1, 0, 0] { try accumulator.add(numeric(value)) }
        #expect(try accumulator.average() == numeric(33_333_333_333_333_333_333, scale: 20))
        var negative = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        for value: Int128 in [-1, 0, 0] { try negative.add(numeric(value)) }
        #expect(try negative.average() == numeric(-33_333_333_333_333_333_333, scale: 20))
    }

    @Test("Widened intermediate permits representable AVG and cancellation SUM")
    func widenedIntermediate() throws {
        let maximum = Int128("99999999999999999999999999999999999999")!
        var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        try accumulator.add(numeric(maximum))
        try accumulator.add(numeric(maximum))
        #expect(try accumulator.average() == numeric(maximum))
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.numericOverflow) { try accumulator.sum() }
        try accumulator.add(numeric(-maximum))
        #expect(try accumulator.sum() == numeric(maximum))
    }

    @Test("Real SUM overflow does not narrow the double AVG transition")
    func floatingWidths() throws {
        var accumulator = DatabaseNumericAggregateAccumulator(inputType: .sql(.real), workMeter: meter())
        try accumulator.add(.primitive(.float32(.greatestFiniteMagnitude)))
        try accumulator.add(.primitive(.float32(.greatestFiniteMagnitude)))
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.numericOverflow) { try accumulator.sum() }
        #expect(try accumulator.average() == .primitive(.float64(Double(Float.greatestFiniteMagnitude))))
    }

    @Test("Selected operand types and nonfinite values fail explicitly")
    func invalidInputs() throws {
        var integer = DatabaseNumericAggregateAccumulator(inputType: .sql(.integer), workMeter: meter())
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.incompatibleNumericKinds) {
            try integer.add(.primitive(.int64(1)))
        }
        var declared = DatabaseNumericAggregateAccumulator(inputType: .primitive(.int64), workMeter: meter())
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.incompatibleNumericKinds) {
            try declared.add(.primitive(.int32(1)))
        }
        var decimal = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: meter())
        for value in [ExactDecimal(coefficient: Int128.max, scale: 0),
                      ExactDecimal(coefficient: 1, scale: 16384)] {
            #expect(throws: DatabaseNumericAggregateAccumulator.Failure.numericOverflow) {
                try decimal.add(.primitive(.decimal(value)))
            }
        }
        var floating = DatabaseNumericAggregateAccumulator(inputType: .sql(.doublePrecision), workMeter: meter())
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.nonFiniteValue) {
            try floating.add(.primitive(.float64(.infinity)))
        }
        #expect(throws: DatabaseNumericAggregateAccumulator.Failure.nonNumericValue) {
            try floating.add(.primitive(.string("1")))
        }
    }

    @Test("Dynamic workspace admits exact peak bytes and rejects one byte below it")
    func workspaceBoundary() throws {
        let maximum = Int128("99999999999999999999999999999999999999")!
        for type: QueryColumnType in [numericType, .primitive(.decimal)] {
            let value = try numeric(maximum)
            let input = type == numericType ? value : .primitive(.decimal(ExactDecimal(coefficient: maximum, scale: 0)))
            let measured = meter()
            func run(_ workMeter: DatabaseWorkMeter) throws {
                var accumulator = DatabaseNumericAggregateAccumulator(inputType: type, workMeter: workMeter)
                try accumulator.add(input)
                try accumulator.add(input)
                #expect(try accumulator.average() == value)
            }
            try run(measured)
            let peak = measured.peakIntermediateBytes
            #expect(peak > 0)
            #expect(measured.retainedIntermediateBytes == 0)
            let exact = meter(bytes: peak)
            try run(exact)
            #expect(exact.retainedIntermediateBytes == 0)
            let short = meter(bytes: peak - 1)
            #expect(throws: DatabaseWorkLimitError.self) { try run(short) }
            #expect(short.retainedIntermediateBytes == 0)
        }
    }

    @Test("Workspace remains reserved while a copied accumulator owner survives")
    func retainedOwner() throws {
        for type: QueryColumnType in [numericType, .primitive(.uint64)] {
            let workMeter = meter()
            var retained: DatabaseNumericAggregateAccumulator?
            do {
                var accumulator = DatabaseNumericAggregateAccumulator(inputType: type, workMeter: workMeter)
                try accumulator.add(type == numericType ? numeric(1) : .primitive(.uint64(1)))
                retained = accumulator
            }
            #expect(workMeter.retainedIntermediateBytes > 0)
            #expect(try retained?.sum() == numeric(1))
            retained = nil
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Work exhaustion propagates unchanged and releases after owner scope")
    func workFailure() throws {
        let workMeter = meter(work: 0)
        #expect(throws: DatabaseWorkLimitError.self) {
            var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: workMeter)
            try accumulator.add(numeric(1))
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Deadline propagation releases aggregate workspace")
    func deadlineFailure() throws {
        let clock = AggregateClock()
        let workMeter = meter(clock: clock)
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .aggregateInput)) {
            var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: workMeter)
            try accumulator.add(numeric(1))
            clock.advance()
            _ = try accumulator.average()
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation propagates unchanged and releases workspace")
    func cancellationFailure() async throws {
        let workMeter = meter()
        let task = Task {
            var accumulator = DatabaseNumericAggregateAccumulator(inputType: numericType, workMeter: workMeter)
            try accumulator.add(numeric(1))
            withUnsafeCurrentTask { $0?.cancel() }
            _ = try accumulator.sum()
        }
        do { try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
}

private final class AggregateClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))
    var now: StorageInstant { instant.withLock { $0 } }
    func advance() { instant.withLock { $0 = $0.advanced(by: .seconds(1)) } }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) {
        instant.withLock { $0 = deadline }
    }
}
