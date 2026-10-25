import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S10 selected table function kernels and lifecycle", .timeLimit(.minutes(1)))
struct S10TableFunctionTests {
    private struct Clock: WallClock {
        var now: Timestamp { Timestamp(secondsSinceUnixEpoch: 946_684_800) }
    }
    private func environment() -> SQLTemporalEnvironment {
        let clock = Clock()
        return SQLTemporalEnvironment(transactionInstant: clock.now, statementInstant: clock.now, wallClock: clock)
    }
    private func meter(bytes: UInt64 = 16 * 1024 * 1024, rows: UInt32 = 1000, work: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: work,
            maximumIntermediateRows: rows, maximumIntermediateBytes: bytes, timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
    }
    private func binder() -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: [], resolveColumn: { _ in throw DatabaseExpressionEvaluationError.typeMismatch(operation: "test column") }, wildcardSlots: { _ in [] })
    }
    private func expression(_ value: QueryValue, _ type: DataType) -> Expression {
        .literal(.queryValue(value, type: .sql(type)))
    }
    private func int(_ value: Int32) -> QueryValue { .primitive(.int32(value)) }
    private var numeric: DataType { .decimal(precision: nil, scale: nil) }
    private var limits: QueryValueLimits { QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 100_000, maximumNestingDepth: 64, maximumCollectionElements: 100_000) }
    private func selected(_ name: String, values: [QueryValue], types: [DataType], ordinal: Bool = false) throws -> CanonicalBoundTableFunction {
        try binder().bindTableFunction(FunctionCall(name: name, arguments: zip(values, types).map { expression($0, $1) }), withOrdinality: ordinal)
    }
    private func producer(_ selected: CanonicalBoundTableFunction, _ values: [QueryValue], meter: DatabaseWorkMeter, environment: SQLTemporalEnvironment? = nil) throws -> any SQLRelationProducer {
        let schema = try CanonicalRelationSchema(unscopedColumns: selected.columns.map(\.name),
            types: selected.columns.map { .some($0.type) }, nullability: selected.columns.map(\.isNullable))
        return try makeSQLTableFunctionProducer(schema: schema, selected: selected,
            operands: values.map { .init(value: $0, leases: [], payloadIsRetained: false) },
            temporalEnvironment: environment ?? self.environment(), workMeter: meter, limits: limits)
    }
    private func decimal(_ coefficient: Int128, scale: Int32 = 0) throws -> QueryValue {
        .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale), displayScale: max(0, scale)))
    }
    private func temporal(_ text: String, _ type: DataType, meter: DatabaseWorkMeter) throws -> QueryValue {
        try CanonicalSQLTemporalEvaluator.cast(.primitive(.string(text)), from: .sql(.text), to: .sql(type),
            context: .explicit, environment: environment(), workMeter: meter)
    }

    private func locating(_ selected: CanonicalBoundTableFunction, at occurrence: QueryOccurrenceID) -> CanonicalBoundTableFunction {
        let bound = selected.bound
        return CanonicalBoundTableFunction(bound: CanonicalBoundExpression(expression: bound.expression,
            type: bound.type, isNullable: bound.isNullable, children: bound.children, occurrence: occurrence),
            columns: selected.columns, kind: selected.kind, withOrdinality: selected.withOrdinality)
    }
    private func expectSemanticFailure(_ cause: DatabaseExpressionEvaluationError, occurrence: QueryOccurrenceID? = nil,
        _ operation: () throws -> Void) throws {
        do { try operation(); Issue.record("Expected the selected semantic error boundary") }
        catch CanonicalReadError.expressionEvaluation(let actual, let actualOccurrence) {
            #expect(actual == cause)
            #expect(actualOccurrence == occurrence)
        }
    }

    @Test func signaturesAreSelectedBeforeRowsAndScalarSRFsFail() throws {
        let empty = try QueryArrayValue(elementType: .date, dimensions: [], elements: [], limits: limits)
        let array = try selected("unnest", values: [.array(empty)], types: [.array(.date)], ordinal: true)
        #expect(array.columns.map(\.type) == [.sql(.date), .sql(.bigint)])
        #expect(array.columns.map(\.isNullable) == [true, false])
        let textArray = try QueryArrayValue(elementType: .text, dimensions: [], elements: [], limits: limits)
        let variadic = try binder().bindTableFunction(FunctionCall(name: "json_build_array",
            arguments: [expression(.array(textArray), .array(.text))], finalArrayIsVariadic: true), withOrdinality: false)
        #expect(variadic.kind == .scalar && variadic.columns[0].type == .sql(.json))
        for name in ["unnest", "generate_subscripts", "generate_series", "regexp_matches", "regexp_split_to_table", "json_each"] {
            #expect(throws: (any Error).self) { _ = try binder().bindScalarExpression(.function(FunctionCall(name: name, arguments: []))) }
        }
        for args: [Expression] in [[.literal(.null), .literal(.null)], [.literal(.sqlUnknownString("1")), .literal(.sqlUnknownString("3"))]] {
            #expect(throws: (any Error).self) { _ = try binder().bindTableFunction(FunctionCall(name: "generate_series", arguments: args), withOrdinality: false) }
        }
    }

    @Test func integerSeriesInclusiveDescendingAndNoSpuriousOverflow() async throws {
        for (values, expected) in [([int(1), int(5), int(2)], [int(1), int(3), int(5)]),
                                   ([int(3), int(1), int(-1)], [int(3), int(2), int(1)]),
                                   ([int(3), int(1), int(1)], [])] {
            let meter = meter()
            let plan = try selected("generate_series", values: values, types: [.integer, .integer, .integer], ordinal: true)
            let source = try producer(plan, values, meter: meter)
            for (index, value) in expected.enumerated() {
                #expect(try await source.next()?.value.values == [value, .primitive(.int64(Int64(index + 1)))])
            }
            #expect(try await source.next() == nil)
            try await source.close()
            #expect(meter.retainedIntermediateBytes == 0 && meter.retainedIntermediateRows == 0)
        }
        for value in [Int64.max, Int64.min] {
            let values: [QueryValue] = [.primitive(.int64(value)), .primitive(.int64(value)), .primitive(.int64(value < 0 ? -1 : 1))]
            let plan = try selected("generate_series", values: values, types: [.bigint, .bigint, .bigint])
            let source = try producer(plan, values, meter: meter())
            #expect(try await source.next()?.value.values == [values[0]])
            #expect(try await source.next() == nil)
            try await source.close()
        }
    }

    @Test func strictNullSuppressesZeroStepAndZeroFailsTyped() async throws {
        let nullValues: [QueryValue] = [.primitive(.null), int(3), int(0)]
        let plan = try selected("generate_series", values: nullValues, types: [.integer, .integer, .integer])
        let source = try producer(plan, nullValues, meter: meter())
        #expect(try await source.next() == nil)
        try await source.close()
        let zeroValues = [int(1), int(3), int(0)]
        let zero = try selected("generate_series", values: zeroValues, types: [.integer, .integer, .integer])
        let at = QueryOccurrenceID(rawValue: 7)
        try expectSemanticFailure(.invalidFunctionArgument(function: "generate_series", reason: "step size cannot equal zero"), occurrence: at) {
            _ = try producer(locating(zero, at: at), zeroValues, meter: meter())
        }
        let decisive = QueryOccurrenceID(rawValue: 8)
        let selected = locating(zero, at: at)
        try expectSemanticFailure(.numericOverflow, occurrence: decisive) {
            throw sqlTableFunctionFailure(CanonicalReadError.expressionEvaluation(.numericOverflow, occurrence: decisive), selected: selected)
        }
        try expectSemanticFailure(.queryValue(.incompatibleType), occurrence: at) {
            throw sqlTableFunctionFailure(QueryValueError.incompatibleType, selected: selected)
        }
        try expectSemanticFailure(.timeZone(.unknownZone), occurrence: at) {
            throw sqlTableFunctionFailure(SQLTimeZoneError.unknownZone, selected: selected)
        }
    }

    @Test func numericSeriesKeepsScaleAndFiniteBoundary() async throws {
        let values = try [decimal(11, scale: 1), decimal(38, scale: 1), decimal(13, scale: 1)]
        let source = try producer(selected("generate_series", values: values, types: [numeric, numeric, numeric]), values, meter: meter())
        for value in try [decimal(11, scale: 1), decimal(24, scale: 1), decimal(37, scale: 1)] {
            #expect(try await source.next()?.value.values == [value])
        }
        #expect(try await source.next() == nil); try await source.close()
        let maximum = try #require(Int128("99999999999999999999999999999999999999"))
        let boundary = try [decimal(maximum), decimal(maximum), decimal(1)]
        let end = try producer(selected("generate_series", values: boundary, types: [numeric, numeric, numeric]), boundary, meter: meter())
        #expect(try await end.next()?.value.values == [boundary[0]])
        #expect(try await end.next() == nil); try await end.close()
        let wide = try #require(Int128("10000000000000000000000000000000000000"))
        let overflowValues = try [decimal(1, scale: 1), decimal(wide * 2), decimal(wide)]
        let at = QueryOccurrenceID(rawValue: 11)
        let overflowPlan = try locating(selected("generate_series", values: overflowValues, types: [numeric, numeric, numeric]), at: at)
        let overflowMeter = meter()
        let overflow = try producer(overflowPlan, overflowValues, meter: overflowMeter)
        #expect(try await overflow.next()?.value.values == [overflowValues[0]])
        for _ in 0..<2 {
            do { _ = try await overflow.next(); Issue.record("Expected finite-profile numeric overflow") }
            catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
                #expect(cause == .numericOverflow && occurrence == at)
            }
        }
        try await overflow.close()
        #expect(overflowMeter.retainedIntermediateBytes == 0 && overflowMeter.retainedIntermediateRows == 0)
    }

    @Test func arrayZipPaddingCompositeFieldsAndActualSubscripts() async throws {
        let descriptor = try QueryCompositeType(columns: [QueryColumn(number: 1, name: "a", type: .sql(.integer), isNullable: false), QueryColumn(number: 2, name: "b", type: .sql(.text), isNullable: true)], limits: limits)
        let composite = try QueryCompositeValue(type: descriptor, fields: [int(7), .primitive(.string("retained"))], limits: limits)
        let first = try QueryArrayValue(elementType: .composite(descriptor), dimensions: [.init(lowerBound: -4, length: 2)], elements: [.composite(composite), .primitive(.null)], limits: limits)
        let second = try QueryArrayValue(elementType: .integer, dimensions: [.init(lowerBound: 1, length: 1)], elements: [int(9)], limits: limits)
        let values: [QueryValue] = [.array(first), .array(second)]
        let plan = try selected("unnest", values: values, types: [.array(.composite(descriptor)), .array(.integer)], ordinal: true)
        #expect(plan.columns.map(\.name) == ["a", "b", "unnest", "ordinality"])
        let meter = meter(); let source = try producer(plan, values, meter: meter)
        var retained = try await source.next()
        #expect(retained?.value.values == [int(7), .primitive(.string("retained")), int(9), .primitive(.int64(1))])
        #expect(try await source.next()?.value.values == [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.int64(2))])
        try await source.close(); try await source.close()
        #expect(retained?.value.values[1] == .primitive(.string("retained")))
        retained = nil
        #expect(meter.retainedIntermediateBytes == 0 && meter.retainedIntermediateRows == 0)
        let subValues: [QueryValue] = [.array(first), int(1), .primitive(.bool(true))]
        let sub = try producer(selected("generate_subscripts", values: subValues, types: [.array(.composite(descriptor)), .integer, .boolean]), subValues, meter: meter)
        #expect(try await sub.next()?.value.values == [int(-3)])
        #expect(try await sub.next()?.value.values == [int(-4)])
        #expect(try await sub.next() == nil); try await sub.close()
    }

    @Test func scalarNullIsOneRowAndJSONKeepsDuplicateOrder() async throws {
        let scalar = try selected("abs", values: [.primitive(.null)], types: [.integer], ordinal: true)
        let one = try producer(scalar, [.primitive(.null)], meter: meter())
        #expect(try await one.next()?.value.values == [.primitive(.null), .primitive(.int64(1))])
        #expect(try await one.next() == nil); try await one.close()
        let json = try QueryJSONValue(bytes: ByteString(utf8: "{\"k\":1,\"k\":null}"), limits: limits)
        let input: [QueryValue] = [.json(json)]
        let source = try producer(selected("json_each_text", values: input, types: [.json]), input, meter: meter())
        #expect(try await source.next()?.value.values == [.primitive(.string("k")), .primitive(.string("1"))])
        #expect(try await source.next()?.value.values == [.primitive(.string("k")), .primitive(.null)])
        #expect(try await source.next() == nil); try await source.close()
        let wrong: [QueryValue] = [.json(try QueryJSONValue(bytes: ByteString(utf8: "[]"), limits: limits))]
        try expectSemanticFailure(.collection(.jsonObjectRootRequired(function: "json_each"))) {
            _ = try producer(selected("json_each", values: wrong, types: [.json]), wrong, meter: meter())
        }
    }

    @Test func patternGlobalCapturesSplitAndStrictNull() async throws {
        let values: [QueryValue] = [.primitive(.string("a1 a2")), .primitive(.string("a([0-9])")), .primitive(.string("g"))]
        let source = try producer(selected("regexp_matches", values: values, types: [.text, .text, .text]), values, meter: meter())
        for expected in ["1", "2"] {
            let row = try #require(await source.next())
            guard case .array(let array) = row.value.values[0] else { Issue.record("Expected captures"); return }
            #expect(array.elements.count == 1 && array.elements[0] == .primitive(.string(expected)))
        }
        #expect(try await source.next() == nil); try await source.close()
        let split: [QueryValue] = [.primitive(.string("ab")), .primitive(.string(""))]
        let pieces = try producer(selected("regexp_split_to_table", values: split, types: [.text, .text]), split, meter: meter())
        #expect(try await pieces.next()?.value.values == [.primitive(.string("a"))])
        #expect(try await pieces.next()?.value.values == [.primitive(.string("b"))])
        #expect(try await pieces.next() == nil); try await pieces.close()
        let null: [QueryValue] = [.primitive(.null), .primitive(.string("["))]
        let strict = try producer(selected("regexp_matches", values: null, types: [.text, .text]), null, meter: meter())
        #expect(try await strict.next() == nil); try await strict.close()
        let invalid: [QueryValue] = [.primitive(.string("a")), .primitive(.string("a")), .primitive(.string("z"))]
        try expectSemanticFailure(.invalidFunctionArgument(function: "regexp_matches", reason: "flag z at 0")) {
            _ = try producer(selected("regexp_matches", values: invalid, types: [.text, .text, .text]), invalid, meter: meter())
        }
    }

    @Test func temporalMonthsAndInfinityRemainDemandDriven() async throws {
        let meter = meter(), stamp = DataType.timestamp(withTimeZone: false)
        let values = try [temporal("2024-01-30", stamp, meter: meter), temporal("2024-04-30", stamp, meter: meter), temporal("1 month", .interval(), meter: meter)]
        let source = try producer(selected("generate_series", values: values, types: [stamp, stamp, .interval()]), values, meter: meter)
        for expected in ["2024-01-30", "2024-02-29", "2024-03-29", "2024-04-29"] {
            #expect(try await source.next()?.value.values == [temporal(expected, stamp, meter: meter)])
        }
        #expect(try await source.next() == nil); try await source.close()
        let infinite: [QueryValue] = [.timestampInfinity(.positive, withTimeZone: false), .timestampInfinity(.positive, withTimeZone: false), values[2]]
        let repeating = try producer(selected("generate_series", values: infinite, types: [stamp, stamp, .interval()]), infinite, meter: meter)
        for _ in 0..<3 { #expect(try await repeating.next()?.value.values == [infinite[0]]) }
        try await repeating.close()
        #expect(meter.retainedIntermediateBytes == 0 && meter.retainedIntermediateRows == 0)
    }

    @Test func explicitSeriesZonePreservesProviderAndBindingsAcrossDST() async throws {
        let meter = meter()
        let database = try SQLTimeZoneDatabase(definitions: [.init(name: "America/New_York", tzif: SQLTimeZoneFixtures.newYork())],
            provenance: ByteString(utf8: "S06 supplied fixture"),
            limits: .init(maximumInputBytes: 100_000, maximumRetainedBytes: 1_000_000, maximumTransitions: 10_000))
        let configuration = try SQLTemporalConfiguration(rulesProvider: database,
            abbreviations: [.init(name: "Custom", offsetSeconds: 1234)],
            limits: .init(maximumAbbreviations: 1, maximumRetainedBytes: 15))
        let replaced = configuration.replacing(timeZone: .fixed(offsetSeconds: 3600))
        #expect(replaced.retainedByteCount == configuration.retainedByteCount)
        #expect(try replaced.fixedOffset(forAbbreviation: "Custom", workMeter: meter) == 1234)
        #expect(try replaced.rulesProvider?.rules(named: "America/New_York", workMeter: meter) != nil)
        let base = environment()
        let env = SQLTemporalEnvironment(transactionInstant: base.transactionInstant, statementInstant: base.statementInstant,
            wallClock: base.wallClock, configuration: configuration)
        let stamp = DataType.timestamp(withTimeZone: true)
        let values = try [temporal("2024-03-09 17:00+00", stamp, meter: meter),
            temporal("2024-03-12 16:00+00", stamp, meter: meter), temporal("1 day", .interval(), meter: meter),
            .primitive(.string("America/New_York"))]
        let source = try producer(selected("generate_series", values: values, types: [stamp, stamp, .interval(), .text]), values, meter: meter, environment: env)
        for expected in ["2024-03-09 17:00+00", "2024-03-10 16:00+00", "2024-03-11 16:00+00", "2024-03-12 16:00+00"] {
            #expect(try await source.next()?.value.values == [temporal(expected, stamp, meter: meter)])
        }
        #expect(try await source.next() == nil); try await source.close()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test func exactAndOnePastBytesRowsWorkAndDeadlineRelease() async throws {
        let values = [int(1), int(2)]
        let plan = try selected("generate_series", values: values, types: [.integer, .integer])
        func run(_ meter: DatabaseWorkMeter) async throws {
            let source = try producer(plan, values, meter: meter)
            do {
                _ = try await source.next()
                try await source.close()
            } catch { try await source.close(); throw error }
        }
        let measured = meter(); try await run(measured)
        #expect(measured.retainedIntermediateBytes == 0 && measured.retainedIntermediateRows == 0)
        let exact = meter(bytes: measured.peakIntermediateBytes, work: measured.consumedWorkUnits)
        try await run(exact)
        #expect(exact.retainedIntermediateBytes == 0 && exact.retainedIntermediateRows == 0)
        for constrained in [meter(bytes: measured.peakIntermediateBytes - 1), meter(rows: 0), meter(work: measured.consumedWorkUnits - 1)] {
            await #expect(throws: DatabaseWorkLimitError.self) { try await run(constrained) }
            #expect(constrained.retainedIntermediateBytes == 0 && constrained.retainedIntermediateRows == 0)
        }
        let clock = SQLArrayTestClock()
        let expired = SQLArrayFixture.meter(clock: clock)
        let source = try producer(plan, values, meter: expired)
        clock.advance()
        await #expect(throws: DatabaseWorkLimitError.self) { _ = try await source.next() }
        try await source.close()
        #expect(expired.retainedIntermediateBytes == 0 && expired.retainedIntermediateRows == 0)
    }

    @Test func admissionAndCancellationReleaseAndFailureStaysTyped() async throws {
        let values = [int(1), int(100)]
        let plan = try selected("generate_series", values: values, types: [.integer, .integer])
        let constrained = meter(bytes: 1)
        #expect(throws: DatabaseWorkLimitError.self) { _ = try producer(plan, values, meter: constrained) }
        #expect(constrained.retainedIntermediateBytes == 0)
        let cancelledMeter = meter(); let source = try producer(plan, values, meter: cancelledMeter)
        let cancelled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await source.next()
        }
        await #expect(throws: CancellationError.self) { _ = try await cancelled.value }
        await #expect(throws: CancellationError.self) { _ = try await source.next() }
        try await source.close()
        #expect(cancelledMeter.retainedIntermediateBytes == 0 && cancelledMeter.retainedIntermediateRows == 0)
        let followup = try producer(plan, values, meter: cancelledMeter)
        #expect(try await followup.next()?.value.values == [int(1)])
        try await followup.close()
    }
}
