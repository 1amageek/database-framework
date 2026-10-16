import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL temporal kernel")
struct SQLTemporalKernelTests {
    private struct Clock: WallClock {
        let instant: Timestamp
        var now: Timestamp { instant }
    }
    private struct Monotonic: StorageMonotonicClock {
        var now: StorageInstant { StorageInstant(durationSinceReference: .zero) }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
    }
    private func meter(work: UInt64 = 1_000_000, bytes: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: work,
            maximumIntermediateRows: 100, maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: Monotonic())
    }
    private func environment() throws -> SQLTemporalEnvironment {
        let instant = try Timestamp(secondsSinceUnixEpoch: 946_684_800, nanoseconds: 123_456_789)
        return SQLTemporalEnvironment(transactionInstant: instant,
            statementInstant: Timestamp(secondsSinceUnixEpoch: 946_684_801), wallClock: Clock(instant: Timestamp(secondsSinceUnixEpoch: 946_684_802)))
    }
    private func cast(_ text: String, _ type: DataType, meter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try CanonicalSQLTemporalEvaluator.cast(.primitive(.string(text)), from: .sql(.text), to: .sql(type), context: .explicit,
            environment: environment(), workMeter: meter ?? self.meter())
    }
    private func scalar(_ value: QueryValue, _ type: DataType) throws -> SQLTemporalIdentity {
        try CanonicalSQLTemporalEvaluator.identity(value, type: .sql(type), environment: environment(), workMeter: meter())
    }
    private func function(_ name: String, _ values: [QueryValue], _ types: [DataType], result: DataType) throws -> QueryValue {
        let children = zip(values, types).map { value, type in
            CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(type))), type: .sql(type), isNullable: value.isNull)
        }
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name, arguments: children.map { $0.expression })), type: .sql(result), isNullable: true, children: children)
        return try CanonicalSQLTemporalEvaluator.evaluate(bound, operands: values, environment: environment(), workMeter: meter())
    }
    private func operation(_ expression: DatabaseKit.Expression, _ values: [QueryValue], _ types: [DataType], result: DataType) throws -> QueryValue {
        let children = zip(values, types).map { value, type in CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(type))), type: .sql(type), isNullable: false) }
        return try CanonicalSQLTemporalEvaluator.evaluate(CanonicalBoundExpression(expression: expression, type: .sql(result), isNullable: false, children: children), operands: values, environment: environment(), workMeter: meter())
    }
    private let zero = DatabaseKit.Expression.literal(.int(0))

    @Test func gregorianRoundTrip() throws {
        for (year, month, day) in [(Int32(-4713), Int32(11), Int32(24)), (0, 2, 29), (1900, 3, 1), (2000, 2, 29), (5_874_897, 12, 31)] {
            let jd = try SQLGregorianCalendar.julianDay(year: year, month: month, day: day)
            let f = try SQLGregorianCalendar.fields(julianDay: jd)
            #expect(f.year == year && f.month == month && f.day == day)
        }
        #expect(try SQLGregorianCalendar.julianDay(year: 2000, month: 1, day: 1) == 2_451_545)
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try SQLGregorianCalendar.julianDay(year: 1900, month: 2, day: 29) }
    }
    @Test func exactTimestampEndpoints() throws {
        for micros: Int128 in [-211_813_488_000_000_000, 9_223_371_331_199_999_999] {
            let instant = try SQLGregorianCalendar.instant(pgMicroseconds: micros)
            #expect(SQLGregorianCalendar.microseconds(instant) == micros)
            let fields = try SQLGregorianCalendar.fields(pgMicroseconds: micros)
            #expect(try SQLGregorianCalendar.microseconds(fields) == micros)
        }
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try SQLGregorianCalendar.instant(pgMicroseconds: 9_223_371_331_200_000_000) }
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try SQLGregorianCalendar.instant(pgMicroseconds: -211_813_488_000_000_001) }
    }
    @Test func lexicalAndTypmodTies() throws {
        #expect(try scalar(cast("2000-01-01 00:00:00.0000005", .timestamp(withTimeZone: false)), .timestamp(withTimeZone: false)).scalar == 0)
        #expect(try scalar(cast("2000-01-01 00:00:00.0000015", .timestamp(withTimeZone: false)), .timestamp(withTimeZone: false)).scalar == 2)
        #expect(try SQLTemporalPrecision.round(-500_000, precision: 0) == -1_000_000)
        #expect(try SQLTemporalPrecision.round(500_000, precision: 0) == 1_000_000)
        #expect(try SQLTemporalPrecision.round(-946_684_799_500_000, precision: 0) == -946_684_800_000_000)
    }
    @Test func documentedCommonDateForms() throws {
        let expected = try cast("2000-01-02", .date)
        for text in ["20000102", "2000/01/02", "2000.002", "2000002", "Jan 2 2000", "January 2 2000", "2 January 2000", "2000-Jan-02", "J2451546"] {
            #expect(try cast(text, .date) == expected, "\(text)")
        }
        #expect(try cast("0001-01-01 BC", .date) == .primitive(.date(CivilDate(year: 0, month: 1, day: 1))))
    }
    @Test func timeEndpointAndCompactForms() throws {
        #expect(try scalar(cast("24:00:00", .time(withTimeZone: false)), .time(withTimeZone: false)).scalar == 86_400_000_000)
        #expect(try cast("123456.25", .time(withTimeZone: false)) == cast("12:34:56.25", .time(withTimeZone: false)))
        #expect(try cast("12:00 AM", .time(withTimeZone: false)) == cast("00:00", .time(withTimeZone: false)))
        #expect(try cast("1:00 PM", .time(withTimeZone: false)) == cast("13:00", .time(withTimeZone: false)))
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try cast("24:00:01", .time(withTimeZone: false)) }
    }
    @Test func lexicalZoneSigns() throws {
        #expect(try scalar(cast("2000-01-01 12:00+05:30", .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == 23_400_000_000)
        #expect(try cast("2000-01-01 12:00+05:30", .timestamp(withTimeZone: false)) == cast("2000-01-01 12:00", .timestamp(withTimeZone: false)))
        #expect(try SQLTemporalInput.numericOffset("+16"[...], posix: true, workMeter: meter()) == -57_600)
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try SQLTemporalInput.numericOffset("+16"[...], posix: false, workMeter: meter()) }
    }
    @Test func relativeInputsUseTransactionSnapshot() throws {
        #expect(try cast("today", .date) == cast("2000-01-01", .date))
        #expect(try cast("yesterday", .date) == cast("1999-12-31", .date))
        #expect(try cast("tomorrow", .date) == cast("2000-01-02", .date))
        #expect(try scalar(cast("epoch", .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == -946_684_800_000_000)
        #expect(try scalar(cast("now", .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == 123456)
    }
    @Test func intervalInputGrammars() throws {
        let expected = try cast("1 year 2 mons 3 days 04:05:06.5", .interval())
        #expect(try cast("P1Y2M3DT4H5M6.5S", .interval()) == expected)
        #expect(try cast("P0001-02-03T04:05:06.5", .interval()) == expected)
        #expect(try cast("P00010203T040506.5", .interval()) == expected)
        #expect(try cast("1-2 3 04:05:06.5", .interval()) == expected)
        let fraction = try #require({ if case .interval(let span) = try cast("1.75 months", .interval()) { return span }; return nil }())
        #expect(fraction.period.months == 1 && fraction.period.days == 22 && fraction.microseconds == 43_200_000_000)
        #expect(try cast("-00:30:00", .interval()) == cast("-30 minutes", .interval()))
        #expect(try cast("-0-2", .interval()) == cast("-2 months", .interval()))
        #expect(try cast("-00:30:00 ago", .interval()) == cast("30 minutes", .interval()))
        #expect(try cast("-0-2 ago", .interval()) == cast("2 months", .interval()))
        #expect(try cast("1:02", .interval(fields: .minuteToSecond)) == cast("62 seconds", .interval()))
        #expect(try cast("1:02.5", .interval()) == cast("62.5 seconds", .interval()))
        #expect(try cast("-0:02.5", .interval()) == cast("-2.5 seconds", .interval()))
        #expect(try cast("1:02", .interval()) == cast("1 hour 2 minutes", .interval()))
    }
    @Test func everyIntervalRange() throws {
        for range in SQLIntervalFields.allCases {
            let precision: Int? = range == .second || range == .dayToSecond || range == .hourToSecond || range == .minuteToSecond ? 2 : nil
            let value = try cast("1 year 2 months 3 days 04:05:06.789", .interval(fields: range, precision: precision))
            #expect(QueryValueValidator.accepts(value, type: .sql(.interval(fields: range, precision: precision))))
        }
        #expect(try cast("-0.5 seconds", .interval(precision: 0)) == cast("-1 second", .interval()))
    }
    @Test func semanticIntervalAndTimetzIdentity() throws {
        let month = try scalar(cast("1 month", .interval()), .interval())
        let days = try scalar(cast("30 days", .interval()), .interval())
        #expect(month == days && month.hashValue == days.hashValue)
        let a = try scalar(cast("12:00+02", .time(withTimeZone: true)), .time(withTimeZone: true))
        let b = try scalar(cast("10:00+00", .time(withTimeZone: true)), .time(withTimeZone: true))
        #expect(a.scalar == b.scalar && a < b && a != b)
    }
    @Test func monthEndAndElapsedArithmetic() throws {
        let result = try operation(.add(zero, zero), [cast("2024-01-31", .date), cast("1 month", .interval())], [.date, .interval()], result: .timestamp(withTimeZone: false))
        #expect(try result == cast("2024-02-29", .timestamp(withTimeZone: false)))
        let a = try cast("2000-01-02 01:00", .timestamp(withTimeZone: false))
        let b = try cast("2000-01-01 00:00", .timestamp(withTimeZone: false))
        #expect(try operation(.subtract(zero, zero), [a,b], [.timestamp(withTimeZone: false), .timestamp(withTimeZone: false)], result: .interval()) == cast("1 day 1 hour", .interval()))
    }
    @Test func intervalScalingRetainsComponents() throws {
        let span = try #require({ if case .interval(let span) = try cast("1 month 1 day 1 second", .interval()) { return span }; return nil }())
        let scaled = try SQLTemporalArithmetic.scale(span, factor: 1.5, dividing: false)
        #expect(scaled.period.months == 1 && scaled.period.days == 16 && scaled.microseconds == 43_201_500_000)
    }
    @Test func extractValueScaleAndFieldMeaning() throws {
        let stamp = try cast("2000-01-02 03:04:12.345678", .timestamp(withTimeZone: false))
        let seconds = try function("extract", [.primitive(.string("second")), stamp], [.text, .timestamp(withTimeZone: false)], result: .decimal(precision: nil, scale: nil))
        #expect(try seconds == .numeric(QueryNumericValue(value: ExactDecimal(coefficient: 12_345_678, scale: 6), displayScale: 6)))
        let span = try cast("1 year 1 month", .interval())
        #expect(try function("date_part", [.primitive(.string("epoch")), span], [.text, .interval()], result: .doublePrecision) == .primitive(.float64((365.25 + 30) * 86400)))
    }
    @Test func infinityResultsAndProfileFailure() throws {
        #expect(try function("extract", [.primitive(.string("day")), .timestampInfinity(.positive, withTimeZone: false)], [.text, .timestamp(withTimeZone: false)], result: .decimal(precision: nil, scale: nil)) == .primitive(.null))
        #expect(throws: DatabaseExpressionEvaluationError.nonFiniteValue) { try function("extract", [.primitive(.string("epoch")), .dateInfinity(.positive)], [.text, .date], result: .decimal(precision: nil, scale: nil)) }
        #expect(try cast("-infinity", .interval()) == .intervalInfinity(.negative))
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try operation(.subtract(zero, zero), [.intervalInfinity(.positive), .intervalInfinity(.positive)], [.interval(), .interval()], result: .interval()) }
    }
    @Test func binFloorsBeforeOrigin() throws {
        let result = try function("date_bin", [cast("15 minutes", .interval()), cast("1999-12-31 23:59:59.999999", .timestamp(withTimeZone: false)), cast("2000-01-01", .timestamp(withTimeZone: false))], [.interval(), .timestamp(withTimeZone: false), .timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: false))
        #expect(try result == cast("1999-12-31 23:45", .timestamp(withTimeZone: false)))
    }
    @Test func symbolicAgeUsesEarlierMonth() throws {
        #expect(try function("age", [cast("2004-06-01", .timestamp(withTimeZone: false)), cast("2004-04-30", .timestamp(withTimeZone: false))], [.timestamp(withTimeZone: false), .timestamp(withTimeZone: false)], result: .interval()) == cast("1 month 1 day", .interval()))
    }
    @Test func constructorsAndTruncation() throws {
        #expect(try function("make_date", [.primitive(.int32(-1)), .primitive(.int32(2)), .primitive(.int32(29))], [.integer,.integer,.integer], result: .date) == cast("0001-02-29 BC", .date))
        #expect(try function("make_time", [.primitive(.int32(24)), .primitive(.int32(0)), .primitive(.float64(0))], [.integer,.integer,.doublePrecision], result: .time(withTimeZone: false)) == cast("24:00", .time(withTimeZone: false)))
        #expect(try function("date_trunc", [.primitive(.string("week")), cast("2021-01-01 13:45", .timestamp(withTimeZone: false))], [.text,.timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: false)) == cast("2020-12-28", .timestamp(withTimeZone: false)))
        for (input, expected) in [("2024-06-01", "2020-01-01"), ("0001-06-01 BC", "0001-01-01 BC"), ("0002-06-01 BC", "0011-01-01 BC")] {
            #expect(try function("date_trunc", [.primitive(.string("decade")), cast(input, .timestamp(withTimeZone: false))], [.text,.timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: false)) == cast(expected, .timestamp(withTimeZone: false)))
        }
    }
    @Test func clockBoundariesAndMicrosecondFloor() throws {
        #expect(try scalar(function("now", [], [], result: .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == 123456)
        #expect(try scalar(function("statement_timestamp", [], [], result: .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == 1_000_000)
        #expect(try scalar(function("clock_timestamp", [], [], result: .timestamp(withTimeZone: true)), .timestamp(withTimeZone: true)).scalar == 2_000_000)
    }
    @Test func syntheticCalendarRejectsUndefinedArithmetic() throws {
        #expect(try SQLGregorianCalendar.syntheticJulianDay(year: 2000, month: 1, day: 1) == 2451545)
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try SQLGregorianCalendar.syntheticJulianDay(year: Int32.max / 12, month: 11, day: Int32.max) }
    }
    @Test func workFailureReleasesAllScratch() throws {
        let workMeter = meter(work: 2)
        do { _ = try cast("2000-01-01 12:34:56", .timestamp(withTimeZone: false), meter: workMeter); Issue.record("Expected work failure") }
        catch is DatabaseWorkLimitError { }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
    @Test func cancellationPropagates() async throws {
        let workMeter = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try cast("2000-01-01", .date, meter: workMeter)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
    @Test func namedZoneGapFoldAndNonHourTransition() throws {
        let workMeter = meter()
        let database = try SQLTimeZoneDatabase(definitions: [
            .init(name: "America/New_York", tzif: SQLTimeZoneFixtures.newYork()),
            .init(name: "Australia/Lord_Howe", tzif: SQLTimeZoneFixtures.lordHowe())],
            provenance: ByteString(utf8: "IANA2026e fixture manifest a9e6c102"),
            limits: .init(maximumInputBytes: 100000, maximumRetainedBytes: 1000000, maximumTransitions: 10000))
        for (name, input, expected) in [
            ("America/New_York", "2024-03-10 02:30", "2024-03-10 07:30+00"),
            ("America/New_York", "2024-11-03 01:30", "2024-11-03 06:30+00"),
            ("America/New_York", "2024-03-10 02:30 America/New_York", "2024-03-10 07:30+00"),
            ("America/New_York", "2024-11-03 01:30 America/New_York", "2024-11-03 06:30+00"),
            ("Australia/Lord_Howe", "2024-10-06 02:15", "2024-10-05 15:45+00"),
            ("Australia/Lord_Howe", "2024-04-07 01:45", "2024-04-06 15:15+00")] {
            let rules = try database.rules(named: name, workMeter: workMeter)
            let configuration = try SQLTemporalConfiguration(timeZone: .named(rules), rulesProvider: database,
                limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0))
            let base = try environment()
            let env = SQLTemporalEnvironment(transactionInstant: base.transactionInstant, statementInstant: base.statementInstant, wallClock: base.wallClock, configuration: configuration)
            let actual = try CanonicalSQLTemporalEvaluator.cast(.primitive(.string(input)), from: .sql(.text), to: .sql(.timestamp(withTimeZone: true)), context: .explicit, environment: env, workMeter: workMeter)
            #expect(try actual == cast(expected, .timestamp(withTimeZone: true)))
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
    @Test func calendarDayDiffersFromTwentyFourHours() throws {
        let workMeter = meter()
        let database = try SQLTimeZoneDatabase(definitions: [.init(name: "America/New_York", tzif: SQLTimeZoneFixtures.newYork())],
            provenance: ByteString(utf8: "IANA2026e fixture manifest a9e6c102"),
            limits: .init(maximumInputBytes: 100000, maximumRetainedBytes: 1000000, maximumTransitions: 10000))
        let rules = try database.rules(named: "America/New_York", workMeter: workMeter)
        let base = try environment()
        let env = SQLTemporalEnvironment(transactionInstant: base.transactionInstant, statementInstant: base.statementInstant, wallClock: base.wallClock,
            configuration: try SQLTemporalConfiguration(timeZone: .named(rules), rulesProvider: database, limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0)))
        let start = try cast("2024-03-09 12:00-05", .timestamp(withTimeZone: true))
        let day = try CanonicalSQLTemporalEvaluator.interval(cast("1 day", .interval()))
        let hours = try CanonicalSQLTemporalEvaluator.interval(cast("24 hours", .interval()))
        let a = try SQLTemporalArithmetic.adding(day, to: start, type: .sql(.timestamp(withTimeZone: true)), environment: env, workMeter: workMeter)
        let b = try SQLTemporalArithmetic.adding(hours, to: start, type: .sql(.timestamp(withTimeZone: true)), environment: env, workMeter: workMeter)
        #expect(try a == cast("2024-03-10 16:00+00", .timestamp(withTimeZone: true)))
        #expect(try b == cast("2024-03-10 17:00+00", .timestamp(withTimeZone: true)))
    }
    @Test func remainingConstructorAndEpochOverloads() throws {
        let parts: [QueryValue] = [.primitive(.int32(2000)), .primitive(.int32(1)), .primitive(.int32(2)), .primitive(.int32(3)), .primitive(.int32(4)), .primitive(.float64(5.25))]
        let types: [DataType] = [.integer,.integer,.integer,.integer,.integer,.doublePrecision]
        #expect(try function("make_timestamp", parts, types, result: .timestamp(withTimeZone: false)) == cast("2000-01-02 03:04:05.25", .timestamp(withTimeZone: false)))
        #expect(try function("make_timestamptz", parts + [.primitive(.string("+05:30"))], types + [.text], result: .timestamp(withTimeZone: true)) == cast("2000-01-02 08:34:05.25+00", .timestamp(withTimeZone: true)))
        let intervalParts: [QueryValue] = [.primitive(.int32(1)), .primitive(.int32(2)), .primitive(.int32(3)), .primitive(.int32(4)), .primitive(.int32(5)), .primitive(.int32(6)), .primitive(.float64(7.25))]
        #expect(try function("make_interval", intervalParts, [.integer,.integer,.integer,.integer,.integer,.integer,.doublePrecision], result: .interval()) == cast("1 year 2 months 25 days 05:06:07.25", .interval()))
        #expect(try function("to_timestamp", [.primitive(.float64(0))], [.doublePrecision], result: .timestamp(withTimeZone: true)) == cast("epoch", .timestamp(withTimeZone: true)))
    }
    @Test func timezoneOverloadsAndIntervalRestrictions() throws {
        let local = try cast("2000-01-01 12:00", .timestamp(withTimeZone: false))
        #expect(try function("timezone", [.primitive(.string("+05:30")), local], [.text,.timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: true)) == cast("2000-01-01 17:30+00", .timestamp(withTimeZone: true)))
        #expect(try function("timezone", [cast("5 hours 30 minutes", .interval()), local], [.interval(),.timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: true)) == cast("2000-01-01 06:30+00", .timestamp(withTimeZone: true)))
        do { _ = try function("timezone", [cast("1 day", .interval()), local], [.interval(),.timestamp(withTimeZone: false)], result: .timestamp(withTimeZone: true)); Issue.record("Expected invalid interval zone") }
        catch let error as DatabaseExpressionEvaluationError { if case .invalidFunctionArgument = error { } else { throw error } }
    }
    @Test func invalidPrimitivePrecisionAndInfinityBridge() throws {
        let workMeter = meter()
        let env = try environment()
        let source = QueryValue.primitive(.time(try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 1)))
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
            try CanonicalSQLTemporalEvaluator.cast(source, from: .primitive(.time, isArray: false), to: .sql(.time(withTimeZone: false)), context: .implicit, environment: env, workMeter: workMeter)
        }
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
            try CanonicalSQLTemporalEvaluator.cast(.timestampInfinity(.positive, withTimeZone: true), from: .sql(.timestamp(withTimeZone: true)), to: .primitive(.timestamp, isArray: false), context: .assignment, environment: env, workMeter: workMeter)
        }
    }
    @Test func strictNullSuppressesInvalidFieldExecution() throws {
        #expect(try function("extract", [.primitive(.string("invalid-unit")), .primitive(.null)], [.text,.timestamp(withTimeZone: false)], result: .decimal(precision: nil, scale: nil)) == .primitive(.null))
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try cast("0000-01-01", .date) }
        #expect(throws: DatabaseExpressionEvaluationError.invalidTemporalInput) { try cast("2000-13", .date) }
        for input in [
            "-170141183460469231731687303715884105727 us -1 us ago",
            "-170141183460469231731687303715884105727 days -1 day ago",
            "-170141183460469231731687303715884105727 months -1 month ago"] {
            let workMeter = meter()
            #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) { try cast(input, .interval(), meter: workMeter) }
            #expect(workMeter.retainedIntermediateBytes == 0 && workMeter.retainedIntermediateRows == 0)
        }
    }

}
