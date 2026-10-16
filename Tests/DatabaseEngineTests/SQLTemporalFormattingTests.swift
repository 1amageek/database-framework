import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("SQL temporal C/POSIX formatting", .timeLimit(.minutes(1)))
struct SQLTemporalFormattingTests {
    private struct Clock: WallClock {
        let now: Timestamp
    }

    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: work,
            maximumIntermediateRows: 100, maximumIntermediateBytes: bytes, timeoutMilliseconds: 10_000),
            monotonicClock: SystemStorageClock())
    }

    private func environment(_ configuration: SQLTemporalConfiguration = .utc) throws -> SQLTemporalEnvironment {
        let instant = Timestamp(secondsSinceUnixEpoch: 1_704_067_200)
        return SQLTemporalEnvironment(transactionInstant: instant, statementInstant: instant,
            wallClock: Clock(now: instant), configuration: configuration)
    }

    private func dateTime(_ year: Int32 = 2024, _ month: UInt8 = 1, _ day: UInt8 = 2,
        _ hour: UInt8 = 13, _ minute: UInt8 = 4, _ second: UInt8 = 5,
        _ micros: UInt32 = 123456) throws -> QueryValue {
        .primitive(.dateTime(CivilDateTime(date: try CivilDate(year: year, month: month, day: day),
            time: try CivilTime(hour: hour, minute: minute, second: second, nanoseconds: micros * 1000))))
    }

    private func execute(_ name: String, _ values: [QueryValue], _ types: [QueryColumnType],
        environment: SQLTemporalEnvironment? = nil, meter: DatabaseWorkMeter? = nil)
        throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let children = zip(values, types).map {
            CanonicalBoundExpression(expression: .literal(.queryValue($0.0, type: $0.1)),
                type: $0.1, isNullable: $0.0.isNull)
        }
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name,
            arguments: children.map(\.expression))), type: name == "to_date" ? .sql(.date) :
                name == "to_timestamp" ? .sql(.timestamp(withTimeZone: true)) : .sql(.text),
            isNullable: true, children: children)
        return try SQLTemporalFormatting.evaluate(bound, operands: values,
            environment: environment ?? self.environment(), workMeter: meter ?? self.meter(),
            limits: QueryValueLimits(maximumRetainedBytes: 1_000_000, maximumTotalNodes: 100,
                maximumNestingDepth: 10, maximumCollectionElements: 100))
    }

    private func format(_ value: QueryValue, _ template: String,
        type: QueryColumnType = .sql(.timestamp(withTimeZone: false)),
        environment: SQLTemporalEnvironment? = nil, meter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try execute("to_char", [value, .primitive(.string(template))], [type, .sql(.text)],
            environment: environment, meter: meter).value
    }

    private func parsed(_ input: String, _ template: String,
        environment: SQLTemporalEnvironment? = nil) throws -> SQLTemporalFormatParser.Parsed {
        try SQLTemporalFormatParser.parse(input[...], template: template[...],
            environment: environment ?? self.environment(), workMeter: meter())
    }

    private func scalar(_ value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment? = nil, meter: DatabaseWorkMeter? = nil) throws -> String {
        try SQLTemporalFormatting.withText(of: value, type: type,
            environment: environment ?? self.environment(), workMeter: meter ?? self.meter()) { String($0) }
    }

    @Test("Gregorian and ISO numeric tokens preserve boundary conventions")
    func numericTokens() throws {
        #expect(try format(dateTime(), "YYYY YYY YY Y Y,YYY CC") == .primitive(.string("2024 024 24 4 2,024 21")))
        #expect(try format(dateTime(), "HH HH12 HH24 MI SS MS US FF1 FF2 FF3 FF4 FF5 FF6 SSSS SSSSS") ==
            .primitive(.string("01 01 13 04 05 123 123456 1 12 123 1234 12345 123456 47045 47045")))
        let boundary = try dateTime(2021, 1, 1, 0, 0, 0, 0)
        #expect(try format(boundary, "IYYY IYY IY I IW ID IDDD DDD D DD WW W J Q") ==
            .primitive(.string("2020 020 20 0 53 5 369 001 6 01 01 1 2459216 1")))
        #expect(try format(dateTime(), "yyyy-mm-dd hh24:mi:ss") == .primitive(.string("2024-01-02 13:04:05")))
    }

    @Test("C names, era, meridiem, roman months and modifiers are deterministic")
    func namesAndModifiers() throws {
        let value = try dateTime()
        #expect(try format(value, "MONTH|Month|month|MON|Mon|mon") ==
            .primitive(.string("JANUARY  |January  |january  |JAN|Jan|jan")))
        #expect(try format(value, "DAY|Day|day|DY|Dy|dy|RM|rm") ==
            .primitive(.string("TUESDAY  |Tuesday  |tuesday  |TUE|Tue|tue|I   |i   ")))
        #expect(try format(value, "AM am A.M. a.m. AD ad A.D. a.d.") ==
            .primitive(.string("PM pm P.M. p.m. AD ad A.D. a.d.")))
        #expect(try format(value, "FMDD DD|TMMonth|FMUS|DDTH DDth DDSP") ==
            .primitive(.string("2 02|January|123456|02ND 02nd 02")))
        #expect(try format(dateTime(0), "YYYY BC B.C. CC") == .primitive(.string("0001 BC B.C. -01")))
    }

    @Test("Quoted Unicode, escaping and unknown letters are literal grammar")
    func literalGrammar() throws {
        #expect(try format(dateTime(), "\"日付 Year \"YYYY \"a\\\"b\" \\\"FX") ==
            .primitive(.string("日付 Year 2024 a\"b \"")))
        #expect(try format(dateTime(), "\"unterminated YYYY") == .primitive(.string("unterminated YYYY")))
        #expect(try format(dateTime(), "xzz") == .primitive(.string("xzz")))
        #expect(try format(dateTime(), "xyz") == .primitive(.string("x4z")))
    }

    @Test("Interval component tokens preserve signs and extended hours")
    func intervals() throws {
        let interval = try QueryIntervalValue(period: CalendarPeriod(months: 14, days: -3),
            time: TimeSpan(seconds: 176_523, nanoseconds: 123_456_000))
        #expect(try format(.interval(interval), "YYYY-MM DD HH24:MI:SS.US", type: .sql(.interval())) ==
            .primitive(.string("0001-02 -3 49:02:03.123456")))
        #expect(try format(.interval(interval), "HH HH12 SSSS DDD CC Q", type: .sql(.interval())) ==
            .primitive(.string("01 01 176523 417 00 1")))
        for token in ["BC", "AD", "MONTH", "MON", "DAY", "DY", "D", "ID", "TZ", "TZH", "TZM", "OF"] {
            #expect(throws: DatabaseExpressionEvaluationError.self) {
                try format(.interval(interval), token, type: .sql(.interval()))
            }
        }
        let extreme = try QueryIntervalValue(period: CalendarPeriod(days: Int64(Int32.max)), time: TimeSpan(seconds: 0))
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
            try format(.interval(extreme), "J", type: .sql(.interval()))
        }
        #expect(try scalar(.interval(extreme), type: .sql(.interval())) == "2147483647 days")
        let exactOrdinal = try QueryIntervalValue(period: CalendarPeriod(months: 71_582_788, days: 7), time: TimeSpan(seconds: 0))
        #expect(try format(.interval(exactOrdinal), "DDD", type: .sql(.interval())) == .primitive(.string("2147483647")))
        let overflowOrdinal = try QueryIntervalValue(period: CalendarPeriod(months: 71_582_788, days: 8), time: TimeSpan(seconds: 0))
        for token in ["DDD", "WW"] {
            #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
                try format(.interval(overflowOrdinal), token, type: .sql(.interval()))
            }
        }
    }

    @Test("Template years preserve partial-year pivots, BC inversion and century precedence")
    func inputYears() throws {
        for (input, year) in [("69", 2069), ("70", 1970), ("99", 1999), ("100", 2100),
            ("519", 2519), ("520", 1520), ("999", 1999), ("1000", 1000)] {
            #expect(try parsed(input, "YYY").fields.year == Int32(year))
        }
        #expect(try parsed("0-01-02", "YYYY-MM-DD").fields.year == 0)
        #expect(try parsed("-44 BC", "YYYY BC").fields.year == 44)
        #expect(try parsed("44 BC", "YYYY BC").fields.year == -43)
        #expect(try parsed("21 00", "CC YY").fields.year == 2100)
        #expect(try parsed("21", "CC").fields.year == 2001)
        #expect(try parsed("19 2024", "CC YYYY").fields.year == 2024)
        #expect(try parsed("20000-1130", "YYYY-MMDD").fields.year == 20000)
        #expect(try parsed("20001130", "YYYYMMDD").fields.year == 2000)
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
            try parsed("200001130", "YYYYMMDD")
        }
    }

    @Test("Gregorian, ISO, ordinal, Julian and ignored input fields follow the shared parser")
    func inputDates() throws {
        let iso = try parsed("2006-42-4", "IYYY-IW-ID").fields
        #expect(iso.year == 2006 && iso.month == 10 && iso.day == 19)
        let ordinal = try parsed("2006-291", "IYYY-IDDD").fields
        #expect(ordinal.year == 2006 && ordinal.month == 10 && ordinal.day == 19)
        #expect(try parsed("2006-42", "IYYY-IW").fields.day == 16)
        #expect(try parsed("2024-060", "YYYY-DDD").fields.month == 2)
        #expect(try parsed("2459216", "J").fields.year == 2021)
        #expect(try parsed("2024 1 12 25 Monday", "YYYY Q MM DD DAY").fields.day == 25)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024-02-30", "YYYY-MM-DD") }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024-12-25", "IYYY-MM-DD") }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024 2025", "YYYY YYYY") }
    }

    @Test("Fraction, meridiem and time field input validate before construction")
    func inputClock() throws {
        #expect(try parsed("12.3", "SS.US").fields.microsecond == 300000)
        #expect(try parsed("12.3", "SS.MS").fields.microsecond == 300000)
        #expect(try parsed("15:12:02.020.001230", "HH24:MI:SS.MS.US").fields.microsecond == 21230)
        #expect(try parsed("12 AM", "HH12 AM").fields.hour == 0)
        #expect(try parsed("12 PM", "HH12 AM").fields.hour == 12)
        #expect(try parsed("1 PM", "FMHH12 AM").fields.hour == 13)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("24", "HH24") }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("0", "HH12") }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("12.1234567", "SS.US") }
    }

    @Test("FX and literal skipping retain PostgreSQL SQL-function parser semantics")
    func inputSeparators() throws {
        #expect(try parsed("2024/01/02", "FXYYYY-MM-DD").fields.day == 2)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024//01/02", "FXYYYY-MM-DD") }
        #expect(try parsed("2024AB01CD02", "YYYY\"zz\"MM\"xx\"DD").fields.day == 2)
        #expect(try parsed(" 2024 - JAN - 02  ", "YYYY-MON-DD").fields.day == 2)
        #expect(try parsed("2024", "YYYY-MM-DD").fields.month == 1)
        #expect(try parsed("2024-01-02ignored", "YYYY-MM-DD").fields.day == 2)
        #expect(try parsed("2024JAN02", "YYYY-MON-DD").fields.day == 2)
    }

    @Test("Explicit abbreviation and numeric offset inputs are distinct from IANA names")
    func inputZones() throws {
        #expect(try parsed("2024-01-02 UTC", "YYYY-MM-DD TZ").offsetSeconds == 0)
        #expect(try parsed("2024-01-02 Z", "YYYY-MM-DD TZ").offsetSeconds == 0)
        #expect(try parsed("2024-01-02 +05:30", "YYYY-MM-DD TZH:TZM").offsetSeconds == 19800)
        #expect(try parsed("2024-01-02 +0530", "YYYY-MM-DD TZHTZM").offsetSeconds == 19800)
        #expect(try parsed("2024-01-02 -05:30", "YYYY-MM-DD OF").offsetSeconds == -19800)
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024-01-02 America/New_York", "YYYY-MM-DD TZ") }
        #expect(throws: DatabaseExpressionEvaluationError.self) { try parsed("2024-01-02 +16:00", "YYYY-MM-DD TZH:TZM") }
        let config = try SQLTemporalConfiguration(abbreviations: [.init(name: "TST", offsetSeconds: 1234)],
            limits: .init(maximumAbbreviations: 1, maximumRetainedBytes: 15))
        #expect(try parsed("2024-01-02 tst", "YYYY-MM-DD TZ", environment: environment(config)).offsetSeconds == 1234)
    }

    @Test("Selected input functions return DATE and TIMESTAMPTZ using explicit session policy")
    func selectedInputFunctions() throws {
        #expect(try execute("to_date", [.primitive(.string("2024-02-29 12:30")),
            .primitive(.string("YYYY-MM-DD HH24:MI"))], [.sql(.text), .sql(.text)]).value ==
            .primitive(.date(try CivilDate(year: 2024, month: 2, day: 29))))
        let result = try execute("to_timestamp", [.primitive(.string("2024-01-02 12:00 +05:30")),
            .primitive(.string("YYYY-MM-DD HH24:MI TZH:TZM"))], [.sql(.text), .sql(.text)]).value
        #expect(result == .primitive(.timestamp(Timestamp(secondsSinceUnixEpoch: 1_704_177_000))))
        let configuration = try SQLTemporalConfiguration(timeZone: .fixed(offsetSeconds: 19800),
            limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0))
        let zoned = try execute("to_timestamp", [.primitive(.string("2024-01-02 12:00")),
            .primitive(.string("YYYY-MM-DD HH24:MI"))], [.sql(.text), .sql(.text)],
            environment: environment(configuration)).value
        #expect(zoned == result)
        #expect(try format(result, "YYYY-MM-DD HH24:MI TZ TZH:TZM OF",
            type: .sql(.timestamp(withTimeZone: true))) == .primitive(.string("2024-01-02 06:30 UTC +00:00 +00")))
    }

    @Test("TO_CHAR NULL behavior and plain infinity output remain different contracts")
    func infinities() throws {
        let values: [(QueryValue, QueryColumnType)] = [
            (.dateInfinity(.positive), .sql(.date)),
            (.timestampInfinity(.negative, withTimeZone: false), .sql(.timestamp(withTimeZone: false))),
            (.timestampInfinity(.positive, withTimeZone: true), .sql(.timestamp(withTimeZone: true))),
            (.intervalInfinity(.negative), .sql(.interval()))
        ]
        for (value, type) in values {
            if case .dateInfinity(let sign) = value {
                #expect(try format(.timestampInfinity(sign, withTimeZone: true), "YYYY TZ",
                    type: .sql(.timestamp(withTimeZone: true))) == .primitive(.null))
            } else { #expect(try format(value, "YYYY TZ", type: type) == .primitive(.null)) }
            #expect(try scalar(value, type: type) == (SQLTemporalFormatting.infinity(value) == .negative ? "-infinity" : "infinity"))
        }
        #expect(try format(dateTime(), "") == .primitive(.null))
        #expect(try execute("to_char", [.primitive(.null), .primitive(.string("YYYY"))],
            [.sql(.timestamp(withTimeZone: false)), .sql(.text)]).value == .primitive(.null))
    }

    @Test("Plain finite text preserves endpoint, fractional, offset and mixed interval forms")
    func plainText() throws {
        #expect(try scalar(dateTime(), type: .sql(.timestamp(withTimeZone: false))) == "2024-01-02 13:04:05.123456")
        #expect(try scalar(dateTime(0, 1, 2, 0, 0, 0, 120000), type: .sql(.timestamp(withTimeZone: false))) == "0001-01-02 00:00:00.12 BC")
        let wall = try QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000)
        #expect(try scalar(.time(wall), type: .sql(.time(withTimeZone: false))) == "24:00:00")
        #expect(try scalar(.timeWithTimeZone(.init(time: wall, utcOffsetSeconds: -3661)),
            type: .sql(.time(withTimeZone: true))) == "24:00:00-01:01:01")
        let mixed = try QueryIntervalValue(period: CalendarPeriod(months: -14, days: 3), time: TimeSpan(seconds: -1))
        #expect(try scalar(.interval(mixed), type: .sql(.interval())) == "-1 years -2 mons +3 days -00:00:01")
        let zero = try QueryIntervalValue(period: CalendarPeriod(), time: TimeSpan(seconds: 0))
        #expect(try scalar(.interval(zero), type: .sql(.interval())) == "00:00:00")
    }

    @Test("Published output keeps its claim and failures release all unpublished memory")
    func outputOwnership() throws {
        let work = meter()
        var result = try execute("to_char", [dateTime(), .primitive(.string("YYYY-MM-DD"))],
            [.sql(.timestamp(withTimeZone: false)), .sql(.text)], meter: work)
        #expect(result.value == .primitive(.string("2024-01-02")))
        #expect(result.lease != nil && work.retainedIntermediateBytes > 0)
        let required = work.retainedIntermediateBytes
        let requiredWork = work.consumedWorkUnits
        result.value = .primitive(.null)
        result.lease = nil
        #expect(work.retainedIntermediateBytes == 0)
        #expect(try format(dateTime(), "YYYY-MM-DD", meter: meter(bytes: required)) == .primitive(.string("2024-01-02")))
        let tooSmall = meter(bytes: required - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try format(dateTime(), "YYYY-MM-DD", meter: tooSmall) }
        #expect(tooSmall.retainedIntermediateBytes == 0)
        #expect(try format(dateTime(), "YYYY-MM-DD", meter: meter(work: requiredWork)) == .primitive(.string("2024-01-02")))
        let shortWork = meter(work: requiredWork - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try format(dateTime(), "YYYY-MM-DD", meter: shortWork) }
        #expect(shortWork.retainedIntermediateBytes == 0)
        let noWork = meter(work: 0)
        #expect(throws: DatabaseWorkLimitError.self) { try format(dateTime(), "YYYY", meter: noWork) }
        #expect(noWork.retainedIntermediateBytes == 0)
        let temporary = meter()
        #expect(try scalar(dateTime(), type: .sql(.timestamp(withTimeZone: false)), meter: temporary) == "2024-01-02 13:04:05.123456")
        #expect(temporary.retainedIntermediateBytes == 0)
        #expect(throws: DatabaseExpressionEvaluationError.temporalValueOutOfRange) {
            try SQLTemporalFormatting.withText(of: dateTime(), type: .sql(.timestamp(withTimeZone: false)),
                environment: environment(), workMeter: temporary) { _ in
                    throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
                }
        }
        #expect(temporary.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation and deadlines interrupt long scans without publishing a result")
    func cancellationAndDeadline() async throws {
        let cancelled = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try format(dateTime(), "YYYY", meter: cancelled)
        }
        do { _ = try await task.value; Issue.record("Expected formatting cancellation") }
        catch is CancellationError { }
        #expect(cancelled.retainedIntermediateBytes == 0)
        let deadline = DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 1_000_000,
            maximumIntermediateBytes: 1_000_000, timeoutMilliseconds: 5), monotonicClock: DeadlineClock())
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .expressionEvaluation)) {
            try format(dateTime(), "\"" + String(repeating: "x", count: 2000) + "\"", meter: deadline)
        }
        #expect(deadline.retainedIntermediateBytes == 0)
    }

    private final class DeadlineClock: StorageMonotonicClock, Sendable {
        private let tick = Mutex(Int64(0))
        var now: StorageInstant {
            tick.withLock { value in
                defer { value += 1 }
                return StorageInstant(durationSinceReference: .milliseconds(value))
            }
        }
        func sleep(until deadline: StorageInstant) async throws(StorageClockError) {}
    }
}
