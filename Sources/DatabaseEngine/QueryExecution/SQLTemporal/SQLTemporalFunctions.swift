import DatabaseKit
import DatabaseTypes

/// The closed selected F07 temporal function implementations.
enum SQLTemporalFunctions {
    enum Unit: String {
        case millennium, century, decade, year, isoyear, quarter, month, week, day, dow, isodow, doy
        case hour, minute, second, millisecond, microsecond, timezone, timezoneHour = "timezone_hour", timezoneMinute = "timezone_minute", epoch, julian
    }

    static func unit(_ value: QueryValue, function: String, workMeter: DatabaseWorkMeter) throws -> Unit {
        guard case .primitive(.string(let text)) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal unit") }
        try workMeter.consume(UInt64(text.utf8.count), at: .expressionEvaluation)
        if SQLTemporalInput.equalASCII(text[...], "millennium") || SQLTemporalInput.equalASCII(text[...], "millennia") { return .millennium }
        if SQLTemporalInput.equalASCII(text[...], "century") || SQLTemporalInput.equalASCII(text[...], "centuries") { return .century }
        if SQLTemporalInput.equalASCII(text[...], "decade") || SQLTemporalInput.equalASCII(text[...], "decades") { return .decade }
        if SQLTemporalInput.equalASCII(text[...], "year") || SQLTemporalInput.equalASCII(text[...], "years") { return .year }
        if SQLTemporalInput.equalASCII(text[...], "isoyear") || SQLTemporalInput.equalASCII(text[...], "isoyears") { return .isoyear }
        if SQLTemporalInput.equalASCII(text[...], "quarter") || SQLTemporalInput.equalASCII(text[...], "quarters") { return .quarter }
        if SQLTemporalInput.equalASCII(text[...], "month") || SQLTemporalInput.equalASCII(text[...], "months") { return .month }
        if SQLTemporalInput.equalASCII(text[...], "week") || SQLTemporalInput.equalASCII(text[...], "weeks") { return .week }
        if SQLTemporalInput.equalASCII(text[...], "day") || SQLTemporalInput.equalASCII(text[...], "days") { return .day }
        if SQLTemporalInput.equalASCII(text[...], "dow") || SQLTemporalInput.equalASCII(text[...], "dows") { return .dow }
        if SQLTemporalInput.equalASCII(text[...], "isodow") || SQLTemporalInput.equalASCII(text[...], "isodows") { return .isodow }
        if SQLTemporalInput.equalASCII(text[...], "doy") || SQLTemporalInput.equalASCII(text[...], "doys") { return .doy }
        if SQLTemporalInput.equalASCII(text[...], "hour") || SQLTemporalInput.equalASCII(text[...], "hours") { return .hour }
        if SQLTemporalInput.equalASCII(text[...], "minute") || SQLTemporalInput.equalASCII(text[...], "minutes") { return .minute }
        if SQLTemporalInput.equalASCII(text[...], "second") || SQLTemporalInput.equalASCII(text[...], "seconds") { return .second }
        if SQLTemporalInput.equalASCII(text[...], "millisecond") || SQLTemporalInput.equalASCII(text[...], "milliseconds") { return .millisecond }
        if SQLTemporalInput.equalASCII(text[...], "microsecond") || SQLTemporalInput.equalASCII(text[...], "microseconds") { return .microsecond }
        if SQLTemporalInput.equalASCII(text[...], "timezone") || SQLTemporalInput.equalASCII(text[...], "timezones") { return .timezone }
        if SQLTemporalInput.equalASCII(text[...], "timezone_hour") || SQLTemporalInput.equalASCII(text[...], "timezone_hours") { return .timezoneHour }
        if SQLTemporalInput.equalASCII(text[...], "timezone_minute") || SQLTemporalInput.equalASCII(text[...], "timezone_minutes") { return .timezoneMinute }
        if SQLTemporalInput.equalASCII(text[...], "epoch") || SQLTemporalInput.equalASCII(text[...], "epochs") { return .epoch }
        if SQLTemporalInput.equalASCII(text[...], "julian") || SQLTemporalInput.equalASCII(text[...], "julians") { return .julian }
        switch text {
        case "ms": return .millisecond
        case "us", "usec": return .microsecond
        default: throw invalid(function, "unrecognized temporal field")
        }
    }

    static func invalid(_ function: String, _ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: function, reason: reason)
    }

    static func current(_ kind: SQLCurrentValueKind, precision: Int?, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> QueryValue {
        let instant = environment.transactionInstant
        let pg = SQLGregorianCalendar.microseconds(instant)
        try SQLGregorianCalendar.admitTimestamp(pg)
        let offset = try CanonicalSQLTemporalEvaluator.offset(at: instant, zone: environment.configuration.timeZone, workMeter: workMeter)
        let local = pg + Int128(offset) * 1_000_000
        switch kind {
        case .date: return try CanonicalSQLTemporalEvaluator.date(SQLGregorianCalendar.fields(pgMicroseconds: local))
        case .timestamp: return try CanonicalSQLTemporalEvaluator.timestamp(SQLTemporalPrecision.round(pg, precision: precision), withTimeZone: true)
        case .localTimestamp: return try CanonicalSQLTemporalEvaluator.timestamp(SQLTemporalPrecision.round(local, precision: precision), withTimeZone: false)
        case .time, .localTime:
            return try CanonicalSQLTemporalEvaluator.time(SQLTemporalPrecision.round(SQLGregorianCalendar.modulo(local, SQLGregorianCalendar.dayMicroseconds), precision: precision), offset: kind == .time ? offset : nil)
        }
    }

    static func evaluate(_ function: String, operands: [QueryValue], children: [CanonicalBoundExpression],
        resultType: QueryColumnType, environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        try workMeter.consume(at: .expressionEvaluation)
        switch function {
        case "now", "transaction_timestamp", "statement_timestamp", "clock_timestamp":
            guard operands.isEmpty else { throw invalid(function, "invalid arity") }
            let instant: Timestamp
            switch function {
            case "clock_timestamp": instant = environment.wallClock.now
            case "statement_timestamp": instant = environment.statementInstant
            default: instant = environment.transactionInstant
            }
            return try CanonicalSQLTemporalEvaluator.timestamp(SQLGregorianCalendar.microseconds(instant), withTimeZone: true)
        case "extract", "date_part":
            guard operands.count == 2, children.count == 2 else { throw invalid(function, "invalid arity") }
            return try extract(unit(operands[0], function: function, workMeter: workMeter), value: operands[1], type: children[1].type, floating: function == "date_part", environment: environment, workMeter: workMeter)
        case "date_trunc":
            guard operands.count == 2 || operands.count == 3 else { throw invalid(function, "invalid arity") }
            let selected = try unit(operands[0], function: function, workMeter: workMeter)
            var zone: SQLTemporalConfiguration.TimeZone? = nil
            if operands.count == 3 {
                guard case .primitive(.string(let name)) = operands[2] else { throw invalid(function, "invalid time zone") }
                zone = try CanonicalSQLTemporalEvaluator.zone(name[...], posix: true, environment: environment, workMeter: workMeter)
            }
            return try truncate(selected, value: operands[1], type: children[1].type, zone: zone, environment: environment, workMeter: workMeter)
        case "date_bin":
            guard operands.count == 3 else { throw invalid(function, "invalid arity") }
            let stride = try CanonicalSQLTemporalEvaluator.interval(operands[0])
            guard stride.period.months == 0 else { throw invalid(function, "month/year strides are not supported") }
            let step = Int128(stride.period.days) * SQLGregorianCalendar.dayMicroseconds + Int128(stride.microseconds)
            guard step > 0, Int64(exactly: step) != nil else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            let value = try SQLTemporalIdentity.make(operands[1], type: children[1].type, workMeter: workMeter)
            let origin = try SQLTemporalIdentity.make(operands[2], type: children[2].type, workMeter: workMeter)
            guard origin.infinityRank == 0 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            if value.infinityRank != 0 { return operands[1] }
            let difference = value.scalar - origin.scalar
            guard Int64(exactly: difference) != nil else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return try CanonicalSQLTemporalEvaluator.timestamp(origin.scalar + SQLGregorianCalendar.floorDivide(difference, by: step) * step, withTimeZone: value.family == .timestampWithTimeZone)
        case "age":
            guard operands.count == 1 || operands.count == 2 else { throw invalid(function, "invalid arity") }
            if operands.count == 2 { return try SQLTemporalArithmetic.age(operands[0], operands[1], type: children[0].type, environment: environment, workMeter: workMeter) }
            let today = try current(.date, precision: nil, environment: environment, workMeter: workMeter)
            let midnight = try CanonicalSQLTemporalEvaluator.cast(today, from: .sql(.date), to: children[0].type, context: .implicit, environment: environment, workMeter: workMeter)
            return try SQLTemporalArithmetic.age(midnight, operands[0], type: children[0].type, environment: environment, workMeter: workMeter)
        case "make_date", "make_time", "make_timestamp", "make_timestamptz", "make_interval":
            return try make(function, operands: operands, environment: environment, workMeter: workMeter)
        case "timezone":
            guard operands.count == 2 else { throw invalid(function, "invalid arity") }
            let zone: SQLTemporalConfiguration.TimeZone
            if case .interval(let span) = operands[0] {
                guard span.period.months == 0, span.period.days == 0, let offset = Int32(exactly: (Int128(span.period.days) * SQLGregorianCalendar.dayMicroseconds + Int128(span.microseconds)) / 1_000_000) else { throw invalid(function, "invalid interval time zone") }
                zone = .fixed(offsetSeconds: offset)
            } else if case .primitive(.string(let name)) = operands[0] {
                zone = try CanonicalSQLTemporalEvaluator.zone(name[...], posix: true, environment: environment, workMeter: workMeter)
            } else { throw invalid(function, "invalid time zone") }
            return try atTimeZone(operands[1], type: children[1].type, zone: zone, environment: environment, workMeter: workMeter)
        case "to_timestamp":
            guard operands.count == 1, case .primitive(.float64(let seconds)) = operands[0] else { throw invalid(function, "selected epoch overload") }
            guard seconds.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            let pg = ((seconds - 946_684_800) * 1_000_000).rounded(.toNearestOrEven)
            guard let microseconds = Int64(exactly: pg) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return try CanonicalSQLTemporalEvaluator.timestamp(Int128(microseconds), withTimeZone: true)
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(function)
        }
    }

    static func make(_ function: String, operands: [QueryValue], environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> QueryValue {
        func integer(_ index: Int) throws -> Int128 {
            guard operands.indices.contains(index) else { throw invalid(function, "invalid arity") }
            return try SQLTemporalArithmetic.integer(operands[index])
        }
        func seconds(_ index: Int) throws -> Double {
            guard operands.indices.contains(index), case .primitive(.float64(let seconds)) = operands[index] else { throw invalid(function, "invalid seconds") }
            guard seconds.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            return seconds
        }
        if function == "make_interval" {
            guard operands.count == 7 else { throw invalid(function, "invalid arity") }
            let time = try seconds(6) * 1_000_000
            guard let micros = Int64(exactly: time.rounded(.toNearestOrEven)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return .interval(try SQLTemporalPrecision.interval(months: integer(0) * 12 + integer(1), days: integer(2) * 7 + integer(3), microseconds: integer(4) * 3_600_000_000 + integer(5) * 60_000_000 + Int128(micros)))
        }
        if function == "make_time" {
            guard operands.count == 3 else { throw invalid(function, "invalid arity") }
            let hour = try integer(0), minute = try integer(1), second = try seconds(2)
            guard hour >= 0, hour <= 24, minute >= 0, minute < 60, second >= 0, second <= 60,
                  hour != 24 || minute == 0 && second == 0 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return try CanonicalSQLTemporalEvaluator.time(hour * 3_600_000_000 + minute * 60_000_000 + Int128((second * 1_000_000).rounded(.toNearestOrEven)))
        }
        let year = try integer(0)
        guard year != 0, let astronomical = Int32(exactly: year < 0 ? year + 1 : year),
              let month = Int32(exactly: try integer(1)), let day = Int32(exactly: try integer(2)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        var fields = SQLGregorianCalendar.Fields(year: astronomical, month: month, day: day)
        if function == "make_date" {
            guard operands.count == 3 else { throw invalid(function, "invalid arity") }
            return try CanonicalSQLTemporalEvaluator.date(fields)
        }
        guard operands.count == 6 || function == "make_timestamptz" && operands.count == 7 else { throw invalid(function, "invalid arity") }
        let hour = try integer(3), minute = try integer(4), second = try seconds(5)
        guard hour >= 0, hour <= 24, minute >= 0, minute < 60, second >= 0, second <= 60,
              hour != 24 || minute == 0 && second == 0 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        let base = try SQLGregorianCalendar.microseconds(fields)
        let local = base + hour * 3_600_000_000 + minute * 60_000_000 + Int128((second * 1_000_000).rounded(.toNearestOrEven))
        fields = try SQLGregorianCalendar.fields(pgMicroseconds: local)
        let offset: Int32?
        if operands.count == 7 {
            guard case .primitive(.string(let name)) = operands[6] else { throw invalid(function, "invalid time zone") }
            offset = try CanonicalSQLTemporalEvaluator.localOffset(localMicroseconds: local,
                zone: CanonicalSQLTemporalEvaluator.zone(name[...], posix: true, environment: environment, workMeter: workMeter), workMeter: workMeter)
        } else { offset = nil }
        return try CanonicalSQLTemporalEvaluator.assemble(fields, withTimeZone: function == "make_timestamptz", explicitOffsetSeconds: offset, precision: nil, environment: environment, workMeter: workMeter)
    }

    static func atTimeZone(_ value: QueryValue, type: QueryColumnType, zone: SQLTemporalConfiguration.TimeZone,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        let key = try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter)
        if key.infinityRank != 0 {
            return .timestampInfinity(key.infinityRank < 0 ? .negative : .positive, withTimeZone: key.family == .timestamp)
        }
        switch key.family {
        case .timestamp:
            let offset = try CanonicalSQLTemporalEvaluator.localOffset(localMicroseconds: key.scalar, zone: zone, workMeter: workMeter)
            return try CanonicalSQLTemporalEvaluator.timestamp(key.scalar - Int128(offset) * 1_000_000, withTimeZone: true)
        case .timestampWithTimeZone:
            let offset = try CanonicalSQLTemporalEvaluator.offset(at: SQLGregorianCalendar.instant(pgMicroseconds: key.scalar), zone: zone, workMeter: workMeter)
            return try CanonicalSQLTemporalEvaluator.timestamp(key.scalar + Int128(offset) * 1_000_000, withTimeZone: false)
        case .timeWithTimeZone:
            let offset = try CanonicalSQLTemporalEvaluator.offset(at: environment.transactionInstant, zone: zone, workMeter: workMeter)
            return try CanonicalSQLTemporalEvaluator.time(SQLGregorianCalendar.modulo(key.scalar + Int128(offset) * 1_000_000, SQLGregorianCalendar.dayMicroseconds), offset: offset)
        default: throw invalid("timezone", "invalid selected operand")
        }
    }

    static func numeric(_ coefficient: Int128, scale: Int32 = 0, floating: Bool) throws -> QueryValue {
        if floating {
            var divisor: Double = 1
            for _ in 0..<scale { divisor *= 10 }
            return .primitive(.float64(Double(coefficient) / divisor))
        }
        return .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale), displayScale: scale))
    }

    static func extract(_ unit: Unit, value: QueryValue, type: QueryColumnType, floating: Bool,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        let key = try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter)
        if key.infinityRank != 0 {
            let null: Bool
            switch key.family {
            case .interval: null = (unit == .microsecond || unit == .millisecond || unit == .second || unit == .minute || unit == .week || unit == .month || unit == .quarter)
            case .date: null = (unit == .day || unit == .month || unit == .quarter || unit == .week || unit == .dow || unit == .isodow || unit == .doy)
            default: null = (unit == .microsecond || unit == .millisecond || unit == .second || unit == .minute || unit == .hour || unit == .day || unit == .month || unit == .quarter || unit == .week || unit == .dow || unit == .isodow || unit == .doy || unit == .timezone || unit == .timezoneHour || unit == .timezoneMinute)
            }
            if null { return .primitive(.null) }
            let monotone: Bool
            if key.family == .interval {
                monotone = unit == .hour || unit == .day || unit == .year || unit == .decade || unit == .century || unit == .millennium || unit == .epoch
            } else {
                monotone = unit == .year || unit == .decade || unit == .century || unit == .millennium || unit == .julian || unit == .isoyear || unit == .epoch
            }
            guard monotone else { throw invalid("extract", "field not supported for selected type") }
            throw DatabaseExpressionEvaluationError.nonFiniteValue
        }
        if case .interval(let span) = value {
            let years = Int128(span.period.months / 12)
            let months = Int128(span.period.months % 12)
            let seconds = Int128(span.microseconds % 60_000_000)
            switch unit {
            case .microsecond: return try numeric(seconds, floating: floating)
            case .millisecond: return try numeric(seconds, scale: 3, floating: floating)
            case .second: return try numeric(seconds, scale: 6, floating: floating)
            case .minute: return try numeric(Int128(span.microseconds / 60_000_000 % 60), floating: floating)
            case .hour: return try numeric(Int128(span.microseconds / 3_600_000_000), floating: floating)
            case .day: return try numeric(Int128(span.period.days), floating: floating)
            case .week: return try numeric(Int128(span.period.days / 7), floating: floating)
            case .month: return try numeric(months, floating: floating)
            case .quarter: return try numeric(span.period.months < 0 ? -((-Int128(span.period.months) % 12) / 3 + 1) : months / 3 + 1, floating: floating)
            case .year: return try numeric(years, floating: floating)
            case .decade: return try numeric(years / 10, floating: floating)
            case .century: return try numeric(years / 100, floating: floating)
            case .millennium: return try numeric(years / 1000, floating: floating)
            case .epoch:
                let seconds = (years * 1461 + months * 120 + Int128(span.period.days) * 4) * 21600
                return try numeric(seconds * 1_000_000 + Int128(span.microseconds), scale: 6, floating: floating)
            default: throw invalid("extract", "field not supported for interval")
            }
        }
        let isTime = key.family == .time || key.family == .timeWithTimeZone
        let isDate = key.family == .date
        if isTime && !(unit == .hour || unit == .minute || unit == .second || unit == .millisecond || unit == .microsecond || unit == .epoch || unit == .timezone || unit == .timezoneHour || unit == .timezoneMinute) { throw invalid("extract", "field not supported for time") }
        if isDate && (unit == .hour || unit == .minute || unit == .second || unit == .millisecond || unit == .microsecond || unit == .timezone || unit == .timezoneHour || unit == .timezoneMinute) { throw invalid("extract", "field not supported for date") }
        let f = try CanonicalSQLTemporalEvaluator.calendarFields(of: value, type: type, environment: environment, workMeter: workMeter)
        let y = Int128(f.year)
        switch unit {
        case .microsecond: return try numeric(Int128(f.second) * 1_000_000 + Int128(f.microsecond), floating: floating)
        case .millisecond: return try numeric(Int128(f.second) * 1_000_000 + Int128(f.microsecond), scale: 3, floating: floating)
        case .second: return try numeric(Int128(f.second) * 1_000_000 + Int128(f.microsecond), scale: 6, floating: floating)
        case .minute: return try numeric(Int128(f.minute), floating: floating)
        case .hour: return try numeric(Int128(f.hour), floating: floating)
        case .day: return try numeric(Int128(f.day), floating: floating)
        case .month: return try numeric(Int128(f.month), floating: floating)
        case .quarter: return try numeric(Int128((f.month - 1) / 3 + 1), floating: floating)
        case .year: return try numeric(y <= 0 ? y - 1 : y, floating: floating)
        case .decade: return try numeric(y >= 0 ? y / 10 : -((9 - y) / 10), floating: floating)
        case .century: return try numeric(y > 0 ? (y + 99) / 100 : -((100 - y) / 100), floating: floating)
        case .millennium: return try numeric(y > 0 ? (y + 999) / 1000 : -((1000 - y) / 1000), floating: floating)
        case .week: return try numeric(Int128(SQLGregorianCalendar.isoFields(julianDay: f.julianDay).week), floating: floating)
        case .isoyear:
            let y = Int128(try SQLGregorianCalendar.isoFields(julianDay: f.julianDay).year)
            return try numeric(y <= 0 ? y - 1 : y, floating: floating)
        case .dow: return try numeric(Int128(SQLGregorianCalendar.dayOfWeek(julianDay: f.julianDay)), floating: floating)
        case .isodow: return try numeric(Int128(SQLGregorianCalendar.isoFields(julianDay: f.julianDay).weekday), floating: floating)
        case .doy: return try numeric(Int128(SQLGregorianCalendar.dayOfYear(julianDay: f.julianDay)), floating: floating)
        case .timezone, .timezoneHour, .timezoneMinute:
            let offset: Int32
            if case .timeWithTimeZone(let t) = value { offset = t.utcOffsetSeconds }
            else if case .primitive(.timestamp(let t)) = value { offset = try CanonicalSQLTemporalEvaluator.offset(at: t, zone: environment.configuration.timeZone, workMeter: workMeter) }
            else { throw invalid("extract", "time zone field requires aware value") }
            return try numeric(Int128(unit == .timezone ? offset : unit == .timezoneHour ? offset / 3600 : offset / 60 % 60), floating: floating)
        case .epoch:
            if isDate { return try numeric(key.scalar * 86400 + 946_684_800, floating: floating) }
            if isTime { return try numeric(key.scalar, scale: 6, floating: floating) }
            return try numeric(key.scalar + 946_684_800_000_000, scale: 6, floating: floating)
        case .julian:
            if isDate { return try numeric(Int128(f.julianDay), floating: floating) }
            let time = Int128(f.hour) * 3_600_000_000 + Int128(f.minute) * 60_000_000 + Int128(f.second) * 1_000_000 + Int128(f.microsecond)
            if floating { return .primitive(.float64(Double(f.julianDay) + Double(time) / 86_400_000_000)) }
            let left = ExactDecimal(coefficient: time, scale: 0)
            let right = ExactDecimal(coefficient: 86_400_000_000, scale: 0)
            let scale = CanonicalSQLNumericScale.divisionScale(left, displayScale: 0, right, displayScale: 0)
            let decimal = try CanonicalSQLNumericEvaluator.decimalOperation(workMeter: workMeter) {
                let fraction = try CanonicalSQLDecimalWorkspaceValue(left, workMeter: workMeter).divided(by: CanonicalSQLDecimalWorkspaceValue(right, workMeter: workMeter), toScale: scale)
                return try fraction.adding(CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: Int128(f.julianDay), scale: 0), workMeter: workMeter)).exactDecimal()
            }
            return .numeric(try QueryNumericValue(value: decimal, displayScale: scale))
        }
    }

    static func truncate(_ unit: Unit, value: QueryValue, type: QueryColumnType, zone: SQLTemporalConfiguration.TimeZone?,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        guard (unit == .millennium || unit == .century || unit == .decade || unit == .year || unit == .quarter || unit == .month || unit == .week || unit == .day || unit == .hour || unit == .minute || unit == .second || unit == .millisecond || unit == .microsecond) else { throw invalid("date_trunc", "field not supported") }
        if SQLTemporalArithmetic.infinity(value) != 0 { return value }
        if case .interval(let span) = value {
            var months = Int128(span.period.months), days = Int128(span.period.days), time = Int128(span.microseconds)
            switch unit {
            case .millennium: months = months / 12000 * 12000; days = 0; time = 0
            case .century: months = months / 1200 * 1200; days = 0; time = 0
            case .decade: months = months / 120 * 120; days = 0; time = 0
            case .year: months = months / 12 * 12; days = 0; time = 0
            case .quarter: months = months / 3 * 3; days = 0; time = 0
            case .month: days = 0; time = 0
            case .day: time = 0
            case .hour: time = time / 3_600_000_000 * 3_600_000_000
            case .minute: time = time / 60_000_000 * 60_000_000
            case .second: time = time / 1_000_000 * 1_000_000
            case .millisecond: time = time / 1000 * 1000
            case .microsecond: break
            default: throw invalid("date_trunc", "week not supported for interval")
            }
            return .interval(try SQLTemporalPrecision.interval(months: months, days: days, microseconds: time))
        }
        let key = try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter)
        let aware = key.family == .timestampWithTimeZone
        let selectedZone = zone ?? environment.configuration.timeZone
        let local = aware ? key.scalar + Int128(try CanonicalSQLTemporalEvaluator.offset(at: SQLGregorianCalendar.instant(pgMicroseconds: key.scalar), zone: selectedZone, workMeter: workMeter)) * 1_000_000 : key.scalar
        var f = try SQLGregorianCalendar.fields(pgMicroseconds: local)
        if unit == .week {
            let iso = try SQLGregorianCalendar.isoFields(julianDay: f.julianDay)
            f = try SQLGregorianCalendar.fields(julianDay: f.julianDay - Int64(iso.weekday - 1))
        } else {
            if unit == .millennium || unit == .century || unit == .decade {
                let year = Int64(f.year)
                let truncated: Int64
                if unit == .decade {
                    truncated = year > 0 ? year / 10 * 10 : -((9 - year) / 10) * 10
                } else {
                    let size: Int64 = unit == .millennium ? 1000 : 100
                    truncated = year > 0 ? ((year + size - 1) / size) * size - size + 1 : -((size - year) / size) * size + 1
                }
                guard let result = Int32(exactly: truncated) else {
                    throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
                }
                f.year = result
            }
            if (unit == .millennium || unit == .century || unit == .decade || unit == .year) { f.month = 1 }
            if unit == .quarter { f.month = (f.month - 1) / 3 * 3 + 1 }
            if (unit == .millennium || unit == .century || unit == .decade || unit == .year || unit == .quarter || unit == .month) { f.day = 1 }
            if (unit == .millennium || unit == .century || unit == .decade || unit == .year || unit == .quarter || unit == .month || unit == .day) { f.hour = 0 }
            if (unit == .millennium || unit == .century || unit == .decade || unit == .year || unit == .quarter || unit == .month || unit == .day || unit == .hour) { f.minute = 0 }
            if !(unit == .microsecond || unit == .millisecond || unit == .second) { f.second = 0 }
            if unit == .millisecond { f.microsecond = f.microsecond / 1000 * 1000 }
            else if unit != .microsecond { f.microsecond = 0 }
        }
        var result = try SQLGregorianCalendar.microseconds(f)
        if aware {
            // Sub-day truncation retains the original offset, matching PG's
            // redotz boundary; calendar-day and coarser units resolve anew.
            let redotz = (unit == .millennium || unit == .century || unit == .decade || unit == .year || unit == .quarter || unit == .month || unit == .week || unit == .day)
            let offset = redotz ? try CanonicalSQLTemporalEvaluator.localOffset(localMicroseconds: result, zone: selectedZone, workMeter: workMeter) : try CanonicalSQLTemporalEvaluator.offset(at: SQLGregorianCalendar.instant(pgMicroseconds: key.scalar), zone: selectedZone, workMeter: workMeter)
            result -= Int128(offset) * 1_000_000
        }
        return try CanonicalSQLTemporalEvaluator.timestamp(result, withTimeZone: aware)
    }
}

