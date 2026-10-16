import DatabaseKit
import DatabaseTypes

/// Execution of already selected temporal signatures with explicit clock/zone authority.
enum CanonicalSQLTemporalEvaluator {
    private static let utcDesignation = SQLTimeZoneRules.Designation(offsetSeconds: 0, abbreviation: ByteString(utf8: "UTC"))
    static func temporal(_ type: QueryColumnType) -> Bool {
        switch type {
        case .sql(.date), .sql(.time), .sql(.timestamp), .sql(.interval),
             .primitive(.date, false), .primitive(.time, false), .primitive(.dateTime, false), .primitive(.timestamp, false): return true
        default: return false
        }
    }

    static func sqlType(_ type: QueryColumnType) throws -> DataType {
        switch type {
        case .sql(let type): return type
        case .primitive(.date, false): return .date
        case .primitive(.time, false): return .time(withTimeZone: false)
        case .primitive(.dateTime, false): return .timestamp(withTimeZone: false)
        case .primitive(.timestamp, false): return .timestamp(withTimeZone: true)
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal type")
        }
    }

    static func identity(_ value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> SQLTemporalIdentity {
        try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter)
    }

    static func compare(_ lhs: QueryValue, _ rhs: QueryValue,
        leftType: QueryColumnType, rightType: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        try identity(lhs, type: leftType, environment: environment, workMeter: workMeter)
            .compare(to: identity(rhs, type: rightType, environment: environment, workMeter: workMeter))
    }

    static func date(_ fields: SQLGregorianCalendar.Fields) throws -> QueryValue {
        let jd = try SQLGregorianCalendar.julianDay(year: fields.year, month: fields.month, day: fields.day)
        try SQLGregorianCalendar.admitDate(jd)
        return .primitive(.date(try CivilDate(year: fields.year, month: UInt8(fields.month), day: UInt8(fields.day))))
    }

    static func timestamp(_ micros: Int128, withTimeZone: Bool) throws -> QueryValue {
        try SQLGregorianCalendar.admitTimestamp(micros)
        if withTimeZone { return .primitive(.timestamp(try SQLGregorianCalendar.instant(pgMicroseconds: micros))) }
        let f = try SQLGregorianCalendar.fields(pgMicroseconds: micros)
        return .primitive(.dateTime(CivilDateTime(
            date: try CivilDate(year: f.year, month: UInt8(f.month), day: UInt8(f.day)),
            time: try CivilTime(hour: UInt8(f.hour), minute: UInt8(f.minute), second: UInt8(f.second), nanoseconds: UInt32(f.microsecond) * 1000))))
    }

    static func time(_ micros: Int128, offset: Int32? = nil) throws -> QueryValue {
        guard micros >= 0, micros <= SQLGregorianCalendar.dayMicroseconds else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        let time = try QueryTimeValue(microsecondsSinceMidnight: Int64(micros))
        if let offset { return .timeWithTimeZone(QueryTimeWithTimeZoneValue(time: time, utcOffsetSeconds: offset)) }
        return .time(time)
    }

    static func assemble(_ fields: SQLGregorianCalendar.Fields, withTimeZone: Bool,
        explicitOffsetSeconds: Int32?, precision: Int?, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> QueryValue {
        var micros = try SQLGregorianCalendar.microseconds(fields)
        if withTimeZone {
            let offset = try explicitOffsetSeconds ?? localOffset(localMicroseconds: micros, zone: environment.configuration.timeZone, workMeter: workMeter)
            micros -= Int128(offset) * 1_000_000
        }
        return try timestamp(SQLTemporalPrecision.round(micros, precision: precision), withTimeZone: withTimeZone)
    }

    static func calendarFields(of value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> SQLGregorianCalendar.Fields {
        let key = try identity(value, type: type, environment: environment, workMeter: workMeter)
        guard key.infinityRank == 0 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        switch key.family {
        case .date: return try SQLGregorianCalendar.fields(julianDay: Int64(key.scalar) + SQLGregorianCalendar.epochJulianDay)
        case .timestamp: return try SQLGregorianCalendar.fields(pgMicroseconds: key.scalar)
        case .timestampWithTimeZone:
            let offset = try offset(at: SQLGregorianCalendar.instant(pgMicroseconds: key.scalar), zone: environment.configuration.timeZone, workMeter: workMeter)
            return try SQLGregorianCalendar.fields(pgMicroseconds: key.scalar + Int128(offset) * 1_000_000)
        case .time, .timeWithTimeZone:
            let micros: Int64
            if case .timeWithTimeZone(let time) = value { micros = time.time.microsecondsSinceMidnight }
            else { micros = Int64(key.scalar) }
            var f = try SQLGregorianCalendar.fields(julianDay: SQLGregorianCalendar.epochJulianDay, microsecondsOfDay: micros == 86_400_000_000 ? 0 : micros)
            if micros == 86_400_000_000 { f.hour = 24 }
            return f
        case .interval: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "interval civil calendar")
        }
    }

    static func designation(at instant: Timestamp, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Designation {
        try workMeter.consume(at: .expressionEvaluation)
        switch environment.configuration.timeZone {
        case .utc: return utcDesignation
        case .fixed(let seconds): return SQLTimeZoneRules.Designation(offsetSeconds: seconds, abbreviation: ByteString())
        case .named(let rules):
            do { return try rules.designation(at: instant, workMeter: workMeter) }
            catch let error as SQLTimeZoneError { throw DatabaseExpressionEvaluationError.timeZone(error) }
        }
    }

    static func outputDesignation(of value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Designation {
        guard case .primitive(.timestamp(let instant)) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "timestamptz designation") }
        return try designation(at: instant, environment: environment, workMeter: workMeter)
    }

    static func offset(at instant: Timestamp, zone: SQLTemporalConfiguration.TimeZone,
        workMeter: DatabaseWorkMeter) throws -> Int32 {
        switch zone {
        case .utc: return 0
        case .fixed(let seconds): return seconds
        case .named(let rules):
            do { return try rules.offset(at: instant, workMeter: workMeter) }
            catch let error as SQLTimeZoneError { throw DatabaseExpressionEvaluationError.timeZone(error) }
        }
    }

    static func localOffset(localMicroseconds local: Int128, zone: SQLTemporalConfiguration.TimeZone,
        workMeter: DatabaseWorkMeter) throws -> Int32 {
        switch zone {
        case .utc: return 0
        case .fixed(let seconds): return seconds
        case .named(let rules):
            // A supplied offset has the full Int32 domain. Enumerate every UTC
            // interval which could contain this local time, without a one-hour
            // transition or two-candidate assumption.
            let lower = max(-211_813_488_000_000_000, local - Int128(Int32.max) * 1_000_000)
            let upper = min(9_223_371_331_199_999_999, local - Int128(Int32.min) * 1_000_000)
            guard lower <= upper else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            do {
                var start = lower
                var current = try rules.offset(at: SQLGregorianCalendar.instant(pgMicroseconds: start), workMeter: workMeter)
                var selected: (instant: Int128, offset: Int32)?
                var gap: Int32?
                while start <= upper {
                    try workMeter.consume(at: .expressionEvaluation)
                    let next = try rules.transition(after: SQLGregorianCalendar.instant(pgMicroseconds: start), workMeter: workMeter)
                    let boundary = next.map { SQLGregorianCalendar.microseconds($0.instant) } ?? (upper + 1)
                    let end = min(boundary, upper + 1)
                    guard end > start else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                    let candidate = local - Int128(current) * 1_000_000
                    if candidate >= start, candidate < end {
                        if let previous = selected {
                            if candidate > previous.instant { selected = (candidate, current) }
                        } else { selected = (candidate, current) }
                    }
                    if let next, boundary <= upper {
                        let before = boundary + Int128(next.offsetBefore) * 1_000_000
                        let after = boundary + Int128(next.offsetAfter) * 1_000_000
                        if after > before, local >= before, local < after { gap = next.offsetBefore }
                        current = next.offsetAfter
                    }
                    start = end
                }
                if let selected { return selected.offset }
                if let gap { return gap }
                throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
            } catch let error as SQLTimeZoneError { throw DatabaseExpressionEvaluationError.timeZone(error) }
        }
    }

    static func zone(_ input: Substring, posix: Bool, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> SQLTemporalConfiguration.TimeZone {
        let text = SQLTemporalInput.trimmed(input)
        if text.utf8.first == 43 || text.utf8.first == 45 {
            return .fixed(offsetSeconds: try SQLTemporalInput.numericOffset(text, posix: posix, workMeter: workMeter))
        }
        if text.count > 3, SQLTemporalInput.equalASCII(text.prefix(3), "utc"),
           text.dropFirst(3).utf8.first == 43 || text.dropFirst(3).utf8.first == 45 {
            return .fixed(offsetSeconds: try SQLTemporalInput.numericOffset(text.dropFirst(3), posix: posix, workMeter: workMeter))
        }
        let reservation = try workMeter.reserveIntermediate(bytes: UInt64(text.utf8.count), at: .expressionEvaluation)
        defer { reservation.release() }
        // The provider's public name boundary requires one owned String.
        let name = String(text)
        if let offset = try environment.configuration.fixedOffset(forAbbreviation: name, workMeter: workMeter) { return .fixed(offsetSeconds: offset) }
        guard let provider = environment.configuration.rulesProvider else {
            throw DatabaseExpressionEvaluationError.timeZone(.rulesUnavailable)
        }
        do { return .named(try provider.rules(named: name, workMeter: workMeter)) }
        catch let error as SQLTimeZoneError { throw DatabaseExpressionEvaluationError.timeZone(error) }
    }

    static func cast(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> QueryValue {
        try workMeter.consume(at: .expressionEvaluation)
        if value.isNull { return .primitive(.null) }
        let targetType = try sqlType(target)
        if case .primitive(.string(let input)) = value {
            let text = SQLTemporalInput.trimmed(input[...])
            try workMeter.consume(UInt64(input.utf8.count), at: .expressionEvaluation)
            if let special = try specialInput(text, target: target, environment: environment, workMeter: workMeter) { return special }
            let sign: QueryTemporalInfinity? = SQLTemporalInput.equalASCII(text, "infinity") ? .positive : SQLTemporalInput.equalASCII(text, "-infinity") ? .negative : nil
            if let sign {
                if case .primitive = target { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                switch targetType {
                case .date: return .dateInfinity(sign)
                case .timestamp(let tz, _): return .timestampInfinity(sign, withTimeZone: tz)
                case .interval: return .intervalInfinity(sign)
                default: throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
                }
            }
            if case .interval(let fields, let precision) = targetType {
                return .interval(try SQLIntervalInput.parse(text, fields: fields, precision: precision, workMeter: workMeter))
            }
            let isDate: Bool
            let hasTime: Bool
            switch targetType {
            case .date: isDate = true; hasTime = false
            case .timestamp: isDate = true; hasTime = true
            case .time: isDate = false; hasTime = true
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "text temporal cast target")
            }
            let parsed = try SQLTemporalInput.parse(text, date: isDate, time: hasTime, workMeter: workMeter)
            let lexicalZone = try parsed.zone.map { try zone($0, posix: false, environment: environment, workMeter: workMeter) }
            switch targetType {
            case .date: return try date(parsed.fields)
            case .timestamp(let tz, let precision):
                let offset: Int32?
                if tz, let selected = lexicalZone {
                    offset = try localOffset(localMicroseconds: SQLGregorianCalendar.microseconds(parsed.fields), zone: selected, workMeter: workMeter)
                } else { offset = nil }
                return try assemble(parsed.fields, withTimeZone: tz, explicitOffsetSeconds: offset, precision: precision, environment: environment, workMeter: workMeter)
            case .time(let tz, let precision):
                let rounded = try SQLTemporalPrecision.round(Int128(parsed.timeMicroseconds), precision: precision)
                let offset: Int32?
                if tz {
                    let transaction = try calendarFields(of: timestamp(SQLGregorianCalendar.microseconds(environment.transactionInstant), withTimeZone: true), type: .sql(.timestamp(withTimeZone: true)), environment: environment, workMeter: workMeter)
                    var f = transaction; f.hour = parsed.fields.hour; f.minute = parsed.fields.minute; f.second = parsed.fields.second; f.microsecond = parsed.fields.microsecond
                    if f.hour == 24 { f.hour = 0; f = try SQLGregorianCalendar.fields(pgMicroseconds: SQLGregorianCalendar.microseconds(f) + SQLGregorianCalendar.dayMicroseconds) }
                    let selected = lexicalZone ?? environment.configuration.timeZone
                    offset = try localOffset(localMicroseconds: SQLGregorianCalendar.microseconds(f), zone: selected, workMeter: workMeter)
                } else { offset = nil }
                return try adaptPrimitiveTime(time(rounded, offset: offset), target: target)
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "text temporal cast")
            }
        }
        let key = try identity(value, type: source, environment: environment, workMeter: workMeter)
        if key.infinityRank != 0 {
            if case .primitive = target { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            let sign: QueryTemporalInfinity = key.infinityRank < 0 ? .negative : .positive
            switch targetType {
            case .date: return .dateInfinity(sign)
            case .timestamp(let tz, _): return .timestampInfinity(sign, withTimeZone: tz)
            case .interval: return .intervalInfinity(sign)
            default: throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
            }
        }
        switch targetType {
        case .date:
            return try date(calendarFields(of: value, type: source, environment: environment, workMeter: workMeter))
        case .timestamp(let tz, let precision):
            var micros: Int128
            if key.family == .timestampWithTimeZone && tz { micros = key.scalar }
            else {
                let fields = try calendarFields(of: value, type: source, environment: environment, workMeter: workMeter)
                micros = try SQLGregorianCalendar.microseconds(fields)
                if tz { micros -= Int128(try localOffset(localMicroseconds: micros, zone: environment.configuration.timeZone, workMeter: workMeter)) * 1_000_000 }
            }
            return try timestamp(SQLTemporalPrecision.round(micros, precision: precision), withTimeZone: tz)
        case .time(let tz, let precision):
            var micros: Int128
            var offset: Int32?
            if case .timeWithTimeZone(let time) = value { micros = Int128(time.time.microsecondsSinceMidnight); offset = tz ? time.utcOffsetSeconds : nil }
            else if key.family == .time { micros = key.scalar }
            else if key.family == .interval { micros = SQLGregorianCalendar.modulo(Int128(try interval(value).microseconds), SQLGregorianCalendar.dayMicroseconds) }
            else {
                let f = try calendarFields(of: value, type: source, environment: environment, workMeter: workMeter)
                micros = Int128(f.hour) * 3_600_000_000 + Int128(f.minute) * 60_000_000 + Int128(f.second) * 1_000_000 + Int128(f.microsecond)
                if tz, case .primitive(.timestamp(let instant)) = value { offset = try self.offset(at: instant, zone: environment.configuration.timeZone, workMeter: workMeter) }
            }
            if tz && offset == nil { offset = try self.offset(at: environment.transactionInstant, zone: environment.configuration.timeZone, workMeter: workMeter) }
            return try adaptPrimitiveTime(time(SQLTemporalPrecision.round(micros, precision: precision), offset: offset), target: target)
        case .interval(let fields, let precision):
            if key.family == .time { return .interval(try SQLTemporalPrecision.interval(months: 0, days: 0, microseconds: key.scalar, fields: fields, precision: precision)) }
            let input = try interval(value)
            return .interval(try SQLTemporalPrecision.interval(months: Int128(input.period.months), days: Int128(input.period.days), microseconds: Int128(input.microseconds), fields: fields, precision: precision))
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal cast")
        }
    }

    static func specialInput(_ text: Substring, target: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue? {
        let targetType = try sqlType(target)
        if SQLTemporalInput.equalASCII(text, "allballs") {
            guard case .time(let tz, let precision) = targetType else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            return try adaptPrimitiveTime(time(SQLTemporalPrecision.round(0, precision: precision), offset: tz ? 0 : nil), target: target)
        }
        if SQLTemporalInput.equalASCII(text, "now") {
            switch targetType {
            case .date: return try SQLTemporalFunctions.current(.date, precision: nil, environment: environment, workMeter: workMeter)
            case .time(let tz, let p): return try adaptPrimitiveTime(SQLTemporalFunctions.current(tz ? .time : .localTime, precision: p, environment: environment, workMeter: workMeter), target: target)
            case .timestamp(let tz, let p): return try SQLTemporalFunctions.current(tz ? .timestamp : .localTimestamp, precision: p, environment: environment, workMeter: workMeter)
            default: throw DatabaseExpressionEvaluationError.invalidTemporalInput
            }
        }
        if SQLTemporalInput.equalASCII(text, "epoch") {
            switch targetType {
            case .date: return try date(SQLGregorianCalendar.Fields(year: 1970, month: 1, day: 1))
            case .timestamp(let tz, let p): return try timestamp(SQLTemporalPrecision.round(-946_684_800_000_000, precision: p), withTimeZone: tz)
            default: throw DatabaseExpressionEvaluationError.invalidTemporalInput
            }
        }
        let delta: Int64
        if SQLTemporalInput.equalASCII(text, "today") { delta = 0 }
        else if SQLTemporalInput.equalASCII(text, "yesterday") { delta = -1 }
        else if SQLTemporalInput.equalASCII(text, "tomorrow") { delta = 1 }
        else { return nil }
        let today = try calendarFields(of: timestamp(SQLGregorianCalendar.microseconds(environment.transactionInstant), withTimeZone: true), type: .sql(.timestamp(withTimeZone: true)), environment: environment, workMeter: workMeter)
        let fields = try SQLGregorianCalendar.fields(julianDay: today.julianDay + delta)
        switch targetType {
        case .date: return try date(fields)
        case .timestamp(let tz, let p): return try assemble(fields, withTimeZone: tz, explicitOffsetSeconds: nil, precision: p, environment: environment, workMeter: workMeter)
        default: throw DatabaseExpressionEvaluationError.invalidTemporalInput
        }
    }

    static func adaptPrimitiveTime(_ value: QueryValue, target: QueryColumnType) throws -> QueryValue {
        guard case .primitive(.time, false) = target, case .time(let time) = value else { return value }
        let t = time.microsecondsSinceMidnight
        guard t < 86_400_000_000 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        return .primitive(.time(try CivilTime(hour: UInt8(t / 3_600_000_000), minute: UInt8(t / 60_000_000 % 60), second: UInt8(t / 1_000_000 % 60), nanoseconds: UInt32(t % 1_000_000) * 1000)))
    }

    static func interval(_ value: QueryValue) throws -> QueryIntervalValue {
        guard case .interval(let interval) = value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected interval") }
        return interval
    }

    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        try workMeter.consume(at: .expressionEvaluation)
        if operands.contains(where: { $0.isNull }) { return .primitive(.null) }
        switch bound.expression {
        case .cast:
            guard operands.count == 1, let child = bound.children.first else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal cast arity") }
            return try cast(operands[0], from: child.type, to: bound.type, context: bound.castContext, environment: environment, workMeter: workMeter)
        case .currentValue(let kind, let precision):
            return try SQLTemporalFunctions.current(kind, precision: precision, environment: environment, workMeter: workMeter)
        case .function(let function):
            return try SQLTemporalFunctions.evaluate(function.name, operands: operands, children: bound.children, resultType: bound.type, environment: environment, workMeter: workMeter)
        default:
            return try SQLTemporalArithmetic.evaluate(bound, operands: operands, environment: environment, workMeter: workMeter)
        }
    }
}
