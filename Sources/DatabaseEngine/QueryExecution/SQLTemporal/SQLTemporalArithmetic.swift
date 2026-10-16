import DatabaseKit
import DatabaseTypes

/// Selected temporal operations; calendar components and elapsed time stay distinct.
enum SQLTemporalArithmetic {
    static func integer(_ value: QueryValue) throws -> Int128 {
        switch value {
        case .primitive(.int32(let value)): return Int128(value)
        case .primitive(.int64(let value)): return Int128(value)
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal integer")
        }
    }

    static func infinity(_ value: QueryValue) -> Int8 {
        switch value {
        case .dateInfinity(let sign), .timestampInfinity(let sign, _), .intervalInfinity(let sign): return sign == .negative ? -1 : 1
        default: return 0
        }
    }

    static func infinite(_ rank: Int8, type: QueryColumnType) throws -> QueryValue {
        let sign: QueryTemporalInfinity = rank < 0 ? .negative : .positive
        switch try CanonicalSQLTemporalEvaluator.sqlType(type) {
        case .date: return .dateInfinity(sign)
        case .timestamp(let tz, _): return .timestampInfinity(sign, withTimeZone: tz)
        case .interval: return .intervalInfinity(sign)
        default: throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
    }

    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        guard let first = operands.first else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal arithmetic arity") }
        let subtract: Bool
        switch bound.expression { case .subtract: subtract = true; default: subtract = false }
        let sign: Int128 = subtract ? -1 : 1
        if case .negate = bound.expression {
            let rank = infinity(first)
            if rank != 0 { return try infinite(-rank, type: bound.type) }
            let value = try CanonicalSQLTemporalEvaluator.interval(first)
            return .interval(try SQLTemporalPrecision.interval(months: -Int128(value.period.months), days: -Int128(value.period.days), microseconds: -Int128(value.microseconds)))
        }
        guard operands.count == 2, bound.children.count == 2 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal arithmetic arity") }
        let second = operands[1]
        let leftType = temporalKind(bound.children[0].type)
        let rightType = temporalKind(bound.children[1].type)
        switch bound.expression {
        case .multiply, .divide:
            let dividing: Bool
            if case .divide = bound.expression { dividing = true } else { dividing = false }
            let intervalValue = leftType == .interval ? first : second
            let factorValue = leftType == .interval ? second : first
            guard case .primitive(.float64(let factor)) = factorValue, factor.isFinite else { throw DatabaseExpressionEvaluationError.nonFiniteValue }
            if dividing && factor == 0 { throw DatabaseExpressionEvaluationError.divisionByZero }
            let rank = infinity(intervalValue)
            if rank != 0 {
                guard factor != 0 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                return try infinite(factor < 0 ? -rank : rank, type: bound.type)
            }
            return .interval(try scale(CanonicalSQLTemporalEvaluator.interval(intervalValue), factor: factor, dividing: dividing))
        default: break
        }
        let lrank = infinity(first)
        let rrank = infinity(second) * (subtract ? -1 : 1)
        if lrank != 0 || rrank != 0 {
            if lrank != 0 && rrank != 0 && lrank != rrank { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return try infinite(lrank == 0 ? rrank : lrank, type: bound.type)
        }
        if leftType == .interval && rightType == .interval {
            let a = try CanonicalSQLTemporalEvaluator.interval(first)
            let b = try CanonicalSQLTemporalEvaluator.interval(second)
            return .interval(try SQLTemporalPrecision.interval(months: Int128(a.period.months) + sign * Int128(b.period.months), days: Int128(a.period.days) + sign * Int128(b.period.days), microseconds: Int128(a.microseconds) + sign * Int128(b.microseconds)))
        }
        if leftType == .date && rightType == .date && subtract {
            let a = try SQLTemporalIdentity.make(first, type: bound.children[0].type, workMeter: workMeter)
            let b = try SQLTemporalIdentity.make(second, type: bound.children[1].type, workMeter: workMeter)
            guard let result = Int32(exactly: a.scalar - b.scalar) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return .primitive(.int32(result))
        }
        if leftType == .date && rightType == nil || !subtract && rightType == .date && leftType == nil {
            let value = leftType == .date ? first : second
            let delta = try integer(leftType == .date ? second : first)
            let key = try SQLTemporalIdentity.make(value, type: .sql(.date), workMeter: workMeter)
            guard let jd = Int64(exactly: key.scalar + sign * delta + Int128(SQLGregorianCalendar.epochJulianDay)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            try SQLGregorianCalendar.admitDate(jd)
            return try CanonicalSQLTemporalEvaluator.date(SQLGregorianCalendar.fields(julianDay: jd))
        }
        if leftType == .date && (rightType == .time || rightType == .timetz) || !subtract && rightType == .date && (leftType == .time || leftType == .timetz) {
            let date = leftType == .date ? first : second
            let time = leftType == .date ? second : first
            let dateKey = try SQLTemporalIdentity.make(date, type: .sql(.date), workMeter: workMeter)
            let timeKey = try SQLTemporalIdentity.make(time, type: .sql(.time(withTimeZone: rightType == .timetz || leftType == .timetz)), workMeter: workMeter)
            return try CanonicalSQLTemporalEvaluator.timestamp(dateKey.scalar * SQLGregorianCalendar.dayMicroseconds + timeKey.scalar,
                withTimeZone: timeKey.family == .timeWithTimeZone)
        }
        if (leftType == .timestamp || leftType == .timestamptz) && leftType == rightType && subtract {
            let a = try SQLTemporalIdentity.make(first, type: bound.children[0].type, workMeter: workMeter)
            let b = try SQLTemporalIdentity.make(second, type: bound.children[1].type, workMeter: workMeter)
            let delta = a.scalar - b.scalar
            guard Int64(exactly: delta) != nil else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return .interval(try SQLTemporalPrecision.interval(months: 0, days: delta / SQLGregorianCalendar.dayMicroseconds, microseconds: delta % SQLGregorianCalendar.dayMicroseconds))
        }
        if leftType == .time && rightType == .time && subtract {
            let a = try SQLTemporalIdentity.make(first, type: bound.children[0].type, workMeter: workMeter)
            let b = try SQLTemporalIdentity.make(second, type: bound.children[1].type, workMeter: workMeter)
            return .interval(try SQLTemporalPrecision.interval(months: 0, days: 0, microseconds: a.scalar - b.scalar))
        }
        if rightType == .interval || !subtract && leftType == .interval {
            let value = rightType == .interval ? first : second
            let type = rightType == .interval ? bound.children[0].type : bound.children[1].type
            let interval = try CanonicalSQLTemporalEvaluator.interval(rightType == .interval ? second : first)
            if leftType == .time || leftType == .timetz || rightType == .time || rightType == .timetz {
                let time: Int128
                let offset: Int32?
                if case .timeWithTimeZone(let t) = value { time = Int128(t.time.microsecondsSinceMidnight); offset = t.utcOffsetSeconds }
                else { time = try SQLTemporalIdentity.make(value, type: type, workMeter: workMeter).scalar; offset = nil }
                return try CanonicalSQLTemporalEvaluator.time(SQLGregorianCalendar.modulo(time + sign * Int128(interval.microseconds), SQLGregorianCalendar.dayMicroseconds), offset: offset)
            }
            return try adding(interval, to: value, type: type, subtract: subtract, environment: environment, workMeter: workMeter)
        }
        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal arithmetic signature")
    }

    private enum Kind { case date, time, timetz, timestamp, timestamptz, interval }
    private static func temporalKind(_ type: QueryColumnType) -> Kind? {
        switch type {
        case .sql(.date), .primitive(.date, false): return .date
        case .sql(.time(false, _)), .primitive(.time, false): return .time
        case .sql(.time(true, _)): return .timetz
        case .sql(.timestamp(false, _)), .primitive(.dateTime, false): return .timestamp
        case .sql(.timestamp(true, _)), .primitive(.timestamp, false): return .timestamptz
        case .sql(.interval): return .interval
        default: return nil
        }
    }

    static func adding(_ interval: QueryIntervalValue, to value: QueryValue, type: QueryColumnType,
        subtract: Bool = false, environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        let sign: Int128 = subtract ? -1 : 1
        let aware: Bool
        if case .sql(.timestamp(true, _)) = type { aware = true }
        else if case .primitive(.timestamp, false) = type { aware = true }
        else { aware = false }
        var current = value
        if case .primitive(.date) = value {
            current = try CanonicalSQLTemporalEvaluator.cast(value, from: .sql(.date), to: .sql(.timestamp(withTimeZone: false)), context: .implicit, environment: environment, workMeter: workMeter)
        }
        let selectedType = QueryColumnType.sql(DataType.timestamp(withTimeZone: aware))
        if interval.period.months != 0 {
            var f = try CanonicalSQLTemporalEvaluator.calendarFields(of: current, type: selectedType, environment: environment, workMeter: workMeter)
            let total = Int128(f.year) * 12 + Int128(f.month) - 1 + sign * Int128(interval.period.months)
            guard let year = Int32(exactly: SQLGregorianCalendar.floorDivide(total, by: 12)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            f.year = year; f.month = Int32(SQLGregorianCalendar.modulo(total, 12)) + 1
            f.day = min(f.day, SQLGregorianCalendar.daysInMonth(year: f.year, month: f.month))
            current = try CanonicalSQLTemporalEvaluator.assemble(f, withTimeZone: aware, explicitOffsetSeconds: nil, precision: nil, environment: environment, workMeter: workMeter)
        }
        if interval.period.days != 0 {
            let f = try CanonicalSQLTemporalEvaluator.calendarFields(of: current, type: selectedType, environment: environment, workMeter: workMeter)
            let local = try SQLGregorianCalendar.microseconds(f) + sign * Int128(interval.period.days) * SQLGregorianCalendar.dayMicroseconds
            current = try CanonicalSQLTemporalEvaluator.assemble(SQLGregorianCalendar.fields(pgMicroseconds: local), withTimeZone: aware, explicitOffsetSeconds: nil, precision: nil, environment: environment, workMeter: workMeter)
        }
        let key = try SQLTemporalIdentity.make(current, type: selectedType, workMeter: workMeter)
        return try CanonicalSQLTemporalEvaluator.timestamp(key.scalar + sign * Int128(interval.microseconds), withTimeZone: aware)
    }

    static func scale(_ span: QueryIntervalValue, factor: Double, dividing: Bool) throws -> QueryIntervalValue {
        func operation(_ value: Double) -> Double { dividing ? value / factor : value * factor }
        func tsround(_ value: Double) -> Double { (value * 1_000_000).rounded(.toNearestOrEven) / 1_000_000 }
        let month = operation(Double(span.period.months))
        let day = operation(Double(span.period.days))
        guard let months = Int32(exactly: month.rounded(.towardZero)), let days = Int32(exactly: day.rounded(.towardZero)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        let monthDays = tsround((month - Double(months)) * 30)
        var seconds = tsround((day - Double(days) + monthDays - monthDays.rounded(.towardZero)) * 86400)
        let carry = seconds / 86400
        guard let carryDays = Int64(exactly: carry.rounded(.towardZero)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        seconds -= Double(carryDays) * 86400
        let micros = (operation(Double(span.microseconds)) + seconds * 1_000_000).rounded(.toNearestOrEven)
        guard let time = Int64(exactly: micros), let remainderDays = Int64(exactly: monthDays.rounded(.towardZero)) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        return try SQLTemporalPrecision.interval(months: Int128(months), days: Int128(days) + Int128(carryDays) + Int128(remainderDays), microseconds: Int128(time))
    }

    static func age(_ a: QueryValue, _ b: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> QueryValue {
        let ak = try SQLTemporalIdentity.make(a, type: type, workMeter: workMeter)
        let bk = try SQLTemporalIdentity.make(b, type: type, workMeter: workMeter)
        if ak.infinityRank != 0 || bk.infinityRank != 0 {
            guard ak.infinityRank != bk.infinityRank else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return try infinite(ak.infinityRank < bk.infinityRank ? -1 : 1, type: .sql(.interval()))
        }
        let negative = ak.scalar < bk.scalar
        let earlier = try CanonicalSQLTemporalEvaluator.calendarFields(of: negative ? a : b, type: type, environment: environment, workMeter: workMeter)
        let later = try CanonicalSQLTemporalEvaluator.calendarFields(of: negative ? b : a, type: type, environment: environment, workMeter: workMeter)
        var years = Int128(later.year) - Int128(earlier.year)
        var months = Int128(later.month) - Int128(earlier.month)
        var days = Int128(later.day) - Int128(earlier.day)
        var micros = (Int128(later.hour) - Int128(earlier.hour)) * 3_600_000_000 + (Int128(later.minute) - Int128(earlier.minute)) * 60_000_000
            + (Int128(later.second) - Int128(earlier.second)) * 1_000_000 + Int128(later.microsecond) - Int128(earlier.microsecond)
        if micros < 0 { micros += SQLGregorianCalendar.dayMicroseconds; days -= 1 }
        if days < 0 { days += Int128(SQLGregorianCalendar.daysInMonth(year: earlier.year, month: earlier.month)); months -= 1 }
        if months < 0 { months += 12; years -= 1 }
        let sign: Int128 = negative ? -1 : 1
        return .interval(try SQLTemporalPrecision.interval(months: sign * (years * 12 + months), days: sign * days, microseconds: sign * micros))
    }
}
