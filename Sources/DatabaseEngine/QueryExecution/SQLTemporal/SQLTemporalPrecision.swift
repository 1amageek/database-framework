import DatabaseKit
import DatabaseTypes

/// PostgreSQL temporal precision and restricted interval field adjustment.
enum SQLTemporalPrecision {
    static func round(_ microseconds: Int128, precision: Int?) throws -> Int128 {
        guard let precision else { return microseconds }
        guard (0...6).contains(precision) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        var unit: Int128 = 1
        for _ in precision..<6 { unit *= 10 }
        let quotient = microseconds / unit
        let remainder = microseconds % unit
        let increment: Int128 = remainder.magnitude * 2 >= UInt128(unit) ? (remainder < 0 ? -1 : 1) : 0
        return (quotient + increment) * unit
    }

    /// A decimal lexical fraction is rounded once, independently of typmod.
    static func fractionalMicroseconds(_ digits: Substring, workMeter: DatabaseWorkMeter) throws -> Int64 {
        var micros: Int64 = 0
        var count = 0
        var firstDiscarded: UInt8 = 0
        var sticky = false
        for digit in digits.utf8 {
            try workMeter.consume(at: .expressionEvaluation)
            guard digit >= 48, digit <= 57 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            if count < 6 { micros = micros * 10 + Int64(digit - 48) }
            else if count == 6 { firstDiscarded = digit - 48 }
            else { sticky = sticky || digit != 48 }
            count += 1
        }
        guard count > 0 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        while count < 6 { micros *= 10; count += 1 }
        if firstDiscarded > 5 || firstDiscarded == 5 && (sticky || !micros.isMultiple(of: 2)) { micros += 1 }
        return micros
    }

    static func interval(months: Int128, days: Int128, microseconds: Int128,
        fields: SQLIntervalFields? = nil, precision: Int? = nil) throws -> QueryIntervalValue {
        var months = months
        var days = days
        var time = microseconds
        switch fields {
        case .year: months = months / 12 * 12; days = 0; time = 0
        case .month, .yearToMonth: days = 0; time = 0
        case .day: time = 0
        case .hour, .dayToHour: time = time / 3_600_000_000 * 3_600_000_000
        case .minute, .dayToMinute, .hourToMinute: time = time / 60_000_000 * 60_000_000
        case .second, .dayToSecond, .hourToSecond, .minuteToSecond, nil: break
        }
        time = try round(time, precision: precision)
        guard let m = Int32(exactly: months), let d = Int32(exactly: days), let t = Int64(exactly: time) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        let seconds = SQLGregorianCalendar.floorDivide(Int128(t), by: 1_000_000)
        let nanos = SQLGregorianCalendar.modulo(Int128(t), 1_000_000) * 1000
        return try QueryIntervalValue(period: CalendarPeriod(months: Int64(m), days: Int64(d)),
            time: TimeSpan(seconds: Int64(seconds), nanoseconds: UInt32(nanos)))
    }
}
