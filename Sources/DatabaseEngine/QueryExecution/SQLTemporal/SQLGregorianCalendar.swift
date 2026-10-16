import DatabaseKit
import DatabaseTypes

/// Checked proleptic Gregorian arithmetic; SQL domain admission is separate.
enum SQLGregorianCalendar {
    static let epochJulianDay: Int64 = 2_451_545
    static let dayMicroseconds: Int128 = 86_400_000_000

    struct Fields: Sendable, Equatable {
        var year: Int32
        var month: Int32
        var day: Int32
        var hour: Int32 = 0
        var minute: Int32 = 0
        var second: Int32 = 0
        var microsecond: Int32 = 0
        var julianDay: Int64 = 0
    }

    struct ISOFields: Sendable, Equatable {
        let year: Int32
        let week: Int32
        let weekday: Int32
    }

    static func floorDivide(_ value: Int128, by divisor: Int128) -> Int128 {
        let quotient = value / divisor
        return value % divisor < 0 ? quotient - 1 : quotient
    }

    static func modulo(_ value: Int128, _ divisor: Int128) -> Int128 {
        value - floorDivide(value, by: divisor) * divisor
    }

    static func daysInMonth(year: Int32, month: Int32) -> Int32 {
        switch month {
        case 2: return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28
        case 4, 6, 9, 11: return 30
        default: return 31
        }
    }

    static func julianDay(year: Int32, month: Int32, day: Int32) throws -> Int64 {
        guard (1...12).contains(month), day >= 1,
              day <= daysInMonth(year: year, month: month) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        // March-based 400-year cycles also work for astronomical year zero.
        let y = Int128(year) - (month <= 2 ? 1 : 0)
        let era = floorDivide(y, by: 400)
        let within = y - era * 400
        let m = Int128(month) + (month > 2 ? -3 : 9)
        let ordinal = (153 * m + 2) / 5 + Int128(day) - 1
        return Int64(era * 146097 + within * 365 + within / 4 - within / 100 + ordinal + 1_721_120)
    }

    static func fields(julianDay: Int64, microsecondsOfDay: Int64 = 0) throws -> Fields {
        guard microsecondsOfDay >= 0, Int128(microsecondsOfDay) < dayMicroseconds else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        let z = Int128(julianDay) - 1_721_120
        let era = floorDivide(z, by: 146097)
        let doe = z - era * 146097
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
        let mp = (5 * doy + 2) / 153
        let day = doy - (153 * mp + 2) / 5 + 1
        let month = mp + (mp < 10 ? 3 : -9)
        let year = yoe + era * 400 + (month <= 2 ? 1 : 0)
        guard let year = Int32(exactly: year) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        return Fields(year: year, month: Int32(month), day: Int32(day),
            hour: Int32(microsecondsOfDay / 3_600_000_000),
            minute: Int32(microsecondsOfDay / 60_000_000 % 60),
            second: Int32(microsecondsOfDay / 1_000_000 % 60),
            microsecond: Int32(microsecondsOfDay % 1_000_000), julianDay: julianDay)
    }

    static func fields(pgMicroseconds: Int128) throws -> Fields {
        let days = floorDivide(pgMicroseconds, by: dayMicroseconds)
        guard let jd = Int64(exactly: days + Int128(epochJulianDay)) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        return try fields(julianDay: jd, microsecondsOfDay: Int64(modulo(pgMicroseconds, dayMicroseconds)))
    }

    static func microseconds(_ fields: Fields) throws -> Int128 {
        guard (0...23).contains(fields.hour), (0...59).contains(fields.minute),
              (0...59).contains(fields.second), (0...999999).contains(fields.microsecond) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        let jd = try julianDay(year: fields.year, month: fields.month, day: fields.day)
        return Int128(jd - epochJulianDay) * dayMicroseconds + Int128(fields.hour) * 3_600_000_000
            + Int128(fields.minute) * 60_000_000 + Int128(fields.second) * 1_000_000 + Int128(fields.microsecond)
    }

    static func dayOfWeek(julianDay: Int64) -> Int32 {
        Int32(modulo(Int128(julianDay) + 1, 7))
    }

    static func dayOfYear(julianDay: Int64) throws -> Int32 {
        let value = try fields(julianDay: julianDay)
        return Int32(julianDay - (try self.julianDay(year: value.year, month: 1, day: 1)) + 1)
    }

    static func isoFields(julianDay: Int64) throws -> ISOFields {
        let weekday = Int32(modulo(Int128(julianDay), 7)) + 1
        let thursday = julianDay + Int64(4 - weekday)
        let year = try fields(julianDay: thursday).year
        let jan4 = try self.julianDay(year: year, month: 1, day: 4)
        let firstMonday = jan4 - Int64(modulo(Int128(jan4), 7))
        return ISOFields(year: year, week: Int32((julianDay - firstMonday) / 7 + 1), weekday: weekday)
    }

    /// Preserves PostgreSQL date2j's truncating arithmetic, including synthetic
    /// interval fields, while rejecting every undefined signed-overflow stage.
    static func syntheticJulianDay(year: Int32, month: Int32, day: Int32) throws -> Int64 {
        func checked(_ value: Int64) throws -> Int64 {
            guard Int32(exactly: value) != nil else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return value
        }
        let m = try checked(Int64(month) + (month > 2 ? 1 : 13))
        let y = try checked(Int64(year) + (month > 2 ? 4800 : 4799))
        let century = y / 100
        var jd = try checked(try checked(y * 365) - 32167)
        let leap = try checked(try checked(y / 4 - century) + century / 4)
        jd = try checked(jd + leap)
        let tail = try checked(try checked(7834 * m) / 256 + Int64(day))
        return try checked(jd + tail)
    }

    static func admitDate(_ julianDay: Int64) throws {
        guard julianDay >= 0, julianDay < 2_147_483_494 else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
    }

    static func admitTimestamp(_ micros: Int128) throws {
        guard micros >= -211_813_488_000_000_000, micros < 9_223_371_331_200_000_000 else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
    }

    static func instant(pgMicroseconds: Int128) throws -> Timestamp {
        try admitTimestamp(pgMicroseconds)
        let unix = pgMicroseconds + 946_684_800_000_000
        let seconds = floorDivide(unix, by: 1_000_000)
        guard let seconds = Int64(exactly: seconds) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        return try Timestamp(secondsSinceUnixEpoch: seconds, nanoseconds: UInt32(modulo(unix, 1_000_000)) * 1000)
    }

    static func microseconds(_ instant: Timestamp) -> Int128 {
        Int128(instant.secondsSinceUnixEpoch) * 1_000_000 + Int128(instant.nanoseconds / 1000) - 946_684_800_000_000
    }
}
