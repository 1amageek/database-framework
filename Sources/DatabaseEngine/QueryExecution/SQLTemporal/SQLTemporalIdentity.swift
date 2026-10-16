import DatabaseKit
import DatabaseTypes

/// Fixed SQL semantic identity, shared by comparison and relational hash keys.
struct SQLTemporalIdentity: Sendable, Hashable, Comparable {
    enum Family: UInt8, Sendable { case date, time, timeWithTimeZone, timestamp, timestampWithTimeZone, interval }
    let family: Family
    let infinityRank: Int8
    let scalar: Int128
    let offsetTie: Int64

    static func make(_ value: QueryValue, type: QueryColumnType, workMeter: DatabaseWorkMeter) throws -> Self {
        try workMeter.consume(at: .expressionEvaluation)
        func finite(_ family: Family, _ scalar: Int128, tie: Int64 = 0) -> Self {
            Self(family: family, infinityRank: 0, scalar: scalar, offsetTie: tie)
        }
        func infinity(_ family: Family, _ sign: QueryTemporalInfinity) -> Self {
            Self(family: family, infinityRank: sign == .negative ? -1 : 1, scalar: 0, offsetTie: 0)
        }
        switch value {
        case .dateInfinity(let sign): return infinity(.date, sign)
        case .timestampInfinity(let sign, let withTimeZone): return infinity(withTimeZone ? .timestampWithTimeZone : .timestamp, sign)
        case .intervalInfinity(let sign): return infinity(.interval, sign)
        case .time(let time): return finite(.time, Int128(time.microsecondsSinceMidnight))
        case .timeWithTimeZone(let time):
            return finite(.timeWithTimeZone, Int128(time.time.microsecondsSinceMidnight) - Int128(time.utcOffsetSeconds) * 1_000_000,
                tie: -Int64(time.utcOffsetSeconds))
        case .interval(let interval):
            return finite(.interval, (Int128(interval.period.months) * 30 + Int128(interval.period.days)) * SQLGregorianCalendar.dayMicroseconds + Int128(interval.microseconds))
        case .primitive(.date(let date)):
            let jd = try SQLGregorianCalendar.julianDay(year: date.year, month: Int32(date.month), day: Int32(date.day))
            try SQLGregorianCalendar.admitDate(jd)
            return finite(.date, Int128(jd - SQLGregorianCalendar.epochJulianDay))
        case .primitive(.time(let time)):
            guard time.nanoseconds.isMultiple(of: 1000) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            return finite(.time, Int128(time.hour) * 3_600_000_000 + Int128(time.minute) * 60_000_000 + Int128(time.second) * 1_000_000 + Int128(time.nanoseconds / 1000))
        case .primitive(.dateTime(let dateTime)):
            guard dateTime.time.nanoseconds.isMultiple(of: 1000) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            let fields = SQLGregorianCalendar.Fields(year: dateTime.date.year, month: Int32(dateTime.date.month), day: Int32(dateTime.date.day),
                hour: Int32(dateTime.time.hour), minute: Int32(dateTime.time.minute), second: Int32(dateTime.time.second), microsecond: Int32(dateTime.time.nanoseconds / 1000))
            let micros = try SQLGregorianCalendar.microseconds(fields)
            try SQLGregorianCalendar.admitTimestamp(micros)
            return finite(.timestamp, micros)
        case .primitive(.timestamp(let timestamp)):
            guard timestamp.nanoseconds.isMultiple(of: 1000) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            let micros = SQLGregorianCalendar.microseconds(timestamp)
            try SQLGregorianCalendar.admitTimestamp(micros)
            return finite(.timestampWithTimeZone, micros)
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal identity")
        }
    }

    static func < (lhs: Self, rhs: Self) -> Bool {
        if lhs.family != rhs.family { return lhs.family.rawValue < rhs.family.rawValue }
        if lhs.infinityRank != rhs.infinityRank { return lhs.infinityRank < rhs.infinityRank }
        if lhs.scalar != rhs.scalar { return lhs.scalar < rhs.scalar }
        return lhs.offsetTie < rhs.offsetTie
    }

    func compare(to other: Self) throws -> QueryComparison {
        guard family == other.family else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal comparison families") }
        if infinityRank != other.infinityRank { return infinityRank < other.infinityRank ? .lessThan : .greaterThan }
        if scalar != other.scalar { return scalar < other.scalar ? .lessThan : .greaterThan }
        if offsetTie != other.offsetTie { return offsetTie < other.offsetTie ? .lessThan : .greaterThan }
        return .equal
    }
}
