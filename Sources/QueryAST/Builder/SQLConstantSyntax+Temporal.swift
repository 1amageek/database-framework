import DatabaseKit
import DatabaseTypes

extension SQLConstantSyntax {
    /// CYCLE marks are immutable constants, so only environment-independent canonical dates normalize here.
    static func cycleDate(_ text: String, limits: QueryStructuralLimits) throws(SQLConstantError) -> Literal {
        guard UInt64(text.utf8.count) <= limits.maximumRetainedBytes else {
            throw .resourceLimit("CYCLE DATE input byte budget exceeded")
        }
        guard let date = XSDDateTimeFormat.date(from: text), date.year > 0,
              QueryLiteralEncoding.iso8601(date) == text,
              QueryValueValidator.accepts(.primitive(.date(date)), type: .sql(.date)) else {
            throw .unsupportedConstant("CYCLE DATE requires a canonical positive-year ISO date")
        }
        return .queryValue(.primitive(.date(date)), type: .sql(.date))
    }

    static func sqlEraText(_ iso: String) throws(SQLConstantError) -> String {
        let negative = iso.first == "-"
        let digits = negative ? iso.dropFirst() : iso[...]
        guard let separator = digits.firstIndex(of: "-"), let magnitude = Int64(digits[..<separator]) else {
            throw .invalidConstant("Invalid canonical temporal year")
        }
        let year = negative ? -magnitude : magnitude
        guard year <= 0 else { return iso }
        let displayYear = Int64(1).subtractingReportingOverflow(year)
        guard !displayYear.overflow else { throw .invalidConstant("Temporal era year overflow") }
        let raw = String(displayYear.partialValue)
        return String(repeating: "0", count: max(0, 4 - raw.count)) + raw + digits[separator...] + " BC"
    }

    static func civilDateTimeText(_ value: CivilDateTime) throws(SQLConstantError) -> String {
        guard value.time.nanoseconds.isMultiple(of: 1000) else {
            throw .unsupportedConstant("SQL timestamps have microsecond resolution")
        }
        let micros = Int64(value.time.hour) * 3_600_000_000 + Int64(value.time.minute) * 60_000_000
            + Int64(value.time.second) * 1_000_000 + Int64(value.time.nanoseconds / 1000)
        let time: QueryTimeValue
        do { time = try QueryTimeValue(microsecondsSinceMidnight: micros) }
        catch { throw .value(error) }
        return try sqlEraText(QueryLiteralEncoding.iso8601(value.date) + "T" + timeText(time))
    }

    static func timeText(_ value: QueryTimeValue) -> String {
        let micros = value.microsecondsSinceMidnight
        func padded(_ value: Int64, width: Int) -> String {
            let text = String(value)
            return String(repeating: "0", count: max(0, width - text.count)) + text
        }
        var text = padded(micros / 3_600_000_000, width: 2) + ":"
            + padded(micros / 60_000_000 % 60, width: 2) + ":"
            + padded(micros / 1_000_000 % 60, width: 2)
        let fraction = micros % 1_000_000
        if fraction != 0 {
            var digits = padded(fraction, width: 6)
            while digits.last == "0" { digits.removeLast() }
            text += "." + digits
        }
        return text
    }

    static func offsetTimeText(_ value: QueryTimeWithTimeZoneValue) throws(SQLConstantError) -> String {
        let offset = Int64(value.utcOffsetSeconds)
        guard (-57_599...57_599).contains(offset) else {
            throw .unsupportedConstant("Wide TIMETZ offsets require a timezone expression")
        }
        let magnitude = offset < 0 ? -offset : offset
        func padded(_ value: Int64) -> String { value < 10 ? "0" + String(value) : String(value) }
        return timeText(value.time) + (offset < 0 ? "-" : "+")
            + padded(magnitude / 3600) + ":" + padded(magnitude / 60 % 60)
            + ":" + padded(magnitude % 60)
    }

    static func infinityText(_ value: QueryTemporalInfinity) -> String {
        switch value { case .negative: return "-infinity"; case .positive: return "infinity" }
    }

    static func intervalFieldsText(_ fields: SQLIntervalFields) -> String {
        switch fields {
        case .year: return "YEAR"
        case .month: return "MONTH"
        case .day: return "DAY"
        case .hour: return "HOUR"
        case .minute: return "MINUTE"
        case .second: return "SECOND"
        case .yearToMonth: return "YEAR TO MONTH"
        case .dayToHour: return "DAY TO HOUR"
        case .dayToMinute: return "DAY TO MINUTE"
        case .dayToSecond: return "DAY TO SECOND"
        case .hourToMinute: return "HOUR TO MINUTE"
        case .hourToSecond: return "HOUR TO SECOND"
        case .minuteToSecond: return "MINUTE TO SECOND"
        }
    }
}
