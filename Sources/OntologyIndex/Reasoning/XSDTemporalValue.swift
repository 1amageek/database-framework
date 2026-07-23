/// Exact XSD 1.1 date, dateTime, or time value used for lexical validation and
/// partial ordering without Foundation date/locale services.
package struct XSDTemporalValue: Sendable {
    package enum Kind: Sendable, Equatable {
        case date
        case dateTime
        case time
    }

    package let kind: Kind
    let date: CivilDate
    let hour: Int
    let minute: Int
    let second: Int
    let fraction: Fraction
    package let timezoneOffsetMinutes: Int?

    package init?(lexicalForm: String, kind: Kind) {
        var parser = TemporalParser(lexicalForm)
        let parsedDate: CivilDate
        switch kind {
        case .date, .dateTime:
            guard let date = parser.readDate() else { return nil }
            parsedDate = date
        case .time:
            // XSD defines time ordering on an arbitrary reference date.
            parsedDate = CivilDate(year: 1972, month: 12, day: 31)
        }

        var parsedHour = 0
        var parsedMinute = 0
        var parsedSecond = 0
        var parsedFraction = Fraction.zero
        if kind != .date {
            if kind == .dateTime, !parser.read(84) { return nil }
            guard let hour = parser.readFixedInteger(count: 2),
                  parser.read(58),
                  let minute = parser.readFixedInteger(count: 2),
                  parser.read(58),
                  let second = parser.readFixedInteger(count: 2),
                  hour <= 24,
                  minute <= 59,
                  second <= 59 else {
                return nil
            }
            parsedHour = hour
            parsedMinute = minute
            parsedSecond = second
            if parser.readIfPresent(46) {
                guard let fraction = parser.readFraction() else { return nil }
                parsedFraction = fraction
            }
            guard parsedHour < 24
                    || (parsedMinute == 0
                        && parsedSecond == 0
                        && parsedFraction.isZero) else {
                return nil
            }
        }

        guard let timezone = parser.readTimezone(), parser.isAtEnd else {
            return nil
        }

        var normalizedDate = parsedDate
        if parsedHour == 24 {
            if kind == .dateTime {
                guard let nextDate = parsedDate.addingOneDay() else { return nil }
                normalizedDate = nextDate
            }
            parsedHour = 0
        }
        self.kind = kind
        self.date = normalizedDate
        self.hour = parsedHour
        self.minute = parsedMinute
        self.second = parsedSecond
        self.fraction = parsedFraction
        self.timezoneOffsetMinutes = timezone
    }

    /// Returns -1, 0, or 1, or nil when XSD's partial order is indeterminate.
    package func compare(to other: XSDTemporalValue) -> Int? {
        guard kind == other.kind else { return nil }
        switch (timezoneOffsetMinutes, other.timezoneOffsetMinutes) {
        case (.some(let lhsOffset), .some(let rhsOffset)):
            guard let lhs = point(normalizedFrom: lhsOffset),
                  let rhs = other.point(normalizedFrom: rhsOffset) else {
                return nil
            }
            return lhs.compare(to: rhs)
        case (.none, .none):
            return point.compare(to: other.point)
        case (.none, .some(let rhsOffset)):
            guard let earliest = point(normalizedFrom: 840),
                  let latest = point(normalizedFrom: -840),
                  let rhs = other.point(normalizedFrom: rhsOffset) else {
                return nil
            }
            if latest.compare(to: rhs) < 0 { return -1 }
            if earliest.compare(to: rhs) > 0 { return 1 }
            return nil
        case (.some(let lhsOffset), .none):
            guard let lhs = point(normalizedFrom: lhsOffset),
                  let earliest = other.point(normalizedFrom: 840),
                  let latest = other.point(normalizedFrom: -840) else {
                return nil
            }
            if lhs.compare(to: earliest) < 0 { return -1 }
            if lhs.compare(to: latest) > 0 { return 1 }
            return nil
        }
    }

    package func isIdentical(to other: XSDTemporalValue) -> Bool {
        kind == other.kind
            && date.year == other.date.year
            && date.month == other.date.month
            && date.day == other.date.day
            && hour == other.hour
            && minute == other.minute
            && second == other.second
            && fraction.compare(to: other.fraction) == 0
            && timezoneOffsetMinutes == other.timezoneOffsetMinutes
    }

    private var point: Point {
        Point(
            date: date,
            hour: hour,
            minute: minute,
            second: second,
            fraction: fraction
        )
    }

    private func point(normalizedFrom offset: Int) -> Point? {
        point.adjusting(minutes: -offset)
    }
}

extension XSDTemporalValue {
    struct CivilDate: Sendable {
        let year: Int64
        let month: Int
        let day: Int

        init?(validatingYear year: Int64, month: Int, day: Int) {
            guard (1...12).contains(month),
                  day >= 1,
                  day <= Self.daysInMonth(year: year, month: month) else {
                return nil
            }
            self.year = year
            self.month = month
            self.day = day
        }

        init(year: Int64, month: Int, day: Int) {
            self.year = year
            self.month = month
            self.day = day
        }

        func addingOneDay() -> CivilDate? {
            let maximumDay = Self.daysInMonth(year: year, month: month)
            if day < maximumDay {
                return CivilDate(year: year, month: month, day: day + 1)
            }
            if month < 12 {
                return CivilDate(year: year, month: month + 1, day: 1)
            }
            let nextYear = year.addingReportingOverflow(1)
            guard !nextYear.overflow else { return nil }
            return CivilDate(year: nextYear.partialValue, month: 1, day: 1)
        }

        func subtractingOneDay() -> CivilDate? {
            if day > 1 {
                return CivilDate(year: year, month: month, day: day - 1)
            }
            if month > 1 {
                let previousMonth = month - 1
                return CivilDate(
                    year: year,
                    month: previousMonth,
                    day: Self.daysInMonth(year: year, month: previousMonth)
                )
            }
            let previousYear = year.subtractingReportingOverflow(1)
            guard !previousYear.overflow else { return nil }
            return CivilDate(
                year: previousYear.partialValue,
                month: 12,
                day: 31
            )
        }

        private static func daysInMonth(year: Int64, month: Int) -> Int {
            switch month {
            case 2:
                let leap = year.isMultiple(of: 4)
                    && (!year.isMultiple(of: 100)
                        || year.isMultiple(of: 400))
                return leap ? 29 : 28
            case 4, 6, 9, 11:
                return 30
            default:
                return 31
            }
        }
    }

    struct Fraction: Sendable {
        static let zero = Fraction(digits: ""[...])

        let digits: Substring

        init(digits: Substring) {
            var normalized = digits
            while normalized.last == "0" {
                normalized = normalized.dropLast()
            }
            self.digits = normalized
        }

        var isZero: Bool { digits.isEmpty }

        func compare(to other: Fraction) -> Int {
            var lhs = digits.utf8.makeIterator()
            var rhs = other.digits.utf8.makeIterator()
            let count = max(digits.utf8.count, other.digits.utf8.count)
            for _ in 0..<count {
                let lhsDigit = (lhs.next() ?? 48) - 48
                let rhsDigit = (rhs.next() ?? 48) - 48
                if lhsDigit != rhsDigit {
                    return lhsDigit < rhsDigit ? -1 : 1
                }
            }
            return 0
        }
    }

    private struct Point {
        let date: CivilDate
        let hour: Int
        let minute: Int
        let second: Int
        let fraction: Fraction

        func adjusting(minutes delta: Int) -> Point? {
            var date = date
            var minuteOfDay = hour * 60 + minute + delta
            while minuteOfDay < 0 {
                guard let previous = date.subtractingOneDay() else { return nil }
                date = previous
                minuteOfDay += 1_440
            }
            while minuteOfDay >= 1_440 {
                guard let next = date.addingOneDay() else { return nil }
                date = next
                minuteOfDay -= 1_440
            }
            return Point(
                date: date,
                hour: minuteOfDay / 60,
                minute: minuteOfDay % 60,
                second: second,
                fraction: fraction
            )
        }

        func compare(to other: Point) -> Int {
            if date.year != other.date.year {
                return date.year < other.date.year ? -1 : 1
            }
            if date.month != other.date.month {
                return date.month < other.date.month ? -1 : 1
            }
            if date.day != other.date.day {
                return date.day < other.date.day ? -1 : 1
            }
            if hour != other.hour { return hour < other.hour ? -1 : 1 }
            if minute != other.minute { return minute < other.minute ? -1 : 1 }
            if second != other.second { return second < other.second ? -1 : 1 }
            return fraction.compare(to: other.fraction)
        }
    }
}

private struct TemporalParser {
    private let source: String
    private let bytes: String.UTF8View
    private var index: String.UTF8View.Index

    init(_ source: String) {
        let bytes = source.utf8
        self.source = source
        self.bytes = bytes
        self.index = bytes.startIndex
    }

    var isAtEnd: Bool { index == bytes.endIndex }

    mutating func read(_ expected: UInt8) -> Bool {
        guard index != bytes.endIndex, bytes[index] == expected else {
            return false
        }
        bytes.formIndex(after: &index)
        return true
    }

    mutating func readIfPresent(_ expected: UInt8) -> Bool {
        read(expected)
    }

    mutating func readDate() -> XSDTemporalValue.CivilDate? {
        let negative = readIfPresent(45)
        let maximumMagnitude = negative
            ? UInt64(Int64.max) + 1
            : UInt64(Int64.max)
        var magnitude: UInt64 = 0
        var digitCount = 0
        var firstDigit: UInt8?
        while index != bytes.endIndex, bytes[index] != 45 {
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { return nil }
            if firstDigit == nil { firstDigit = byte }
            let digit = UInt64(byte - 48)
            guard magnitude <= (maximumMagnitude - digit) / 10 else {
                return nil
            }
            magnitude = magnitude * 10 + digit
            digitCount += 1
            bytes.formIndex(after: &index)
        }
        guard digitCount >= 4,
              digitCount == 4 || firstDigit != 48,
              read(45),
              let month = readFixedInteger(count: 2),
              read(45),
              let day = readFixedInteger(count: 2) else {
            return nil
        }
        let year: Int64
        if negative {
            guard magnitude != 0 else { return nil }
            year = magnitude == UInt64(Int64.max) + 1
                ? .min
                : -Int64(magnitude)
        } else {
            year = Int64(magnitude)
        }
        return XSDTemporalValue.CivilDate(
            validatingYear: year,
            month: month,
            day: day
        )
    }

    mutating func readFixedInteger(count: Int) -> Int? {
        var value = 0
        for _ in 0..<count {
            guard index != bytes.endIndex else { return nil }
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { return nil }
            value = value * 10 + Int(byte - 48)
            bytes.formIndex(after: &index)
        }
        return value
    }

    mutating func readFraction() -> XSDTemporalValue.Fraction? {
        let start = index
        while index != bytes.endIndex {
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { break }
            bytes.formIndex(after: &index)
        }
        guard start != index else { return nil }
        return XSDTemporalValue.Fraction(digits: source[start..<index])
    }

    /// Returns nil for an absent timezone as a successful parse. The outer
    /// optional distinguishes parser failure by requiring the caller to verify
    /// that all input was consumed.
    mutating func readTimezone() -> Int?? {
        guard index != bytes.endIndex else { return .some(nil) }
        if readIfPresent(90) { return .some(0) }
        let sign: Int
        if readIfPresent(43) {
            sign = 1
        } else if readIfPresent(45) {
            sign = -1
        } else {
            return nil
        }
        guard let hour = readFixedInteger(count: 2),
              read(58),
              let minute = readFixedInteger(count: 2),
              hour <= 14,
              minute <= 59,
              hour < 14 || minute == 0 else {
            return nil
        }
        return .some(sign * (hour * 60 + minute))
    }
}
