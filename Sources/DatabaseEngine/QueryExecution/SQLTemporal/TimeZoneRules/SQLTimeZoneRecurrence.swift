import DatabaseTypes

/// A bounded parsed POSIX footer, with abbreviation ranges in the original owner.
struct SQLTimeZoneRecurrence: Sendable {
    private enum Day: Sendable {
        case julian(Int)
        case ordinal(Int)
        case month(Int, Int, Int)
    }
    private struct Rule: Sendable {
        let day: Day
        let second: Int32
    }
    private let standardOffset: Int32
    private let daylightOffset: Int32?
    private let standardName: Range<Int>
    private let daylightName: Range<Int>
    private let start: Rule?
    private let end: Rule?

    init(_ bytes: UnsafeRawBufferPointer, range: Range<Int>, version: UInt8) throws(SQLTimeZoneError) {
        // The cursor borrows this parser call's input and never survives initialization.
        var cursor = Cursor(bytes: bytes, position: range.lowerBound, end: range.upperBound)
        standardName = try cursor.name()
        standardOffset = -(try cursor.time(maximumHour: 24, signed: true))
        if cursor.position == cursor.end {
            daylightOffset = nil
            daylightName = cursor.position..<cursor.position
            start = nil
            end = nil
        } else {
            daylightName = try cursor.name()
            if cursor.peek != 44 {
                daylightOffset = -(try cursor.time(maximumHour: 24, signed: true))
            } else {
                daylightOffset = standardOffset + 3600
            }
            try cursor.expect(44)
            start = try cursor.rule(version: version)
            try cursor.expect(44)
            end = try cursor.rule(version: version)
            guard cursor.position == cursor.end else { throw .malformedData }
        }
    }

    func selection(at second: Int64) -> (offset: Int32, abbreviation: Range<Int>) {
        guard let daylightOffset, let start, let end else { return (standardOffset, standardName) }
        let year = Self.year(at: second)
        if perpetualDaylight(year: year) { return (daylightOffset, daylightName) }
        var latest: Int128?
        var latestIsStart = false
        for candidateYear in (year - 2)...(year + 2) {
            guard !perpetualDaylight(year: candidateYear) else { continue }
            for index in 0..<2 {
                let isStart = index == 0
                // The preceding event may lie just outside Int64's timeline;
                // retain its widened instant when selecting the boundary state.
                let event = Self.days(year: candidateYear, month: 1, day: 1) * 86400
                    + relativeSecond(rule: isStart ? start : end, year: candidateYear)
                    - Int128(isStart ? standardOffset : daylightOffset)
                if event <= Int128(second), latest == nil || event > latest! {
                    latest = event
                    latestIsStart = isStart
                }
            }
        }
        return latest == nil || latestIsStart ? (daylightOffset, daylightName) : (standardOffset, standardName)
    }

    func designation(at second: Int64, bytes: ByteString, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Designation {
        try workMeter.consume(daylightOffset == nil ? 1 : 11, at: .expressionEvaluation)
        let result = selection(at: second)
        let name = bytes[result.abbreviation]
        if name.count == 3, name[0] == 45, name[1] == 48, name[2] == 48 { throw SQLTimeZoneError.rangeUnavailable }
        return SQLTimeZoneRules.Designation(offsetSeconds: result.offset, abbreviation: name)
    }

    func transition(beforeOrAt second: Int64, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Transition? {
        try search(second: second, before: true, workMeter: workMeter)
    }

    func transition(after second: Int64, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Transition? {
        try search(second: second, before: false, workMeter: workMeter)
    }

    private func search(second: Int64, before: Bool, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Transition? {
        try workMeter.consume(at: .expressionEvaluation)
        guard daylightOffset != nil else { return nil }
        let year = Self.year(at: second)
        var best: SQLTimeZoneRules.Transition?
        for candidateYear in (year - 2)...(year + 2) {
            try workMeter.consume(2, at: .expressionEvaluation)
            for index in 0..<2 {
                guard let event = event(year: candidateYear, isStart: index == 0) else { continue }
                let eligible = before ? event.instant.secondsSinceUnixEpoch <= second : event.instant.secondsSinceUnixEpoch > second
                if eligible, best == nil || (before ? event.instant > best!.instant : event.instant < best!.instant) { best = event }
            }
        }
        return best
    }

    private func event(year: Int64, isStart: Bool) -> SQLTimeZoneRules.Transition? {
        guard let daylightOffset, let start, let end, !perpetualDaylight(year: year),
              let second = eventSecond(rule: isStart ? start : end, year: year,
                                       offset: isStart ? standardOffset : daylightOffset) else { return nil }
        return SQLTimeZoneRules.Transition(instant: Timestamp(secondsSinceUnixEpoch: second),
            offsetBefore: isStart ? standardOffset : daylightOffset,
            offsetAfter: isStart ? daylightOffset : standardOffset)
    }

    private func perpetualDaylight(year: Int64) -> Bool {
        guard let daylightOffset, let start, let end else { return false }
        let first = relativeSecond(rule: start, year: year) - Int128(standardOffset)
        let last = relativeSecond(rule: end, year: year) - Int128(daylightOffset)
        return first == last || (first < last && last - first >= (Self.isLeap(year) ? 366 : 365) * 86400)
    }

    private func eventSecond(rule: Rule, year: Int64, offset: Int32) -> Int64? {
        Int64(exactly: Self.days(year: year, month: 1, day: 1) * 86400
            + relativeSecond(rule: rule, year: year) - Int128(offset))
    }

    private func relativeSecond(rule: Rule, year: Int64) -> Int128 {
        let day: Int
        switch rule.day {
        case .julian(let number): day = number - 1 + (Self.isLeap(year) && number >= 60 ? 1 : 0)
        case .ordinal(let number): day = number
        case .month(let month, let week, let weekday):
            let first = Self.days(year: year, month: month, day: 1)
            let firstWeekday = Int(Self.mod(first + 4, 7))
            var date = 1 + (weekday - firstWeekday + 7) % 7 + (week - 1) * 7
            let length = Self.monthLength(year: year, month: month)
            if date > length { date -= 7 }
            day = Int(first - Self.days(year: year, month: 1, day: 1)) + date - 1
        }
        return Int128(day) * 86400 + Int128(rule.second)
    }

    private static func isLeap(_ year: Int64) -> Bool {
        year.isMultiple(of: 4) && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
    }

    private static func monthLength(year: Int64, month: Int) -> Int {
        switch month {
        case 2: return isLeap(year) ? 29 : 28
        case 4, 6, 9, 11: return 30
        default: return 31
        }
    }

    private static func floor(_ value: Int128, _ divisor: Int128) -> Int128 {
        let q = value / divisor
        return value % divisor < 0 ? q - 1 : q
    }
    private static func mod(_ value: Int128, _ divisor: Int128) -> Int128 {
        value - floor(value, divisor) * divisor
    }
    private static func days(year: Int64, month: Int, day: Int) -> Int128 {
        let y = Int128(year) - (month <= 2 ? 1 : 0)
        let era = floor(y, 400)
        let yoe = y - era * 400
        let m = Int128(month + (month > 2 ? -3 : 9))
        let doy = (153 * m + 2) / 5 + Int128(day) - 1
        return era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
    }
    private static func year(at second: Int64) -> Int64 {
        let z = floor(Int128(second), 86400) + 719468
        let era = floor(z, 146097)
        let doe = z - era * 146097
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
        let mp = (5 * doy + 2) / 153
        return Int64(yoe + era * 400 + (mp < 10 ? 0 : 1))
    }

    private struct Cursor {
        let bytes: UnsafeRawBufferPointer
        var position: Int
        let end: Int
        var peek: UInt8? { position < end ? bytes[position] : nil }

        mutating func expect(_ value: UInt8) throws(SQLTimeZoneError) {
            guard peek == value else { throw .malformedData }
            position += 1
        }
        mutating func number(maximum: Int) throws(SQLTimeZoneError) -> Int {
            var value = 0
            let beginning = position
            while let byte = peek, byte >= 48, byte <= 57 {
                let digit = Int(byte - 48)
                guard value <= (maximum - digit) / 10, digit <= maximum else { throw .malformedData }
                value = value * 10 + digit
                guard value <= maximum else { throw .malformedData }
                position += 1
            }
            guard position > beginning else { throw .malformedData }
            return value
        }
        mutating func name() throws(SQLTimeZoneError) -> Range<Int> {
            let quoted = peek == 60
            if quoted { position += 1 }
            let beginning = position
            while let byte = peek {
                let alpha = (byte >= 65 && byte <= 90) || (byte >= 97 && byte <= 122)
                guard alpha || (quoted && ((byte >= 48 && byte <= 57) || byte == 43 || byte == 45)) else { break }
                position += 1
            }
            guard position - beginning >= 3 else { throw .malformedData }
            let result = beginning..<position
            if quoted { try expect(62) }
            return result
        }
        mutating func time(maximumHour: Int, signed: Bool) throws(SQLTimeZoneError) -> Int32 {
            var sign: Int32 = 1
            if peek == 45 || peek == 43 {
                guard signed else { throw .malformedData }
                if peek == 45 { sign = -1 }
                position += 1
            }
            var result = try number(maximum: maximumHour) * 3600
            if peek == 58 {
                position += 1
                result += try number(maximum: 59) * 60
                if peek == 58 { position += 1; result += try number(maximum: 59) }
            }
            return Int32(result) * sign
        }
        mutating func rule(version: UInt8) throws(SQLTimeZoneError) -> Rule {
            let day: Day
            if peek == 74 {
                position += 1
                let number = try number(maximum: 365)
                guard number >= 1 else { throw .malformedData }
                day = .julian(number)
            } else if peek == 77 {
                position += 1
                let month = try number(maximum: 12)
                try expect(46)
                let week = try number(maximum: 5)
                try expect(46)
                let weekday = try number(maximum: 6)
                guard month >= 1, week >= 1 else { throw .malformedData }
                day = .month(month, week, weekday)
            } else {
                day = .ordinal(try number(maximum: 365))
            }
            var second: Int32 = 7200
            if peek == 47 {
                position += 1
                second = try time(maximumHour: version == 50 ? 24 : 167, signed: version != 50)
            }
            return Rule(day: day, second: second)
        }
    }
}
