import DatabaseKit

/// Template-directed input fields; this does not invoke the lexical CAST parser.
enum SQLTemporalFormatParser {
    struct Parsed {
        let fields: SQLGregorianCalendar.Fields
        let offsetSeconds: Int32?
        let precision: Int?
    }

    private struct Fields {
        var year: Int64 = 0, yearWidth = 0, century: Int64 = 0, bc: Int64 = 0
        var month: Int64 = 0, day: Int64 = 0, ordinal: Int64 = 0, weekday: Int64 = 0
        var week: Int64 = 0, monthWeek: Int64 = 0, julian: Int64 = 0
        var hour: Int64 = 0, minute: Int64 = 0, second: Int64 = 0, seconds: Int64 = 0
        var milliseconds: Int64 = 0, microseconds: Int64 = 0, twelveHour = false
        var meridiem: Int64 = 0, convention = 0, precision: Int? = nil
        var zoneHour: Int64 = 0, zoneMinute: Int64 = 0, zoneSign: Int64 = 0
        var abbreviationOffset: Int32? = nil


    }

    private struct Cursor {
        let text: Substring
        let meter: DatabaseWorkMeter
        var index: String.Index
        init(_ text: Substring, meter: DatabaseWorkMeter) {
            self.text = text; self.meter = meter; index = text.startIndex
        }
        var scalar: UInt32? { index < text.endIndex ? text.unicodeScalars[index].value : nil }
        mutating func advance() throws {
            guard index < text.endIndex else { return }
            try meter.consume(at: .expressionEvaluation)
            index = text.unicodeScalars.index(after: index)
        }
        mutating func spaces() throws -> Int {
            var count = 0
            while let scalar, SQLTemporalFormat.isSpace(scalar) { try advance(); count += 1 }
            return count
        }
        func matches(_ name: String) -> Bool {
            var position = index
            for byte in name.utf8 {
                guard position < text.endIndex else { return false }
                let scalar = text.unicodeScalars[position].value
                let folded = (65...90).contains(scalar) ? scalar + 32 : scalar
                let expected = (65...90).contains(byte) ? byte + 32 : byte
                if folded != UInt32(expected) { return false }
                position = text.unicodeScalars.index(after: position)
            }
            return true
        }
        mutating func name(_ names: [String]) throws -> Int {
            for position in names.indices {
                try meter.consume(at: .expressionEvaluation)
                if matches(names[position]) {
                    for _ in names[position].utf8 { try advance() }
                    return position
                }
            }
            throw DatabaseExpressionEvaluationError.invalidTemporalInput
        }
        mutating func number(width: Int, variable: Bool) throws -> (value: Int64, used: Int) {
            let leading = try spaces()
            var sign: Int64 = 1, used = 0
            if scalar == 43 || scalar == 45 {
                sign = scalar == 45 ? -1 : 1; try advance(); used += 1
            }
            var value: Int64 = 0, digits = 0
            while let scalar, (48...57).contains(scalar), variable || used < width {
                try advance(); used += 1; digits += 1
                value = value * 10 + Int64(scalar - 48)
                guard value <= (sign < 0 ? 2_147_483_648 : 2_147_483_647) else {
                    throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
                }
            }
            guard digits > 0, variable || used == width else {
                throw DatabaseExpressionEvaluationError.invalidTemporalInput
            }
            return (value * sign, used + leading)
        }
    }

    static func parse(_ input: Substring, template: Substring,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws -> Parsed {
        var scanner = SQLTemporalFormat.Scanner(template, workMeter: workMeter)
        var cursor = Cursor(input, meter: workMeter)
        var fields = Fields(), fixed = false, extraSkip = 0, first = true
        var node = try scanner.next()
        while let current = node, cursor.scalar != nil {
            let next = try scanner.next()
            if !fixed && !current.token.isFixed && (first || current.token != nil) {
                extraSkip += try cursor.spaces()
            }
            first = false
            guard cursor.scalar != nil else { break }
            guard let token = current.token else {
                switch current.kind {
                case .separator, .space:
                    if fixed { try cursor.advance() }
                    else {
                        extraSkip -= 1
                        if let scalar = cursor.scalar,
                           SQLTemporalFormat.isSpace(scalar) || SQLTemporalFormat.isSeparator(scalar) {
                            try cursor.advance(); extraSkip += 1
                        }
                    }
                case .literal:
                    if !fixed && extraSkip > 0 { extraSkip -= 1 }
                    else { try cursor.advance() }
                case .token: break
                }
                node = next
                continue
            }
            if token.convention != 0 {
                guard fields.convention == 0 || fields.convention == token.convention else {
                    throw SQLTemporalFormat.invalid("mixed Gregorian and ISO template fields")
                }
                fields.convention = token.convention
            }
            let variable = current.fill || current.ordinal != nil || next == nil ||
                next?.token == nil || next?.token?.numeric == false
            func partialYear(_ value: Int64) -> Int64 {
                value < 70 ? value + 2000 : value < 100 ? value + 1900 :
                    value < 520 ? value + 2000 : value < 1000 ? value + 1000 : value
            }
            switch token {
            case .fixed: fixed = true
            case .meridiem:
                try assign(&fields.meridiem, Int64(try cursor.name(current.spelling.contains(".") ? ["A.M.", "P.M."] : ["AM", "PM"])))
                fields.twelveHour = true
            case .era:
                try assign(&fields.bc, Int64(try cursor.name(current.spelling.contains(".") ? ["A.D.", "B.C."] : ["AD", "BC"])))
            case .monthName(let full):
                try assign(&fields.month, Int64(try cursor.name(full ? SQLTemporalFormat.months : SQLTemporalFormat.shortMonths)) + 1)
            case .weekdayName(let full):
                try assign(&fields.weekday, Int64(try cursor.name(full ? SQLTemporalFormat.weekdays : SQLTemporalFormat.shortWeekdays)) + 1)
            case .roman:
                // Longest roman numeral wins, so I does not consume the start of VIII.
                let position = try cursor.name(["VIII", "VII", "XII", "III", "VI", "XI", "IV", "IX", "II", "V", "X", "I"])
                try assign(&fields.month, [8, 7, 12, 3, 6, 11, 4, 9, 2, 5, 10, 1][position])
            case .zone:
                if let scalar = cursor.scalar, (65...90).contains(scalar) || (97...122).contains(scalar) {
                    let start = cursor.index
                    var letters = 0
                    while letters < 10, let scalar = cursor.scalar,
                          (65...90).contains(scalar) || (97...122).contains(scalar) {
                        try cursor.advance(); letters += 1
                    }
                    let end = cursor.index
                    var candidate = end, found: Int32? = nil
                    while candidate > start {
                        let text = input[start..<candidate]
                        let claim = try workMeter.reserveIntermediate(bytes: UInt64(text.utf8.count), at: .expressionEvaluation)
                        do { found = try environment.configuration.fixedOffset(forAbbreviation: String(text), workMeter: workMeter) }
                        catch { claim.release(); throw error }
                        claim.release()
                        if found != nil { cursor.index = candidate; break }
                        candidate = input.unicodeScalars.index(before: candidate)
                    }
                    guard let found else { throw DatabaseExpressionEvaluationError.timeZone(.unknownZone) }
                    fields.abbreviationOffset = found; fields.zoneSign = 0
                } else {
                    try zone(&cursor, fields: &fields, extraSkip: extraSkip, optionalMinutes: true, variable: variable)
                }
            case .offset:
                try zone(&cursor, fields: &fields, extraSkip: extraSkip, optionalMinutes: true, variable: variable)
            case .zoneHour:
                try zone(&cursor, fields: &fields, extraSkip: extraSkip, optionalMinutes: false, variable: variable)
            case .zoneMinute:
                if fields.zoneSign == 0 { fields.zoneSign = 1 }
                try assign(&fields.zoneMinute, cursor.number(width: 2, variable: variable).value)
            case .commaYear:
                let high = try cursor.number(width: 1, variable: true).value
                guard cursor.scalar == 44 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
                try cursor.advance()
                let low = try cursor.number(width: 3, variable: false).value
                let year = high * 1000 + low
                guard Int32(exactly: year) != nil else { throw SQLTemporalFormat.rangeFailure }
                try assign(&fields.year, year); fields.yearWidth = 4
            default:
                let parsed = try cursor.number(width: token.width, variable: variable)
                switch token {
                case .hour12: try assign(&fields.hour, parsed.value); fields.twelveHour = true
                case .hour24: try assign(&fields.hour, parsed.value)
                case .minute: try assign(&fields.minute, parsed.value)
                case .second: try assign(&fields.second, parsed.value)
                case .secondsOfDay: try assign(&fields.seconds, parsed.value)
                case .fraction(let digits):
                    if current.spelling == "MS" || current.spelling == "ms" {
                        // MS accepts at most three significant decimal positions.
                        try assign(&fields.milliseconds, parsed.value)
                        if parsed.used == 1 { fields.milliseconds *= 100 }
                        else if parsed.used == 2 { fields.milliseconds *= 10 }
                    } else {
                        try assign(&fields.microseconds, parsed.value)
                        if (1...5).contains(parsed.used) {
                            for _ in parsed.used..<6 { fields.microseconds *= 10 }
                        }
                    }
                    if current.spelling.utf8.first == 70 || current.spelling.utf8.first == 102 { fields.precision = digits }
                case .year(let width), .isoYear(let width):
                    try assign(&fields.year, parsed.value)
                    if width < 4 && parsed.used < 4 { fields.year = partialYear(fields.year) }
                    fields.yearWidth = width
                case .month: try assign(&fields.month, parsed.value)
                case .day: try assign(&fields.day, parsed.value)
                case .ordinal, .isoOrdinal: try assign(&fields.ordinal, parsed.value)
                case .weekday: try assign(&fields.weekday, parsed.value)
                case .isoWeekday: try assign(&fields.weekday, parsed.value == 7 ? 1 : parsed.value + 1)
                case .weekOfYear, .isoWeek: try assign(&fields.week, parsed.value)
                case .weekOfMonth: try assign(&fields.monthWeek, parsed.value)
                case .century: try assign(&fields.century, parsed.value)
                case .julian: try assign(&fields.julian, parsed.value)
                case .quarter: break
                default: throw SQLTemporalFormat.invalid("invalid numeric template directive")
                }
            }
            if current.ordinal != nil { try cursor.advance(); try cursor.advance() }
            if !fixed { extraSkip = try cursor.spaces() }
            node = next
        }
        return try assemble(fields)
    }

    private static func assign(_ field: inout Int64, _ value: Int64) throws {
        if field != 0 && field != value {
            throw SQLTemporalFormat.invalid("conflicting template fields")
        }
        field = value
    }

    private static func zone(_ cursor: inout Cursor, fields: inout Fields,
        extraSkip: Int, optionalMinutes: Bool, variable: Bool) throws {
        if cursor.scalar == 43 || cursor.scalar == 45 || cursor.scalar == 32 {
            fields.zoneSign = cursor.scalar == 45 ? -1 : 1
            try cursor.advance()
        } else if extraSkip > 0, cursor.index > cursor.text.startIndex,
                  cursor.text.unicodeScalars[cursor.text.unicodeScalars.index(before: cursor.index)].value == 45 {
            fields.zoneSign = -1
        } else { fields.zoneSign = 1 }
        try assign(&fields.zoneHour, cursor.number(width: 2, variable: variable).value)
        if optionalMinutes && cursor.scalar == 58 {
            try cursor.advance()
            try assign(&fields.zoneMinute, cursor.number(width: 2, variable: variable).value)
        }
    }

    private static func assemble(_ source: Fields) throws -> Parsed {
        var fields = source
        var year: Int64 = 0, month: Int64 = 1, day: Int64 = 1
        if fields.year != 0 {
            if fields.century != 0 && fields.yearWidth <= 2 {
                let century = fields.bc != 0 ? -fields.century : fields.century
                let low = fields.year % 100
                year = low == 0 ? century * 100 + (century >= 0 ? 0 : 1) :
                    century >= 0 ? (century - 1) * 100 + low : (century + 1) * 100 - low + 1
            } else {
                year = fields.bc != 0 ? -fields.year : fields.year
                if year < 0 { year += 1 }
            }
        } else if fields.century != 0 {
            let century = fields.bc != 0 ? -fields.century : fields.century
            year = century >= 0 ? (century - 1) * 100 + 1 : century * 100 + 1
        }
        guard let initialYear = Int32(exactly: year) else { throw SQLTemporalFormat.rangeFailure }
        if fields.julian != 0 {
            let civil = try SQLGregorianCalendar.fields(julianDay: fields.julian)
            year = Int64(civil.year); month = Int64(civil.month); day = Int64(civil.day)
        }
        func isoMonday() throws -> Int64 {
            let jan4 = try SQLGregorianCalendar.julianDay(year: initialYear, month: 1, day: 4)
            return jan4 - Int64(SQLGregorianCalendar.dayOfWeek(julianDay: jan4 - 1))
        }
        if fields.week != 0 {
            if fields.convention == 2 {
                let weekday = fields.weekday == 0 ? 0 : fields.weekday > 1 ? fields.weekday - 2 : 6
                let civil = try SQLGregorianCalendar.fields(julianDay: isoMonday() + (fields.week - 1) * 7 + weekday)
                year = Int64(civil.year); month = Int64(civil.month); day = Int64(civil.day)
            } else { fields.ordinal = (fields.week - 1) * 7 + 1 }
        }
        if fields.monthWeek != 0 { fields.day = (fields.monthWeek - 1) * 7 + 1 }
        if fields.day != 0 { day = fields.day }
        if fields.month != 0 { month = fields.month }
        if fields.ordinal != 0 && (month <= 1 || day <= 1) {
            guard year != 0 || fields.bc != 0 else { throw SQLTemporalFormat.invalid("ordinal date requires a year") }
            if fields.convention == 2 {
                let civil = try SQLGregorianCalendar.fields(julianDay: isoMonday() + fields.ordinal - 1)
                year = Int64(civil.year); month = Int64(civil.month); day = Int64(civil.day)
            } else {
                guard let y = Int32(exactly: year) else { throw SQLTemporalFormat.rangeFailure }
                var m: Int32 = 1, sum: Int64 = 0
                while m <= 12 {
                    let end = sum + Int64(SQLGregorianCalendar.daysInMonth(year: y, month: m))
                    if fields.ordinal <= end { break }
                    sum = end; m += 1
                }
                if month <= 1 { month = Int64(m) }
                if day <= 1 { day = fields.ordinal - sum }
            }
        }
        var hour = fields.seconds / 3600
        var minute = fields.seconds % 3600 / 60
        var second = fields.seconds % 60
        if fields.hour != 0 { hour = fields.hour }
        if fields.minute != 0 { minute = fields.minute }
        if fields.second != 0 { second = fields.second }
        if fields.twelveHour {
            guard (1...12).contains(hour) else { throw SQLTemporalFormat.invalid("invalid 12-hour field") }
            if fields.meridiem != 0 && hour < 12 { hour += 12 }
            else if fields.meridiem == 0 && hour == 12 { hour = 0 }
        }
        let microsecond = fields.milliseconds * 1000 + fields.microseconds
        guard (0...23).contains(hour), (0...59).contains(minute), (0...59).contains(second),
              (0...999999).contains(microsecond),
              let y = Int32(exactly: year), let m = Int32(exactly: month), let d = Int32(exactly: day) else {
            throw SQLTemporalFormat.rangeFailure
        }
        let julian = try SQLGregorianCalendar.julianDay(year: y, month: m, day: d)
        var offset = fields.abbreviationOffset
        if fields.zoneSign != 0 {
            guard (0...15).contains(fields.zoneHour), (0...59).contains(fields.zoneMinute) else {
                throw SQLTemporalFormat.rangeFailure
            }
            offset = Int32(fields.zoneSign * (fields.zoneHour * 3600 + fields.zoneMinute * 60))
        }
        return Parsed(fields: SQLGregorianCalendar.Fields(year: y, month: m, day: d,
            hour: Int32(hour), minute: Int32(minute), second: Int32(second),
            microsecond: Int32(microsecond), julianDay: julian), offsetSeconds: offset, precision: fields.precision)
    }
}

private extension Optional where Wrapped == SQLTemporalFormat.Token {
    var isFixed: Bool { if case .some(.fixed) = self { true } else { false } }
}
