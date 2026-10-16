import DatabaseKit
import DatabaseTypes

/// The C/POSIX temporal template grammar and its allocation-free replay renderer.
struct SQLTemporalFormat {
    enum Token: Sendable {
        case hour12, hour24, minute, second, fraction(Int), secondsOfDay, meridiem
        case year(Int), commaYear, isoYear(Int), era, monthName(Bool), month
        case weekdayName(Bool), weekday, isoWeekday, ordinal, isoOrdinal, day
        case weekOfMonth, weekOfYear, isoWeek, century, julian, quarter, roman
        case zone, zoneHour, zoneMinute, offset, fixed

        var numeric: Bool {
            switch self {
            case .meridiem, .era, .monthName, .weekdayName, .roman, .zone,
                 .zoneHour, .offset, .fixed: false
            default: true
            }
        }

        var convention: Int {
            switch self {
            case .year, .commaYear, .monthName, .month, .ordinal, .day, .weekday,
                 .weekOfYear, .weekOfMonth, .roman: 1
            case .isoYear, .isoWeekday, .isoOrdinal, .isoWeek: 2
            default: 0
            }
        }

        var width: Int {
            switch self {
            case .hour12, .hour24, .minute, .second, .month, .day, .weekOfYear,
                 .isoWeek, .zoneHour, .zoneMinute, .century: 2
            case .fraction(let digits), .year(let digits), .isoYear(let digits): digits
            case .ordinal, .isoOrdinal: 3
            case .secondsOfDay: 4
            default: 1
            }
        }
    }

    struct Node {
        enum Kind { case token, literal, separator, space }
        let kind: Kind
        let token: Token?
        let spelling: Substring
        var fill = false
        var translation = false
        var ordinal: Bool? = nil
    }

    struct Scanner {
        let text: Substring
        let meter: DatabaseWorkMeter
        var position: String.Index
        var quoted = false

        init(_ text: Substring, workMeter: DatabaseWorkMeter) {
            self.text = text
            self.meter = workMeter
            self.position = text.startIndex
        }

        mutating func next() throws -> Node? {
            try meter.consume(at: .expressionEvaluation)
            while position < text.endIndex {
                if quoted {
                    if scalar == 34 { advance(); quoted = false; continue }
                    if scalar == 92 {
                        let after = text.unicodeScalars.index(after: position)
                        if after < text.endIndex { position = after }
                    }
                    return literal(.literal)
                }
                var fill = false, translation = false
                if matches("FM") || matches("fm") { fill = true; advanceASCII(2) }
                else if matches("TM") || matches("tm") { translation = true; advanceASCII(2) }
                guard position < text.endIndex else { return nil }
                for (spelling, token) in Self.tokens {
                    try meter.consume(at: .expressionEvaluation)
                    if matches(spelling) {
                        let start = position
                        advanceASCII(spelling.utf8.count)
                        var node = Node(kind: .token, token: token, spelling: text[start..<position],
                                        fill: fill, translation: translation)
                        if matches("TH") { node.ordinal = true; advanceASCII(2) }
                        else if matches("th") { node.ordinal = false; advanceASCII(2) }
                        else if matches("SP") { advanceASCII(2) }
                        return node
                    }
                }
                if scalar == 34 { advance(); quoted = true; continue }
                if scalar == 92 {
                    let after = text.unicodeScalars.index(after: position)
                    if after < text.endIndex, text.unicodeScalars[after].value == 34 {
                        position = after
                        return literal(.literal)
                    }
                }
                return literal(SQLTemporalFormat.isSpace(scalar) ? .space :
                    SQLTemporalFormat.isSeparator(scalar) ? .separator : .literal)
            }
            return nil
        }

        private var scalar: UInt32 { text.unicodeScalars[position].value }
        private mutating func advance() { position = text.unicodeScalars.index(after: position) }
        private mutating func advanceASCII(_ count: Int) {
            for _ in 0..<count { advance() }
        }
        private func matches(_ pattern: String) -> Bool {
            text[position...].utf8.starts(with: pattern.utf8)
        }
        private mutating func literal(_ kind: Node.Kind) -> Node {
            let start = position
            advance()
            return Node(kind: kind, token: nil, spelling: text[start..<position])
        }

        // Longest spellings precede their prefixes; all constants are ASCII.
        private static let tokens: [(String, Token)] = {
            let upper: [(String, Token)] = [
                ("A.D.", .era), ("A.M.", .meridiem), ("AD", .era), ("AM", .meridiem),
                ("B.C.", .era), ("BC", .era), ("CC", .century),
                ("DAY", .weekdayName(true)), ("DDD", .ordinal), ("DD", .day),
                ("DY", .weekdayName(false)), ("D", .weekday),
                ("FF1", .fraction(1)), ("FF2", .fraction(2)), ("FF3", .fraction(3)),
                ("FF4", .fraction(4)), ("FF5", .fraction(5)), ("FF6", .fraction(6)),
                ("FX", .fixed), ("HH24", .hour24), ("HH12", .hour12), ("HH", .hour12),
                ("IDDD", .isoOrdinal), ("ID", .isoWeekday), ("IW", .isoWeek),
                ("IYYY", .isoYear(4)), ("IYY", .isoYear(3)), ("IY", .isoYear(2)), ("I", .isoYear(1)),
                ("J", .julian), ("MI", .minute), ("MM", .month),
                ("MONTH", .monthName(true)), ("MON", .monthName(false)), ("MS", .fraction(3)),
                ("OF", .offset), ("P.M.", .meridiem), ("PM", .meridiem),
                ("Q", .quarter), ("RM", .roman), ("SSSSS", .secondsOfDay),
                ("SSSS", .secondsOfDay), ("SS", .second), ("TZH", .zoneHour),
                ("TZM", .zoneMinute), ("TZ", .zone), ("US", .fraction(6)),
                ("WW", .weekOfYear), ("W", .weekOfMonth),
                ("Y,YYY", .commaYear), ("YYYY", .year(4)), ("YYY", .year(3)),
                ("YY", .year(2)), ("Y", .year(1))
            ]
            var result = upper
            for (name, token) in upper {
                let lower = String(decoding: name.utf8.map { (65...90).contains($0) ? $0 + 32 : $0 }, as: UTF8.self)
                result.append((lower, token))
            }
            result.append(("Month", .monthName(true))); result.append(("Mon", .monthName(false)))
            result.append(("Day", .weekdayName(true))); result.append(("Dy", .weekdayName(false)))
            result.sort { $0.0.utf8.count > $1.0.utf8.count }
            return result
        }()
    }

    struct Parts {
        var year: Int64 = 0, month: Int64 = 1, day: Int64 = 1
        var hour: Int64 = 0, minute: Int64 = 0, second: Int64 = 0, microsecond: Int64 = 0
        var julianDay: Int64 = 0
        var interval = false

        init(_ fields: SQLGregorianCalendar.Fields) {
            year = Int64(fields.year); month = Int64(fields.month); day = Int64(fields.day)
            hour = Int64(fields.hour); minute = Int64(fields.minute); second = Int64(fields.second)
            microsecond = Int64(fields.microsecond); julianDay = fields.julianDay
        }

        init(_ value: QueryIntervalValue) {
            year = value.period.months / 12; month = value.period.months % 12; day = value.period.days
            hour = value.microseconds / 3_600_000_000
            minute = value.microseconds / 60_000_000 % 60
            second = value.microseconds / 1_000_000 % 60
            microsecond = value.microseconds % 1_000_000
            interval = true
        }

        func calendarDay() throws -> Int64 {
            if !interval { return julianDay }
            guard let year = Int32(exactly: year), let month = Int32(exactly: month),
                  let day = Int32(exactly: day) else { throw SQLTemporalFormat.rangeFailure }
            return try SQLGregorianCalendar.syntheticJulianDay(year: year, month: month, day: day)
        }
    }

    static let months = ["January", "February", "March", "April", "May", "June",
                         "July", "August", "September", "October", "November", "December"]
    static let weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    static let shortMonths = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    static let shortWeekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    static let romans = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"]
    static var rangeFailure: DatabaseExpressionEvaluationError {
        .temporalValueOutOfRange
    }
    static func invalid(_ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: "temporal formatting", reason: reason)
    }
    static func isSpace(_ value: UInt32) -> Bool { value == 32 || (9...13).contains(value) }
    static func isSeparator(_ value: UInt32) -> Bool {
        value > 32 && value < 127 && !(48...57).contains(value) &&
            !(65...90).contains(value) && !(97...122).contains(value)
    }

    static func render(_ template: Substring, parts: Parts,
        designation: SQLTimeZoneRules.Designation?, workMeter: DatabaseWorkMeter,
        emit: (UInt8) throws -> Void) throws {
        var scanner = Scanner(template, workMeter: workMeter)
        while let node = try scanner.next() {
            guard let token = node.token else {
                for byte in node.spelling.utf8 { try emit(byte) }
                continue
            }
            func number(_ value: Int64, width: Int = 0, honorsFill: Bool = true) throws {
                try integer(value, width: honorsFill && node.fill ? 0 : width, emit: emit)
                if let uppercase = node.ordinal {
                    let suffix: String
                    let magnitude = value.magnitude
                    if (11...13).contains(magnitude % 100) { suffix = "th" }
                    else { suffix = magnitude % 10 == 1 ? "st" : magnitude % 10 == 2 ? "nd" : magnitude % 10 == 3 ? "rd" : "th" }
                    for byte in suffix.utf8 { try emit(uppercase ? byte - 32 : byte) }
                }
            }
            func text(_ value: String, padding: Int = 0) throws {
                let first = node.spelling.utf8.first ?? 0
                let second = node.spelling.utf8.dropFirst().first ?? 0
                let lower = (97...122).contains(first)
                let upper = !lower && !(97...122).contains(second)
                var count = 0
                for byte in value.utf8 {
                    try emit(lower && (65...90).contains(byte) ? byte + 32 :
                        upper && (97...122).contains(byte) ? byte - 32 : byte)
                    count += 1
                }
                if !node.fill && !node.translation {
                    while count < padding { try emit(32); count += 1 }
                }
            }
            func requireCalendar() throws {
                if parts.interval { throw invalid("calendar or zone directive is invalid for INTERVAL") }
            }
            switch token {
            case .fixed: break
            case .hour12: try number(parts.hour % 12 == 0 ? 12 : parts.hour % 12, width: parts.hour >= 0 ? 2 : 3)
            case .hour24: try number(parts.hour, width: parts.hour >= 0 ? 2 : 3)
            case .minute: try number(parts.minute, width: parts.minute >= 0 ? 2 : 3)
            case .second: try number(parts.second, width: parts.second >= 0 ? 2 : 3)
            case .fraction(let digits):
                var divisor: Int64 = 1
                for _ in digits..<6 { divisor *= 10 }
                try number(parts.microsecond / divisor, width: digits, honorsFill: false)
            case .secondsOfDay: try number(parts.hour * 3600 + parts.minute * 60 + parts.second)
            case .meridiem: try text(parts.hour % 24 >= 12 ? (node.spelling.contains(".") ? "P.M." : "PM") : (node.spelling.contains(".") ? "A.M." : "AM"))
            case .era:
                try requireCalendar()
                try text(parts.year <= 0 ? (node.spelling.contains(".") ? "B.C." : "BC") : (node.spelling.contains(".") ? "A.D." : "AD"))
            case .year(let width), .isoYear(let width):
                var year = parts.year
                if case .isoYear = token { year = try iso(parts).year }
                if !parts.interval && year <= 0 { year = 1 - year }
                var modulus: Int64 = 1
                if width < 4 { for _ in 0..<width { modulus *= 10 }; year %= modulus }
                try number(year, width: width == 1 ? 1 : year >= 0 ? width : width + 1)
            case .commaYear:
                let year = !parts.interval && parts.year <= 0 ? 1 - parts.year : parts.year
                try integer(year / 1000, emit: emit); try emit(44)
                try number(year - year / 1000 * 1000, width: 3, honorsFill: false)
            case .century:
                let value = parts.interval ? parts.year / 100 : parts.year > 0 ? (parts.year - 1) / 100 + 1 : parts.year / 100 - 1
                try number(value, width: value >= 0 ? 2 : 3)
            case .month: try number(parts.month, width: parts.month >= 0 ? 2 : 3)
            case .monthName(let full):
                try requireCalendar()
                if parts.month != 0 {
                    guard (1...12).contains(parts.month) else { throw rangeFailure }
                    let name = months[Int(parts.month - 1)]
                    try text(full ? name : shortMonths[Int(parts.month - 1)], padding: full ? 9 : 0)
                }
            case .weekdayName(let full):
                try requireCalendar()
                let weekday = Int(SQLGregorianCalendar.dayOfWeek(julianDay: try parts.calendarDay()))
                guard weekdays.indices.contains(weekday) else { throw rangeFailure }
                let name = weekdays[weekday]
                try text(full ? name : shortWeekdays[weekday], padding: full ? 9 : 0)
            case .weekday, .isoWeekday:
                try requireCalendar()
                let weekday = Int64(SQLGregorianCalendar.dayOfWeek(julianDay: try parts.calendarDay()))
                try number(token.isISOWeekday ? (weekday == 0 ? 7 : weekday) : weekday + 1)
            case .ordinal:
                let ordinal = parts.interval ? try approximateOrdinal(parts) :
                    Int64(try SQLGregorianCalendar.dayOfYear(julianDay: parts.calendarDay()))
                try number(ordinal, width: 3)
            case .isoOrdinal:
                try number(iso(parts).ordinal, width: 3)
            case .day: try number(parts.day, width: 2)
            case .weekOfMonth:
                let shifted = parts.interval ? try checked32(parts.day - 1) : parts.day - 1
                try number(shifted / 7 + 1)
            case .weekOfYear:
                let ordinal = parts.interval ? try approximateOrdinal(parts) :
                    Int64(try SQLGregorianCalendar.dayOfYear(julianDay: parts.calendarDay()))
                let shifted = parts.interval ? try checked32(ordinal - 1) : ordinal - 1
                try number(shifted / 7 + 1, width: 2)
            case .isoWeek: try number(iso(parts).week, width: 2)
            case .julian: try number(parts.calendarDay())
            case .quarter: if parts.month != 0 { try number((parts.month - 1) / 3 + 1) }
            case .roman:
                if parts.month != 0 || parts.year != 0 {
                    let month = parts.month == 0 ? (parts.year >= 0 ? 12 : 1) : parts.month < 0 ? 13 + parts.month : parts.month
                    guard (1...12).contains(month) else { throw rangeFailure }
                    try text(romans[Int(month - 1)], padding: 4)
                }
            case .zone:
                try requireCalendar()
                if let designation {
                    let lower = node.spelling.utf8.first == 116
                    try designation.abbreviation.withUnsafeBytes { bytes in
                        for byte in bytes { try emit(lower && (65...90).contains(byte) ? byte + 32 : byte) }
                    }
                }
            case .zoneHour, .zoneMinute, .offset:
                try requireCalendar()
                let offset = Int64(designation?.offsetSeconds ?? 0)
                if case .zoneMinute = token { try integer(Int64(offset.magnitude % 3600 / 60), width: 2, emit: emit) }
                else {
                    try emit(offset >= 0 ? 43 : 45)
                    try integer(Int64(offset.magnitude / 3600), width: token.isOffset && node.fill ? 0 : 2, emit: emit)
                    if token.isOffset && offset.magnitude % 3600 != 0 {
                        try emit(58); try integer(Int64(offset.magnitude % 3600 / 60), width: 2, emit: emit)
                    }
                }
            }
        }
    }

    static func integer(_ value: Int64, width: Int = 0, emit: (UInt8) throws -> Void) throws {
        var divisor: UInt64 = 1, digits = 1
        while value.magnitude / divisor >= 10 { divisor *= 10; digits += 1 }
        if value < 0 { try emit(45) }
        var padding = max(0, width - digits - (value < 0 ? 1 : 0))
        while padding > 0 { try emit(48); padding -= 1 }
        repeat {
            try emit(UInt8(value.magnitude / divisor % 10) + 48)
            divisor /= 10
        } while divisor != 0
    }

    private static func iso(_ parts: Parts) throws -> (year: Int64, week: Int64, ordinal: Int64) {
        let day = try parts.calendarDay()
        if !parts.interval {
            let fields = try SQLGregorianCalendar.isoFields(julianDay: day)
            return (Int64(fields.year), Int64(fields.week), Int64(fields.week - 1) * 7 + Int64(fields.weekday))
        }
        func firstMonday(_ year: Int64) throws -> Int64 {
            guard let year = Int32(exactly: year) else { throw rangeFailure }
            let jan4 = try SQLGregorianCalendar.syntheticJulianDay(year: year, month: 1, day: 4)
            let preceding = try checked32(jan4 - 1)
            return try checked32(jan4 - Int64(SQLGregorianCalendar.dayOfWeek(julianDay: preceding)))
        }
        // PostgreSQL anchors interval ISO fields in the original synthetic year,
        // even when a retained day component spans many real calendar years.
        var year = parts.year
        var monday = try firstMonday(year)
        if day < monday { year -= 1; monday = try firstMonday(year) }
        var week = try checked32(day - monday) / 7 + 1
        if week >= 52 {
            let next = try firstMonday(parts.year + 1)
            if day >= next { year = parts.year + 1; monday = next; week = try checked32(day - next) / 7 + 1 }
        }
        return (year, week, try checked32(checked32(day - monday) + 1))
    }

    private static func approximateOrdinal(_ parts: Parts) throws -> Int64 {
        // tm_yday is a signed int in the reference; reject undefined arithmetic
        // only when DDD/WW consumes it, preserving other full-domain components.
        let years = try checked32(parts.year * 12)
        let months = try checked32(years + parts.month)
        let days = try checked32(months * 30)
        return try checked32(days + parts.day)
    }

    private static func checked32(_ value: Int64) throws -> Int64 {
        guard Int32(exactly: value) != nil else { throw rangeFailure }
        return value
    }
}

private extension SQLTemporalFormat.Token {
    var isISOWeekday: Bool { if case .isoWeekday = self { true } else { false } }
    var isOffset: Bool { if case .offset = self { true } else { false } }
}
