import DatabaseKit

/// Bounded ASCII temporal lexical parsing with borrowed input slices.
enum SQLTemporalInput {
    struct Parsed {
        var fields: SQLGregorianCalendar.Fields
        var timeMicroseconds: Int64
        var zone: Substring?
    }

    struct Cursor {
        var remaining: Substring
        let workMeter: DatabaseWorkMeter

        mutating func spaces() throws {
            while let first = remaining.utf8.first, first == 32 || first == 9 || first == 10 || first == 13 {
                try advance()
            }
        }
        mutating func advance() throws {
            try workMeter.consume(at: .expressionEvaluation)
            remaining.removeFirst()
        }
        mutating func take(_ byte: UInt8) throws -> Bool {
            guard remaining.utf8.first == byte else { return false }
            try advance(); return true
        }
        mutating func require(_ byte: UInt8) throws {
            guard try take(byte) else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        }
        mutating func integer() throws -> Int128 {
            var value: Int128 = 0
            var count = 0
            while let byte = remaining.utf8.first, byte >= 48, byte <= 57 {
                let (multiplied, overflow) = value.multipliedReportingOverflow(by: 10)
                let (next, additionOverflow) = multiplied.addingReportingOverflow(Int128(byte - 48))
                guard !overflow, !additionOverflow else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                value = next; count += 1; try advance()
            }
            guard count > 0 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            return value
        }
        mutating func fraction() throws -> Int64 {
            guard try take(46) else { return 0 }
            let start = remaining.startIndex
            while let byte = remaining.utf8.first, byte >= 48, byte <= 57 { try advance() }
            return try SQLTemporalPrecision.fractionalMicroseconds(remaining.base[start..<remaining.startIndex], workMeter: workMeter)
        }
    }

    static func cursor(_ input: Substring, workMeter: DatabaseWorkMeter) throws -> Cursor {
        for byte in input.utf8 {
            try workMeter.consume(at: .expressionEvaluation)
            guard byte < 128 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        }
        var cursor = Cursor(remaining: input, workMeter: workMeter)
        try cursor.spaces()
        return cursor
    }

    static func equalASCII(_ lhs: Substring, _ rhs: StaticString) -> Bool {
        rhs.withUTF8Buffer { bytes in
            guard lhs.utf8.count == bytes.count else { return false }
            var index = 0
            for byte in lhs.utf8 {
                let folded = byte >= 65 && byte <= 90 ? byte + 32 : byte
                guard folded == bytes[index] else { return false }
                index += 1
            }
            return true
        }
    }

    static func trimmed(_ input: Substring) -> Substring {
        var result = input
        while let b = result.utf8.first, b == 32 || b == 9 || b == 10 || b == 13 { result.removeFirst() }
        while let b = result.utf8.last, b == 32 || b == 9 || b == 10 || b == 13 { result.removeLast() }
        return result
    }

    static func parse(_ input: Substring, date: Bool, time: Bool, workMeter: DatabaseWorkMeter) throws -> Parsed {
        var c = try cursor(input, workMeter: workMeter)
        var fields = SQLGregorianCalendar.Fields(year: 2000, month: 1, day: 1)
        let isBC = equalASCII(trimmed(input).suffix(2), "bc")
        if date {
            let afterJulianPrefix = c.remaining.dropFirst().utf8.first
            if (c.remaining.utf8.first == 74 || c.remaining.utf8.first == 106),
               let afterJulianPrefix, afterJulianPrefix >= 48, afterJulianPrefix <= 57 {
                try c.advance()
                let jd = try c.integer()
                guard let jd = Int64(exactly: jd) else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                fields = try SQLGregorianCalendar.fields(julianDay: jd)
            } else if let first = c.remaining.utf8.first, first >= 65 && first <= 90 || first >= 97 && first <= 122 {
                let month = try monthName(&c)
                try c.spaces()
                let day = try c.integer()
                _ = try c.take(44); try c.spaces()
                let year = try c.integer()
                fields = try civil(year: isBC ? year : adjustedYear(year), month: month, day: day)
            } else {
                let start = c.remaining.startIndex
                let first = try c.integer()
                let count = c.remaining.base.distance(from: start, to: c.remaining.startIndex)
                let delimiter = c.remaining.utf8.first
                if let delimiter, delimiter == 45 || delimiter == 47 || delimiter == 46 {
                    try c.advance()
                    if let next = c.remaining.utf8.first, next >= 65 && next <= 90 || next >= 97 && next <= 122 {
                        let month = try monthName(&c)
                        try c.require(delimiter)
                        let third = try c.integer()
                        fields = first > 31 ? try civil(year: first, month: month, day: third) : try civil(year: isBC ? third : adjustedYear(third), month: month, day: first)
                    } else {
                        let nextStart = c.remaining.startIndex
                        let second = try c.integer()
                        let secondCount = c.remaining.base.distance(from: nextStart, to: c.remaining.startIndex)
                        if c.remaining.utf8.first == delimiter {
                            try c.advance()
                            fields = try civil(year: count <= 2 && !isBC ? adjustedYear(first) : first, month: Int32(exactly: second), day: c.integer())
                        } else if secondCount == 3 {
                            fields = try ordinal(year: first, day: second)
                        } else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
                    }
                } else if count == 8 {
                    fields = try civil(year: first / 10000, month: Int32(first / 100 % 100), day: first % 100)
                } else if count == 7 {
                    fields = try ordinal(year: first / 1000, day: first % 1000)
                } else if count == 6 {
                    fields = try civil(year: adjustedYear(first / 10000), month: Int32(first / 100 % 100), day: first % 100)
                } else {
                    try c.spaces()
                    let month = try monthName(&c)
                    try c.spaces()
                    let year = try c.integer()
                    fields = first > 31 ? try civil(year: first, month: month, day: year) : try civil(year: isBC ? year : adjustedYear(year), month: month, day: first)
                }
            }
            try c.spaces()
            if c.remaining.utf8.first == 84 || c.remaining.utf8.first == 116 { try c.advance() }
        }
        var micros: Int128 = 0
        if time && c.remaining.utf8.first.map({ $0 >= 48 && $0 <= 57 }) == true {
            let start = c.remaining.startIndex
            let first = try c.integer()
            let digits = c.remaining.base.distance(from: start, to: c.remaining.startIndex)
            var hour: Int128
            var minute: Int128
            var second: Int128 = 0
            var fraction: Int64 = 0
            if try c.take(58) {
                hour = first; minute = try c.integer()
                if try c.take(58) { second = try c.integer(); fraction = try c.fraction() }
            } else if digits == 6 {
                hour = first / 10000; minute = first / 100 % 100; second = first % 100
                fraction = try c.fraction()
            } else if digits == 4 { hour = first / 100; minute = first % 100 }
            else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            try c.spaces()
            if c.remaining.count >= 2 {
                let meridiem = c.remaining.prefix(2)
                let following = c.remaining.dropFirst(2).utf8.first
                let tokenEnds = following == nil || following == 32 || following == 9 || following == 10 || following == 13 || following == 43 || following == 45
                if tokenEnds && (equalASCII(meridiem, "am") || equalASCII(meridiem, "pm")) {
                    guard hour >= 1, hour <= 12 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                    hour = hour % 12 + (equalASCII(meridiem, "pm") ? 12 : 0)
                    try c.advance(); try c.advance(); try c.spaces()
                }
            }
            guard hour <= 24, minute <= 59, second <= 60,
                  hour != 24 || (minute == 0 && second == 0 && fraction == 0) else {
                throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
            }
            micros = hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + Int128(fraction)
            guard micros <= SQLGregorianCalendar.dayMicroseconds else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
            try c.spaces()
        } else if time && !date { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        var tail = trimmed(c.remaining)
        if tail.count >= 2, equalASCII(tail.suffix(2), "bc") {
            fields.year = 1 - fields.year
            tail = trimmed(tail.dropLast(2))
        } else if tail.count >= 2, equalASCII(tail.suffix(2), "ad") { tail = trimmed(tail.dropLast(2)) }
        let zone = tail.isEmpty ? nil : tail
        if !time && zone != nil { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        fields.julianDay = try SQLGregorianCalendar.julianDay(year: fields.year, month: fields.month, day: fields.day)
        if date && micros == SQLGregorianCalendar.dayMicroseconds {
            fields = try SQLGregorianCalendar.fields(julianDay: fields.julianDay + 1)
        } else {
            fields.hour = Int32(micros / 3_600_000_000)
            fields.minute = Int32(micros / 60_000_000 % 60)
            fields.second = Int32(micros / 1_000_000 % 60)
            fields.microsecond = Int32(micros % 1_000_000)
        }
        return Parsed(fields: fields, timeMicroseconds: Int64(micros), zone: zone)
    }

    static func adjustedYear(_ year: Int128) -> Int128 { year < 70 ? year + 2000 : year < 100 ? year + 1900 : year }

    static func civil(year: Int128, month: Int32?, day: Int128) throws -> SQLGregorianCalendar.Fields {
        guard year > 0, let y = Int32(exactly: year), let m = month, let d = Int32(exactly: day) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        return SQLGregorianCalendar.Fields(year: y, month: m, day: d)
    }

    static func ordinal(year: Int128, day: Int128) throws -> SQLGregorianCalendar.Fields {
        guard year > 0, let y = Int32(exactly: year), day >= 1, day <= (SQLGregorianCalendar.daysInMonth(year: y, month: 2) == 29 ? 366 : 365) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        return try SQLGregorianCalendar.fields(julianDay: julianDayForYear(y) + Int64(day) - 1)
    }

    static func julianDayForYear(_ year: Int32) throws -> Int64 {
        try SQLGregorianCalendar.julianDay(year: year, month: 1, day: 1)
    }

    static func monthName(_ c: inout Cursor) throws -> Int32 {
        let start = c.remaining.startIndex
        while let b = c.remaining.utf8.first, b >= 65 && b <= 90 || b >= 97 && b <= 122 { try c.advance() }
        let token = c.remaining.base[start..<c.remaining.startIndex]
        _ = try c.take(46)
        if equalASCII(token, "jan") || equalASCII(token, "january") { return 1 }
        if equalASCII(token, "feb") || equalASCII(token, "february") { return 2 }
        if equalASCII(token, "mar") || equalASCII(token, "march") { return 3 }
        if equalASCII(token, "apr") || equalASCII(token, "april") { return 4 }
        if equalASCII(token, "may") { return 5 }
        if equalASCII(token, "jun") || equalASCII(token, "june") { return 6 }
        if equalASCII(token, "jul") || equalASCII(token, "july") { return 7 }
        if equalASCII(token, "aug") || equalASCII(token, "august") { return 8 }
        if equalASCII(token, "sep") || equalASCII(token, "sept") || equalASCII(token, "september") { return 9 }
        if equalASCII(token, "oct") || equalASCII(token, "october") { return 10 }
        if equalASCII(token, "nov") || equalASCII(token, "november") { return 11 }
        if equalASCII(token, "dec") || equalASCII(token, "december") { return 12 }
        throw DatabaseExpressionEvaluationError.invalidTemporalInput
    }

    static func numericOffset(_ input: Substring, posix: Bool, workMeter: DatabaseWorkMeter) throws -> Int32 {
        var c = try cursor(input, workMeter: workMeter)
        var sign: Int128 = 1
        if try c.take(45) { sign = -1 }
        else if !(try c.take(43)) { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        let digitsStart = c.remaining.startIndex
        let number = try c.integer()
        let digits = c.remaining.base.distance(from: digitsStart, to: c.remaining.startIndex)
        var hour = number
        var minute: Int128 = 0
        var second: Int128 = 0
        if try c.take(58) {
            minute = try c.integer()
            if try c.take(58) { second = try c.integer() }
        } else if digits > 2 {
            if digits == 4 { hour = number / 100; minute = number % 100 }
            else if digits == 6 { hour = number / 10000; minute = number / 100 % 100; second = number % 100 }
            else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        }
        try c.spaces()
        // PostgreSQL's textual POSIX zones and ISO input offsets have distinct bounds.
        guard c.remaining.isEmpty, minute < 60, second < 60, hour <= (posix ? 167 : 15),
              let result = Int32(exactly: sign * (hour * 3600 + minute * 60 + second) * (posix ? -1 : 1)) else {
            throw DatabaseExpressionEvaluationError.temporalValueOutOfRange
        }
        return result
    }
}
