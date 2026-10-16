import DatabaseKit

/// Signed PostgreSQL, SQL-standard and ISO interval input without token arrays.
enum SQLIntervalInput {
    struct Number {
        var coefficient: Int128
        var denominator: Int128
        var lexicalSign: Int128 = 1
    }

    static func number(_ c: inout SQLTemporalInput.Cursor) throws -> Number {
        var sign: Int128 = 1
        if try c.take(45) { sign = -1 }
        else { _ = try c.take(43) }
        var coefficient = try c.integer()
        var denominator: Int128 = 1
        if try c.take(46) {
            var count = 0
            while let byte = c.remaining.utf8.first, byte >= 48, byte <= 57 {
                let (next, overflow) = coefficient.multipliedReportingOverflow(by: 10)
                let (sum, overflow2) = next.addingReportingOverflow(Int128(byte - 48))
                let (den, overflow3) = denominator.multipliedReportingOverflow(by: 10)
                guard !overflow, !overflow2, !overflow3 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                coefficient = sum; denominator = den; count += 1; try c.advance()
            }
            guard count > 0 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        }
        return Number(coefficient: try product(coefficient, sign), denominator: denominator, lexicalSign: sign)
    }

    static func product(_ lhs: Int128, _ rhs: Int128) throws -> Int128 {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        return result
    }

    static func add(_ lhs: inout Int128, _ rhs: Int128) throws {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
        lhs = result
    }

    static func nearestEven(_ numerator: Int128, _ denominator: Int128) -> Int128 {
        let q = numerator / denominator
        let r = numerator % denominator
        let magnitude = r.magnitude
        let half = UInt128(denominator) / 2
        let greater = magnitude > half
        let tie = UInt128(denominator).isMultiple(of: 2) && magnitude == half
        return q + (greater || tie && !q.isMultiple(of: 2) ? (r < 0 ? -1 : 1) : 0)
    }

    static func parse(_ input: Substring, fields: SQLIntervalFields?, precision: Int?, workMeter: DatabaseWorkMeter) throws -> QueryIntervalValue {
        var c = try SQLTemporalInput.cursor(input, workMeter: workMeter)
        _ = try c.take(64); try c.spaces()
        var iso = false
        var isoSign: Int128 = 1
        if c.remaining.hasPrefix("-P") || c.remaining.hasPrefix("-p") { isoSign = -1; try c.advance() }
        if c.remaining.utf8.first == 80 || c.remaining.utf8.first == 112 { iso = true; try c.advance() }
        if iso, let first = c.remaining.utf8.first, first >= 48 && first <= 57 {
            var probe = c
            let start = probe.remaining.startIndex
            _ = try probe.integer()
            let digits = probe.remaining.base.distance(from: start, to: probe.remaining.startIndex)
            if probe.remaining.utf8.first == 45 || digits == 8 && (probe.remaining.isEmpty || probe.remaining.utf8.first == 84) {
                return try alternativeISO(&c, sign: isoSign, fields: fields, precision: precision)
            }
        }
        var inTime = false
        var months: Int128 = 0
        var days: Int128 = 0
        var micros: Int128 = 0
        var count = 0
        var ago = false
        func accumulate(_ n: Number, months factorMonths: Int128 = 0, days factorDays: Int128 = 0, micros factorMicros: Int128 = 0) throws {
            if factorMonths != 0 {
                let amount = try product(n.coefficient, factorMonths)
                try add(&months, amount / n.denominator)
                let dayNumerator = try product(amount % n.denominator, 30)
                try add(&days, dayNumerator / n.denominator)
                try add(&micros, nearestEven(try product(dayNumerator % n.denominator, SQLGregorianCalendar.dayMicroseconds), n.denominator))
            } else if factorDays != 0 {
                let amount = try product(n.coefficient, factorDays)
                try add(&days, amount / n.denominator)
                try add(&micros, nearestEven(try product(amount % n.denominator, SQLGregorianCalendar.dayMicroseconds), n.denominator))
            } else {
                try add(&micros, nearestEven(try product(n.coefficient, factorMicros), n.denominator))
            }
        }
        while !c.remaining.isEmpty {
            try c.spaces()
            if c.remaining.isEmpty { break }
            if SQLTemporalInput.equalASCII(SQLTemporalInput.trimmed(c.remaining), "ago") {
                ago = true; c.remaining = ""[...]; break
            }
            if iso && (c.remaining.utf8.first == 84 || c.remaining.utf8.first == 116) {
                guard !inTime else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
                inTime = true; try c.advance(); continue
            }
            let n = try number(&c)
            if !iso, c.remaining.utf8.first == 58 {
                let sign = n.lexicalSign
                try c.advance()
                let next = try number(&c)
                let threeFields = try c.take(58)
                let minuteSecond = !threeFields && (fields == .minuteToSecond || next.denominator != 1)
                guard n.denominator == 1 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
                if minuteSecond {
                    // PostgreSQL treats mm:ss.fraction as MINUTE TO SECOND even
                    // without a field qualifier. Keep the lexical -0 sign.
                    guard Int32(exactly: n.coefficient) != nil, next.coefficient >= 0,
                          next.coefficient / next.denominator < 60 else {
                        throw DatabaseExpressionEvaluationError.invalidTemporalInput
                    }
                    try accumulate(n, micros: 60_000_000)
                    try accumulate(Number(coefficient: product(sign, next.coefficient), denominator: next.denominator), micros: 1_000_000)
                } else {
                    let second = threeFields ? try number(&c) : Number(coefficient: 0, denominator: 1)
                    guard next.denominator == 1, next.coefficient >= 0, next.coefficient < 60,
                          second.coefficient >= 0, second.coefficient / second.denominator < 60 else {
                        throw DatabaseExpressionEvaluationError.invalidTemporalInput
                    }
                    try accumulate(n, micros: 3_600_000_000)
                    try accumulate(Number(coefficient: product(sign, next.coefficient), denominator: 1), micros: 60_000_000)
                    try accumulate(Number(coefficient: product(sign, second.coefficient), denominator: second.denominator), micros: 1_000_000)
                }
            } else if !iso, c.remaining.utf8.first == 45, n.denominator == 1 {
                try c.advance()
                let m = try c.integer()
                guard m < 12 else { throw DatabaseExpressionEvaluationError.temporalValueOutOfRange }
                try add(&months, try product(n.coefficient, 12))
                try add(&months, try product(n.lexicalSign, m))
            } else {
                if !iso { try c.spaces() }
                let start = c.remaining.startIndex
                while let b = c.remaining.utf8.first, (b >= 65 && b <= 90) || (b >= 97 && b <= 122) {
                    try c.advance()
                    if iso { break }
                }
                let unit = c.remaining.base[start..<c.remaining.startIndex]
                if SQLTemporalInput.equalASCII(unit, "y") || SQLTemporalInput.equalASCII(unit, "year") || SQLTemporalInput.equalASCII(unit, "years") { try accumulate(n, months: 12) }
                else if SQLTemporalInput.equalASCII(unit, "mon") || SQLTemporalInput.equalASCII(unit, "mons") || SQLTemporalInput.equalASCII(unit, "month") || SQLTemporalInput.equalASCII(unit, "months") || iso && !inTime && SQLTemporalInput.equalASCII(unit, "m") { try accumulate(n, months: 1) }
                else if SQLTemporalInput.equalASCII(unit, "w") || SQLTemporalInput.equalASCII(unit, "week") || SQLTemporalInput.equalASCII(unit, "weeks") { try accumulate(n, days: 7) }
                else if SQLTemporalInput.equalASCII(unit, "d") || SQLTemporalInput.equalASCII(unit, "day") || SQLTemporalInput.equalASCII(unit, "days") { try accumulate(n, days: 1) }
                else if SQLTemporalInput.equalASCII(unit, "h") || SQLTemporalInput.equalASCII(unit, "hour") || SQLTemporalInput.equalASCII(unit, "hours") { try accumulate(n, micros: 3_600_000_000) }
                else if SQLTemporalInput.equalASCII(unit, "m") || SQLTemporalInput.equalASCII(unit, "min") || SQLTemporalInput.equalASCII(unit, "minute") || SQLTemporalInput.equalASCII(unit, "minutes") { try accumulate(n, micros: 60_000_000) }
                else if SQLTemporalInput.equalASCII(unit, "s") || SQLTemporalInput.equalASCII(unit, "sec") || SQLTemporalInput.equalASCII(unit, "second") || SQLTemporalInput.equalASCII(unit, "seconds") { try accumulate(n, micros: 1_000_000) }
                else if SQLTemporalInput.equalASCII(unit, "ms") || SQLTemporalInput.equalASCII(unit, "millisecond") || SQLTemporalInput.equalASCII(unit, "milliseconds") { try accumulate(n, micros: 1000) }
                else if SQLTemporalInput.equalASCII(unit, "us") || SQLTemporalInput.equalASCII(unit, "microsecond") || SQLTemporalInput.equalASCII(unit, "microseconds") { try accumulate(n, micros: 1) }
                else if unit.isEmpty && !iso {
                    if !c.remaining.isEmpty { try accumulate(n, days: 1) }
                    else {
                        switch fields {
                        case .year: try accumulate(n, months: 12)
                        case .month, .yearToMonth: try accumulate(n, months: 1)
                        case .day: try accumulate(n, days: 1)
                        case .hour, .dayToHour: try accumulate(n, micros: 3_600_000_000)
                        case .minute, .dayToMinute, .hourToMinute: try accumulate(n, micros: 60_000_000)
                        default: try accumulate(n, micros: 1_000_000)
                        }
                    }
                } else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            }
            count += 1
        }
        guard count > 0 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        let sign: Int128 = (ago ? -1 : 1) * isoSign
        return try SQLTemporalPrecision.interval(months: product(months, sign), days: product(days, sign), microseconds: product(micros, sign), fields: fields, precision: precision)
    }
    static func alternativeISO(_ c: inout SQLTemporalInput.Cursor, sign: Int128,
        fields: SQLIntervalFields?, precision: Int?) throws -> QueryIntervalValue {
        let start = c.remaining.startIndex
        let first = try c.integer()
        let digits = c.remaining.base.distance(from: start, to: c.remaining.startIndex)
        let years: Int128
        var months: Int128 = 0
        var days: Int128 = 0
        if try c.take(45) {
            years = first
            months = try c.integer()
            if try c.take(45) { days = try c.integer() }
        } else {
            guard digits == 8 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
            years = first / 10000; months = first / 100 % 100; days = first % 100
        }
        var micros: Int128 = 0
        if try c.take(84) {
            let hourStart = c.remaining.startIndex
            let first = try c.integer()
            let hourDigits = c.remaining.base.distance(from: hourStart, to: c.remaining.startIndex)
            let hour: Int128
            var minute: Int128 = 0
            var second = Number(coefficient: 0, denominator: 1)
            if try c.take(58) {
                hour = first; minute = try c.integer()
                if try c.take(58) { second = try number(&c) }
            } else {
                guard hourDigits == 6 else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
                hour = first / 10000; minute = first / 100 % 100
                second = Number(coefficient: first % 100, denominator: 1)
                if try c.take(46) {
                    let fractionStart = c.remaining.startIndex
                    while let byte = c.remaining.utf8.first, byte >= 48 && byte <= 57 { try c.advance() }
                    micros = Int128(try SQLTemporalPrecision.fractionalMicroseconds(c.remaining.base[fractionStart..<c.remaining.startIndex], workMeter: c.workMeter))
                }
            }
            try add(&micros, try product(hour, 3_600_000_000))
            try add(&micros, try product(minute, 60_000_000))
            try add(&micros, nearestEven(try product(second.coefficient, 1_000_000), second.denominator))
        }
        try c.spaces()
        guard c.remaining.isEmpty else { throw DatabaseExpressionEvaluationError.invalidTemporalInput }
        var totalMonths = try product(years, 12)
        try add(&totalMonths, months)
        return try SQLTemporalPrecision.interval(months: product(totalMonths, sign), days: product(days, sign), microseconds: product(micros, sign), fields: fields, precision: precision)
    }

}
