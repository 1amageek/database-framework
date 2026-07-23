/// Exact XSD duration value represented as signed months and signed decimal
/// seconds. Component magnitudes are bounded explicitly by validation limits.
package struct XSDDurationValue: Sendable {
    package enum ParseFailure: Error, Sendable, Equatable {
        case invalid
        case componentLimit(actual: Int, limit: Int)
        case arithmeticLimit
    }

    let months: Int64
    let wholeSeconds: Int64
    let fraction: XSDTemporalValue.Fraction
    let fractionSign: Int8

    static func parse(
        _ source: String,
        componentDigitLimit: Int,
        fractionDigitLimit: Int
    ) -> Result<XSDDurationValue, ParseFailure> {
        var parser = DurationParser(source)
        let negative = parser.readIfPresent(45)
        guard parser.read(80) else { return .failure(.invalid) }

        var years: Int64 = 0
        var monthComponent: Int64 = 0
        var days: Int64 = 0
        var hours: Int64 = 0
        var minutes: Int64 = 0
        var seconds: Int64 = 0
        var fraction = XSDTemporalValue.Fraction.zero
        var sawComponent = false
        var sawTimeMarker = false
        var sawTimeComponent = false
        var lastDateRank = -1
        var lastTimeRank = -1

        while !parser.isAtEnd {
            if parser.readIfPresent(84) {
                guard !sawTimeMarker else { return .failure(.invalid) }
                sawTimeMarker = true
                continue
            }
            let component: DurationParser.Component
            switch parser.readComponent(
                digitLimit: componentDigitLimit,
                fractionDigitLimit: fractionDigitLimit
            ) {
            case .success(let value):
                component = value
            case .failure(let failure):
                return .failure(failure)
            }

            let rank: Int
            if sawTimeMarker {
                switch component.designator {
                case 72:
                    rank = 0
                    hours = component.whole
                case 77:
                    rank = 1
                    minutes = component.whole
                case 83:
                    rank = 2
                    seconds = component.whole
                    fraction = component.fraction
                default:
                    return .failure(.invalid)
                }
                guard rank > lastTimeRank else { return .failure(.invalid) }
                lastTimeRank = rank
                sawTimeComponent = true
            } else {
                guard component.fraction.isZero else { return .failure(.invalid) }
                switch component.designator {
                case 89:
                    rank = 0
                    years = component.whole
                case 77:
                    rank = 1
                    monthComponent = component.whole
                case 68:
                    rank = 2
                    days = component.whole
                default:
                    return .failure(.invalid)
                }
                guard rank > lastDateRank else { return .failure(.invalid) }
                lastDateRank = rank
            }
            sawComponent = true
        }

        guard sawComponent, !sawTimeMarker || sawTimeComponent else {
            return .failure(.invalid)
        }

        guard let totalMonths = checkedMultiply(years, by: 12),
              let unsignedMonths = checkedAdd(totalMonths, monthComponent),
              let daySeconds = checkedMultiply(days, by: 86_400),
              let hourSeconds = checkedMultiply(hours, by: 3_600),
              let minuteSeconds = checkedMultiply(minutes, by: 60),
              let dayAndHour = checkedAdd(daySeconds, hourSeconds),
              let dayHourMinute = checkedAdd(dayAndHour, minuteSeconds),
              let unsignedSeconds = checkedAdd(dayHourMinute, seconds) else {
            return .failure(.arithmeticLimit)
        }

        let signedMonths: Int64
        let signedSeconds: Int64
        if negative {
            guard unsignedMonths != .min, unsignedSeconds != .min else {
                return .failure(.arithmeticLimit)
            }
            signedMonths = -unsignedMonths
            signedSeconds = -unsignedSeconds
        } else {
            signedMonths = unsignedMonths
            signedSeconds = unsignedSeconds
        }
        let fractionSign: Int8 = fraction.isZero ? 0 : (negative ? -1 : 1)
        return .success(XSDDurationValue(
            months: signedMonths,
            wholeSeconds: signedSeconds,
            fraction: fraction,
            fractionSign: fractionSign
        ))
    }

    func compare(
        to other: XSDDurationValue
    ) -> Result<XSDOrder, ParseFailure> {
        let referenceDates: [(Int64, Int, Int)] = [
            (1696, 9, 1),
            (1697, 2, 1),
            (1903, 3, 1),
            (1903, 7, 1),
        ]
        var established: XSDOrder?
        for reference in referenceDates {
            guard let lhs = point(
                year: reference.0,
                month: reference.1,
                day: reference.2
            ), let rhs = other.point(
                year: reference.0,
                month: reference.1,
                day: reference.2
            ) else {
                return .failure(.arithmeticLimit)
            }
            let order = lhs.compare(to: rhs)
            if let established, established != order {
                return .success(.unordered)
            }
            established = order
        }
        return .success(established ?? .equal)
    }

    func isIdentical(to other: XSDDurationValue) -> Bool {
        months == other.months
            && wholeSeconds == other.wholeSeconds
            && fractionSign == other.fractionSign
            && fraction.compare(to: other.fraction) == 0
    }

    private func point(year: Int64, month: Int, day: Int) -> TimelinePoint? {
        let yearMonths = year.multipliedReportingOverflow(by: 12)
        guard !yearMonths.overflow else { return nil }
        let baseMonths = yearMonths.partialValue
            .addingReportingOverflow(Int64(month - 1))
        guard !baseMonths.overflow else { return nil }
        let adjustedMonths = baseMonths.partialValue
            .addingReportingOverflow(months)
        guard !adjustedMonths.overflow else { return nil }

        let divided = floorDivision(adjustedMonths.partialValue, by: 12)
        let adjustedYear = divided.quotient
        let adjustedMonth = Int(divided.remainder) + 1
        let adjustedDay = min(
            day,
            daysInMonth(year: adjustedYear, month: adjustedMonth)
        )
        guard let ordinal = daysFromCivil(
            year: adjustedYear,
            month: adjustedMonth,
            day: adjustedDay
        ) else {
            return nil
        }
        let daySeconds = ordinal.multipliedReportingOverflow(by: 86_400)
        guard !daySeconds.overflow else { return nil }
        let totalSeconds = daySeconds.partialValue
            .addingReportingOverflow(wholeSeconds)
        guard !totalSeconds.overflow else { return nil }
        return TimelinePoint(
            wholeSeconds: totalSeconds.partialValue,
            fraction: fraction,
            fractionSign: fractionSign
        )
    }

    private func floorDivision(
        _ value: Int64,
        by divisor: Int64
    ) -> (quotient: Int64, remainder: Int64) {
        var quotient = value / divisor
        var remainder = value % divisor
        if remainder < 0 {
            quotient -= 1
            remainder += divisor
        }
        return (quotient, remainder)
    }

    private func daysInMonth(year: Int64, month: Int) -> Int {
        switch month {
        case 2:
            let leap = year.isMultiple(of: 4)
                && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
            return leap ? 29 : 28
        case 4, 6, 9, 11:
            return 30
        default:
            return 31
        }
    }

    /// Howard Hinnant's proleptic Gregorian transform with checked arithmetic.
    private func daysFromCivil(year: Int64, month: Int, day: Int) -> Int64? {
        let adjustedYear = month <= 2 ? year - 1 : year
        let era = floorDivision(adjustedYear, by: 400).quotient
        let yearOfEra = adjustedYear - era * 400
        let adjustedMonth = Int64(month + (month > 2 ? -3 : 9))
        let dayOfYear = (153 * adjustedMonth + 2) / 5 + Int64(day - 1)
        let dayOfEra = yearOfEra * 365 + yearOfEra / 4
            - yearOfEra / 100 + dayOfYear
        let eraDays = era.multipliedReportingOverflow(by: 146_097)
        guard !eraDays.overflow else { return nil }
        let result = eraDays.partialValue.addingReportingOverflow(dayOfEra)
        return result.overflow ? nil : result.partialValue
    }

    private static func checkedMultiply(_ lhs: Int64, by rhs: Int64) -> Int64? {
        let result = lhs.multipliedReportingOverflow(by: rhs)
        return result.overflow ? nil : result.partialValue
    }

    private static func checkedAdd(_ lhs: Int64, _ rhs: Int64) -> Int64? {
        let result = lhs.addingReportingOverflow(rhs)
        return result.overflow ? nil : result.partialValue
    }

    private struct TimelinePoint {
        let wholeSeconds: Int64
        let fraction: XSDTemporalValue.Fraction
        let fractionSign: Int8

        func compare(to other: TimelinePoint) -> XSDOrder {
            let lhs = normalized
            let rhs = other.normalized
            if lhs.wholeSeconds != rhs.wholeSeconds {
                return lhs.wholeSeconds < rhs.wholeSeconds ? .less : .greater
            }
            let fractionOrder = lhs.compareFraction(to: rhs)
            if fractionOrder < 0 { return .less }
            if fractionOrder > 0 { return .greater }
            return .equal
        }

        private var normalized: NormalizedTimelinePoint {
            guard fractionSign < 0, !fraction.isZero else {
                return NormalizedTimelinePoint(
                    wholeSeconds: wholeSeconds,
                    fraction: fraction,
                    isComplement: false
                )
            }
            return NormalizedTimelinePoint(
                wholeSeconds: wholeSeconds - 1,
                fraction: fraction,
                isComplement: true
            )
        }
    }

    private struct NormalizedTimelinePoint {
        let wholeSeconds: Int64
        let fraction: XSDTemporalValue.Fraction
        let isComplement: Bool

        func compareFraction(to other: NormalizedTimelinePoint) -> Int {
            let count = max(fraction.digits.utf8.count, other.fraction.digits.utf8.count)
            var lhs = fraction.digits.utf8.makeIterator()
            var rhs = other.fraction.digits.utf8.makeIterator()
            let lhsLength = fraction.digits.utf8.count
            let rhsLength = other.fraction.digits.utf8.count
            for position in 0..<count {
                let lhsSource = (lhs.next() ?? 48) - 48
                let rhsSource = (rhs.next() ?? 48) - 48
                let lhsDigit = complementDigit(
                    source: lhsSource,
                    position: position,
                    length: lhsLength,
                    enabled: isComplement
                )
                let rhsDigit = complementDigit(
                    source: rhsSource,
                    position: position,
                    length: rhsLength,
                    enabled: other.isComplement
                )
                if lhsDigit != rhsDigit { return lhsDigit < rhsDigit ? -1 : 1 }
            }
            return 0
        }

        private func complementDigit(
            source: UInt8,
            position: Int,
            length: Int,
            enabled: Bool
        ) -> UInt8 {
            guard enabled else { return source }
            guard position < length else { return 0 }
            return position == length - 1 ? 10 - source : 9 - source
        }
    }
}

private struct DurationParser {
    struct Component {
        let whole: Int64
        let fraction: XSDTemporalValue.Fraction
        let designator: UInt8
    }

    private let source: String
    private let bytes: String.UTF8View
    private var index: String.UTF8View.Index

    init(_ source: String) {
        self.source = source
        self.bytes = source.utf8
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

    mutating func readComponent(
        digitLimit: Int,
        fractionDigitLimit: Int
    ) -> Result<Component, XSDDurationValue.ParseFailure> {
        var whole: Int64 = 0
        var digitCount = 0
        while index != bytes.endIndex {
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { break }
            digitCount += 1
            guard digitCount <= digitLimit else {
                return .failure(.componentLimit(actual: digitCount, limit: digitLimit))
            }
            let digit = Int64(byte - 48)
            let multiplied = whole.multipliedReportingOverflow(by: 10)
            guard !multiplied.overflow else { return .failure(.arithmeticLimit) }
            let added = multiplied.partialValue.addingReportingOverflow(digit)
            guard !added.overflow else { return .failure(.arithmeticLimit) }
            whole = added.partialValue
            bytes.formIndex(after: &index)
        }
        guard digitCount > 0 else { return .failure(.invalid) }

        var fraction = XSDTemporalValue.Fraction.zero
        let hadDecimalPoint = readIfPresent(46)
        if hadDecimalPoint {
            let start = index
            var fractionDigitCount = 0
            while index != bytes.endIndex {
                let byte = bytes[index]
                guard byte >= 48, byte <= 57 else { break }
                fractionDigitCount += 1
                guard fractionDigitCount <= fractionDigitLimit else {
                    return .failure(.componentLimit(
                        actual: fractionDigitCount,
                        limit: fractionDigitLimit
                    ))
                }
                bytes.formIndex(after: &index)
            }
            guard start != index else { return .failure(.invalid) }
            fraction = XSDTemporalValue.Fraction(digits: source[start..<index])
        }

        guard index != bytes.endIndex else { return .failure(.invalid) }
        let designator = bytes[index]
        bytes.formIndex(after: &index)
        if hadDecimalPoint, designator != 83 { return .failure(.invalid) }
        return .success(Component(
            whole: whole,
            fraction: fraction,
            designator: designator
        ))
    }
}
