/// Exact, allocation-free view over an XSD integer or decimal lexical value.
///
/// The source `String` owns the UTF-8 storage. Comparisons traverse that storage
/// directly and never convert an unbounded decimal through `Int64`, `Double`,
/// `Decimal`, or an intermediate digit array.
package struct XSDDecimalValue: Sendable {
    package let source: String
    package let sign: Int8
    package let integerMagnitudeDigits: Int
    package let significantDigitCount: Int
    package let totalDigits: Int
    package let fractionDigits: Int

    package init?(integer source: String) {
        guard let parsed = Self.parse(source, allowsDecimalPoint: false) else {
            return nil
        }
        self = parsed
    }

    package init?(decimal source: String) {
        guard let parsed = Self.parse(source, allowsDecimalPoint: true) else {
            return nil
        }
        self = parsed
    }

    package func compare(to other: XSDDecimalValue) -> Int {
        if sign != other.sign {
            return sign < other.sign ? -1 : 1
        }
        guard sign != 0 else { return 0 }

        let absoluteComparison: Int
        if integerMagnitudeDigits != other.integerMagnitudeDigits {
            absoluteComparison = integerMagnitudeDigits
                < other.integerMagnitudeDigits ? -1 : 1
        } else {
            var lhs = SignificantDigitIterator(
                source: source,
                count: significantDigitCount
            )
            var rhs = SignificantDigitIterator(
                source: other.source,
                count: other.significantDigitCount
            )
            let count = max(significantDigitCount, other.significantDigitCount)
            var comparison = 0
            for _ in 0..<count {
                let lhsDigit = lhs.next() ?? 0
                let rhsDigit = rhs.next() ?? 0
                if lhsDigit != rhsDigit {
                    comparison = lhsDigit < rhsDigit ? -1 : 1
                    break
                }
            }
            absoluteComparison = comparison
        }
        return sign < 0 ? -absoluteComparison : absoluteComparison
    }

    package func isWithin(minimum: String, maximum: String) -> Bool {
        guard let minimum = XSDDecimalValue(integer: minimum),
              let maximum = XSDDecimalValue(integer: maximum) else {
            preconditionFailure("Static XSD integer bounds must be valid")
        }
        return compare(to: minimum) >= 0 && compare(to: maximum) <= 0
    }

    package func compare(toNonNegativeInt other: Int) -> Int {
        precondition(other >= 0)
        if sign < 0 { return -1 }
        if sign == 0 { return other == 0 ? 0 : -1 }
        if other == 0 { return 1 }

        return withUnsafeTemporaryAllocation(
            of: UInt8.self,
            capacity: MemoryLayout<Int>.size * 3
        ) { reversedDigits in
            var remaining = other
            var digitCount = 0
            while remaining > 0 {
                reversedDigits[digitCount] = UInt8(remaining % 10)
                digitCount += 1
                remaining /= 10
            }
            if integerMagnitudeDigits != digitCount {
                return integerMagnitudeDigits < digitCount ? -1 : 1
            }

            var lhs = SignificantDigitIterator(
                source: source,
                count: significantDigitCount
            )
            for position in 0..<digitCount {
                let lhsDigit = lhs.next() ?? 0
                let rhsDigit = reversedDigits[digitCount - position - 1]
                if lhsDigit != rhsDigit {
                    return lhsDigit < rhsDigit ? -1 : 1
                }
            }
            return 0
        }
    }

    package var lexicalFractionDigitCount: Int {
        var afterDecimalPoint = false
        var count = 0
        for byte in source.utf8 {
            if byte == 46 {
                afterDecimalPoint = true
            } else if afterDecimalPoint, byte >= 48, byte <= 57 {
                count += 1
            }
        }
        return count
    }

    private static func parse(
        _ source: String,
        allowsDecimalPoint: Bool
    ) -> XSDDecimalValue? {
        let bytes = source.utf8
        var index = bytes.startIndex
        let lexicalSign: Int8
        if index != bytes.endIndex, bytes[index] == 45 {
            lexicalSign = -1
            bytes.formIndex(after: &index)
        } else {
            lexicalSign = 1
            if index != bytes.endIndex, bytes[index] == 43 {
                bytes.formIndex(after: &index)
            }
        }

        var digitOffset = 0
        var decimalPointDigitOffset: Int?
        var firstNonzeroDigitOffset: Int?
        var lastNonzeroDigitOffset: Int?
        while index != bytes.endIndex {
            let byte = bytes[index]
            if byte == 46 {
                guard allowsDecimalPoint,
                      decimalPointDigitOffset == nil else {
                    return nil
                }
                decimalPointDigitOffset = digitOffset
            } else {
                guard byte >= 48, byte <= 57 else { return nil }
                if byte != 48 {
                    if firstNonzeroDigitOffset == nil {
                        firstNonzeroDigitOffset = digitOffset
                    }
                    lastNonzeroDigitOffset = digitOffset
                }
                digitOffset += 1
            }
            bytes.formIndex(after: &index)
        }
        guard digitOffset > 0 else { return nil }

        let decimalPoint = decimalPointDigitOffset ?? digitOffset
        guard let firstNonzeroDigitOffset,
              let lastNonzeroDigitOffset else {
            return XSDDecimalValue(
                source: source,
                sign: 0,
                integerMagnitudeDigits: 0,
                significantDigitCount: 0,
                totalDigits: 1,
                fractionDigits: 0
            )
        }
        let significantDigitCount = lastNonzeroDigitOffset
            - firstNonzeroDigitOffset + 1
        let integerMagnitudeDigits = decimalPoint - firstNonzeroDigitOffset
        return XSDDecimalValue(
            source: source,
            sign: lexicalSign,
            integerMagnitudeDigits: integerMagnitudeDigits,
            significantDigitCount: significantDigitCount,
            totalDigits: max(significantDigitCount, integerMagnitudeDigits),
            fractionDigits: max(
                0,
                significantDigitCount - integerMagnitudeDigits
            )
        )
    }

    private init(
        source: String,
        sign: Int8,
        integerMagnitudeDigits: Int,
        significantDigitCount: Int,
        totalDigits: Int,
        fractionDigits: Int
    ) {
        self.source = source
        self.sign = sign
        self.integerMagnitudeDigits = integerMagnitudeDigits
        self.significantDigitCount = significantDigitCount
        self.totalDigits = totalDigits
        self.fractionDigits = fractionDigits
    }
}

private struct SignificantDigitIterator {
    private let bytes: String.UTF8View
    private var index: String.UTF8View.Index
    private var started = false
    private var remaining: Int

    init(source: String, count: Int) {
        let bytes = source.utf8
        self.bytes = bytes
        self.index = bytes.startIndex
        self.remaining = count
    }

    mutating func next() -> UInt8? {
        guard remaining > 0 else { return nil }
        while index != bytes.endIndex {
            let byte = bytes[index]
            bytes.formIndex(after: &index)
            guard byte >= 48, byte <= 57 else { continue }
            if !started, byte == 48 {
                continue
            }
            started = true
            remaining -= 1
            return byte - 48
        }
        return nil
    }
}
