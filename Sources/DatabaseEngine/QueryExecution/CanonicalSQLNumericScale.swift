import DatabaseTypes

/// PostgreSQL's normalized base-10000 leading group, without lexical expansion.
enum CanonicalSQLNumericScale {
    static func leading(_ value: ExactDecimal) -> (weight: Int64, digit: Int) {
        guard value.coefficient != 0 else { return (0, 0) }
        var magnitude = value.coefficient.magnitude
        var digitCount: Int64 = 1
        while magnitude >= 10 { magnitude /= 10; digitCount += 1 }
        let leadingPower = digitCount - 1 - Int64(value.scale)
        let weight = leadingPower >= 0 ? leadingPower / 4 : (leadingPower - 3) / 4
        let groupDigits = Int(leadingPower - weight * 4 + 1)
        var first = value.coefficient.magnitude
        if digitCount > groupDigits {
            for _ in 0..<(Int(digitCount) - groupDigits) { first /= 10 }
        } else {
            for _ in 0..<(groupDigits - Int(digitCount)) { first *= 10 }
        }
        return (weight, Int(first))
    }

    static func divisionScale(
        leftWeight: Int64, leftDigit: Int, leftDisplayScale: Int32,
        rightWeight: Int64, rightDigit: Int, rightDisplayScale: Int32
    ) -> Int32 {
        let quotientWeight = leftWeight - rightWeight - (leftDigit <= rightDigit ? 1 : 0)
        return Int32(min(1000, max(0, 16 - quotientWeight * 4,
                                  Int64(leftDisplayScale), Int64(rightDisplayScale))))
    }

    static func divisionScale(_ left: ExactDecimal, displayScale leftScale: Int32,
                              _ right: ExactDecimal, displayScale rightScale: Int32) -> Int32 {
        let lhs = leading(left), rhs = leading(right)
        return divisionScale(leftWeight: lhs.weight, leftDigit: lhs.digit, leftDisplayScale: leftScale,
                             rightWeight: rhs.weight, rightDigit: rhs.digit, rightDisplayScale: rightScale)
    }
}
