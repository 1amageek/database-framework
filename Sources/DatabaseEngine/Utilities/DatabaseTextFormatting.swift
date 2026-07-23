/// Locale-independent text formatting used by database runtime targets.
///
/// The implementation intentionally uses only the Swift standard library so
/// diagnostic formatting and identifier generation do not link locale data.
public enum DatabaseTextFormatting {
    /// Formats a floating-point value with a fixed number of decimal places.
    ///
    /// Rounding is decimal round-to-nearest, ties-to-even, applied to Swift's
    /// canonical locale-independent `Double` spelling. A negative sign is
    /// retained when a negative value rounds to zero, including negative zero.
    ///
    /// - Parameters:
    ///   - value: The value to format.
    ///   - fractionDigits: A value in `0...18`.
    /// - Returns: A fixed-decimal string, or `nan`, `inf`, or `-inf`.
    public static func fixedDecimal(
        _ value: Double,
        fractionDigits: Int
    ) -> String {
        precondition(
            (0...18).contains(fractionDigits),
            "fractionDigits must be in 0...18"
        )

        if value.isNaN {
            return "nan"
        }
        if value == .infinity {
            return "inf"
        }
        if value == -.infinity {
            return "-inf"
        }

        let isNegative = value.sign == .minus
        let canonical = String(value.magnitude)
        let canonicalBytes = canonical.utf8

        var digits: [UInt8] = []
        digits.reserveCapacity(canonicalBytes.count)

        var fractionalDigitCount = 0
        var hasDecimalPoint = false
        var isParsingExponent = false
        var exponentSign = 1
        var exponent = 0

        for byte in canonicalBytes {
            if isParsingExponent {
                if byte == 0x2D {
                    exponentSign = -1
                } else if byte != 0x2B {
                    exponent = exponent * 10 + Int(byte - 0x30)
                }
                continue
            }

            if byte == 0x65 || byte == 0x45 {
                isParsingExponent = true
            } else if byte == 0x2E {
                hasDecimalPoint = true
            } else {
                digits.append(byte - 0x30)
                if hasDecimalPoint {
                    fractionalDigitCount += 1
                }
            }
        }

        var firstSignificantDigit = 0
        while firstSignificantDigit < digits.count,
              digits[firstSignificantDigit] == 0 {
            firstSignificantDigit += 1
        }

        let isZero = firstSignificantDigit == digits.count
        let significantDigitCount = isZero
            ? 0
            : digits.count - firstSignificantDigit
        let decimalPoint = isZero
            ? 0
            : digits.count - fractionalDigitCount
                + exponentSign * exponent - firstSignificantDigit

        @inline(__always)
        func digit(at logicalIndex: Int) -> UInt8 {
            guard logicalIndex >= 0,
                  logicalIndex < significantDigitCount else {
                return 0
            }
            return digits[firstSignificantDigit + logicalIndex]
        }

        var integerDigitCount = max(decimalPoint, 1)
        var retainedDigits: [UInt8] = []
        retainedDigits.reserveCapacity(integerDigitCount + fractionDigits + 1)

        if decimalPoint <= 0 {
            retainedDigits.append(0)
        } else {
            for index in 0..<decimalPoint {
                retainedDigits.append(digit(at: index))
            }
        }

        for offset in 0..<fractionDigits {
            retainedDigits.append(digit(at: decimalPoint + offset))
        }

        let firstDiscardedIndex = decimalPoint + fractionDigits
        let firstDiscardedDigit = digit(at: firstDiscardedIndex)
        var hasNonzeroTail = false
        if firstDiscardedIndex + 1 < significantDigitCount {
            for index in (firstDiscardedIndex + 1)..<significantDigitCount {
                if digit(at: index) != 0 {
                    hasNonzeroTail = true
                    break
                }
            }
        }

        let retainedLeastSignificantDigit = retainedDigits.last ?? 0
        let shouldRoundUp = firstDiscardedDigit > 5
            || (firstDiscardedDigit == 5
                && (hasNonzeroTail || retainedLeastSignificantDigit % 2 == 1))

        if shouldRoundUp {
            var index = retainedDigits.count
            while index > 0 {
                index -= 1
                if retainedDigits[index] < 9 {
                    retainedDigits[index] += 1
                    break
                }
                retainedDigits[index] = 0
            }
            if index == 0, retainedDigits[0] == 0 {
                retainedDigits.insert(1, at: 0)
                integerDigitCount += 1
            }
        }

        let signByteCount = isNegative ? 1 : 0
        let decimalPointByteCount = fractionDigits > 0 ? 1 : 0
        let outputByteCount = signByteCount
            + retainedDigits.count
            + decimalPointByteCount

        return String(unsafeUninitializedCapacity: outputByteCount) { output in
            var outputIndex = 0
            if isNegative {
                output[outputIndex] = 0x2D
                outputIndex += 1
            }

            for index in retainedDigits.indices {
                if fractionDigits > 0, index == integerDigitCount {
                    output[outputIndex] = 0x2E
                    outputIndex += 1
                }
                output[outputIndex] = retainedDigits[index] + 0x30
                outputIndex += 1
            }
            return outputIndex
        }
    }

    /// Encodes a byte collection as lowercase hexadecimal text.
    ///
    /// The input is read directly and only the final `String` storage is
    /// allocated; no mapped byte or component-string array is materialized.
    @inlinable
    public static func lowercaseHex<ByteCollection: Collection>(
        _ bytes: ByteCollection
    ) -> String where ByteCollection.Element == UInt8 {
        let (capacity, overflow) = bytes.count.multipliedReportingOverflow(by: 2)
        precondition(!overflow, "Hex output exceeds addressable memory")

        return String(unsafeUninitializedCapacity: capacity) { output in
            var outputIndex = 0
            for byte in bytes {
                let high = byte >> 4
                let low = byte & 0x0F
                output[outputIndex] = high < 10 ? high + 0x30 : high + 0x57
                output[outputIndex + 1] = low < 10 ? low + 0x30 : low + 0x57
                outputIndex += 2
            }
            return outputIndex
        }
    }

}
