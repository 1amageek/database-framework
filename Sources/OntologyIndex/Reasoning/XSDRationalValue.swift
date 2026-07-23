/// Exact arbitrary-precision OWL rational value.
///
/// Components are parsed directly from the literal's UTF-8 view into base-1e9
/// limbs. Comparisons use bounded cross multiplication and never pass through a
/// floating-point representation.
package struct XSDRationalValue: Sendable {
    package enum ComparisonFailure: Error, Sendable, Equatable {
        case workLimit(limit: Int, actual: Int)
    }

    private let sign: Int8
    private let numerator: XSDUnsignedInteger
    private let denominator: XSDUnsignedInteger
    private let comparisonWorkLimit: Int

    package init?(
        lexicalForm: String,
        comparisonWorkLimit: Int
    ) {
        guard let separator = lexicalForm.firstIndex(of: "/"),
              lexicalForm[lexicalForm.index(after: separator)...]
                .firstIndex(of: "/") == nil else {
            return nil
        }
        let numeratorSource = lexicalForm[..<separator]
        let denominatorStart = lexicalForm.index(after: separator)
        let denominatorSource = lexicalForm[denominatorStart...]
        guard let signedNumerator = XSDUnsignedInteger.signedInteger(
                numeratorSource,
                allowsPlus: true
              ),
              let denominator = XSDUnsignedInteger.unsignedInteger(
                denominatorSource
              ),
              !denominator.isZero else {
            return nil
        }
        sign = signedNumerator.magnitude.isZero
            ? 0
            : signedNumerator.sign
        numerator = signedNumerator.magnitude
        self.denominator = denominator
        self.comparisonWorkLimit = comparisonWorkLimit
    }

    private init(
        decimal: XSDDecimalValue,
        comparisonWorkLimit: Int
    ) {
        sign = decimal.sign
        numerator = XSDUnsignedInteger.decimalCoefficient(decimal.source)
        denominator = XSDUnsignedInteger.powerOfTen(
            decimal.lexicalFractionDigitCount
        )
        self.comparisonWorkLimit = comparisonWorkLimit
    }

    package func compare(
        to other: XSDRationalValue
    ) -> Result<Int, ComparisonFailure> {
        if sign != other.sign {
            return .success(sign < other.sign ? -1 : 1)
        }
        guard sign != 0 else { return .success(0) }

        let workLimit = min(comparisonWorkLimit, other.comparisonWorkLimit)
        let lhsWork = numerator.multiplicationWork(with: other.denominator)
        let rhsWork = other.numerator.multiplicationWork(with: denominator)
        let (actualWork, overflow) = lhsWork.addingReportingOverflow(rhsWork)
        guard !overflow, actualWork <= workLimit else {
            return .failure(.workLimit(
                limit: workLimit,
                actual: overflow ? Int.max : actualWork
            ))
        }

        let lhs = numerator.multiplied(by: other.denominator)
        let rhs = other.numerator.multiplied(by: denominator)
        let absoluteOrder = lhs.compare(to: rhs)
        return .success(sign < 0 ? -absoluteOrder : absoluteOrder)
    }

    package func compare(
        to decimal: XSDDecimalValue
    ) -> Result<Int, ComparisonFailure> {
        compare(to: XSDRationalValue(
            decimal: decimal,
            comparisonWorkLimit: comparisonWorkLimit
        ))
    }
}

private struct XSDUnsignedInteger: Sendable {
    private static let base: UInt64 = 1_000_000_000
    private let limbs: [UInt32]

    var isZero: Bool { limbs.isEmpty }

    static func signedInteger(
        _ source: Substring,
        allowsPlus: Bool
    ) -> (sign: Int8, magnitude: XSDUnsignedInteger)? {
        let bytes = source.utf8
        var index = bytes.startIndex
        let lexicalSign: Int8
        if index != bytes.endIndex, bytes[index] == 45 {
            lexicalSign = -1
            bytes.formIndex(after: &index)
        } else if index != bytes.endIndex, bytes[index] == 43 {
            guard allowsPlus else { return nil }
            lexicalSign = 1
            bytes.formIndex(after: &index)
        } else {
            lexicalSign = 1
        }
        guard index != bytes.endIndex else { return nil }
        while index != bytes.endIndex {
            let byte = bytes[index]
            guard byte >= 48, byte <= 57 else { return nil }
            bytes.formIndex(after: &index)
        }
        let magnitude = decimalDigits(source)
        return (magnitude.isZero ? 0 : lexicalSign, magnitude)
    }

    static func unsignedInteger(
        _ source: Substring
    ) -> XSDUnsignedInteger? {
        guard let parsed = signedInteger(source, allowsPlus: false),
              parsed.sign >= 0,
              source.first != "-" else {
            return nil
        }
        return parsed.magnitude
    }

    static func decimalCoefficient(_ source: String) -> XSDUnsignedInteger {
        decimalDigits(source[...])
    }

    static func powerOfTen(_ exponent: Int) -> XSDUnsignedInteger {
        precondition(exponent >= 0)
        let zeroLimbCount = exponent / 9
        let remainder = exponent % 9
        var limbs = [UInt32](repeating: 0, count: zeroLimbCount)
        var mostSignificant: UInt32 = 1
        for _ in 0..<remainder {
            mostSignificant *= 10
        }
        limbs.append(mostSignificant)
        return XSDUnsignedInteger(limbs: limbs)
    }

    func multiplicationWork(with other: XSDUnsignedInteger) -> Int {
        let (pairs, overflow) = limbs.count.multipliedReportingOverflow(
            by: other.limbs.count
        )
        return overflow ? Int.max : pairs
    }

    func multiplied(by other: XSDUnsignedInteger) -> XSDUnsignedInteger {
        guard !isZero, !other.isZero else {
            return XSDUnsignedInteger(limbs: [])
        }
        var result = [UInt32](
            repeating: 0,
            count: limbs.count + other.limbs.count + 1
        )
        for lhsIndex in limbs.indices {
            var carry: UInt64 = 0
            for rhsIndex in other.limbs.indices {
                let resultIndex = lhsIndex + rhsIndex
                let total = UInt64(result[resultIndex])
                    + UInt64(limbs[lhsIndex]) * UInt64(other.limbs[rhsIndex])
                    + carry
                result[resultIndex] = UInt32(total % Self.base)
                carry = total / Self.base
            }
            var resultIndex = lhsIndex + other.limbs.count
            while carry > 0 {
                let total = UInt64(result[resultIndex]) + carry
                result[resultIndex] = UInt32(total % Self.base)
                carry = total / Self.base
                resultIndex += 1
            }
        }
        while result.last == 0 {
            result.removeLast()
        }
        return XSDUnsignedInteger(limbs: result)
    }

    func compare(to other: XSDUnsignedInteger) -> Int {
        if limbs.count != other.limbs.count {
            return limbs.count < other.limbs.count ? -1 : 1
        }
        for index in limbs.indices.reversed() {
            if limbs[index] != other.limbs[index] {
                return limbs[index] < other.limbs[index] ? -1 : 1
            }
        }
        return 0
    }

    private static func decimalDigits(
        _ source: Substring
    ) -> XSDUnsignedInteger {
        var limbs: [UInt32] = []
        limbs.reserveCapacity((source.utf8.count + 8) / 9)
        var limb: UInt32 = 0
        var multiplier: UInt32 = 1
        var digitsInLimb = 0
        for byte in source.utf8.reversed() {
            guard byte >= 48, byte <= 57 else { continue }
            limb += UInt32(byte - 48) * multiplier
            digitsInLimb += 1
            if digitsInLimb == 9 {
                limbs.append(limb)
                limb = 0
                multiplier = 1
                digitsInLimb = 0
            } else {
                multiplier *= 10
            }
        }
        if digitsInLimb > 0 {
            limbs.append(limb)
        }
        while limbs.last == 0 {
            limbs.removeLast()
        }
        return XSDUnsignedInteger(limbs: limbs)
    }
}
