import DatabaseTypes

package enum RelationalValueIdentityError: Error, Sendable {
    case nonFiniteNumericValue
    case invalidObject
}

package struct RelationalValueIdentityResult: Sendable {
    package let value: FieldValue
    package let changedRepresentation: Bool
}

/// Defines the one value identity used by relational equality, grouping,
/// joins, duplicate elimination, and materialized aggregation indexes.
package enum RelationalValueIdentity {
    package static func compareNumeric(
        _ lhs: FieldValue,
        _ rhs: FieldValue
    ) -> Int? {
        guard let lhs = RelationalExactNumericValue(lhs),
              let rhs = RelationalExactNumericValue(rhs) else {
            return nil
        }
        return lhs.compare(to: rhs)
    }

    package static func canonicalize(
        _ value: FieldValue
    ) throws(RelationalValueIdentityError) -> RelationalValueIdentityResult {
        switch value {
        case .int8(let integer):
            return changed(.int64(Int64(integer)))
        case .int16(let integer):
            return changed(.int64(Int64(integer)))
        case .int32(let integer):
            return changed(.int64(Int64(integer)))
        case .int64:
            return unchanged(value)
        case .uint8(let integer):
            return changed(.int64(Int64(integer)))
        case .uint16(let integer):
            return changed(.int64(Int64(integer)))
        case .uint32(let integer):
            return changed(.int64(Int64(integer)))
        case .uint64(let integer):
            if integer <= UInt64(Int64.max) {
                return changed(.int64(Int64(integer)))
            }
            return unchanged(value)
        case .float32(let number):
            guard number.isFinite else { throw .nonFiniteNumericValue }
            let result = try canonicalize(.float64(Double(number)))
            return RelationalValueIdentityResult(
                value: result.value,
                changedRepresentation: result.value != value
            )
        case .float64(let number):
            guard number.isFinite else { throw .nonFiniteNumericValue }
            let normalized = number == 0 ? 0 : number
            if let decimal = exactDecimal(normalized) {
                let canonical = integralValue(decimal) ?? .decimal(decimal)
                return RelationalValueIdentityResult(
                    value: canonical,
                    changedRepresentation: canonical != value
                )
            }
            return RelationalValueIdentityResult(
                value: .float64(normalized),
                changedRepresentation:
                    normalized.bitPattern != number.bitPattern
            )
        case .decimal(let decimal):
            let canonical = integralValue(decimal) ?? .decimal(decimal)
            return RelationalValueIdentityResult(
                value: canonical,
                changedRepresentation: canonical != value
            )
        case .array(let values):
            var canonical: [FieldValue]?
            for index in values.indices {
                let result = try canonicalize(values[index])
                if result.changedRepresentation {
                    if canonical == nil { canonical = values }
                    canonical?[index] = result.value
                }
            }
            guard let canonical else { return unchanged(value) }
            return changed(.array(canonical))
        case .object(let object):
            let fields = object.fields
            var canonical: [(key: String, value: FieldValue)]?
            for index in fields.indices {
                let result = try canonicalize(fields[index].value)
                if result.changedRepresentation {
                    if canonical == nil { canonical = fields }
                    canonical?[index] = (
                        key: fields[index].key,
                        value: result.value
                    )
                }
            }
            guard let canonical else { return unchanged(value) }
            do {
                return changed(.object(try FieldObject(canonical)))
            } catch {
                throw .invalidObject
            }
        case .null, .bool, .string, .bytes, .date, .time, .dateTime,
                .timestamp, .timeSpan, .calendarPeriod, .geographicPoint,
                .geographicPosition, .vector, .uuid, .reference, .rdfTerm:
            return unchanged(value)
        }
    }

    private static func exactDecimal(_ value: Double) -> ExactDecimal? {
        RelationalExactNumericValue(.float64(value))?.exactDecimal
    }

    private static func integralValue(
        _ decimal: ExactDecimal
    ) -> FieldValue? {
        guard decimal.scale <= 0 else { return nil }
        if decimal.coefficient == 0 { return .int64(0) }

        let exponent = -Int64(decimal.scale)
        guard exponent <= 19 else { return nil }
        var integer = decimal.coefficient
        for _ in 0..<exponent {
            let product = integer.multipliedReportingOverflow(by: 10)
            guard !product.overflow else { return nil }
            integer = product.partialValue
        }
        if let signed = Int64(exactly: integer) {
            return .int64(signed)
        }
        if integer >= 0, let unsigned = UInt64(exactly: integer) {
            return .uint64(unsigned)
        }
        return nil
    }

    private static func changed(
        _ value: FieldValue
    ) -> RelationalValueIdentityResult {
        RelationalValueIdentityResult(
            value: value,
            changedRepresentation: true
        )
    }

    private static func unchanged(
        _ value: FieldValue
    ) -> RelationalValueIdentityResult {
        RelationalValueIdentityResult(
            value: value,
            changedRepresentation: false
        )
    }
}

private struct RelationalExactNumericValue {
    let isNegative: Bool
    let digits: [UInt8]
    let scale: Int64

    init?(_ value: FieldValue) {
        switch value {
        case .int8(let value): self.init(Int128(value))
        case .int16(let value): self.init(Int128(value))
        case .int32(let value): self.init(Int128(value))
        case .int64(let value): self.init(Int128(value))
        case .uint8(let value): self.init(UInt128(value))
        case .uint16(let value): self.init(UInt128(value))
        case .uint32(let value): self.init(UInt128(value))
        case .uint64(let value): self.init(UInt128(value))
        case .float32(let value):
            guard value.isFinite else { return nil }
            self.init(Double(value))
        case .float64(let value):
            guard value.isFinite else { return nil }
            self.init(value)
        case .decimal(let value): self.init(value)
        default: return nil
        }
    }

    private init(_ value: Double) {
        let bits = value.bitPattern
        let negative = bits >> 63 != 0
        let exponentBits = Int((bits >> 52) & 0x7FF)
        let fraction = bits & 0x000F_FFFF_FFFF_FFFF
        if exponentBits == 0, fraction == 0 {
            self.init(isNegative: false, digits: [0], scale: 0)
            return
        }

        var significand = exponentBits == 0
            ? fraction
            : fraction | (UInt64(1) << 52)
        var binaryExponent = exponentBits == 0
            ? 1 - 1023 - 52
            : exponentBits - 1023 - 52
        while binaryExponent < 0, significand.isMultiple(of: 2) {
            significand /= 2
            binaryExponent += 1
        }

        var coefficient = RelationalDecimalMagnitude(significand)
        let scale: Int64
        if binaryExponent >= 0 {
            for _ in 0..<binaryExponent { coefficient.multiply(by: 2) }
            scale = 0
        } else {
            let decimalPlaces = -binaryExponent
            for _ in 0..<decimalPlaces { coefficient.multiply(by: 5) }
            scale = Int64(decimalPlaces)
        }
        self.init(
            isNegative: negative,
            digits: coefficient.decimalDigits,
            scale: scale
        )
    }

    private init(_ value: ExactDecimal) {
        self.init(
            isNegative: value.coefficient < 0,
            digits: Array(String(value.coefficient.magnitude).utf8).map {
                $0 - 48
            },
            scale: Int64(value.scale)
        )
    }

    private init(_ value: Int128) {
        self.init(
            isNegative: value < 0,
            digits: Array(String(value.magnitude).utf8).map { $0 - 48 },
            scale: 0
        )
    }

    private init(_ value: UInt128) {
        self.init(
            isNegative: false,
            digits: Array(String(value).utf8).map { $0 - 48 },
            scale: 0
        )
    }

    private init(isNegative: Bool, digits: [UInt8], scale: Int64) {
        var digits = digits
        var scale = scale
        while digits.count > 1, digits.last == 0 {
            digits.removeLast()
            scale -= 1
        }
        self.isNegative = digits == [0] ? false : isNegative
        self.digits = digits
        self.scale = digits == [0] ? 0 : scale
    }

    var exactDecimal: ExactDecimal? {
        var magnitude: UInt128 = 0
        let maximum = isNegative
            ? UInt128(Int128.max) + 1
            : UInt128(Int128.max)
        for digit in digits {
            guard magnitude <= maximum / 10 else { return nil }
            magnitude *= 10
            guard magnitude <= maximum - UInt128(digit) else { return nil }
            magnitude += UInt128(digit)
        }
        guard let scale = Int32(exactly: scale) else { return nil }

        let coefficient: Int128
        if isNegative {
            if magnitude == UInt128(Int128.max) + 1 {
                coefficient = Int128.min
            } else {
                guard let signed = Int128(exactly: magnitude) else { return nil }
                coefficient = -signed
            }
        } else {
            guard let signed = Int128(exactly: magnitude) else { return nil }
            coefficient = signed
        }
        return ExactDecimal(coefficient: coefficient, scale: scale)
    }

    func compare(to other: Self) -> Int {
        if digits == [0], other.digits == [0] { return 0 }
        if isNegative != other.isNegative { return isNegative ? -1 : 1 }

        let leftLeadingPower = Int64(digits.count - 1) - scale
        let rightLeadingPower = Int64(other.digits.count - 1) - other.scale
        let magnitudeComparison: Int
        if leftLeadingPower != rightLeadingPower {
            magnitudeComparison = leftLeadingPower < rightLeadingPower ? -1 : 1
        } else {
            magnitudeComparison = compareAlignedDigits(to: other)
        }
        return isNegative ? -magnitudeComparison : magnitudeComparison
    }

    private func compareAlignedDigits(to other: Self) -> Int {
        let count = max(digits.count, other.digits.count)
        for index in 0..<count {
            let left = index < digits.count ? digits[index] : 0
            let right = index < other.digits.count ? other.digits[index] : 0
            if left != right { return left < right ? -1 : 1 }
        }
        return 0
    }
}

private struct RelationalDecimalMagnitude {
    private static let base: UInt64 = 1_000_000_000
    private var limbs: [UInt32]

    init(_ value: UInt64) {
        let lower = UInt32(value % Self.base)
        let upper = UInt32(value / Self.base)
        self.limbs = upper == 0 ? [lower] : [lower, upper]
    }

    mutating func multiply(by factor: UInt32) {
        var carry: UInt64 = 0
        for index in limbs.indices {
            let product = UInt64(limbs[index]) * UInt64(factor) + carry
            limbs[index] = UInt32(product % Self.base)
            carry = product / Self.base
        }
        if carry != 0 { limbs.append(UInt32(carry)) }
    }

    var decimalDigits: [UInt8] {
        guard let mostSignificant = limbs.last else { return [0] }
        var text = String(mostSignificant)
        if limbs.count > 1 {
            for limb in limbs.dropLast().reversed() {
                let part = String(limb)
                text += String(repeating: "0", count: 9 - part.utf8.count)
                text += part
            }
        }
        return text.utf8.map { $0 - 48 }
    }
}
